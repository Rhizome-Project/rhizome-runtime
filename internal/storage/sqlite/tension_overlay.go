package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	tensionLifecycleEmergent   = "EMERGENT"
	tensionLifecycleActive     = "ACTIVE"
	tensionLifecycleDormant    = "DORMANT"
	tensionLifecycleResolved   = "RESOLVED"
	tensionLifecycleDiscarded  = "DISCARDED"
	tensionLifecycleArchived   = "ARCHIVED"
	tensionLifecycleSuperseded = "SUPERSEDED"
	tensionLifecycleDisputed   = "DISPUTED"
	tensionLifecycleRecovered  = "RECOVERED"
	tensionLifecycleMeta       = "META" // legacy compatibility only; unified stack must not persist this lifecycle for new writes

	tensionReviewPending   = "PENDING"
	tensionReviewConfirmed = "CONFIRMED"
	tensionReviewDiscarded = "DISCARDED"

	tensionRefreshRuntimeEventWindow = 200
	tensionRefreshClusterWindow      = 128
	tensionRefreshQueueWindow        = 1000
	tensionRefreshClaimWindow        = 1000
	tensionRetireRefreshThreshold    = 2
)

type TensionRefreshInput struct {
	WorkspaceID                string
	ActorID                    string
	ProtoClusterID             string
	Limit                      int
	ClusterLimit               int
	PromptContextEnvelope      map[string]any
	PromptContextSurface       string
	PromptContextPrincipalType string
	PromptContextPrincipalID   string
}

type TensionFilter struct {
	WorkspaceID         string `json:"workspace_id"`
	TensionType         string `json:"tension_type,omitempty"`
	LifecycleState      string `json:"lifecycle_state,omitempty"`
	ReviewStatus        string `json:"review_status,omitempty"`
	ExcludeReviewStatus string `json:"exclude_review_status,omitempty"`
	ProtoClusterID      string `json:"proto_cluster_id,omitempty"`
	TaskID              string `json:"task_id,omitempty"`
	AgentID             string `json:"agent_id,omitempty"`
	Limit               int    `json:"limit,omitempty"`
}

type TensionMutationInput struct {
	WorkspaceID                       string
	TensionID                         string
	ActorID                           string
	Reason                            string
	PromptContextEnvelope             map[string]any
	PromptContextSurface              string
	PromptContextPrincipalType        string
	PromptContextPrincipalID          string
	PromptContextAllowLifecycleUpdate bool
}

type TensionDependencyMutationInput struct {
	WorkspaceID                string
	TensionID                  string
	DependsOnTensionID         string
	DependencyType             string
	ActorID                    string
	Reason                     string
	PromptContextEnvelope      map[string]any
	PromptContextSurface       string
	PromptContextPrincipalType string
	PromptContextPrincipalID   string
}

type TensionRecord struct {
	TensionID           string   `json:"tension_id"`
	WorkspaceID         string   `json:"workspace_id"`
	ProtoClusterID      string   `json:"proto_cluster_id"`
	Kind                string   `json:"kind,omitempty"`
	TensionType         string   `json:"tension_type"`
	LifecycleState      string   `json:"lifecycle_state"`
	ReviewStatus        string   `json:"review_status"`
	Title               string   `json:"title"`
	Summary             string   `json:"summary,omitempty"`
	AnchorKind          string   `json:"anchor_kind"`
	AnchorRef           string   `json:"anchor_ref"`
	TaskIDs             []string `json:"task_ids,omitempty"`
	SessionIDs          []string `json:"session_ids,omitempty"`
	DocKeys             []string `json:"doc_keys,omitempty"`
	ArtifactRefs        []string `json:"artifact_refs,omitempty"`
	SegmentRefs         []string `json:"segment_refs,omitempty"`
	AgentIDs            []string `json:"agent_ids,omitempty"`
	ConstraintRefs      []string `json:"constraint_refs,omitempty"`
	Members             []string `json:"members,omitempty"`
	BlockedByTensionIDs []string `json:"blocked_by_tension_ids,omitempty"`
	BlocksTensionIDs    []string `json:"blocks_tension_ids,omitempty"`
	BaseScore           int      `json:"base_score"`
	SurfaceScore        int      `json:"surface_score"`
	BaseImportance      float64  `json:"base_importance,omitempty"`
	VisibilityScore     float64  `json:"visibility_score,omitempty"`
	SurfacedPriority    float64  `json:"surfaced_priority,omitempty"`
	CrowdingRatio       float64  `json:"crowding_ratio,omitempty"`
	ArchivePropensity   float64  `json:"archive_propensity,omitempty"`
	RecoveryRisk        float64  `json:"recovery_risk,omitempty"`
	LeaseSensitive      bool     `json:"lease_sensitive,omitempty"`
	EvidenceCount       int      `json:"evidence_count"`
	LastSeenEventID     string   `json:"last_seen_event_id,omitempty"`
	LastSeenAt          string   `json:"last_seen_at,omitempty"`
	LastDetectedAt      string   `json:"last_detected_at,omitempty"`
	LastRefreshedAt     string   `json:"last_refreshed_at,omitempty"`
	StaleRefreshCount   int      `json:"stale_refresh_count,omitempty"`
	ConfirmedBy         string   `json:"confirmed_by,omitempty"`
	ArchivedBy          string   `json:"archived_by,omitempty"`
	DiscardedReason     string   `json:"dismissed_reason,omitempty"`
	CreatedAt           string   `json:"created_at"`
	UpdatedAt           string   `json:"updated_at"`
}

type TensionEvidenceRecord struct {
	TensionID    string `json:"tension_id"`
	WorkspaceID  string `json:"workspace_id"`
	EvidenceKind string `json:"evidence_kind"`
	EvidenceRef  string `json:"evidence_ref"`
	EventID      string `json:"event_id,omitempty"`
	Weight       int    `json:"weight"`
	Summary      string `json:"summary,omitempty"`
	CreatedAt    string `json:"created_at"`
}

type TensionFrontierItem struct {
	TensionID         string   `json:"tension_id"`
	ProtoClusterID    string   `json:"proto_cluster_id"`
	Kind              string   `json:"kind,omitempty"`
	TensionType       string   `json:"tension_type"`
	ReviewStatus      string   `json:"review_status"`
	Title             string   `json:"title"`
	Summary           string   `json:"summary,omitempty"`
	Members           []string `json:"members,omitempty"`
	SurfaceScore      int      `json:"surface_score"`
	BaseScore         int      `json:"base_score"`
	BaseImportance    float64  `json:"base_importance,omitempty"`
	VisibilityScore   float64  `json:"visibility_score,omitempty"`
	SurfacedPriority  float64  `json:"surfaced_priority,omitempty"`
	CrowdingRatio     float64  `json:"crowding_ratio,omitempty"`
	ArchivePropensity float64  `json:"archive_propensity,omitempty"`
	RecoveryRisk      float64  `json:"recovery_risk,omitempty"`
	LeaseSensitive    bool     `json:"lease_sensitive,omitempty"`
	EvidenceCount     int      `json:"evidence_count"`
	LastSeenAt        string   `json:"last_seen_at,omitempty"`
}

type TensionReport struct {
	WorkspaceID          string                 `json:"workspace_id"`
	GeneratedAt          string                 `json:"generated_at"`
	TimeAuthority        WorkspaceTimeAuthority `json:"time_authority"`
	Filter               TensionFilter          `json:"filter"`
	FrontierCapacity     int                    `json:"frontier_capacity,omitempty"`
	FreeAgentCount       int                    `json:"free_agent_count,omitempty"`
	TotalCount           int                    `json:"total_count"`
	ActiveCount          int                    `json:"active_count"`
	ArchivedCount        int                    `json:"archived_count"`
	PendingCount         int                    `json:"pending_count"`
	CountsByType         map[string]int         `json:"counts_by_type,omitempty"`
	CountsByReviewStatus map[string]int         `json:"counts_by_review_status,omitempty"`
	Frontier             []TensionFrontierItem  `json:"frontier,omitempty"`
}

type TensionDetail struct {
	TimeAuthority WorkspaceTimeAuthority    `json:"time_authority"`
	Tension       TensionRecord             `json:"tension"`
	Dependencies  []TensionDependencyEdge   `json:"dependencies,omitempty"`
	Dependents    []TensionDependencyEdge   `json:"dependents,omitempty"`
	Evidence      []TensionEvidenceRecord   `json:"evidence,omitempty"`
	Events        []RuntimeEventRecord      `json:"events,omitempty"`
	Claims        []KnowledgeClaimRecord    `json:"claims,omitempty"`
	Queues        []OperatorQueueRecord     `json:"queues,omitempty"`
	Docs          []WorkspaceDocSummary     `json:"docs,omitempty"`
	Artifacts     []WorkspaceArtifactRecord `json:"artifacts,omitempty"`
	ProtoCluster  *ProtoClusterReport       `json:"proto_cluster,omitempty"`
}

type TensionRefreshResult struct {
	WorkspaceID       string                 `json:"workspace_id"`
	ProtoClusterID    string                 `json:"proto_cluster_id,omitempty"`
	RefreshedAt       string                 `json:"refreshed_at"`
	TimeAuthority     WorkspaceTimeAuthority `json:"time_authority"`
	EvaluatedClusters int                    `json:"evaluated_clusters"`
	CreatedCount      int                    `json:"created_count"`
	UpdatedCount      int                    `json:"updated_count"`
	RecoveredCount    int                    `json:"recovered_count"`
	RetiredCount      int                    `json:"retired_count"`
	SkippedDismissed  int                    `json:"skipped_dismissed"`
	Events            []RuntimeEventRecord   `json:"events,omitempty"`
	Report            TensionReport          `json:"report"`
}

type TensionMutationResult struct {
	Tension TensionRecord      `json:"tension"`
	Event   RuntimeEventRecord `json:"event"`
}

type TensionDependencyMutationResult struct {
	Edge    TensionDependencyEdge `json:"edge"`
	Event   RuntimeEventRecord    `json:"event,omitempty"`
	Changed bool                  `json:"changed"`
}

type TensionCoalitionMemberMutationInput struct {
	WorkspaceID                string
	TensionID                  string
	CoalitionID                string
	AgentID                    string
	ActorType                  string
	ActorID                    string
	SuccessCriterion           string
	Reason                     string
	RequireActorMembership     bool
	RejectActorSelfMutation    bool
	CoalitionAction            string
	PromptContextEnvelope      map[string]any
	PromptContextSurface       string
	PromptContextPrincipalType string
	PromptContextPrincipalID   string
}

type TensionCoalitionMemberMutationResult struct {
	Coalition WorkspaceCoalition     `json:"coalition,omitempty"`
	Tension   TensionRecord          `json:"tension,omitempty"`
	Factors   AgentAttachmentFactors `json:"factors,omitempty"`
	Event     RuntimeEventRecord     `json:"event,omitempty"`
	Changed   bool                   `json:"changed"`
}

type TensionDependencyEdge struct {
	WorkspaceID        string `json:"workspace_id"`
	TensionID          string `json:"tension_id"`
	DependsOnTensionID string `json:"depends_on_tension_id"`
	DependencyType     string `json:"dependency_type"`
}

type tensionClusterContext struct {
	cluster      ProtoClusterReport
	recentEvents []RuntimeEventRecord
	openQueues   []OperatorQueueRecord
	claims       []KnowledgeClaimRecord
}

type tensionEvidenceInput struct {
	kind      string
	ref       string
	eventID   string
	weight    int
	summary   string
	createdAt string
}

type tensionCandidate struct {
	record       TensionRecord
	evidence     []TensionEvidenceRecord
	evidenceRefs []string
	openQueue    bool
	lastEventID  string
	lastSeenAt   string
}

type tensionRelationshipBundle struct {
	outgoing map[string][]TensionDependencyEdge
	incoming map[string][]TensionDependencyEdge
	members  map[string][]string
}

type tensionAdvisoryHints struct {
	coalitionOccupancy map[string]int
	activeSessionIDs   map[string]struct{}
	activeTaskSessions map[string]int
}

const (
	tensionFrontierHardCap      = 32
	tensionFrontierBaseSlots    = 1
	tensionFrontierPerFreeAgent = 2
)

func (s *Store) ListTensions(ctx context.Context, filter TensionFilter) ([]TensionRecord, error) {
	filter = normalizeTensionFilter(filter)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return nil, err
	}
	items, err := s.listAllTensions(ctx, filter)
	if err != nil {
		return nil, err
	}
	if len(items) > filter.Limit {
		items = items[:filter.Limit]
	}
	if err := s.hydrateTensionRecords(ctx, filter.WorkspaceID, items, authority.ReferenceAt); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) ListTensionFrontier(ctx context.Context, filter TensionFilter) ([]TensionFrontierItem, error) {
	requestedLimit := filter.Limit
	filter = normalizeTensionFilter(filter)
	authority, err := s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return nil, err
	}
	items, err := s.listFrontierTensions(ctx, filter)
	if err != nil {
		return nil, err
	}
	if err := s.hydrateTensionRecords(ctx, filter.WorkspaceID, items, authority.ReferenceAt); err != nil {
		return nil, err
	}
	sort.Slice(items, func(i, j int) bool {
		return compareTensionFrontierRecords(items[i], items[j])
	})
	capacity, _, err := s.computeTensionFrontierCapacity(ctx, filter.WorkspaceID)
	if err != nil {
		return nil, err
	}
	limit := capacity
	if requestedLimit > 0 && requestedLimit < limit {
		limit = requestedLimit
	}
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	out := make([]TensionFrontierItem, 0, len(items))
	for _, item := range items {
		out = append(out, tensionFrontierItemFromRecord(item))
	}
	return out, nil
}

func (s *Store) listFrontierTensions(ctx context.Context, filter TensionFilter) ([]TensionRecord, error) {
	activeItems, err := s.listAllTensions(ctx, TensionFilter{
		WorkspaceID:         filter.WorkspaceID,
		TensionType:         filter.TensionType,
		LifecycleState:      firstNonEmpty(filter.LifecycleState, tensionLifecycleActive),
		ReviewStatus:        filter.ReviewStatus,
		ExcludeReviewStatus: tensionReviewDiscarded,
		ProtoClusterID:      filter.ProtoClusterID,
		TaskID:              filter.TaskID,
		AgentID:             filter.AgentID,
	})
	if err != nil {
		return nil, err
	}
	if filter.LifecycleState != "" || !frontierIncludesEmergentMetaTensions(filter.TensionType) {
		return activeItems, nil
	}

	metaItems, err := s.listAllTensions(ctx, TensionFilter{
		WorkspaceID:         filter.WorkspaceID,
		TensionType:         "meta-tension",
		LifecycleState:      tensionLifecycleEmergent,
		ReviewStatus:        filter.ReviewStatus,
		ExcludeReviewStatus: tensionReviewDiscarded,
		ProtoClusterID:      filter.ProtoClusterID,
		TaskID:              filter.TaskID,
		AgentID:             filter.AgentID,
	})
	if err != nil {
		return nil, err
	}

	items := make([]TensionRecord, 0, len(activeItems)+len(metaItems))
	items = append(items, activeItems...)
	items = append(items, metaItems...)
	sort.Slice(items, func(i, j int) bool {
		return compareTensionFrontierRecords(items[i], items[j])
	})
	return items, nil
}

func frontierIncludesEmergentMetaTensions(tensionType string) bool {
	return tensionType == "" || isMetaTensionType(tensionType)
}

func compareTensionFrontierRecords(left, right TensionRecord) bool {
	leftRank := tensionFrontierRank(left)
	rightRank := tensionFrontierRank(right)
	if math.Abs(leftRank-rightRank) > 0.000001 {
		return leftRank > rightRank
	}
	if left.SurfaceScore != right.SurfaceScore {
		return left.SurfaceScore > right.SurfaceScore
	}
	if left.UpdatedAt != right.UpdatedAt {
		return left.UpdatedAt > right.UpdatedAt
	}
	return left.TensionID < right.TensionID
}

func tensionFrontierItemFromRecord(item TensionRecord) TensionFrontierItem {
	return TensionFrontierItem{
		TensionID:         item.TensionID,
		ProtoClusterID:    item.ProtoClusterID,
		Kind:              item.Kind,
		TensionType:       item.TensionType,
		ReviewStatus:      item.ReviewStatus,
		Title:             item.Title,
		Summary:           item.Summary,
		Members:           append([]string{}, item.Members...),
		SurfaceScore:      item.SurfaceScore,
		BaseScore:         item.BaseScore,
		BaseImportance:    item.BaseImportance,
		VisibilityScore:   item.VisibilityScore,
		SurfacedPriority:  item.SurfacedPriority,
		CrowdingRatio:     item.CrowdingRatio,
		ArchivePropensity: item.ArchivePropensity,
		RecoveryRisk:      item.RecoveryRisk,
		LeaseSensitive:    item.LeaseSensitive,
		EvidenceCount:     item.EvidenceCount,
		LastSeenAt:        item.LastSeenAt,
	}
}

func (s *Store) BuildTensionReport(ctx context.Context, filter TensionFilter) (TensionReport, error) {
	requestedLimit := filter.Limit
	filter = normalizeTensionFilter(filter)
	if filter.WorkspaceID == "" {
		return TensionReport{}, errors.New("workspace_id is required")
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return TensionReport{}, err
	}
	frontierCapacity, freeAgentCount, err := s.computeTensionFrontierCapacity(ctx, filter.WorkspaceID)
	if err != nil {
		return TensionReport{}, err
	}
	all, err := s.listAllTensions(ctx, filter)
	if err != nil {
		return TensionReport{}, err
	}
	report := TensionReport{
		WorkspaceID:          filter.WorkspaceID,
		GeneratedAt:          generatedAtFromWorkspaceTimeAuthority(authority),
		TimeAuthority:        authority,
		Filter:               filter,
		FrontierCapacity:     frontierCapacity,
		FreeAgentCount:       freeAgentCount,
		CountsByType:         map[string]int{},
		CountsByReviewStatus: map[string]int{},
	}
	for _, item := range all {
		report.TotalCount++
		report.CountsByType[item.TensionType]++
		report.CountsByReviewStatus[item.ReviewStatus]++
		switch item.LifecycleState {
		case tensionLifecycleArchived:
			report.ArchivedCount++
		default:
			report.ActiveCount++
		}
		if item.ReviewStatus == tensionReviewPending {
			report.PendingCount++
		}
	}
	frontier, err := s.ListTensionFrontier(ctx, TensionFilter{
		WorkspaceID:    filter.WorkspaceID,
		TensionType:    filter.TensionType,
		LifecycleState: filter.LifecycleState,
		ReviewStatus:   filter.ReviewStatus,
		ProtoClusterID: filter.ProtoClusterID,
		TaskID:         filter.TaskID,
		AgentID:        filter.AgentID,
		Limit:          requestedLimit,
	})
	if err != nil {
		return TensionReport{}, err
	}
	report.Frontier = frontier
	return report, nil
}

func (s *Store) GetTension(ctx context.Context, workspaceID, tensionID string) (TensionDetail, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	tensionID = strings.TrimSpace(tensionID)
	if workspaceID == "" {
		return TensionDetail{}, errors.New("workspace_id is required")
	}
	if tensionID == "" {
		return TensionDetail{}, errors.New("tension_id is required")
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return TensionDetail{}, err
	}
	record, err := s.loadTensionRecord(ctx, nil, workspaceID, tensionID)
	if err != nil {
		return TensionDetail{}, err
	}
	rels, err := s.loadTensionRelationships(ctx, workspaceID, []string{record.TensionID})
	if err != nil {
		return TensionDetail{}, err
	}
	applyTensionStructuralFields(&record)
	record.Members = append([]string{}, rels.members[record.TensionID]...)
	record.BlockedByTensionIDs = dependencySourceIDs(rels.incoming[record.TensionID])
	record.BlocksTensionIDs = dependencyTargetIDs(rels.outgoing[record.TensionID])
	evidence, err := s.listTensionEvidence(ctx, workspaceID, tensionID)
	if err != nil {
		return TensionDetail{}, err
	}
	if len(record.Members) == 0 {
		record.Members = memberRefsFromEvidence(evidence)
	}
	hints, err := s.loadTensionAdvisoryHints(ctx, workspaceID, []TensionRecord{record})
	if err != nil {
		return TensionDetail{}, err
	}
	applyTensionAdvisoryFields(&record, authority.ReferenceAt, hints)
	events, err := s.loadTensionEvents(ctx, workspaceID, evidence)
	if err != nil {
		return TensionDetail{}, err
	}
	claims, err := s.loadTensionClaims(ctx, workspaceID, record, evidence)
	if err != nil {
		return TensionDetail{}, err
	}
	queues, err := s.loadTensionQueues(ctx, workspaceID, record, evidence)
	if err != nil {
		return TensionDetail{}, err
	}
	docs, err := s.loadTensionDocs(ctx, workspaceID, record.DocKeys)
	if err != nil {
		return TensionDetail{}, err
	}
	artifacts, err := s.loadTensionArtifacts(ctx, workspaceID, record.ArtifactRefs)
	if err != nil {
		return TensionDetail{}, err
	}
	cluster, err := s.loadProtoClusterForTension(ctx, record)
	if err != nil {
		return TensionDetail{}, err
	}
	return TensionDetail{
		TimeAuthority: authority,
		Tension:       record,
		Dependencies:  append([]TensionDependencyEdge{}, rels.incoming[record.TensionID]...),
		Dependents:    append([]TensionDependencyEdge{}, rels.outgoing[record.TensionID]...),
		Evidence:      evidence,
		Events:        events,
		Claims:        claims,
		Queues:        queues,
		Docs:          docs,
		Artifacts:     artifacts,
		ProtoCluster:  cluster,
	}, nil
}

func (s *Store) hydrateTensionRecords(ctx context.Context, workspaceID string, records []TensionRecord, referenceAt string) error {
	if len(records) == 0 {
		return nil
	}
	ids := make([]string, 0, len(records))
	for _, record := range records {
		if tensionID := strings.TrimSpace(record.TensionID); tensionID != "" {
			ids = append(ids, tensionID)
		}
	}
	rels, err := s.loadTensionRelationships(ctx, workspaceID, ids)
	if err != nil {
		return err
	}
	hints, err := s.loadTensionAdvisoryHints(ctx, workspaceID, records)
	if err != nil {
		return err
	}
	for idx := range records {
		applyTensionStructuralFields(&records[idx])
		records[idx].Members = append([]string{}, rels.members[records[idx].TensionID]...)
		records[idx].BlockedByTensionIDs = dependencySourceIDs(rels.incoming[records[idx].TensionID])
		records[idx].BlocksTensionIDs = dependencyTargetIDs(rels.outgoing[records[idx].TensionID])
		applyTensionAdvisoryFields(&records[idx], referenceAt, hints)
	}
	return nil
}

func applyTensionStructuralFields(record *TensionRecord) {
	if record == nil {
		return
	}
	if isMetaTensionType(record.TensionType) {
		record.Kind = "meta"
	} else {
		record.Kind = "atomic"
	}
	record.BaseImportance = normalizedTensionScore(record.BaseScore)
	record.SurfacedPriority = normalizedTensionScore(record.SurfaceScore)
	record.VisibilityScore = tensionVisibilityScore(record.BaseImportance, record.SurfacedPriority)
}

func normalizedTensionScore(score int) float64 {
	if score <= 0 {
		return 0
	}
	value := float64(score) / 100.0
	if value > 1 {
		value = 1
	}
	return math.Round(value*1000) / 1000
}

func tensionVisibilityScore(baseImportance, surfacedPriority float64) float64 {
	if surfacedPriority <= 0 {
		return 0
	}
	if baseImportance <= 0 {
		return 1
	}
	value := surfacedPriority / baseImportance
	if value > 1 {
		value = 1
	}
	return math.Round(value*1000) / 1000
}

func applyTensionAdvisoryFields(record *TensionRecord, referenceAt string, hints tensionAdvisoryHints) {
	if record == nil {
		return
	}
	idleNorm := tensionIdleNorm(*record, referenceAt)
	record.CrowdingRatio = tensionCrowdingRatio(*record, hints)
	record.LeaseSensitive = tensionLeaseSensitive(*record, hints)
	record.RecoveryRisk = tensionRecoveryRisk(*record, idleNorm)
	record.ArchivePropensity = tensionArchivePropensity(*record, idleNorm)
}

func tensionFrontierRank(record TensionRecord) float64 {
	rank := record.SurfacedPriority
	if rank <= 0 {
		rank = normalizedTensionScore(record.SurfaceScore)
	}
	rank *= 1 - 0.40*record.CrowdingRatio
	rank *= 1 - 0.30*record.ArchivePropensity
	rank *= 1 + 0.20*record.RecoveryRisk
	if record.LeaseSensitive {
		rank += 0.05
	}
	if rank < 0 {
		rank = 0
	}
	return roundedTensionFloat(rank)
}

func tensionCrowdingRatio(record TensionRecord, hints tensionAdvisoryHints) float64 {
	occupierCount := maxInt(len(uniqueSortedStrings(record.AgentIDs)), len(uniqueSortedStrings(record.SessionIDs)))
	if hinted := tensionOccupierCount(record, hints); hinted > occupierCount {
		occupierCount = hinted
	}
	if occupierCount <= 0 {
		return 0
	}
	capacity := coalitionSizeCapForTensionType(record.TensionType)
	if capacity <= 0 {
		capacity = 3
	}
	return clampTensionUnit(float64(occupierCount) / float64(capacity))
}

func tensionLeaseSensitive(record TensionRecord, hints tensionAdvisoryHints) bool {
	if tensionOccupierCount(record, hints) > 0 {
		return true
	}
	if len(uniqueSortedStrings(record.SessionIDs)) > 0 {
		return true
	}
	if len(record.SegmentRefs) > 0 && (len(record.AgentIDs) > 0 || len(record.ConstraintRefs) > 0) {
		return true
	}
	return false
}

func tensionRecoveryRisk(record TensionRecord, idleNorm float64) float64 {
	recentActivity := 1 - idleNorm
	recurrence := clampTensionUnit(float64(record.StaleRefreshCount) / 3.0)
	evidenceMass := clampTensionUnit(float64(maxInt(record.EvidenceCount-1, 0)) / 4.0)
	dependencyMass := clampTensionUnit(float64(len(record.BlockedByTensionIDs)+len(record.BlocksTensionIDs)+len(record.Members)) / 4.0)
	leaseMass := 0.0
	if record.LeaseSensitive {
		leaseMass = 1.0
	}
	risk := 0.30*recentActivity + 0.25*recurrence + 0.20*evidenceMass + 0.15*dependencyMass + 0.10*leaseMass
	return roundedTensionFloat(clampTensionUnit(risk))
}

func tensionArchivePropensity(record TensionRecord, idleNorm float64) float64 {
	relationClear := 0.0
	if len(record.BlockedByTensionIDs) == 0 && len(record.BlocksTensionIDs) == 0 && len(record.Members) == 0 {
		relationClear = 1.0
	}
	archive := 0.40*idleNorm +
		0.20*(1-record.BaseImportance) +
		0.15*(1-record.SurfacedPriority) +
		0.15*relationClear +
		0.10*(1-record.RecoveryRisk)
	if record.LeaseSensitive {
		archive -= 0.35
	}
	if idleNorm < 0.15 {
		archive -= 0.15
	}
	return roundedTensionFloat(clampTensionUnit(archive))
}

func tensionIdleNorm(record TensionRecord, referenceAt string) float64 {
	idle := tensionReferenceAge(referenceAt, tensionMeaningfulTimestamp(record))
	if idle < 0 {
		return 0
	}
	return roundedTensionFloat(clampTensionUnit(idle.Hours() / (24.0 * 7.0)))
}

func tensionMeaningfulTimestamp(record TensionRecord) string {
	value := ""
	value = controlLaterTimestamp(value, record.LastSeenAt)
	value = controlLaterTimestamp(value, record.LastDetectedAt)
	value = controlLaterTimestamp(value, record.LastRefreshedAt)
	value = controlLaterTimestamp(value, record.UpdatedAt)
	value = controlLaterTimestamp(value, record.CreatedAt)
	return value
}

func tensionReferenceAge(referenceAt, observedAt string) time.Duration {
	observedAt = strings.TrimSpace(observedAt)
	if observedAt == "" {
		return -1
	}
	observed, err := time.Parse(time.RFC3339Nano, observedAt)
	if err != nil {
		return -1
	}
	reference := time.Now().UTC()
	if strings.TrimSpace(referenceAt) != "" {
		if parsedReference, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(referenceAt)); err == nil {
			reference = parsedReference
		}
	}
	if reference.Before(observed) {
		return 0
	}
	return reference.Sub(observed)
}

func clampTensionUnit(value float64) float64 {
	switch {
	case value < 0:
		return 0
	case value > 1:
		return 1
	default:
		return value
	}
}

func roundedTensionFloat(value float64) float64 {
	return math.Round(value*1000) / 1000
}

func tensionOccupierCount(record TensionRecord, hints tensionAdvisoryHints) int {
	count := hints.coalitionOccupancy[strings.TrimSpace(record.TensionID)]
	for _, sessionID := range uniqueSortedStrings(record.SessionIDs) {
		if _, ok := hints.activeSessionIDs[sessionID]; ok {
			count++
		}
	}
	for _, taskID := range uniqueSortedStrings(record.TaskIDs) {
		count = maxInt(count, hints.activeTaskSessions[strings.TrimSpace(taskID)])
	}
	return count
}

func (s *Store) loadTensionAdvisoryHints(ctx context.Context, workspaceID string, records []TensionRecord) (tensionAdvisoryHints, error) {
	hints := tensionAdvisoryHints{
		coalitionOccupancy: map[string]int{},
		activeSessionIDs:   map[string]struct{}{},
		activeTaskSessions: map[string]int{},
	}
	if len(records) == 0 {
		return hints, nil
	}
	tensionIDs := make([]string, 0, len(records))
	taskIDs := []string{}
	sessionIDs := []string{}
	for _, record := range records {
		if tensionID := strings.TrimSpace(record.TensionID); tensionID != "" {
			tensionIDs = append(tensionIDs, tensionID)
		}
		taskIDs = append(taskIDs, uniqueSortedStrings(record.TaskIDs)...)
		sessionIDs = append(sessionIDs, uniqueSortedStrings(record.SessionIDs)...)
	}
	tensionIDs = uniqueSortedStrings(tensionIDs)
	taskIDs = uniqueSortedStrings(taskIDs)
	sessionIDs = uniqueSortedStrings(sessionIDs)
	if len(tensionIDs) > 0 {
		occupancy, err := s.loadTensionCoalitionOccupancy(ctx, workspaceID, tensionIDs)
		if err != nil {
			return tensionAdvisoryHints{}, err
		}
		hints.coalitionOccupancy = occupancy
	}
	if len(taskIDs) == 0 && len(sessionIDs) == 0 {
		return hints, nil
	}
	activeSessionIDs, activeTaskSessions, err := s.loadTensionActiveSessionHints(ctx, workspaceID, taskIDs, sessionIDs)
	if err != nil {
		return tensionAdvisoryHints{}, err
	}
	hints.activeSessionIDs = activeSessionIDs
	hints.activeTaskSessions = activeTaskSessions
	return hints, nil
}

func (s *Store) loadTensionCoalitionOccupancy(ctx context.Context, workspaceID string, tensionIDs []string) (map[string]int, error) {
	occupancy := make(map[string]int, len(tensionIDs))
	if len(tensionIDs) == 0 {
		return occupancy, nil
	}
	currentEpoch, err := s.currentControlEpoch(ctx, s.db, workspaceID)
	if err != nil {
		return nil, err
	}
	for _, tensionID := range uniqueSortedStrings(tensionIDs) {
		candidate, _, _, err := s.selectLiveCoalitionCandidateByTension(ctx, s.db, workspaceID, tensionID, currentEpoch)
		if err != nil {
			return nil, err
		}
		if candidate == nil {
			continue
		}
		tension, err := s.loadTensionRecord(ctx, nil, workspaceID, tensionID)
		switch {
		case err != nil:
			continue
		case !coalitionEligibleTension(tension):
			continue
		}
		occupancy[strings.TrimSpace(tensionID)] = candidate.memberCount
	}
	return occupancy, nil
}

func (s *Store) loadTensionActiveSessionHints(ctx context.Context, workspaceID string, taskIDs, sessionIDs []string) (map[string]struct{}, map[string]int, error) {
	activeSessionIDs := map[string]struct{}{}
	activeTaskSessions := map[string]int{}
	if len(taskIDs) == 0 && len(sessionIDs) == 0 {
		return activeSessionIDs, activeTaskSessions, nil
	}
	args := []any{workspaceID}
	clauses := make([]string, 0, 2)
	if len(sessionIDs) > 0 {
		clauses = append(clauses, `session_id IN (`+placeholders(len(sessionIDs))+`)`)
		for _, sessionID := range sessionIDs {
			args = append(args, sessionID)
		}
	}
	if len(taskIDs) > 0 {
		clauses = append(clauses, `COALESCE(task_id, '') IN (`+placeholders(len(taskIDs))+`)`)
		for _, taskID := range taskIDs {
			args = append(args, taskID)
		}
	}
	query := `SELECT session_id, workspace_id, agent_id, COALESCE(task_id,''), status, started_at, COALESCE(completed_at,'')
		FROM agent_sessions
		WHERE workspace_id = ? AND (` + strings.Join(clauses, ` OR `) + `)`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("query tension active session hints: %w", err)
	}
	defer rows.Close()

	sessionOrder := make([]string, 0, len(sessionIDs)+len(taskIDs))
	states := make(map[string]AgentSessionStateRecord, len(sessionIDs)+len(taskIDs))
	for rows.Next() {
		var record AgentSessionStateRecord
		if err := rows.Scan(
			&record.SessionID,
			&record.WorkspaceID,
			&record.AgentID,
			&record.TaskID,
			&record.Status,
			&record.StartedAt,
			&record.CompletedAt,
		); err != nil {
			return nil, nil, fmt.Errorf("scan tension active session hint: %w", err)
		}
		record.UpdatedAt = record.StartedAt
		sessionOrder = append(sessionOrder, record.SessionID)
		states[record.SessionID] = record
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate tension active session hints: %w", err)
	}
	if len(states) == 0 {
		return activeSessionIDs, activeTaskSessions, nil
	}
	sessionUpdates, err := s.listLatestSessionCoordinationUpdates(ctx, workspaceID, sessionOrder)
	if err != nil {
		return nil, nil, err
	}
	for sessionID, update := range sessionUpdates {
		record := states[sessionID]
		record.Status = update.Status
		record.UpdatedAt = update.UpdatedAt
		if update.TaskID != "" {
			record.TaskID = update.TaskID
		}
		states[sessionID] = record
	}
	for _, record := range states {
		if !model.IsSessionStatusActive(record.Status) {
			continue
		}
		activeSessionIDs[strings.TrimSpace(record.SessionID)] = struct{}{}
		if taskID := strings.TrimSpace(record.TaskID); taskID != "" {
			activeTaskSessions[taskID]++
		}
	}
	return activeSessionIDs, activeTaskSessions, nil
}

func memberRefsFromEvidence(evidence []TensionEvidenceRecord) []string {
	members := make([]string, 0, len(evidence))
	for _, item := range evidence {
		if strings.EqualFold(strings.TrimSpace(item.EvidenceKind), "member_tension") {
			if ref := strings.TrimSpace(item.EvidenceRef); ref != "" {
				members = append(members, ref)
			}
		}
	}
	return uniqueSortedStrings(members)
}

func dependencyTargetIDs(edges []TensionDependencyEdge) []string {
	out := make([]string, 0, len(edges))
	for _, edge := range edges {
		if target := strings.TrimSpace(edge.DependsOnTensionID); target != "" {
			out = append(out, target)
		}
	}
	return uniqueSortedStrings(out)
}

func dependencySourceIDs(edges []TensionDependencyEdge) []string {
	out := make([]string, 0, len(edges))
	for _, edge := range edges {
		if source := strings.TrimSpace(edge.TensionID); source != "" {
			out = append(out, source)
		}
	}
	return uniqueSortedStrings(out)
}

func (s *Store) loadTensionRelationships(ctx context.Context, workspaceID string, tensionIDs []string) (tensionRelationshipBundle, error) {
	tensionIDs = uniqueSortedStrings(tensionIDs)
	bundle := tensionRelationshipBundle{
		outgoing: make(map[string][]TensionDependencyEdge, len(tensionIDs)),
		incoming: make(map[string][]TensionDependencyEdge, len(tensionIDs)),
		members:  make(map[string][]string, len(tensionIDs)),
	}
	if len(tensionIDs) == 0 {
		return bundle, nil
	}
	targets := stringSet(tensionIDs)
	args := make([]any, 0, 1+len(tensionIDs)*2)
	args = append(args, workspaceID)
	for _, tensionID := range tensionIDs {
		args = append(args, tensionID)
	}
	for _, tensionID := range tensionIDs {
		args = append(args, tensionID)
	}
	query := `SELECT workspace_id, tension_id, depends_on_tension_id, dependency_type
		FROM workspace_tension_dependencies
		WHERE workspace_id = ? AND (tension_id IN (` + placeholders(len(tensionIDs)) + `) OR depends_on_tension_id IN (` + placeholders(len(tensionIDs)) + `))
		ORDER BY tension_id, depends_on_tension_id, dependency_type`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return tensionRelationshipBundle{}, fmt.Errorf("query tension relationships: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var edge TensionDependencyEdge
		if err := rows.Scan(&edge.WorkspaceID, &edge.TensionID, &edge.DependsOnTensionID, &edge.DependencyType); err != nil {
			return tensionRelationshipBundle{}, fmt.Errorf("scan tension relationship: %w", err)
		}
		if strings.EqualFold(strings.TrimSpace(edge.DependencyType), "SUBSUMED_BY") {
			bundle.members[edge.DependsOnTensionID] = uniqueSortedStrings(append(bundle.members[edge.DependsOnTensionID], edge.TensionID))
			continue
		}
		if _, ok := targets[edge.TensionID]; ok {
			bundle.outgoing[edge.TensionID] = append(bundle.outgoing[edge.TensionID], edge)
		}
		if _, ok := targets[edge.DependsOnTensionID]; ok {
			bundle.incoming[edge.DependsOnTensionID] = append(bundle.incoming[edge.DependsOnTensionID], edge)
		}
	}
	if err := rows.Err(); err != nil {
		return tensionRelationshipBundle{}, fmt.Errorf("iterate tension relationships: %w", err)
	}
	for key := range bundle.outgoing {
		sort.Slice(bundle.outgoing[key], func(i, j int) bool {
			if bundle.outgoing[key][i].DependsOnTensionID != bundle.outgoing[key][j].DependsOnTensionID {
				return bundle.outgoing[key][i].DependsOnTensionID < bundle.outgoing[key][j].DependsOnTensionID
			}
			return bundle.outgoing[key][i].DependencyType < bundle.outgoing[key][j].DependencyType
		})
	}
	for key := range bundle.incoming {
		sort.Slice(bundle.incoming[key], func(i, j int) bool {
			if bundle.incoming[key][i].TensionID != bundle.incoming[key][j].TensionID {
				return bundle.incoming[key][i].TensionID < bundle.incoming[key][j].TensionID
			}
			return bundle.incoming[key][i].DependencyType < bundle.incoming[key][j].DependencyType
		})
	}
	return bundle, nil
}

func (s *Store) computeTensionFrontierCapacity(ctx context.Context, workspaceID string) (int, int, error) {
	agents, err := s.ListWorkspaceAgents(ctx, workspaceID)
	if err != nil {
		return 0, 0, err
	}
	sessionLimit := len(agents) * 4
	if sessionLimit < 50 {
		sessionLimit = 50
	}
	activeSessions, err := s.ListWorkspaceSessionStates(ctx, workspaceID, true, sessionLimit)
	if err != nil {
		return 0, 0, err
	}
	busy := make(map[string]struct{}, len(activeSessions))
	for _, session := range activeSessions {
		if agentID := strings.TrimSpace(session.AgentID); agentID != "" {
			busy[agentID] = struct{}{}
		}
	}
	freeAgentCount := 0
	for _, agent := range agents {
		if _, ok := busy[strings.TrimSpace(agent.AgentID)]; ok {
			continue
		}
		freeAgentCount++
	}
	capacity := tensionFrontierBaseSlots + tensionFrontierPerFreeAgent*freeAgentCount
	if capacity > tensionFrontierHardCap {
		capacity = tensionFrontierHardCap
	}
	if capacity <= 0 {
		capacity = tensionFrontierBaseSlots
	}
	return capacity, freeAgentCount, nil
}

func normalizeTensionFilter(filter TensionFilter) TensionFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.TensionType = normalizeTensionType(filter.TensionType)
	filter.LifecycleState = normalizeTensionLifecycle(filter.LifecycleState)
	filter.ReviewStatus = normalizeTensionReviewStatus(filter.ReviewStatus)
	filter.ExcludeReviewStatus = normalizeTensionReviewStatus(filter.ExcludeReviewStatus)
	filter.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	return filter
}

func normalizeTensionType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ""
	case "failure":
		return "failure"
	case "repair":
		return "repair"
	case "bottleneck":
		return "bottleneck"
	case "contradiction":
		return "contradiction"
	case "ambiguity":
		return "ambiguity"
	case "gap":
		return "gap"
	case "bridge":
		return "bridge"
	case "fork_candidate":
		return "fork_candidate"
	case "dissent_followup":
		return "dissent_followup"
	case "review_scarcity":
		return "review_scarcity"
	case "cache_drift":
		return "cache_drift"
	case "load_spike":
		return "load_spike"
	case "meta_tension":
		return "meta-tension"
	case "meta-tension":
		return "meta-tension"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func normalizeTensionLifecycle(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "":
		return ""
	case tensionLifecycleEmergent:
		return tensionLifecycleEmergent
	case tensionLifecycleActive:
		return tensionLifecycleActive
	case tensionLifecycleResolved:
		return tensionLifecycleResolved
	case tensionLifecycleDiscarded:
		return tensionLifecycleDiscarded
	case tensionLifecycleArchived:
		return tensionLifecycleArchived
	case tensionLifecycleSuperseded:
		return tensionLifecycleSuperseded
	case tensionLifecycleDisputed:
		return tensionLifecycleDisputed
	case tensionLifecycleRecovered:
		return tensionLifecycleRecovered
	case tensionLifecycleMeta:
		return tensionLifecycleEmergent
	default:
		return strings.ToUpper(strings.TrimSpace(value))
	}
}

func normalizeTensionReviewStatus(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "":
		return ""
	case tensionReviewPending:
		return tensionReviewPending
	case tensionReviewConfirmed:
		return tensionReviewConfirmed
	case tensionReviewDiscarded:
		return tensionReviewDiscarded
	default:
		return strings.ToUpper(strings.TrimSpace(value))
	}
}

func tensionRecordMatchesFilter(record TensionRecord, filter TensionFilter) bool {
	if filter.TaskID != "" && !containsString(record.TaskIDs, filter.TaskID) {
		return false
	}
	if filter.AgentID != "" && !containsString(record.AgentIDs, filter.AgentID) {
		return false
	}
	return true
}

func (s *Store) listAllTensions(ctx context.Context, filter TensionFilter) ([]TensionRecord, error) {
	query := strings.Builder{}
	query.WriteString(`SELECT tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status,
		title, summary, anchor_kind, anchor_ref, task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json,
		segment_refs_json, agent_ids_json, constraint_refs_json, base_score, surface_score, evidence_count,
		last_seen_event_id, last_seen_at, last_detected_at, last_refreshed_at, stale_refresh_count,
		confirmed_by, archived_by, dismissed_reason, created_at, updated_at
	  FROM workspace_tensions
	  WHERE workspace_id = ?`)
	args := []any{filter.WorkspaceID}
	if filter.TensionType != "" {
		query.WriteString(` AND tension_type = ?`)
		args = append(args, filter.TensionType)
	}
	if filter.LifecycleState != "" {
		query.WriteString(` AND lifecycle_state = ?`)
		args = append(args, filter.LifecycleState)
	}
	if filter.ReviewStatus != "" {
		query.WriteString(` AND review_status = ?`)
		args = append(args, filter.ReviewStatus)
	}
	if filter.ExcludeReviewStatus != "" {
		query.WriteString(` AND review_status != ?`)
		args = append(args, filter.ExcludeReviewStatus)
	}
	if filter.ProtoClusterID != "" {
		query.WriteString(` AND proto_cluster_id = ?`)
		args = append(args, filter.ProtoClusterID)
	}
	if filter.TaskID != "" {
		query.WriteString(` AND EXISTS(SELECT 1 FROM json_each(task_ids_json) WHERE value = ?)`)
		args = append(args, filter.TaskID)
	}
	if filter.AgentID != "" {
		query.WriteString(` AND EXISTS(SELECT 1 FROM json_each(agent_ids_json) WHERE value = ?)`)
		args = append(args, filter.AgentID)
	}
	query.WriteString(` ORDER BY surface_score DESC, updated_at DESC, tension_id ASC`)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query all tensions: %w", err)
	}
	defer rows.Close()

	out := []TensionRecord{}
	for rows.Next() {
		record, err := scanTensionRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all tensions: %w", err)
	}
	return out, nil
}

func scanTensionRecord(scanner interface{ Scan(dest ...any) error }) (TensionRecord, error) {
	var record TensionRecord
	var taskIDsJSON, sessionIDsJSON, docKeysJSON, artifactRefsJSON, segmentRefsJSON, agentIDsJSON, constraintRefsJSON string
	if err := scanner.Scan(
		&record.TensionID,
		&record.WorkspaceID,
		&record.ProtoClusterID,
		&record.TensionType,
		&record.LifecycleState,
		&record.ReviewStatus,
		&record.Title,
		&record.Summary,
		&record.AnchorKind,
		&record.AnchorRef,
		&taskIDsJSON,
		&sessionIDsJSON,
		&docKeysJSON,
		&artifactRefsJSON,
		&segmentRefsJSON,
		&agentIDsJSON,
		&constraintRefsJSON,
		&record.BaseScore,
		&record.SurfaceScore,
		&record.EvidenceCount,
		&record.LastSeenEventID,
		&record.LastSeenAt,
		&record.LastDetectedAt,
		&record.LastRefreshedAt,
		&record.StaleRefreshCount,
		&record.ConfirmedBy,
		&record.ArchivedBy,
		&record.DiscardedReason,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return TensionRecord{}, fmt.Errorf("scan tension record: %w", err)
	}
	record.TaskIDs = decodeStringJSONArray(taskIDsJSON)
	record.SessionIDs = decodeStringJSONArray(sessionIDsJSON)
	record.DocKeys = decodeStringJSONArray(docKeysJSON)
	record.ArtifactRefs = decodeStringJSONArray(artifactRefsJSON)
	record.SegmentRefs = decodeStringJSONArray(segmentRefsJSON)
	record.AgentIDs = decodeStringJSONArray(agentIDsJSON)
	record.ConstraintRefs = decodeStringJSONArray(constraintRefsJSON)
	applyTensionStructuralFields(&record)
	return record, nil
}

func (s *Store) ConfirmTension(ctx context.Context, input TensionMutationInput) (TensionMutationResult, error) {
	return s.transitionTension(ctx, input, func(record *TensionRecord) (string, error) {
		if record.LifecycleState != tensionLifecycleEmergent && record.LifecycleState != tensionLifecycleDormant && record.LifecycleState != tensionLifecycleActive {
			return "", errors.New("tension cannot be confirmed/activated from its current state")
		}
		record.LifecycleState = tensionLifecycleActive
		record.ReviewStatus = tensionReviewConfirmed
		record.ConfirmedBy = strings.TrimSpace(input.ActorID)
		return "tension.confirmed", nil
	})
}

func (s *Store) DiscardTension(ctx context.Context, input TensionMutationInput) (TensionMutationResult, error) {
	return s.transitionTension(ctx, input, func(record *TensionRecord) (string, error) {
		if record.LifecycleState != tensionLifecycleEmergent && record.LifecycleState != tensionLifecycleActive && record.LifecycleState != tensionLifecycleDormant {
			return "", errors.New("cannot discard tension from its current state")
		}
		record.LifecycleState = tensionLifecycleDiscarded
		record.ReviewStatus = tensionReviewDiscarded
		record.DiscardedReason = strings.TrimSpace(input.Reason)
		return "tension.discarded", nil
	})
}

func (s *Store) ArchiveTension(ctx context.Context, input TensionMutationInput) (TensionMutationResult, error) {
	return s.transitionTension(ctx, input, func(record *TensionRecord) (string, error) {
		if record.LifecycleState != tensionLifecycleResolved && record.LifecycleState != tensionLifecycleDiscarded {
			return "", errors.New("only resolved or discarded tensions can be archived")
		}
		record.LifecycleState = tensionLifecycleArchived
		record.ArchivedBy = strings.TrimSpace(input.ActorID)
		return "tension.archived", nil
	})
}

func (s *Store) ResolveTension(ctx context.Context, input TensionMutationInput) (TensionMutationResult, error) {
	return s.transitionTension(ctx, input, func(record *TensionRecord) (string, error) {
		if record.LifecycleState != tensionLifecycleActive && record.LifecycleState != tensionLifecycleDormant {
			return "", errors.New("only active or dormant tensions can be resolved")
		}
		record.LifecycleState = tensionLifecycleResolved
		return "tension.resolved", nil
	})
}

func (s *Store) SupersedeTension(ctx context.Context, input TensionMutationInput) (TensionMutationResult, error) {
	return s.transitionTension(ctx, input, func(record *TensionRecord) (string, error) {
		if record.LifecycleState != tensionLifecycleActive && record.LifecycleState != tensionLifecycleDormant && record.LifecycleState != tensionLifecycleEmergent {
			return "", errors.New("only emergent, active, or dormant tensions can be superseded")
		}
		record.LifecycleState = tensionLifecycleSuperseded
		return "tension.superseded", nil
	})
}

func (s *Store) DormantTension(ctx context.Context, input TensionMutationInput) (TensionMutationResult, error) {
	return s.transitionTension(ctx, input, func(record *TensionRecord) (string, error) {
		if record.LifecycleState != tensionLifecycleActive {
			return "", errors.New("only active tensions can become dormant")
		}
		record.LifecycleState = tensionLifecycleDormant
		return "tension.dormant", nil
	})
}

func (s *Store) ArchiveResolvedTensions(ctx context.Context, workspaceID string, olderThanHours int) (int, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return 0, errors.New("workspace_id is required")
	}
	cutoff := time.Now().UTC().Add(-time.Duration(olderThanHours) * time.Hour).Format(time.RFC3339Nano)

	query := `
		SELECT tension_id FROM workspace_tensions
		WHERE workspace_id = ? AND lifecycle_state = ? AND updated_at < ?
	`
	rows, err := s.db.QueryContext(ctx, query, workspaceID, tensionLifecycleResolved, cutoff)
	if err != nil {
		return 0, fmt.Errorf("query resolved tensions: %w", err)
	}
	defer rows.Close()

	var tensionIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		tensionIDs = append(tensionIDs, id)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	archivedCount := 0
	for _, id := range tensionIDs {
		_, err := s.ArchiveTension(ctx, TensionMutationInput{
			WorkspaceID: workspaceID,
			TensionID:   id,
			ActorID:     "system:auto_archiver",
			Reason:      fmt.Sprintf("auto-archived after %d hours in RESOLVED state", olderThanHours),
		})
		if err == nil {
			archivedCount++
		}
	}

	return archivedCount, nil
}

func (s *Store) transitionTension(ctx context.Context, input TensionMutationInput, apply func(record *TensionRecord) (string, error)) (TensionMutationResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.TensionID = strings.TrimSpace(input.TensionID)
	input.ActorID = strings.TrimSpace(input.ActorID)
	input.Reason = strings.TrimSpace(input.Reason)
	input.PromptContextSurface = strings.TrimSpace(input.PromptContextSurface)
	input.PromptContextPrincipalType = strings.TrimSpace(input.PromptContextPrincipalType)
	input.PromptContextPrincipalID = strings.TrimSpace(input.PromptContextPrincipalID)
	if input.WorkspaceID == "" {
		return TensionMutationResult{}, errors.New("workspace_id is required")
	}
	if input.TensionID == "" {
		return TensionMutationResult{}, errors.New("tension_id is required")
	}
	if input.ActorID == "" {
		return TensionMutationResult{}, errors.New("actor_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TensionMutationResult{}, fmt.Errorf("begin tension mutation tx: %w", err)
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInputTx(ctx, tx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		_ = tx.Rollback()
		return TensionMutationResult{}, err
	}
	var result TensionMutationResult
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		record, err := s.loadTensionRecord(ctx, tx, input.WorkspaceID, input.TensionID)
		if err != nil {
			return err
		}
		eventType, err := apply(&record)
		if err != nil {
			return err
		}
		if err := validateTensionMutationPromptContextSurface(input.PromptContextEnvelope, input.PromptContextSurface, eventType, input.PromptContextAllowLifecycleUpdate); err != nil {
			return err
		}
		record.UpdatedAt = now
		if err := s.upsertTensionTx(ctx, tx, record); err != nil {
			return err
		}
		if !coalitionEligibleTension(record) {
			if err := s.disbandLiveCoalitionsForTension(ctx, tx, input.WorkspaceID, record.TensionID); err != nil {
				return fmt.Errorf("disband ineligible tension coalitions: %w", err)
			}
		}

		if err := s.executeTensionCascadeRecoveryTx(ctx, tx, authority, input.WorkspaceID, record, now, input.ActorID); err != nil {
			return fmt.Errorf("execute cascade recovery: %w", err)
		}

		evidence, err := s.listTensionEvidenceTx(ctx, tx, input.WorkspaceID, input.TensionID)
		if err != nil {
			return err
		}
		promptContext := tensionRuntimePromptContext{
			Envelope:      input.PromptContextEnvelope,
			Surface:       tensionPromptContextSurface(input.PromptContextSurface, eventType),
			PrincipalType: input.PromptContextPrincipalType,
			PrincipalID:   input.PromptContextPrincipalID,
		}
		event, err := s.appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx, tx, authority, record, evidenceRefsFromEvidence(evidence), eventType, "operator", input.ActorID, input.Reason, promptContext)
		if err != nil {
			return err
		}
		result = TensionMutationResult{Tension: record, Event: event}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return TensionMutationResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return TensionMutationResult{}, fmt.Errorf("commit tension mutation tx: %w", err)
	}
	return result, nil
}

func (s *Store) executeTensionCascadeRecoveryTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID string, parent TensionRecord, now, actorID string) error {
	if isMetaTensionType(parent.TensionType) {
		return nil
	}
	isSuccess := parent.LifecycleState == tensionLifecycleResolved || parent.LifecycleState == tensionLifecycleArchived
	isFailure := parent.LifecycleState == tensionLifecycleSuperseded || parent.LifecycleState == tensionLifecycleDisputed || parent.LifecycleState == tensionLifecycleDiscarded

	if !isSuccess && !isFailure {
		return nil
	}

	query := `SELECT d.tension_id
              FROM workspace_tension_dependencies d
              WHERE d.workspace_id = ? AND d.depends_on_tension_id = ? AND d.dependency_type = 'SUBSUMED_BY'`

	rows, err := tx.QueryContext(ctx, query, workspaceID, parent.TensionID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var children []string
	for rows.Next() {
		var childID string
		if err := rows.Scan(&childID); err != nil {
			return err
		}
		children = append(children, childID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, childID := range children {
		child, err := s.loadTensionRecord(ctx, tx, workspaceID, childID)
		if err != nil {
			continue
		}

		if isSuccess {
			child.LifecycleState = parent.LifecycleState
			child.UpdatedAt = now
			if err := s.upsertTensionTx(ctx, tx, child); err != nil {
				return err
			}
			if _, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
				WorkspaceID: workspaceID,
				EventType:   fmt.Sprintf("tension.%s", strings.ToLower(parent.LifecycleState)),
				EntityType:  "tension",
				EntityID:    child.TensionID,
				PayloadJSON: fmt.Sprintf(`{"tension_id":"%s","cascaded_from":"%s"}`, child.TensionID, parent.TensionID),
				CreatedAt:   now,
			}); err != nil {
				return err
			}
		} else if isFailure {
			child.LifecycleState = tensionLifecycleActive
			child.UpdatedAt = now
			if err := s.upsertTensionTx(ctx, tx, child); err != nil {
				return err
			}
			if _, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
				WorkspaceID: workspaceID,
				EventType:   "tension.active",
				EntityType:  "tension",
				EntityID:    child.TensionID,
				PayloadJSON: fmt.Sprintf(`{"tension_id":"%s","recovered_from":"%s"}`, child.TensionID, parent.TensionID),
				CreatedAt:   now,
			}); err != nil {
				return err
			}

			delQuery := `DELETE FROM workspace_tension_dependencies WHERE workspace_id = ? AND tension_id = ? AND depends_on_tension_id = ? AND dependency_type = 'SUBSUMED_BY'`
			if _, err := tx.ExecContext(ctx, delQuery, workspaceID, childID, parent.TensionID); err != nil {
				return err
			}
		}
		// Deep cascade recovery for grandchildren
		if err := s.executeTensionCascadeRecoveryTx(ctx, tx, authority, workspaceID, child, now, actorID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) loadTensionRecord(ctx context.Context, tx *sql.Tx, workspaceID, tensionID string) (TensionRecord, error) {
	const query = `SELECT tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status,
		title, summary, anchor_kind, anchor_ref, task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json,
		segment_refs_json, agent_ids_json, constraint_refs_json, base_score, surface_score, evidence_count,
		last_seen_event_id, last_seen_at, last_detected_at, last_refreshed_at, stale_refresh_count,
		confirmed_by, archived_by, dismissed_reason, created_at, updated_at
	  FROM workspace_tensions
	  WHERE workspace_id = ? AND tension_id = ?`
	var row *sql.Row
	if tx != nil {
		row = tx.QueryRowContext(ctx, query, workspaceID, tensionID)
	} else {
		row = s.db.QueryRowContext(ctx, query, workspaceID, tensionID)
	}
	record, err := scanTensionRecord(row)
	if err != nil {
		if strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
			return TensionRecord{}, fmt.Errorf("tension not found: %s/%s", workspaceID, tensionID)
		}
		if errors.Is(err, sql.ErrNoRows) {
			return TensionRecord{}, fmt.Errorf("tension not found: %s/%s", workspaceID, tensionID)
		}
		return TensionRecord{}, err
	}
	return record, nil
}

func (s *Store) listTensionEvidence(ctx context.Context, workspaceID, tensionID string) ([]TensionEvidenceRecord, error) {
	return s.listTensionEvidenceTx(ctx, nil, workspaceID, tensionID)
}

func (s *Store) listTensionEvidenceTx(ctx context.Context, tx *sql.Tx, workspaceID, tensionID string) ([]TensionEvidenceRecord, error) {
	const query = `SELECT tension_id, workspace_id, evidence_kind, evidence_ref, event_id, weight, summary, created_at
		   FROM workspace_tension_evidence
		  WHERE workspace_id = ? AND tension_id = ?
		  ORDER BY created_at DESC, evidence_kind, evidence_ref, event_id`
	var (
		rows *sql.Rows
		err  error
	)
	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, workspaceID, tensionID)
	} else {
		rows, err = s.db.QueryContext(ctx, query, workspaceID, tensionID)
	}
	if err != nil {
		return nil, fmt.Errorf("query tension evidence: %w", err)
	}
	defer rows.Close()

	out := []TensionEvidenceRecord{}
	for rows.Next() {
		var record TensionEvidenceRecord
		if err := rows.Scan(&record.TensionID, &record.WorkspaceID, &record.EvidenceKind, &record.EvidenceRef, &record.EventID, &record.Weight, &record.Summary, &record.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan tension evidence: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tension evidence: %w", err)
	}
	return out, nil
}

func (s *Store) loadTensionEvents(ctx context.Context, workspaceID string, evidence []TensionEvidenceRecord) ([]RuntimeEventRecord, error) {
	eventIDs := []string{}
	for _, item := range evidence {
		if item.EvidenceKind != "runtime_event" || strings.TrimSpace(item.EventID) == "" {
			continue
		}
		eventIDs = append(eventIDs, strings.TrimSpace(item.EventID))
	}
	return s.listRuntimeEventsByIDs(ctx, workspaceID, uniqueSortedStrings(eventIDs))
}

func (s *Store) loadTensionClaims(ctx context.Context, workspaceID string, record TensionRecord, evidence []TensionEvidenceRecord) ([]KnowledgeClaimRecord, error) {
	claimIDs := []string{}
	for _, ref := range record.ConstraintRefs {
		if id, ok := cutPrefixedRef(ref, "claim:"); ok {
			claimIDs = append(claimIDs, id)
		}
	}
	for _, item := range evidence {
		if item.EvidenceKind == "claim" {
			claimIDs = append(claimIDs, item.EvidenceRef)
		}
	}
	return s.listKnowledgeClaimsByIDs(ctx, workspaceID, uniqueSortedStrings(claimIDs))
}

func (s *Store) loadTensionQueues(ctx context.Context, workspaceID string, record TensionRecord, evidence []TensionEvidenceRecord) ([]OperatorQueueRecord, error) {
	queueKeys := []string{}
	for _, ref := range record.ConstraintRefs {
		if id, ok := cutPrefixedRef(ref, "queue:"); ok {
			queueKeys = append(queueKeys, id)
		}
	}
	for _, item := range evidence {
		if item.EvidenceKind == "queue" {
			queueKeys = append(queueKeys, item.EvidenceRef)
		}
	}
	return s.listOperatorQueuesByKeys(ctx, workspaceID, uniqueSortedStrings(queueKeys))
}

func (s *Store) loadTensionDocs(ctx context.Context, workspaceID string, docKeys []string) ([]WorkspaceDocSummary, error) {
	if len(docKeys) == 0 {
		return nil, nil
	}
	items, err := s.ListWorkspaceDocs(ctx, workspaceID, true)
	if err != nil {
		return nil, err
	}
	allowed := stringSet(docKeys)
	out := []WorkspaceDocSummary{}
	for _, item := range items {
		if _, ok := allowed[item.DocKey]; ok {
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DocKey < out[j].DocKey })
	return out, nil
}

func (s *Store) loadTensionArtifacts(ctx context.Context, workspaceID string, refs []string) ([]WorkspaceArtifactRecord, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	return s.listWorkspaceArtifactsByRef(ctx, workspaceID, refs)
}

func (s *Store) loadProtoClusterForTension(ctx context.Context, record TensionRecord) (*ProtoClusterReport, error) {
	if strings.TrimSpace(record.ProtoClusterID) == "" {
		return nil, nil
	}
	clusters, err := s.ListProtoClusters(ctx, InstrumentationReportFilter{
		WorkspaceID:  record.WorkspaceID,
		Limit:        1000,
		ClusterLimit: 1000,
	})
	if err != nil {
		return nil, err
	}
	for idx := range clusters {
		if strings.TrimSpace(clusters[idx].ProtoClusterID) == strings.TrimSpace(record.ProtoClusterID) {
			cluster := clusters[idx]
			return &cluster, nil
		}
	}
	return nil, nil
}

func (s *Store) RefreshTensions(ctx context.Context, input TensionRefreshInput) (TensionRefreshResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.ActorID = strings.TrimSpace(input.ActorID)
	input.ProtoClusterID = strings.TrimSpace(input.ProtoClusterID)
	input.PromptContextSurface = strings.TrimSpace(input.PromptContextSurface)
	input.PromptContextPrincipalType = strings.TrimSpace(input.PromptContextPrincipalType)
	input.PromptContextPrincipalID = strings.TrimSpace(input.PromptContextPrincipalID)
	if input.WorkspaceID == "" {
		return TensionRefreshResult{}, errors.New("workspace_id is required")
	}
	if input.ActorID == "" {
		input.ActorID = "tension.refresh"
	}
	if err := validateTensionRefreshPromptContextSurface(input.PromptContextEnvelope, input.PromptContextSurface); err != nil {
		return TensionRefreshResult{}, err
	}
	promptContext := tensionRuntimePromptContext{
		Envelope:              input.PromptContextEnvelope,
		Surface:               tensionPromptContextSurface(input.PromptContextSurface, "workspace.tension.refresh"),
		PrincipalType:         input.PromptContextPrincipalType,
		PrincipalID:           input.PromptContextPrincipalID,
		RefreshProtoClusterID: input.ProtoClusterID,
	}
	input.Limit = tensionRefreshRuntimeEventWindow
	input.ClusterLimit = tensionRefreshClusterWindow
	now := time.Now().UTC().Format(time.RFC3339Nano)

	report, err := s.BuildInstrumentationReport(ctx, InstrumentationReportFilter{
		WorkspaceID:  input.WorkspaceID,
		Limit:        input.Limit,
		ClusterLimit: input.ClusterLimit,
	})
	if err != nil {
		return TensionRefreshResult{}, err
	}
	recentEvents, err := s.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID:      input.WorkspaceID,
		ExcludeSynthetic: true,
		Limit:            input.Limit,
	})
	if err != nil {
		return TensionRefreshResult{}, fmt.Errorf("list tension refresh runtime events: %w", err)
	}
	openQueues, err := s.ListOperatorQueueItems(ctx, OperatorQueueFilter{
		WorkspaceID: input.WorkspaceID,
		Status:      "OPEN",
		Limit:       tensionRefreshQueueWindow,
	})
	if err != nil {
		return TensionRefreshResult{}, err
	}
	claims, err := s.ListKnowledgeClaims(ctx, KnowledgeClaimFilter{
		WorkspaceID:     input.WorkspaceID,
		IncludeArchived: false,
		Limit:           tensionRefreshClaimWindow,
	})
	if err != nil {
		return TensionRefreshResult{}, err
	}
	contexts, err := s.buildTensionClusterContexts(ctx, input.WorkspaceID, input.ProtoClusterID, report, recentEvents, openQueues, claims)
	if err != nil {
		return TensionRefreshResult{}, err
	}
	candidates, err := s.detectTensionCandidates(ctx, input.WorkspaceID, contexts)
	if err != nil {
		return TensionRefreshResult{}, err
	}
	existing, err := s.listAllTensions(ctx, TensionFilter{
		WorkspaceID:    input.WorkspaceID,
		ProtoClusterID: input.ProtoClusterID,
	})
	if err != nil {
		return TensionRefreshResult{}, err
	}
	existingByID := make(map[string]TensionRecord, len(existing))
	for _, item := range existing {
		existingByID[item.TensionID] = item
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TensionRefreshResult{}, fmt.Errorf("begin tension refresh tx: %w", err)
	}
	result := TensionRefreshResult{
		WorkspaceID:       input.WorkspaceID,
		ProtoClusterID:    input.ProtoClusterID,
		EvaluatedClusters: len(contexts),
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInputTx(ctx, tx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		_ = tx.Rollback()
		return TensionRefreshResult{}, err
	}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		seenTensionIDs := make(map[string]struct{}, len(candidates))
		for _, candidate := range candidates {
			record := candidate.record
			record.UpdatedAt = now
			record.LastSeenAt = candidate.lastSeenAt
			record.LastSeenEventID = candidate.lastEventID
			record.LastRefreshedAt = now
			existingRecord, hasExisting := existingByID[record.TensionID]
			if hasExisting {
				record.CreatedAt = existingRecord.CreatedAt
				record.ConfirmedBy = existingRecord.ConfirmedBy
				record.ArchivedBy = existingRecord.ArchivedBy
				record.DiscardedReason = existingRecord.DiscardedReason
				record.LifecycleState = existingRecord.LifecycleState
				record.ReviewStatus = existingRecord.ReviewStatus
			} else {
				record.CreatedAt = now
				record.LifecycleState = tensionLifecycleActive
				record.ReviewStatus = tensionReviewPending
			}
			hasNewEvidence := !hasExisting || tensionHasNewEvidence(existingRecord, record)
			if hasExisting && !hasNewEvidence {
				record.LastSeenAt = existingRecord.LastSeenAt
				record.LastSeenEventID = existingRecord.LastSeenEventID
				record.LastDetectedAt = existingRecord.LastDetectedAt
				record.StaleRefreshCount = existingRecord.StaleRefreshCount + 1
			} else {
				record.LastDetectedAt = now
				record.StaleRefreshCount = 0
			}
			record.SurfaceScore = tensionSurfaceScore(record.BaseScore, record.LastSeenAt, candidate.openQueue, hasNewEvidence)
			seenTensionIDs[record.TensionID] = struct{}{}

			if hasExisting && existingRecord.ReviewStatus == tensionReviewDiscarded {
				result.SkippedDismissed++
				record.UpdatedAt = existingRecord.UpdatedAt
				if err := s.upsertTensionTx(ctx, tx, record); err != nil {
					return err
				}
				existingByID[record.TensionID] = record
				continue
			}
			if hasExisting && !hasNewEvidence {
				if existingRecord.LifecycleState != tensionLifecycleArchived && record.StaleRefreshCount >= tensionRetireRefreshThreshold {
					record.LifecycleState = tensionLifecycleArchived
					record.ArchivedBy = "system:auto-retire"
					record.UpdatedAt = now
					if err := s.upsertTensionTx(ctx, tx, record); err != nil {
						return err
					}
					evidenceRefs := candidate.evidenceRefs
					if len(evidenceRefs) == 0 {
						evidence, err := s.listTensionEvidenceTx(ctx, tx, input.WorkspaceID, record.TensionID)
						if err != nil {
							return err
						}
						evidenceRefs = evidenceRefsFromEvidence(evidence)
					}
					event, err := s.appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx, tx, authority, record, evidenceRefs, "tension.archived", "system", input.ActorID, "stale_after_refresh", promptContext)
					if err != nil {
						return err
					}
					result.RetiredCount++
					result.Events = append(result.Events, event)
				} else {
					record.UpdatedAt = existingRecord.UpdatedAt
					if err := s.upsertTensionTx(ctx, tx, record); err != nil {
						return err
					}
				}
				existingByID[record.TensionID] = record
				continue
			}

			eventType := ""
			needsPersist := !hasExisting || tensionRecordChanged(existingRecord, record)
			needsMetadataPersist := hasExisting && tensionRecordRefreshMetadataChanged(existingRecord, record)
			switch {
			case !hasExisting:
				eventType = "tension.detected"
				result.CreatedCount++
			case existingRecord.LifecycleState == tensionLifecycleArchived && hasNewEvidence:
				record.LifecycleState = tensionLifecycleActive
				record.ReviewStatus = tensionReviewPending
				record.ArchivedBy = ""
				record.DiscardedReason = ""
				eventType = "tension.recovered"
				result.RecoveredCount++
			case needsPersist && (hasNewEvidence || tensionRecordStructureChanged(existingRecord, record)):
				if existingRecord.ReviewStatus == tensionReviewConfirmed {
					record.ReviewStatus = tensionReviewConfirmed
				}
				eventType = "tension.updated"
				result.UpdatedCount++
			case !needsPersist:
				if needsMetadataPersist {
					record.UpdatedAt = existingRecord.UpdatedAt
					if err := s.upsertTensionTx(ctx, tx, record); err != nil {
						return err
					}
					existingByID[record.TensionID] = record
				}
				continue
			}

			if err := s.upsertTensionTx(ctx, tx, record); err != nil {
				return err
			}
			if err := s.upsertTensionEvidenceTx(ctx, tx, input.WorkspaceID, record.TensionID, candidate.evidence); err != nil {
				return err
			}
			if eventType != "" {
				event, err := s.appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx, tx, authority, record, candidate.evidenceRefs, eventType, "system", input.ActorID, "", promptContext)
				if err != nil {
					return err
				}
				result.Events = append(result.Events, event)
			}
			existingByID[record.TensionID] = record
		}
		for _, existingRecord := range existing {
			if _, ok := seenTensionIDs[existingRecord.TensionID]; ok {
				continue
			}
			record := existingRecord
			record.LastRefreshedAt = now
			record.StaleRefreshCount = existingRecord.StaleRefreshCount + 1
			if record.ReviewStatus == tensionReviewDiscarded {
				record.StaleRefreshCount = 0
				record.UpdatedAt = existingRecord.UpdatedAt
				if err := s.upsertTensionTx(ctx, tx, record); err != nil {
					return err
				}
				existingByID[record.TensionID] = record
				continue
			}
			if record.LifecycleState != tensionLifecycleArchived && record.StaleRefreshCount >= tensionRetireRefreshThreshold {
				record.LifecycleState = tensionLifecycleArchived
				record.ArchivedBy = "system:auto-retire"
				record.UpdatedAt = now
				if err := s.upsertTensionTx(ctx, tx, record); err != nil {
					return err
				}
				evidence, err := s.listTensionEvidenceTx(ctx, tx, input.WorkspaceID, record.TensionID)
				if err != nil {
					return err
				}
				event, err := s.appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx, tx, authority, record, evidenceRefsFromEvidence(evidence), "tension.archived", "system", input.ActorID, "stale_after_refresh", promptContext)
				if err != nil {
					return err
				}
				result.RetiredCount++
				result.Events = append(result.Events, event)
				existingByID[record.TensionID] = record
				continue
			}
			record.UpdatedAt = existingRecord.UpdatedAt
			if err := s.upsertTensionTx(ctx, tx, record); err != nil {
				return err
			}
			existingByID[record.TensionID] = record
		}
		event, err := s.appendTensionRefreshSummaryEventTx(ctx, tx, authority, input, result, promptContext, now)
		if err != nil {
			return err
		}
		result.Events = append(result.Events, event)
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return TensionRefreshResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return TensionRefreshResult{}, fmt.Errorf("commit tension refresh tx: %w", err)
	}
	reportOut, err := s.BuildTensionReport(ctx, TensionFilter{
		WorkspaceID:    input.WorkspaceID,
		ProtoClusterID: input.ProtoClusterID,
		Limit:          20,
	})
	if err != nil {
		return TensionRefreshResult{}, err
	}
	result.TimeAuthority = reportOut.TimeAuthority
	result.RefreshedAt = generatedAtFromWorkspaceTimeAuthority(result.TimeAuthority)
	result.Report = reportOut
	return result, nil
}

func (s *Store) buildTensionClusterContexts(ctx context.Context, workspaceID, targetClusterID string, report InstrumentationReport, recentEvents []RuntimeEventRecord, openQueues []OperatorQueueRecord, claims []KnowledgeClaimRecord) (map[string]*tensionClusterContext, error) {
	contexts := map[string]*tensionClusterContext{}
	reportMap := map[string]ProtoClusterReport{}
	for _, cluster := range report.Clusters {
		if targetClusterID != "" && cluster.ProtoClusterID != targetClusterID {
			continue
		}
		reportMap[cluster.ProtoClusterID] = cluster
		contexts[cluster.ProtoClusterID] = &tensionClusterContext{cluster: cluster}
	}
	taskHydrationCache := map[string]TaskHydrationBundle{}
	updateLinkCache := map[string]instrumentationAgentUpdateLinks{}

	chronological := append([]RuntimeEventRecord(nil), recentEvents...)
	for left, right := 0, len(chronological)-1; left < right; left, right = left+1, right-1 {
		chronological[left], chronological[right] = chronological[right], chronological[left]
	}
	for _, event := range chronological {
		if isSyntheticTensionEvent(event) {
			continue
		}
		resolution, err := s.resolveProtoClusterForEvent(ctx, workspaceID, event, taskHydrationCache, updateLinkCache)
		if err != nil {
			return nil, err
		}
		if targetClusterID != "" && resolution.protoClusterID != targetClusterID {
			continue
		}
		cluster := ensureTensionClusterContext(contexts, reportMap, resolution)
		cluster.recentEvents = append(cluster.recentEvents, event)
		if len(cluster.recentEvents) > 200 {
			cluster.recentEvents = append([]RuntimeEventRecord(nil), cluster.recentEvents[len(cluster.recentEvents)-200:]...)
		}
	}
	for _, queue := range openQueues {
		resolution := resolveProtoClusterFromQueue(workspaceID, RuntimeReplayQueue{
			QueueID:    queue.QueueID,
			QueueKey:   queue.QueueKey,
			QueueType:  queue.QueueType,
			Status:     queue.Status,
			SourceKind: queue.SourceKind,
			SourceID:   queue.SourceID,
			AgentID:    queue.AgentID,
			SessionID:  queue.SessionID,
			TaskID:     queue.TaskID,
			UpdatedAt:  queue.UpdatedAt,
			ResolvedAt: queue.ResolvedAt,
			AssignedTo: queue.AssignedTo,
			Resolution: queue.Resolution,
			Urgency:    queue.Urgency,
			Title:      queue.Title,
			Summary:    queue.Summary,
			CreatedAt:  queue.CreatedAt,
			DueAt:      queue.DueAt,
		})
		if targetClusterID != "" && resolution.protoClusterID != targetClusterID {
			continue
		}
		cluster := ensureTensionClusterContext(contexts, reportMap, resolution)
		cluster.openQueues = append(cluster.openQueues, queue)
	}
	for _, claim := range claims {
		resolution, err := s.resolveProtoClusterFromClaim(ctx, workspaceID, claim, taskHydrationCache)
		if err != nil {
			return nil, err
		}
		if targetClusterID != "" && resolution.protoClusterID != targetClusterID {
			continue
		}
		cluster := ensureTensionClusterContext(contexts, reportMap, resolution)
		cluster.claims = append(cluster.claims, claim)
	}
	return contexts, nil
}

func ensureTensionClusterContext(contexts map[string]*tensionClusterContext, reportMap map[string]ProtoClusterReport, resolution instrumentationResolution) *tensionClusterContext {
	if existing, ok := contexts[resolution.protoClusterID]; ok {
		return existing
	}
	cluster, ok := reportMap[resolution.protoClusterID]
	if !ok {
		cluster = ProtoClusterReport{
			ProtoClusterID: resolution.protoClusterID,
			ResolutionKind: resolution.resolutionKind,
			TaskIDs:        append([]string{}, resolution.taskIDs...),
			SessionIDs:     append([]string{}, resolution.sessionIDs...),
			DocKeys:        append([]string{}, resolution.docKeys...),
			ArtifactRefs:   append([]string{}, resolution.artifactRefs...),
			AgentIDs:       append([]string{}, resolution.agentIDs...),
		}
	}
	context := &tensionClusterContext{cluster: cluster}
	contexts[resolution.protoClusterID] = context
	return context
}

func (s *Store) resolveProtoClusterFromClaim(ctx context.Context, workspaceID string, claim KnowledgeClaimRecord, taskHydrationCache map[string]TaskHydrationBundle) (instrumentationResolution, error) {
	resolution := instrumentationResolution{
		taskIDs:    collectNonEmpty(strings.TrimSpace(claim.TaskID)),
		sessionIDs: collectNonEmpty(strings.TrimSpace(claim.SessionID)),
		agentIDs:   collectNonEmpty(strings.TrimSpace(claim.AgentID)),
	}
	if len(resolution.taskIDs) > 0 {
		bundle, err := s.instrumentationTaskHydration(ctx, workspaceID, resolution.taskIDs[0], taskHydrationCache)
		if err != nil {
			if errors.Is(err, ErrTaskNotFound) || errors.Is(err, ErrWorkspaceTaskAbsent) {
				resolution.taskIDs = nil
			} else {
				return instrumentationResolution{}, err
			}
		}
		if len(resolution.taskIDs) > 0 {
			for _, doc := range bundle.Docs {
				resolution.docKeys = append(resolution.docKeys, strings.TrimSpace(doc.DocKey))
			}
			for _, artifact := range bundle.Artifacts {
				resolution.artifactRefs = append(resolution.artifactRefs, strings.TrimSpace(artifact.ArtifactRef))
			}
		}
	}
	resolution.taskIDs = uniqueSortedStrings(resolution.taskIDs)
	resolution.sessionIDs = uniqueSortedStrings(resolution.sessionIDs)
	resolution.docKeys = uniqueSortedStrings(resolution.docKeys)
	resolution.artifactRefs = uniqueSortedStrings(resolution.artifactRefs)
	resolution.agentIDs = uniqueSortedStrings(resolution.agentIDs)
	switch {
	case len(resolution.taskIDs) > 0:
		resolution.protoClusterID = "task:" + workspaceID + "/" + resolution.taskIDs[0]
		resolution.resolutionKind = "task"
	case len(resolution.sessionIDs) > 0:
		resolution.protoClusterID = "session:" + workspaceID + "/" + resolution.sessionIDs[0]
		resolution.resolutionKind = "session"
	case len(resolution.docKeys) > 0:
		resolution.protoClusterID = "workspace_doc:" + workspaceID + "/" + resolution.docKeys[0]
		resolution.resolutionKind = "doc"
	case len(resolution.artifactRefs) > 0:
		resolution.protoClusterID = "artifact:" + workspaceID + "/" + resolution.artifactRefs[0]
		resolution.resolutionKind = "artifact"
	case strings.TrimSpace(claim.SourceID) != "":
		resolution.protoClusterID = "source:" + workspaceID + "/" + strings.TrimSpace(claim.SourceKind) + "/" + strings.TrimSpace(claim.SourceID)
		resolution.resolutionKind = "source"
	default:
		resolution.protoClusterID = "claim:" + workspaceID + "/" + strings.TrimSpace(claim.ClaimID)
		resolution.resolutionKind = "claim"
	}
	return resolution, nil
}

func (s *Store) detectTensionCandidates(ctx context.Context, workspaceID string, contexts map[string]*tensionClusterContext) ([]tensionCandidate, error) {
	clusterIDs := make([]string, 0, len(contexts))
	for clusterID := range contexts {
		clusterIDs = append(clusterIDs, clusterID)
	}
	sort.Strings(clusterIDs)
	candidates := []tensionCandidate{}
	for _, clusterID := range clusterIDs {
		cluster := contexts[clusterID]
		candidates = append(candidates, detectContradictionTensions(workspaceID, cluster)...)
		candidates = append(candidates, detectBottleneckTension(workspaceID, cluster)...)
		candidates = append(candidates, detectAmbiguityTension(workspaceID, cluster)...)
		candidates = append(candidates, s.detectAnomalyAlertTension(ctx, workspaceID, cluster)...)
		gapCandidate, err := s.detectGapTension(ctx, workspaceID, cluster)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, gapCandidate...)
		candidates = append(candidates, detectBridgeTension(workspaceID, cluster)...)
	}
	for idx := range candidates {
		enriched, err := s.enrichTensionCandidate(ctx, candidates[idx])
		if err != nil {
			return nil, err
		}
		candidates[idx] = enriched
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].record.TensionID < candidates[j].record.TensionID
	})
	return candidates, nil
}

func detectContradictionTensions(workspaceID string, cluster *tensionClusterContext) []tensionCandidate {
	type contradictionGroup struct {
		subject   string
		claimIDs  []string
		evidence  []tensionEvidenceInput
		queueKeys []string
	}
	claimsByID := map[string]KnowledgeClaimRecord{}
	for _, claim := range cluster.claims {
		claimsByID[claim.ClaimID] = claim
	}
	groups := map[string]*contradictionGroup{}
	for _, event := range cluster.recentEvents {
		if !isContradictionEventType(event.EventType) {
			continue
		}
		payload := instrumentationDecodePayloadMap(event.PayloadJSON)
		claimID := firstNonEmpty(strings.TrimSpace(event.EntityID), instrumentationPayloadString(payload, "claim_id"))
		subject := instrumentationPayloadString(payload, "subject")
		if claim, ok := claimsByID[claimID]; ok {
			subject = firstNonEmpty(strings.TrimSpace(claim.Subject), subject)
		}
		subject = strings.TrimSpace(subject)
		if subject == "" {
			continue
		}
		key := strings.ToLower(subject)
		group := groups[key]
		if group == nil {
			group = &contradictionGroup{subject: subject}
			groups[key] = group
		}
		if claimID != "" {
			group.claimIDs = append(group.claimIDs, claimID)
		}
		group.evidence = append(group.evidence, tensionEvidenceInput{
			kind:      "runtime_event",
			ref:       event.EventID,
			eventID:   event.EventID,
			weight:    2,
			summary:   clipSummary(event.EventType+" "+subject, 160),
			createdAt: event.CreatedAt,
		})
	}
	for _, queue := range cluster.openQueues {
		if !strings.EqualFold(strings.TrimSpace(queue.SourceKind), "knowledge_claim") {
			continue
		}
		claim, ok := claimsByID[strings.TrimSpace(queue.SourceID)]
		if !ok || strings.TrimSpace(claim.Subject) == "" {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(claim.Subject))
		group := groups[key]
		if group == nil {
			continue
		}
		group.queueKeys = append(group.queueKeys, queue.QueueKey)
		group.evidence = append(group.evidence, tensionEvidenceInput{
			kind:      "queue",
			ref:       queue.QueueKey,
			weight:    1,
			summary:   clipSummary(firstNonEmpty(queue.Summary, queue.Title, queue.QueueKey), 160),
			createdAt: queue.UpdatedAt,
		})
	}

	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := []tensionCandidate{}
	for _, key := range keys {
		group := groups[key]
		claimIDs := uniqueSortedStrings(group.claimIDs)
		if len(group.evidence) < 2 && len(claimIDs) < 2 {
			continue
		}
		constraintRefs := []string{}
		for _, claimID := range claimIDs {
			constraintRefs = append(constraintRefs, "claim:"+claimID)
		}
		for _, queueKey := range uniqueSortedStrings(group.queueKeys) {
			constraintRefs = append(constraintRefs, "queue:"+queueKey)
		}
		title := "Contradiction: " + clipSummary(group.subject, 96)
		summary := "Conflicting claim lifecycle or review escalation around " + clipSummary(group.subject, 160)
		out = append(out, buildTensionCandidate(workspaceID, cluster, "contradiction", title, summary, "claim_subject", group.subject, group.subject, constraintRefs, group.evidence, false))
	}
	return out
}

func (s *Store) detectAnomalyAlertTension(ctx context.Context, workspaceID string, cluster *tensionClusterContext) []tensionCandidate {
	flags := s.GetRSPCapabilityFlags(ctx, workspaceID)
	if !flags.GovernedHintsLive && !flags.StrongConsequencesLive {
		return nil
	}

	out := []tensionCandidate{}
	for _, event := range cluster.recentEvents {
		if event.EventType != "ANOMALY_ALERT" {
			continue
		}

		payload := instrumentationDecodePayloadMap(event.PayloadJSON)
		actuationClass := instrumentationPayloadString(payload, "actuation_class")
		if actuationClass != "governed_hint" && actuationClass != "strong_consequence" {
			continue
		}
		alertType := instrumentationPayloadString(payload, "alert_type")
		reason := instrumentationPayloadString(payload, "reason")

		tensionType := "failure"
		if alertType == "MOTIF_THRASH" || alertType == "MOTIF_BOUNCE" {
			tensionType = "dissent_followup"
		}

		constraintRefs := []string{}
		refPrefix := strings.ToLower(event.EntityType)
		if refPrefix != "" && event.EntityID != "" {
			constraintRefs = append(constraintRefs, refPrefix+":"+event.EntityID)
		}

		evidence := []tensionEvidenceInput{{
			kind:      "runtime_event",
			ref:       event.EventID,
			eventID:   event.EventID,
			weight:    10,
			summary:   clipSummary("RSP Alert: "+reason, 160),
			createdAt: event.CreatedAt,
		}}

		title := "RSP Anomaly: " + alertType
		summary := reason
		keyVal := alertType + "::" + event.EntityID

		out = append(out, buildTensionCandidate(workspaceID, cluster, tensionType, title, summary, "anomaly_alert", keyVal, keyVal, constraintRefs, evidence, false))
	}
	return out
}

func detectBottleneckTension(workspaceID string, cluster *tensionClusterContext) []tensionCandidate {
	blockedEvents := []tensionEvidenceInput{}
	for _, event := range cluster.recentEvents {
		switch strings.TrimSpace(event.EventType) {
		case "session.blocked", "task.blocked":
			blockedEvents = append(blockedEvents, tensionEvidenceInput{
				kind:      "runtime_event",
				ref:       event.EventID,
				eventID:   event.EventID,
				weight:    2,
				summary:   clipSummary(event.EventType, 160),
				createdAt: event.CreatedAt,
			})
		}
	}
	if len(blockedEvents) == 0 && len(cluster.openQueues) == 0 {
		return nil
	}
	queue := selectPrimaryQueue(cluster.openQueues, nil)
	persistentQueue := queue != nil && tensionQueueLooksPersistent(*queue)
	if len(blockedEvents) < 2 && !(queue != nil && len(blockedEvents) >= 1) && !persistentQueue {
		return nil
	}
	evidence := append([]tensionEvidenceInput{}, blockedEvents...)
	constraintRefs := []string{}
	anchorKind := "proto_cluster"
	anchorRef := cluster.cluster.ProtoClusterID
	fingerprint := cluster.cluster.ProtoClusterID
	title := "Bottleneck: " + clipSummary(cluster.cluster.ProtoClusterID, 96)
	summary := "Persistent blocker or open operator queue within " + clipSummary(cluster.cluster.ProtoClusterID, 160)
	if queue != nil {
		evidence = append(evidence, tensionEvidenceInput{
			kind:      "queue",
			ref:       queue.QueueKey,
			weight:    2,
			summary:   clipSummary(firstNonEmpty(queue.Summary, queue.Title, queue.QueueKey), 160),
			createdAt: queue.UpdatedAt,
		})
		constraintRefs = append(constraintRefs, "queue:"+queue.QueueKey)
		anchorKind = "queue_key"
		anchorRef = queue.QueueKey
		fingerprint = queue.QueueKey
		title = "Bottleneck: " + clipSummary(firstNonEmpty(queue.Title, queue.QueueKey), 96)
		summary = firstNonEmpty(queue.Summary, summary)
	}
	return []tensionCandidate{buildTensionCandidate(workspaceID, cluster, "bottleneck", title, summary, anchorKind, anchorRef, fingerprint, constraintRefs, evidence, len(cluster.openQueues) > 0)}
}

func detectAmbiguityTension(workspaceID string, cluster *tensionClusterContext) []tensionCandidate {
	evidence := []tensionEvidenceInput{}
	decisionSignals := 0
	handoffSignals := 0
	for _, event := range cluster.recentEvents {
		switch strings.TrimSpace(event.EventType) {
		case "session.decision_needed":
			decisionSignals++
			evidence = append(evidence, tensionEvidenceInput{
				kind:      "runtime_event",
				ref:       event.EventID,
				eventID:   event.EventID,
				weight:    2,
				summary:   "session decision needed",
				createdAt: event.CreatedAt,
			})
		case "session.takeover":
			handoffSignals++
			evidence = append(evidence, tensionEvidenceInput{
				kind:      "runtime_event",
				ref:       event.EventID,
				eventID:   event.EventID,
				weight:    1,
				summary:   "session takeover churn",
				createdAt: event.CreatedAt,
			})
		}
	}
	var queue *OperatorQueueRecord
	for idx := range cluster.openQueues {
		queueType := normalizeOperatorQueueType(cluster.openQueues[idx].QueueType)
		if queueType == "DECISION" || queueType == "HANDOFF" {
			queue = &cluster.openQueues[idx]
			if queueType == "DECISION" {
				decisionSignals++
			} else {
				handoffSignals++
			}
			evidence = append(evidence, tensionEvidenceInput{
				kind:      "queue",
				ref:       queue.QueueKey,
				weight:    1,
				summary:   clipSummary(firstNonEmpty(queue.Summary, queue.Title, queue.QueueKey), 160),
				createdAt: queue.UpdatedAt,
			})
		}
	}
	if len(evidence) == 0 {
		return nil
	}
	confirmed := 0
	for _, claim := range cluster.claims {
		if normalizeKnowledgeClaimStatus(claim.Status) == "CONFIRMED" {
			confirmed++
		}
	}
	if decisionSignals+handoffSignals < 2 {
		return nil
	}
	if confirmed > 0 && decisionSignals+handoffSignals < 3 {
		return nil
	}
	anchorKind := "proto_cluster"
	anchorRef := cluster.cluster.ProtoClusterID
	fingerprint := cluster.cluster.ProtoClusterID
	constraintRefs := []string{}
	title := "Ambiguity: " + clipSummary(cluster.cluster.ProtoClusterID, 96)
	summary := "Repeated handoff or decision-needed churn without claim convergence"
	if queue != nil {
		anchorKind = "queue_key"
		anchorRef = queue.QueueKey
		fingerprint = queue.QueueKey
		constraintRefs = append(constraintRefs, "queue:"+queue.QueueKey)
		title = "Ambiguity: " + clipSummary(firstNonEmpty(queue.Title, queue.QueueKey), 96)
		summary = firstNonEmpty(queue.Summary, summary)
	}
	return []tensionCandidate{buildTensionCandidate(workspaceID, cluster, "ambiguity", title, summary, anchorKind, anchorRef, fingerprint, constraintRefs, evidence, queue != nil)}
}

func (s *Store) detectGapTension(ctx context.Context, workspaceID string, cluster *tensionClusterContext) ([]tensionCandidate, error) {
	evidence := []tensionEvidenceInput{}
	knowledgeSurfaceMutations := 0
	for _, event := range cluster.recentEvents {
		switch strings.TrimSpace(event.EventType) {
		case "workspace_doc.upserted", "workspace_memory.recorded", "workspace_memory.restored", "workspace_artifact.created":
			knowledgeSurfaceMutations++
			evidence = append(evidence, tensionEvidenceInput{
				kind:      "runtime_event",
				ref:       event.EventID,
				eventID:   event.EventID,
				weight:    1,
				summary:   clipSummary(event.EventType, 160),
				createdAt: event.CreatedAt,
			})
		case "agent_update.posted":
			evidence = append(evidence, tensionEvidenceInput{
				kind:      "runtime_event",
				ref:       event.EventID,
				eventID:   event.EventID,
				weight:    1,
				summary:   clipSummary(event.EventType, 160),
				createdAt: event.CreatedAt,
			})
		}
	}
	if len(cluster.cluster.DocKeys) == 0 && len(cluster.cluster.ArtifactRefs) == 0 {
		return nil, nil
	}
	if knowledgeSurfaceMutations < 2 || len(evidence) < 3 {
		return nil, nil
	}
	confirmed := 0
	for _, claim := range cluster.claims {
		if normalizeKnowledgeClaimStatus(claim.Status) == "CONFIRMED" {
			confirmed++
		}
	}
	if confirmed > 0 {
		return nil, nil
	}
	anchorKind := "proto_cluster"
	anchorRef := cluster.cluster.ProtoClusterID
	fingerprint := cluster.cluster.ProtoClusterID
	title := "Gap: " + clipSummary(cluster.cluster.ProtoClusterID, 96)
	summary := "Knowledge-surface churn without confirmed review closure"
	if len(cluster.cluster.DocKeys) > 0 {
		anchorKind = "workspace_doc"
		anchorRef = cluster.cluster.DocKeys[0]
		fingerprint = cluster.cluster.DocKeys[0]
		title = "Gap: " + clipSummary(cluster.cluster.DocKeys[0], 96)
		summary = "Workspace doc changed repeatedly without confirmed closure"
	} else if len(cluster.cluster.ArtifactRefs) > 0 {
		anchorKind = "artifact_ref"
		anchorRef = cluster.cluster.ArtifactRefs[0]
		fingerprint = cluster.cluster.ArtifactRefs[0]
		title = "Gap: " + clipSummary(cluster.cluster.ArtifactRefs[0], 96)
		summary = "Artifact changed repeatedly without confirmed closure"
	}
	return []tensionCandidate{buildTensionCandidate(workspaceID, cluster, "gap", title, summary, anchorKind, anchorRef, fingerprint, nil, evidence, false)}, nil
}

func detectBridgeTension(workspaceID string, cluster *tensionClusterContext) []tensionCandidate {
	evidence := []tensionEvidenceInput{}
	agents := map[string]struct{}{}
	communicationCount := 0
	for _, event := range cluster.recentEvents {
		fromAgent, toAgent := instrumentationCommunicationPair(event)
		if fromAgent == "" || toAgent == "" {
			continue
		}
		communicationCount++
		agents[fromAgent] = struct{}{}
		agents[toAgent] = struct{}{}
		evidence = append(evidence, tensionEvidenceInput{
			kind:      "runtime_event",
			ref:       event.EventID,
			eventID:   event.EventID,
			weight:    1,
			summary:   clipSummary(event.EventType+": "+fromAgent+" -> "+toAgent, 160),
			createdAt: event.CreatedAt,
		})
	}
	anchorCount := len(cluster.cluster.TaskIDs) + len(cluster.cluster.DocKeys) + len(cluster.cluster.ArtifactRefs)
	anchorFamilies := 0
	if len(cluster.cluster.TaskIDs) > 0 {
		anchorFamilies++
	}
	if len(cluster.cluster.DocKeys) > 0 {
		anchorFamilies++
	}
	if len(cluster.cluster.ArtifactRefs) > 0 {
		anchorFamilies++
	}
	if communicationCount < 6 || len(agents) < 3 || anchorCount < 3 || anchorFamilies < 2 {
		return nil
	}
	title := "Bridge: " + clipSummary(cluster.cluster.ProtoClusterID, 96)
	summary := "Cross-agent messaging is bridging multiple anchors in the same locality"
	return []tensionCandidate{buildTensionCandidate(workspaceID, cluster, "bridge", title, summary, "proto_cluster", cluster.cluster.ProtoClusterID, cluster.cluster.ProtoClusterID, nil, evidence, false)}
}

func buildTensionCandidate(workspaceID string, cluster *tensionClusterContext, tensionType, title, summary, anchorKind, anchorRef, stableFingerprint string, constraintRefs []string, evidenceInputs []tensionEvidenceInput, openQueue bool) tensionCandidate {
	evidenceInputs = dedupeTensionEvidenceInputs(evidenceInputs)
	evidence := make([]TensionEvidenceRecord, 0, len(evidenceInputs))
	evidenceRefs := make([]string, 0, len(evidenceInputs))
	lastSeenAt := ""
	lastEventID := ""
	lastRuntimeEventAt := ""
	for _, item := range evidenceInputs {
		evidence = append(evidence, TensionEvidenceRecord{
			TensionID:    "",
			WorkspaceID:  workspaceID,
			EvidenceKind: item.kind,
			EvidenceRef:  item.ref,
			EventID:      item.eventID,
			Weight:       maxInt(item.weight, 1),
			Summary:      strings.TrimSpace(item.summary),
			CreatedAt:    firstNonEmpty(strings.TrimSpace(item.createdAt), time.Now().UTC().Format(time.RFC3339Nano)),
		})
		if item.kind == "runtime_event" && item.eventID != "" {
			if lastRuntimeEventAt == "" || strings.TrimSpace(item.createdAt) >= lastRuntimeEventAt {
				lastRuntimeEventAt = strings.TrimSpace(item.createdAt)
				lastEventID = item.eventID
			}
		}
		if lastSeenAt == "" || strings.TrimSpace(item.createdAt) > lastSeenAt {
			lastSeenAt = strings.TrimSpace(item.createdAt)
		}
		refValue := item.ref
		if refValue == "" {
			refValue = item.eventID
		}
		if refValue != "" {
			evidenceRefs = append(evidenceRefs, item.kind+":"+refValue)
		}
	}
	taskIDs := uniqueSortedStrings(cluster.cluster.TaskIDs)
	sessionIDs := uniqueSortedStrings(cluster.cluster.SessionIDs)
	docKeys := uniqueSortedStrings(cluster.cluster.DocKeys)
	artifactRefs := uniqueSortedStrings(cluster.cluster.ArtifactRefs)
	agentIDs := uniqueSortedStrings(cluster.cluster.AgentIDs)
	constraintRefs = uniqueSortedStrings(constraintRefs)
	baseScore := tensionBaseScore(tensionType, len(evidence), openQueue, len(taskIDs) > 1 || len(sessionIDs) > 1)
	tensionID := buildTensionID(workspaceID, tensionType, cluster.cluster.ProtoClusterID, anchorKind, anchorRef, stableFingerprint)
	record := TensionRecord{
		TensionID:       tensionID,
		WorkspaceID:     workspaceID,
		ProtoClusterID:  cluster.cluster.ProtoClusterID,
		TensionType:     tensionType,
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewPending,
		Title:           clipSummary(strings.TrimSpace(title), 160),
		Summary:         clipSummary(strings.TrimSpace(summary), 240),
		AnchorKind:      strings.TrimSpace(anchorKind),
		AnchorRef:       strings.TrimSpace(anchorRef),
		TaskIDs:         taskIDs,
		SessionIDs:      sessionIDs,
		DocKeys:         docKeys,
		ArtifactRefs:    artifactRefs,
		SegmentRefs:     tensionSegmentRootRefs(workspaceID, docKeys, artifactRefs),
		AgentIDs:        agentIDs,
		ConstraintRefs:  constraintRefs,
		BaseScore:       baseScore,
		SurfaceScore:    baseScore,
		EvidenceCount:   len(evidence),
		LastSeenEventID: lastEventID,
		LastSeenAt:      lastSeenAt,
	}
	for idx := range evidence {
		evidence[idx].TensionID = tensionID
	}
	return tensionCandidate{
		record:       record,
		evidence:     evidence,
		evidenceRefs: uniqueSortedStrings(evidenceRefs),
		openQueue:    openQueue,
		lastEventID:  lastEventID,
		lastSeenAt:   lastSeenAt,
	}
}

func (s *Store) enrichTensionCandidate(ctx context.Context, candidate tensionCandidate) (tensionCandidate, error) {
	record := candidate.record
	segmentRefs, err := s.resolveTensionSegmentRefs(ctx, candidate)
	if err != nil {
		return tensionCandidate{}, err
	}
	record.SegmentRefs = segmentRefs
	candidate.record = record
	return candidate, nil
}

func (s *Store) resolveTensionSegmentRefs(ctx context.Context, candidate tensionCandidate) ([]string, error) {
	record := candidate.record
	out := tensionSegmentRootRefs(record.WorkspaceID, record.DocKeys, record.ArtifactRefs)
	segments, err := s.collectWorkspaceSegmentsBestEffort(ctx, record.WorkspaceID, record.DocKeys, record.ArtifactRefs)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return uniqueSortedStrings(out), nil
	}
	queryTexts := []string{record.Title, record.Summary, record.AnchorRef}
	for _, item := range candidate.evidence {
		queryTexts = append(queryTexts, item.Summary, item.EvidenceRef)
	}
	grouped := map[string][]WorkspaceSegmentRecord{}
	for _, segment := range segments {
		grouped[segment.SourceKind+"|"+segment.SourceRef] = append(grouped[segment.SourceKind+"|"+segment.SourceRef], segment)
	}
	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		sourceSegments := grouped[key]
		sort.Slice(sourceSegments, func(i, j int) bool {
			if sourceSegments[i].StartLine != sourceSegments[j].StartLine {
				return sourceSegments[i].StartLine < sourceSegments[j].StartLine
			}
			return sourceSegments[i].SegmentRef < sourceSegments[j].SegmentRef
		})
		matched := bestMatchingWorkspaceSegments(sourceSegments, record, queryTexts)
		out = append(out, matched...)
	}
	return uniqueSortedStrings(out), nil
}

func (s *Store) collectWorkspaceSegmentsBestEffort(ctx context.Context, workspaceID string, docKeys, artifactRefs []string) ([]WorkspaceSegmentRecord, error) {
	out := []WorkspaceSegmentRecord{}
	for _, docKey := range uniqueSortedStrings(docKeys) {
		doc, err := s.GetWorkspaceDoc(ctx, workspaceID, docKey)
		if err != nil {
			continue
		}
		out = append(out, buildWorkspaceDocSegments(workspaceID, doc)...)
	}
	for _, artifactRef := range uniqueSortedStrings(artifactRefs) {
		artifact, err := s.loadWorkspaceArtifactByRef(ctx, workspaceID, artifactRef)
		if err != nil {
			continue
		}
		out = append(out, buildWorkspaceArtifactSegments(workspaceID, artifact)...)
	}
	return out, nil
}

func bestMatchingWorkspaceSegments(segments []WorkspaceSegmentRecord, record TensionRecord, queryTexts []string) []string {
	if len(segments) == 0 {
		return nil
	}
	rootRef := ""
	type scoredSegment struct {
		ref   string
		score int
	}
	scored := []scoredSegment{}
	for _, segment := range segments {
		if segment.IsRoot {
			rootRef = segment.SegmentRef
			continue
		}
		score, directSourceMatch, tokenHits := workspaceSegmentMatchScore(segment, record, queryTexts)
		if directSourceMatch {
			if score < 1 {
				continue
			}
		} else if score < 3 || tokenHits < 2 {
			continue
		}
		scored = append(scored, scoredSegment{ref: segment.SegmentRef, score: score})
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].ref < scored[j].ref
	})
	out := []string{}
	if rootRef != "" {
		out = append(out, rootRef)
	}
	for idx, item := range scored {
		if idx >= 2 {
			break
		}
		out = append(out, item.ref)
	}
	return out
}

func workspaceSegmentMatchScore(segment WorkspaceSegmentRecord, record TensionRecord, queryTexts []string) (int, bool, int) {
	haystack := strings.ToLower(strings.Join([]string{
		strings.TrimSpace(segment.Title),
		strings.TrimSpace(segment.Summary),
		strings.TrimSpace(segment.SourceRef),
		strings.TrimSpace(segment.SourceTitle),
	}, " "))
	if haystack == "" {
		return 0, false, 0
	}
	score := 0
	directSourceMatch := false
	if record.AnchorKind == "workspace_doc" && strings.EqualFold(strings.TrimSpace(record.AnchorRef), strings.TrimSpace(segment.SourceRef)) {
		score += 2
		directSourceMatch = true
	}
	if record.AnchorKind == "artifact_ref" && strings.EqualFold(strings.TrimSpace(record.AnchorRef), strings.TrimSpace(segment.SourceRef)) {
		score += 2
		directSourceMatch = true
	}
	tokenHits := 0
	for _, text := range queryTexts {
		for _, token := range workspaceSegmentMatchTokens(text) {
			if strings.Contains(haystack, token) {
				score++
				tokenHits++
			}
		}
	}
	return score, directSourceMatch, tokenHits
}

func workspaceSegmentMatchTokens(values ...string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		for _, token := range strings.FieldsFunc(strings.ToLower(strings.TrimSpace(value)), func(r rune) bool {
			return !(unicode.IsLetter(r) || unicode.IsDigit(r))
		}) {
			if len(token) < 4 {
				continue
			}
			if _, ok := seen[token]; ok {
				continue
			}
			seen[token] = struct{}{}
			out = append(out, token)
		}
	}
	sort.Strings(out)
	return out
}

type EnsureGovernedTensionInput struct {
	WorkspaceID    string
	TensionType    string
	ProtoClusterID string
	AnchorRef      string
	Title          string
	Summary        string
	EvidenceRefs   []string
}

func (s *Store) EnsureGovernedTension(ctx context.Context, input EnsureGovernedTensionInput) error {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.TensionType = normalizeTensionType(input.TensionType)
	input.ProtoClusterID = strings.TrimSpace(input.ProtoClusterID)
	input.AnchorRef = strings.TrimSpace(input.AnchorRef)

	if input.WorkspaceID == "" || input.TensionType == "" {
		return errors.New("workspace_id and tension_type are required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
			return err
		}

		var existingID string
		err := tx.QueryRowContext(ctx, `
			SELECT tension_id
			FROM workspace_tensions
			WHERE workspace_id = ? AND tension_type = ?
			  AND proto_cluster_id = ? AND anchor_ref = ?
			  AND lifecycle_state IN (?, ?)
			LIMIT 1
		`, input.WorkspaceID, input.TensionType, input.ProtoClusterID, input.AnchorRef, tensionLifecycleEmergent, tensionLifecycleActive).Scan(&existingID)

		if err == nil && existingID != "" {
			record, loadErr := s.loadTensionRecord(ctx, tx, input.WorkspaceID, existingID)
			if loadErr != nil {
				return loadErr
			}
			evidence := governedTensionEvidenceRecords(existingID, input.WorkspaceID, input.EvidenceRefs, now)
			if err := s.upsertTensionEvidenceTx(ctx, tx, input.WorkspaceID, existingID, evidence); err != nil {
				return err
			}
			rows, listErr := s.listTensionEvidenceTx(ctx, tx, input.WorkspaceID, existingID)
			if listErr != nil {
				return listErr
			}
			record.Title = firstNonEmpty(strings.TrimSpace(input.Title), record.Title)
			record.Summary = firstNonEmpty(strings.TrimSpace(input.Summary), record.Summary)
			record.EvidenceCount = len(rows)
			record.BaseScore = tensionBaseScore(record.TensionType, record.EvidenceCount, false, false)
			record.SurfaceScore = maxInt(record.SurfaceScore, record.BaseScore)
			record.LastSeenEventID = firstNonEmpty(lastGovernedTensionEvidenceRef(input.EvidenceRefs), record.LastSeenEventID)
			record.LastSeenAt = now
			record.LastDetectedAt = now
			record.LastRefreshedAt = now
			record.UpdatedAt = now
			if err := s.upsertTensionTx(ctx, tx, record); err != nil {
				return err
			}
			if _, err := s.appendTensionRuntimeEventWithAuthorityTx(ctx, tx, authority, record, input.EvidenceRefs, "tension.refreshed", "system", "rsp_listener", "Refreshed by statistical anomaly handoff"); err != nil {
				return err
			}
			return nil
		} else if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("check existing governed tension: %w", err)
		}

		record := TensionRecord{
			TensionID:       buildTensionID(input.WorkspaceID, input.TensionType, input.ProtoClusterID, "entity_id", input.AnchorRef, ""),
			WorkspaceID:     input.WorkspaceID,
			TensionType:     input.TensionType,
			LifecycleState:  tensionLifecycleEmergent,
			ReviewStatus:    tensionReviewPending,
			ProtoClusterID:  input.ProtoClusterID,
			AnchorKind:      "entity_id",
			AnchorRef:       input.AnchorRef,
			Title:           input.Title,
			Summary:         input.Summary,
			CreatedAt:       now,
			UpdatedAt:       now,
			LastDetectedAt:  now,
			LastRefreshedAt: now,
			BaseScore:       tensionBaseScore(input.TensionType, len(input.EvidenceRefs), false, false),
			SurfaceScore:    tensionBaseScore(input.TensionType, len(input.EvidenceRefs), false, false),
			EvidenceCount:   len(input.EvidenceRefs),
			LastSeenEventID: lastGovernedTensionEvidenceRef(input.EvidenceRefs),
			LastSeenAt:      now,
		}

		evidence := governedTensionEvidenceRecords(record.TensionID, input.WorkspaceID, input.EvidenceRefs, now)

		if err := s.upsertTensionTx(ctx, tx, record); err != nil {
			return err
		}
		if err := s.upsertTensionEvidenceTx(ctx, tx, input.WorkspaceID, record.TensionID, evidence); err != nil {
			return err
		}
		if _, err := s.appendTensionRuntimeEventWithAuthorityTx(ctx, tx, authority, record, input.EvidenceRefs, "tension.emerged", "system", "rsp_listener", "Created by statistical anomaly handoff"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	return tx.Commit()
}

func governedTensionEvidenceRecords(tensionID, workspaceID string, evidenceRefs []string, createdAt string) []TensionEvidenceRecord {
	out := make([]TensionEvidenceRecord, 0, len(evidenceRefs))
	for _, ref := range uniqueSortedStrings(evidenceRefs) {
		out = append(out, TensionEvidenceRecord{
			TensionID:    strings.TrimSpace(tensionID),
			WorkspaceID:  strings.TrimSpace(workspaceID),
			EvidenceKind: "rsp_anomaly",
			EvidenceRef:  ref,
			EventID:      ref,
			Weight:       5,
			Summary:      "Triggered by RSP anomaly alert",
			CreatedAt:    createdAt,
		})
	}
	return out
}

func lastGovernedTensionEvidenceRef(evidenceRefs []string) string {
	refs := uniqueSortedStrings(evidenceRefs)
	if len(refs) == 0 {
		return ""
	}
	return refs[len(refs)-1]
}

func (s *Store) upsertTensionTx(ctx context.Context, tx *sql.Tx, record TensionRecord) error {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO workspace_tensions(
		    tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary,
		    anchor_kind, anchor_ref, task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json,
		    segment_refs_json, agent_ids_json, constraint_refs_json, base_score, surface_score, evidence_count,
		    last_seen_event_id, last_seen_at, last_detected_at, last_refreshed_at, stale_refresh_count,
		    confirmed_by, archived_by, dismissed_reason, created_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		  ON CONFLICT(tension_id) DO UPDATE SET
		    workspace_id = excluded.workspace_id,
		    proto_cluster_id = excluded.proto_cluster_id,
		    tension_type = excluded.tension_type,
		    lifecycle_state = excluded.lifecycle_state,
		    review_status = excluded.review_status,
		    title = excluded.title,
		    summary = excluded.summary,
		    anchor_kind = excluded.anchor_kind,
		    anchor_ref = excluded.anchor_ref,
		    task_ids_json = excluded.task_ids_json,
		    session_ids_json = excluded.session_ids_json,
		    doc_keys_json = excluded.doc_keys_json,
		    artifact_refs_json = excluded.artifact_refs_json,
		    segment_refs_json = excluded.segment_refs_json,
		    agent_ids_json = excluded.agent_ids_json,
		    constraint_refs_json = excluded.constraint_refs_json,
		    base_score = excluded.base_score,
		    surface_score = excluded.surface_score,
		    evidence_count = excluded.evidence_count,
		    last_seen_event_id = excluded.last_seen_event_id,
		    last_seen_at = excluded.last_seen_at,
		    last_detected_at = excluded.last_detected_at,
		    last_refreshed_at = excluded.last_refreshed_at,
		    stale_refresh_count = excluded.stale_refresh_count,
		    confirmed_by = excluded.confirmed_by,
		    archived_by = excluded.archived_by,
		    dismissed_reason = excluded.dismissed_reason,
		    updated_at = excluded.updated_at`,
		record.TensionID,
		record.WorkspaceID,
		record.ProtoClusterID,
		record.TensionType,
		record.LifecycleState,
		record.ReviewStatus,
		record.Title,
		record.Summary,
		record.AnchorKind,
		record.AnchorRef,
		mustJSON(record.TaskIDs),
		mustJSON(record.SessionIDs),
		mustJSON(record.DocKeys),
		mustJSON(record.ArtifactRefs),
		mustJSON(record.SegmentRefs),
		mustJSON(record.AgentIDs),
		mustJSON(record.ConstraintRefs),
		record.BaseScore,
		record.SurfaceScore,
		record.EvidenceCount,
		record.LastSeenEventID,
		record.LastSeenAt,
		record.LastDetectedAt,
		record.LastRefreshedAt,
		record.StaleRefreshCount,
		record.ConfirmedBy,
		record.ArchivedBy,
		record.DiscardedReason,
		record.CreatedAt,
		record.UpdatedAt,
	); err != nil {
		return fmt.Errorf("upsert tension: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, record.UpdatedAt, record.WorkspaceID); err != nil {
		return fmt.Errorf("touch workspace after tension update: %w", err)
	}
	return nil
}

func (s *Store) persistRefreshedTensionMetadataTx(ctx context.Context, tx *sql.Tx, record TensionRecord, refreshedAt string) error {
	record.LastRefreshedAt = strings.TrimSpace(refreshedAt)
	record.StaleRefreshCount = 0
	return s.upsertTensionTx(ctx, tx, record)
}

func (s *Store) upsertTensionEvidenceTx(ctx context.Context, tx *sql.Tx, workspaceID, tensionID string, evidence []TensionEvidenceRecord) error {
	for _, item := range evidence {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT OR IGNORE INTO workspace_tension_evidence(
			    tension_id, workspace_id, evidence_kind, evidence_ref, event_id, weight, summary, created_at
			  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			tensionID,
			workspaceID,
			item.EvidenceKind,
			item.EvidenceRef,
			item.EventID,
			item.Weight,
			item.Summary,
			item.CreatedAt,
		); err != nil {
			return fmt.Errorf("upsert tension evidence: %w", err)
		}
	}
	return nil
}

func (s *Store) appendTensionRuntimeEventTx(ctx context.Context, tx *sql.Tx, record TensionRecord, evidenceRefs []string, eventType, actorType, actorID, reason string) (RuntimeEventRecord, error) {
	payload := tensionRuntimeEventPayload(record, evidenceRefs, eventType, reason)
	payload["actor_type"] = strings.TrimSpace(actorType)
	payload["actor_id"] = strings.TrimSpace(actorID)
	return s.appendRuntimeEventTx(ctx, tx, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: record.WorkspaceID,
		EventType:   eventType,
		EntityType:  "tension",
		EntityID:    record.TensionID,
		ActorType:   strings.TrimSpace(actorType),
		ActorID:     strings.TrimSpace(actorID),
		AgentID:     tensionPrimaryAgentID(record),
		SessionID:   tensionPrimarySessionID(record),
		TaskID:      tensionPrimaryTaskID(record),
		PayloadJSON: mustJSON(payload),
		CreatedAt:   record.UpdatedAt,
	})
}

func (s *Store) appendTensionRuntimeEventWithAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record TensionRecord, evidenceRefs []string, eventType, actorType, actorID, reason string) (RuntimeEventRecord, error) {
	return s.appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx, tx, authority, record, evidenceRefs, eventType, actorType, actorID, reason, tensionRuntimePromptContext{})
}

type tensionRuntimePromptContext struct {
	Envelope              map[string]any
	Surface               string
	PrincipalType         string
	PrincipalID           string
	RefreshProtoClusterID string
	DependsOnTensionID    string
	DependencyType        string
	SCCMemberIDs          []string
	SCCHash               string
	SCCMemberCount        int
	CondenseAction        string
	CoalitionID           string
	CoalitionAgentID      string
	CoalitionRole         string
	CoalitionStatus       string
	CoalitionAction       string
	CoalitionMemberCount  int
}

func (s *Store) appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record TensionRecord, evidenceRefs []string, eventType, actorType, actorID, reason string, promptContext tensionRuntimePromptContext) (RuntimeEventRecord, error) {
	payload := tensionRuntimeEventPayload(record, evidenceRefs, eventType, reason)
	payload["actor_type"] = strings.TrimSpace(actorType)
	payload["actor_id"] = strings.TrimSpace(actorID)
	if dependsOnTensionID := strings.TrimSpace(promptContext.DependsOnTensionID); dependsOnTensionID != "" {
		payload["depends_on_tension_id"] = dependsOnTensionID
	}
	if dependencyType := strings.TrimSpace(promptContext.DependencyType); dependencyType != "" {
		payload["dependency_type"] = dependencyType
	}
	if memberIDs := uniqueSortedStrings(promptContext.SCCMemberIDs); len(memberIDs) > 0 {
		payload["scc_member_tension_ids"] = memberIDs
	}
	if sccHash := strings.TrimSpace(promptContext.SCCHash); sccHash != "" {
		payload["scc_hash"] = sccHash
	}
	if promptContext.SCCMemberCount > 0 {
		payload["scc_member_count"] = promptContext.SCCMemberCount
	}
	if action := strings.TrimSpace(promptContext.CondenseAction); action != "" {
		payload["condense_action"] = action
	}
	if coalitionID := strings.TrimSpace(promptContext.CoalitionID); coalitionID != "" {
		payload["coalition_id"] = coalitionID
	}
	if coalitionAgentID := strings.TrimSpace(promptContext.CoalitionAgentID); coalitionAgentID != "" {
		payload["coalition_agent_id"] = coalitionAgentID
	}
	if role := strings.TrimSpace(promptContext.CoalitionRole); role != "" {
		payload["coalition_role"] = role
	}
	if status := strings.TrimSpace(promptContext.CoalitionStatus); status != "" {
		payload["coalition_status"] = status
	}
	if action := strings.TrimSpace(promptContext.CoalitionAction); action != "" {
		payload["coalition_action"] = action
	}
	if promptContext.CoalitionMemberCount >= 0 && strings.TrimSpace(promptContext.CoalitionID) != "" {
		payload["coalition_member_count"] = promptContext.CoalitionMemberCount
	}
	var err error
	payload, err = attachWorkspaceTensionPromptContextEnvelope(payload, promptContext.Envelope, tensionPromptContextSurface(promptContext.Surface, eventType), tensionPromptContextFields(record, eventType, actorType, actorID, promptContext))
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: record.WorkspaceID,
		EventType:   eventType,
		EntityType:  "tension",
		EntityID:    record.TensionID,
		ActorType:   strings.TrimSpace(actorType),
		ActorID:     strings.TrimSpace(actorID),
		AgentID:     tensionPrimaryAgentID(record),
		SessionID:   tensionPrimarySessionID(record),
		TaskID:      tensionPrimaryTaskID(record),
		PayloadJSON: mustJSON(payload),
		CreatedAt:   record.UpdatedAt,
	})
}

func (s *Store) appendTensionRefreshSummaryEventTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input TensionRefreshInput, result TensionRefreshResult, promptContext tensionRuntimePromptContext, createdAt string) (RuntimeEventRecord, error) {
	entityID := strings.TrimSpace(input.ProtoClusterID)
	if entityID == "" {
		entityID = strings.TrimSpace(input.WorkspaceID)
	}
	payload := map[string]any{
		"workspace_id":       strings.TrimSpace(input.WorkspaceID),
		"proto_cluster_id":   strings.TrimSpace(input.ProtoClusterID),
		"typed_event_type":   "TENSION_REFRESH",
		"event_kind":         "tension.refreshed",
		"evaluated_clusters": result.EvaluatedClusters,
		"created_count":      result.CreatedCount,
		"updated_count":      result.UpdatedCount,
		"recovered_count":    result.RecoveredCount,
		"retired_count":      result.RetiredCount,
		"skipped_dismissed":  result.SkippedDismissed,
		"actor_type":         "system",
		"actor_id":           strings.TrimSpace(input.ActorID),
	}
	fields := map[string]string{
		"workspace_id":     strings.TrimSpace(input.WorkspaceID),
		"proto_cluster_id": strings.TrimSpace(input.ProtoClusterID),
		"event_kind":       "tension.refreshed",
		"actor_type":       "system",
		"actor_id":         strings.TrimSpace(input.ActorID),
	}
	if principalType := strings.TrimSpace(promptContext.PrincipalType); principalType != "" {
		fields["principal_type"] = principalType
	}
	if principalID := strings.TrimSpace(promptContext.PrincipalID); principalID != "" {
		fields["principal_id"] = principalID
	}
	if refreshProtoClusterID := strings.TrimSpace(promptContext.RefreshProtoClusterID); refreshProtoClusterID != "" {
		fields["refresh_proto_cluster_id"] = refreshProtoClusterID
	}
	var err error
	payload, err = attachWorkspaceTensionPromptContextEnvelope(payload, promptContext.Envelope, tensionPromptContextSurface(promptContext.Surface, "tension.refreshed"), fields)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: strings.TrimSpace(input.WorkspaceID),
		EventType:   "tension.refreshed",
		EntityType:  "workspace_tension_refresh",
		EntityID:    entityID,
		ActorType:   "system",
		ActorID:     strings.TrimSpace(input.ActorID),
		PayloadJSON: mustJSON(payload),
		CreatedAt:   createdAt,
	})
}

func tensionPromptContextSurface(surface, eventType string) string {
	if surface = strings.TrimSpace(surface); surface != "" {
		return surface
	}
	return tensionPromptContextDefaultSurfaceForEventType(eventType)
}

func tensionPromptContextDefaultSurfaceForEventType(eventType string) string {
	switch strings.TrimSpace(eventType) {
	case "tension.confirmed":
		return "workspace.tension.confirm"
	case "tension.discarded":
		return "workspace.tension.discard"
	case "tension.archived":
		return "workspace.tension.archive"
	case "tension.resolved":
		return "workspace.tension.resolve"
	case "tension.dormant":
		return "workspace.tension.dormant"
	case "tension.dependency.added":
		return "workspace.tension.add.dependency"
	case "tension.dependency.removed":
		return "workspace.tension.remove.dependency"
	case "tension.emergent", "tension.condensed":
		return "workspace.tension.condense"
	case "tension.agent.attached":
		return "workspace.tension.agent.attach"
	case "tension.agent.detached":
		return "workspace.tension.agent.detach"
	default:
		return "workspace.tension.refresh"
	}
}

func validateTensionRefreshPromptContextSurface(envelope map[string]any, surface string) error {
	if envelope == nil {
		return nil
	}
	surface = strings.TrimSpace(surface)
	if surface == "" || surface == "workspace.tension.refresh" {
		return nil
	}
	return fmt.Errorf("workspace_tension prompt context surface %q is not valid for workspace.tension.refresh", surface)
}

func validateTensionMutationPromptContextSurface(envelope map[string]any, surface, eventType string, allowLifecycleUpdate bool) error {
	if envelope == nil {
		return nil
	}
	surface = strings.TrimSpace(surface)
	if surface == "" {
		return nil
	}
	expected := tensionPromptContextDefaultSurfaceForEventType(eventType)
	if surface == expected {
		return nil
	}
	switch strings.TrimSpace(eventType) {
	case "tension.agent.attached":
		switch surface {
		case "agent.tension.attach", "coalition.offer", "coalition.invite":
			return nil
		}
	case "tension.agent.detached":
		switch surface {
		case "agent.tension.detach", "coalition.leave", "coalition.kick":
			return nil
		}
	}
	if allowLifecycleUpdate && surface == "workspace.tension.lifecycle.update" {
		switch strings.TrimSpace(eventType) {
		case "tension.resolved", "tension.discarded", "tension.archived":
			return nil
		}
	}
	return fmt.Errorf("workspace_tension prompt context surface %q is not valid for %s", surface, strings.TrimSpace(eventType))
}

func tensionPromptContextFields(record TensionRecord, eventType, actorType, actorID string, promptContext tensionRuntimePromptContext) map[string]string {
	fields := map[string]string{
		"workspace_id":     strings.TrimSpace(record.WorkspaceID),
		"tension_id":       strings.TrimSpace(record.TensionID),
		"proto_cluster_id": strings.TrimSpace(record.ProtoClusterID),
		"tension_type":     strings.TrimSpace(record.TensionType),
		"lifecycle_state":  strings.TrimSpace(record.LifecycleState),
		"review_status":    strings.TrimSpace(record.ReviewStatus),
		"event_kind":       strings.TrimSpace(eventType),
		"actor_type":       strings.TrimSpace(actorType),
		"actor_id":         strings.TrimSpace(actorID),
	}
	if principalType := strings.TrimSpace(promptContext.PrincipalType); principalType != "" {
		fields["principal_type"] = principalType
	}
	if principalID := strings.TrimSpace(promptContext.PrincipalID); principalID != "" {
		fields["principal_id"] = principalID
	}
	if refreshProtoClusterID := strings.TrimSpace(promptContext.RefreshProtoClusterID); refreshProtoClusterID != "" {
		fields["refresh_proto_cluster_id"] = refreshProtoClusterID
	}
	if dependsOnTensionID := strings.TrimSpace(promptContext.DependsOnTensionID); dependsOnTensionID != "" {
		fields["depends_on_tension_id"] = dependsOnTensionID
	}
	if dependencyType := strings.TrimSpace(promptContext.DependencyType); dependencyType != "" {
		fields["dependency_type"] = dependencyType
	}
	if memberIDs := uniqueSortedStrings(promptContext.SCCMemberIDs); len(memberIDs) > 0 {
		fields["scc_member_tension_ids"] = strings.Join(memberIDs, ",")
	}
	if sccHash := strings.TrimSpace(promptContext.SCCHash); sccHash != "" {
		fields["scc_hash"] = sccHash
	}
	if promptContext.SCCMemberCount > 0 {
		fields["scc_member_count"] = fmt.Sprintf("%d", promptContext.SCCMemberCount)
	}
	if action := strings.TrimSpace(promptContext.CondenseAction); action != "" {
		fields["condense_action"] = action
	}
	if coalitionID := strings.TrimSpace(promptContext.CoalitionID); coalitionID != "" {
		fields["coalition_id"] = coalitionID
	}
	if coalitionAgentID := strings.TrimSpace(promptContext.CoalitionAgentID); coalitionAgentID != "" {
		fields["coalition_agent_id"] = coalitionAgentID
	}
	if role := strings.TrimSpace(promptContext.CoalitionRole); role != "" {
		fields["coalition_role"] = role
	}
	if status := strings.TrimSpace(promptContext.CoalitionStatus); status != "" {
		fields["coalition_status"] = status
	}
	if action := strings.TrimSpace(promptContext.CoalitionAction); action != "" {
		fields["coalition_action"] = action
	}
	if promptContext.CoalitionMemberCount >= 0 && strings.TrimSpace(promptContext.CoalitionID) != "" {
		fields["coalition_member_count"] = fmt.Sprintf("%d", promptContext.CoalitionMemberCount)
	}
	if anchorKind := strings.TrimSpace(record.AnchorKind); anchorKind != "" {
		fields["anchor_kind"] = anchorKind
	}
	if anchorRef := strings.TrimSpace(record.AnchorRef); anchorRef != "" {
		fields["anchor_ref"] = anchorRef
	}
	return fields
}

func tensionRuntimeEventPayload(record TensionRecord, evidenceRefs []string, eventType, reason string) map[string]any {
	evidenceRefs = uniqueSortedStrings(evidenceRefs)
	if len(evidenceRefs) == 0 {
		evidenceRefs = uniqueSortedStrings(append(append([]string{}, record.ConstraintRefs...), record.SegmentRefs...))
	}
	payload := map[string]any{
		"workspace_id":      record.WorkspaceID,
		"tension_id":        record.TensionID,
		"proto_cluster_id":  record.ProtoClusterID,
		"tension_type":      record.TensionType,
		"lifecycle_state":   record.LifecycleState,
		"review_status":     record.ReviewStatus,
		"anchor_kind":       record.AnchorKind,
		"anchor_ref":        record.AnchorRef,
		"task_ids":          append([]string{}, record.TaskIDs...),
		"session_ids":       append([]string{}, record.SessionIDs...),
		"doc_keys":          append([]string{}, record.DocKeys...),
		"agent_ids":         append([]string{}, record.AgentIDs...),
		"artifact_refs":     append([]string{}, record.ArtifactRefs...),
		"segment_refs":      append([]string{}, record.SegmentRefs...),
		"constraint_refs":   append([]string{}, record.ConstraintRefs...),
		"evidence_refs":     append([]string{}, evidenceRefs...),
		"base_score":        record.BaseScore,
		"surface_score":     record.SurfaceScore,
		"last_detected_at":  record.LastDetectedAt,
		"last_refreshed_at": record.LastRefreshedAt,
		"typed_event_type":  "TENSION_UPDATE",
		"title":             record.Title,
		"summary":           record.Summary,
	}
	if trimmed := strings.TrimSpace(reason); trimmed != "" {
		payload["reason"] = trimmed
	}
	payload["event_kind"] = strings.TrimSpace(eventType)
	return payload
}

func tensionPrimaryTaskID(record TensionRecord) string {
	for _, taskID := range record.TaskIDs {
		if trimmed := strings.TrimSpace(taskID); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func tensionPrimarySessionID(record TensionRecord) string {
	for _, sessionID := range record.SessionIDs {
		if trimmed := strings.TrimSpace(sessionID); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func tensionPrimaryAgentID(record TensionRecord) string {
	for _, agentID := range record.AgentIDs {
		if trimmed := strings.TrimSpace(agentID); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func tensionRecordChanged(previous, next TensionRecord) bool {
	if previous.ProtoClusterID != next.ProtoClusterID || previous.TensionType != next.TensionType || previous.LifecycleState != next.LifecycleState || previous.ReviewStatus != next.ReviewStatus {
		return true
	}
	if previous.Title != next.Title || previous.Summary != next.Summary || previous.AnchorKind != next.AnchorKind || previous.AnchorRef != next.AnchorRef {
		return true
	}
	if previous.BaseScore != next.BaseScore || previous.SurfaceScore != next.SurfaceScore || previous.EvidenceCount != next.EvidenceCount {
		return true
	}
	if previous.LastSeenEventID != next.LastSeenEventID || previous.LastSeenAt != next.LastSeenAt {
		return true
	}
	if previous.ConfirmedBy != next.ConfirmedBy || previous.ArchivedBy != next.ArchivedBy || previous.DiscardedReason != next.DiscardedReason {
		return true
	}
	return !equalStringSlices(previous.TaskIDs, next.TaskIDs) ||
		!equalStringSlices(previous.SessionIDs, next.SessionIDs) ||
		!equalStringSlices(previous.DocKeys, next.DocKeys) ||
		!equalStringSlices(previous.ArtifactRefs, next.ArtifactRefs) ||
		!equalStringSlices(previous.SegmentRefs, next.SegmentRefs) ||
		!equalStringSlices(previous.AgentIDs, next.AgentIDs) ||
		!equalStringSlices(previous.ConstraintRefs, next.ConstraintRefs)
}

func tensionRecordStructureChanged(previous, next TensionRecord) bool {
	if previous.ProtoClusterID != next.ProtoClusterID || previous.TensionType != next.TensionType || previous.LifecycleState != next.LifecycleState || previous.ReviewStatus != next.ReviewStatus {
		return true
	}
	if previous.Title != next.Title || previous.Summary != next.Summary || previous.AnchorKind != next.AnchorKind || previous.AnchorRef != next.AnchorRef {
		return true
	}
	if previous.BaseScore != next.BaseScore || previous.EvidenceCount != next.EvidenceCount || previous.LastSeenEventID != next.LastSeenEventID || previous.LastSeenAt != next.LastSeenAt {
		return true
	}
	if previous.ConfirmedBy != next.ConfirmedBy || previous.ArchivedBy != next.ArchivedBy || previous.DiscardedReason != next.DiscardedReason {
		return true
	}
	return !equalStringSlices(previous.TaskIDs, next.TaskIDs) ||
		!equalStringSlices(previous.SessionIDs, next.SessionIDs) ||
		!equalStringSlices(previous.DocKeys, next.DocKeys) ||
		!equalStringSlices(previous.ArtifactRefs, next.ArtifactRefs) ||
		!equalStringSlices(previous.SegmentRefs, next.SegmentRefs) ||
		!equalStringSlices(previous.AgentIDs, next.AgentIDs) ||
		!equalStringSlices(previous.ConstraintRefs, next.ConstraintRefs)
}

func tensionRecordRefreshMetadataChanged(previous, next TensionRecord) bool {
	return previous.LastDetectedAt != next.LastDetectedAt ||
		previous.LastRefreshedAt != next.LastRefreshedAt ||
		previous.StaleRefreshCount != next.StaleRefreshCount
}

func tensionHasNewEvidence(previous, next TensionRecord) bool {
	previousEventID := strings.TrimSpace(previous.LastSeenEventID)
	nextEventID := strings.TrimSpace(next.LastSeenEventID)
	if nextEventID != "" {
		return previousEventID != nextEventID
	}
	if previousEventID != "" {
		return false
	}
	if strings.TrimSpace(previous.LastSeenAt) != strings.TrimSpace(next.LastSeenAt) {
		return true
	}
	return next.EvidenceCount > previous.EvidenceCount
}

func tensionBaseScore(tensionType string, evidenceCount int, openQueue, multiScope bool) int {
	score := map[string]int{
		"contradiction": 70,
		"bottleneck":    65,
		"ambiguity":     55,
		"gap":           45,
		"bridge":        40,
	}[normalizeTensionType(tensionType)]
	if score == 0 {
		score = 40
	}
	if evidenceCount > 1 {
		score += minInt((evidenceCount-1)*5, 15)
	}
	if openQueue {
		score += 10
	}
	if multiScope {
		score += 10
	}
	return clampInt(score, 0, 100)
}

func tensionSurfaceScore(baseScore int, lastSeenAt string, openQueue, hasNewEvidence bool) int {
	score := baseScore
	if age := timeSince(lastSeenAt); age >= 0 {
		switch {
		case age < 2*time.Hour:
			score += 10
		case age < 24*time.Hour:
			score += 5
		case age > 72*time.Hour && !openQueue:
			score -= 10
		}
	}
	if hasNewEvidence {
		score += 10
	}
	return clampInt(score, 0, 100)
}

func buildTensionID(workspaceID, tensionType, protoClusterID, anchorKind, anchorRef, stableFingerprint string) string {
	base := strings.Join([]string{
		strings.TrimSpace(protoClusterID),
		strings.TrimSpace(anchorKind),
		strings.TrimSpace(anchorRef),
		strings.TrimSpace(stableFingerprint),
	}, "|")
	sum := sha256.Sum256([]byte(base))
	return "tension:" + strings.TrimSpace(workspaceID) + "/" + normalizeTensionType(tensionType) + "/" + hex.EncodeToString(sum[:8])
}

func tensionSegmentRootRefs(workspaceID string, docKeys, artifactRefs []string) []string {
	out := make([]string, 0, len(docKeys)+len(artifactRefs))
	for _, docKey := range uniqueSortedStrings(docKeys) {
		out = append(out, "workspace_doc:"+workspaceID+"/"+docKey+"#root")
	}
	for _, artifactRef := range uniqueSortedStrings(artifactRefs) {
		out = append(out, "artifact:"+workspaceID+"/"+artifactRef+"#root")
	}
	return uniqueSortedStrings(out)
}

func isSyntheticTensionEvent(event RuntimeEventRecord) bool {
	return isSyntheticOperationalEvent(event)
}

func isContradictionEventType(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "knowledge_claim.disputed", "knowledge_claim.superseded", "knowledge_claim.review_escalated":
		return true
	default:
		return false
	}
}

func tensionQueueLooksPersistent(queue OperatorQueueRecord) bool {
	if queue.EscalationCount > 0 {
		return true
	}
	if normalizeUrgency(queue.Urgency) >= normalizeUrgency("HIGH") {
		return true
	}
	if dueAt := derefString(queue.DueAt); dueAt != "" {
		if age := timeSince(dueAt); age >= 0 {
			return true
		}
	}
	return false
}

func selectPrimaryQueue(queues []OperatorQueueRecord, allowedTypes map[string]struct{}) *OperatorQueueRecord {
	if len(queues) == 0 {
		return nil
	}
	candidates := make([]OperatorQueueRecord, 0, len(queues))
	for _, queue := range queues {
		if len(allowedTypes) > 0 {
			if _, ok := allowedTypes[normalizeOperatorQueueType(queue.QueueType)]; !ok {
				continue
			}
		}
		candidates = append(candidates, queue)
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if normalizeUrgency(candidates[i].Urgency) != normalizeUrgency(candidates[j].Urgency) {
			return normalizeUrgency(candidates[i].Urgency) > normalizeUrgency(candidates[j].Urgency)
		}
		if candidates[i].EscalationCount != candidates[j].EscalationCount {
			return candidates[i].EscalationCount > candidates[j].EscalationCount
		}
		if derefString(candidates[i].DueAt) != derefString(candidates[j].DueAt) {
			return derefString(candidates[i].DueAt) < derefString(candidates[j].DueAt)
		}
		return candidates[i].QueueKey < candidates[j].QueueKey
	})
	return &candidates[0]
}

func dedupeTensionEvidenceInputs(items []tensionEvidenceInput) []tensionEvidenceInput {
	if len(items) == 0 {
		return nil
	}
	index := map[string]tensionEvidenceInput{}
	for _, item := range items {
		key := strings.Join([]string{strings.TrimSpace(item.kind), strings.TrimSpace(item.ref), strings.TrimSpace(item.eventID)}, "|")
		existing, ok := index[key]
		if !ok || strings.TrimSpace(item.createdAt) > strings.TrimSpace(existing.createdAt) || item.weight > existing.weight {
			index[key] = item
		}
	}
	keys := make([]string, 0, len(index))
	for key := range index {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]tensionEvidenceInput, 0, len(keys))
	for _, key := range keys {
		out = append(out, index[key])
	}
	return out
}

func evidenceRefsFromEvidence(evidence []TensionEvidenceRecord) []string {
	refs := make([]string, 0, len(evidence))
	for _, item := range evidence {
		refValue := strings.TrimSpace(item.EvidenceRef)
		if refValue == "" {
			refValue = strings.TrimSpace(item.EventID)
		}
		if refValue == "" {
			continue
		}
		refs = append(refs, strings.TrimSpace(item.EvidenceKind)+":"+refValue)
	}
	return uniqueSortedStrings(refs)
}

func (s *Store) listRuntimeEventsByIDs(ctx context.Context, workspaceID string, eventIDs []string) ([]RuntimeEventRecord, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	query := `SELECT ` + runtimeEventSelectColumns + `
	   FROM runtime_events
	  WHERE workspace_id = ? AND event_id IN (` + placeholders(len(eventIDs)) + `)
	  ORDER BY COALESCE(ingest_seq,0) DESC, created_at DESC, event_id DESC`
	args := make([]any, 0, len(eventIDs)+1)
	args = append(args, workspaceID)
	for _, eventID := range eventIDs {
		args = append(args, eventID)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query runtime events by id: %w", err)
	}
	defer rows.Close()
	out := []RuntimeEventRecord{}
	for rows.Next() {
		var record RuntimeEventRecord
		if err := scanRuntimeEvent(rows, &record); err != nil {
			return nil, fmt.Errorf("scan runtime event by id: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtime events by id: %w", err)
	}
	return out, nil
}

func (s *Store) listKnowledgeClaimsByIDs(ctx context.Context, workspaceID string, claimIDs []string) ([]KnowledgeClaimRecord, error) {
	if len(claimIDs) == 0 {
		return nil, nil
	}
	query := `SELECT ` + knowledgeClaimSelectColumns("") + `
	   FROM knowledge_claims
	  WHERE workspace_id = ? AND claim_id IN (` + placeholders(len(claimIDs)) + `)
	  ORDER BY updated_at DESC, claim_id DESC`
	args := make([]any, 0, len(claimIDs)+1)
	args = append(args, workspaceID)
	for _, claimID := range claimIDs {
		args = append(args, claimID)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query claims by id: %w", err)
	}
	defer rows.Close()
	return collectKnowledgeClaimRows(rows)
}

func (s *Store) listOperatorQueuesByKeys(ctx context.Context, workspaceID string, queueKeys []string) ([]OperatorQueueRecord, error) {
	if len(queueKeys) == 0 {
		return nil, nil
	}
	query := `SELECT ` + operatorQueueSelectColumns("") + `
	   FROM operator_queue_items
	  WHERE workspace_id = ? AND queue_key IN (` + placeholders(len(queueKeys)) + `)
	  ORDER BY updated_at DESC, queue_id DESC`
	args := make([]any, 0, len(queueKeys)+1)
	args = append(args, workspaceID)
	for _, key := range queueKeys {
		args = append(args, key)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query queues by key: %w", err)
	}
	defer rows.Close()
	return collectOperatorQueueRows(rows)
}

func (s *Store) listWorkspaceArtifactsByRef(ctx context.Context, workspaceID string, refs []string) ([]WorkspaceArtifactRecord, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	query := `SELECT artifact_id, workspace_id, task_id, update_id, title, artifact_ref, kind, content_type, created_by, metadata_json, created_at
	   FROM workspace_artifacts
	  WHERE workspace_id = ? AND artifact_ref IN (` + placeholders(len(refs)) + `)
	  ORDER BY created_at DESC, artifact_id DESC`
	args := make([]any, 0, len(refs)+1)
	args = append(args, workspaceID)
	for _, ref := range refs {
		args = append(args, ref)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query artifacts by ref: %w", err)
	}
	defer rows.Close()
	out := []WorkspaceArtifactRecord{}
	for rows.Next() {
		var row WorkspaceArtifactRecord
		var taskID sql.NullString
		var updateID sql.NullString
		if err := rows.Scan(&row.ArtifactID, &row.WorkspaceID, &taskID, &updateID, &row.Title, &row.ArtifactRef, &row.Kind, &row.ContentType, &row.CreatedBy, &row.MetadataJSON, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan artifact by ref: %w", err)
		}
		row.TaskID = nullStringPtr(taskID)
		row.UpdateID = nullStringPtr(updateID)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate artifacts by ref: %w", err)
	}
	return out, nil
}

func decodeStringJSONArray(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return uniqueSortedStrings(out)
}

func placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	parts := make([]string, count)
	for idx := range parts {
		parts[idx] = "?"
	}
	return strings.Join(parts, ", ")
}

func cutPrefixedRef(value, prefix string) (string, bool) {
	if !strings.HasPrefix(strings.TrimSpace(value), prefix) {
		return "", false
	}
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(value), prefix)), true
}

func equalStringSlices(left, right []string) bool {
	left = uniqueSortedStrings(left)
	right = uniqueSortedStrings(right)
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

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out[trimmed] = struct{}{}
		}
	}
	return out
}

func containsString(values []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func clampInt(value, minValue, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func normalizeUrgency(value string) int {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "CRITICAL":
		return 4
	case "HIGH":
		return 3
	case "NORMAL":
		return 2
	case "LOW":
		return 1
	default:
		return 0
	}
}

func timeSince(value string) time.Duration {
	ts := strings.TrimSpace(value)
	if ts == "" {
		return -1
	}
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return -1
	}
	return time.Since(parsed)
}

func (s *Store) AddTensionDependency(ctx context.Context, workspaceID, tensionID, dependsOnTensionID, dependencyType string) error {
	workspaceID, tensionID, dependsOnTensionID, dependencyType, err := normalizeTensionDependencyMutationFields(workspaceID, tensionID, dependsOnTensionID, dependencyType)
	if err != nil {
		return err
	}
	_, _, err = s.addTensionDependencyTx(ctx, s.db, workspaceID, tensionID, dependsOnTensionID, dependencyType, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) AddTensionDependencyWithContext(ctx context.Context, input TensionDependencyMutationInput) (TensionDependencyMutationResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.TensionID = strings.TrimSpace(input.TensionID)
	input.DependsOnTensionID = strings.TrimSpace(input.DependsOnTensionID)
	input.DependencyType = strings.TrimSpace(input.DependencyType)
	input.ActorID = strings.TrimSpace(input.ActorID)
	if input.DependencyType == "" {
		input.DependencyType = "BLOCKS"
	}
	if input.WorkspaceID == "" || input.TensionID == "" || input.DependsOnTensionID == "" {
		return TensionDependencyMutationResult{}, errors.New("workspace_id, tension_id, and depends_on_tension_id are required")
	}
	if input.TensionID == input.DependsOnTensionID {
		return TensionDependencyMutationResult{}, errors.New("tension dependency cannot reference itself")
	}
	if input.ActorID == "" {
		return TensionDependencyMutationResult{}, errors.New("actor_id is required")
	}
	if err := validateTensionMutationPromptContextSurface(input.PromptContextEnvelope, input.PromptContextSurface, "tension.dependency.added", false); err != nil {
		return TensionDependencyMutationResult{}, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return TensionDependencyMutationResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TensionDependencyMutationResult{}, fmt.Errorf("begin tension dependency add tx: %w", err)
	}
	defer tx.Rollback()

	var result TensionDependencyMutationResult
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		record, err := s.loadTensionRecord(ctx, tx, input.WorkspaceID, input.TensionID)
		if err != nil {
			return err
		}
		if _, err := s.loadTensionRecord(ctx, tx, input.WorkspaceID, input.DependsOnTensionID); err != nil {
			return err
		}
		edge, changed, err := s.addTensionDependencyTx(ctx, tx, input.WorkspaceID, input.TensionID, input.DependsOnTensionID, input.DependencyType, now)
		if err != nil {
			return err
		}
		result = TensionDependencyMutationResult{Edge: edge, Changed: changed}
		if !changed {
			return nil
		}
		record.UpdatedAt = now
		event, err := s.appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx, tx, authority, record, nil, "tension.dependency.added", "operator", input.ActorID, input.Reason, tensionRuntimePromptContext{
			Envelope:           input.PromptContextEnvelope,
			Surface:            tensionPromptContextSurface(input.PromptContextSurface, "tension.dependency.added"),
			PrincipalType:      input.PromptContextPrincipalType,
			PrincipalID:        input.PromptContextPrincipalID,
			DependsOnTensionID: input.DependsOnTensionID,
			DependencyType:     edge.DependencyType,
		})
		if err != nil {
			return err
		}
		result.Event = event
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return TensionDependencyMutationResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return TensionDependencyMutationResult{}, fmt.Errorf("commit tension dependency add tx: %w", err)
	}
	return result, nil
}

func (s *Store) RemoveTensionDependency(ctx context.Context, workspaceID, tensionID, dependsOnTensionID string) error {
	workspaceID, tensionID, dependsOnTensionID, _, err := normalizeTensionDependencyMutationFields(workspaceID, tensionID, dependsOnTensionID, "")
	if err != nil {
		return err
	}
	_, _, err = s.removeTensionDependencyTx(ctx, s.db, workspaceID, tensionID, dependsOnTensionID)
	return err
}

func (s *Store) RemoveTensionDependencyWithContext(ctx context.Context, input TensionDependencyMutationInput) (TensionDependencyMutationResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.TensionID = strings.TrimSpace(input.TensionID)
	input.DependsOnTensionID = strings.TrimSpace(input.DependsOnTensionID)
	input.ActorID = strings.TrimSpace(input.ActorID)
	if input.WorkspaceID == "" || input.TensionID == "" || input.DependsOnTensionID == "" {
		return TensionDependencyMutationResult{}, errors.New("workspace_id, tension_id, and depends_on_tension_id are required")
	}
	if input.TensionID == input.DependsOnTensionID {
		return TensionDependencyMutationResult{}, errors.New("tension dependency cannot reference itself")
	}
	if input.ActorID == "" {
		return TensionDependencyMutationResult{}, errors.New("actor_id is required")
	}
	if err := validateTensionMutationPromptContextSurface(input.PromptContextEnvelope, input.PromptContextSurface, "tension.dependency.removed", false); err != nil {
		return TensionDependencyMutationResult{}, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return TensionDependencyMutationResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TensionDependencyMutationResult{}, fmt.Errorf("begin tension dependency remove tx: %w", err)
	}
	defer tx.Rollback()

	var result TensionDependencyMutationResult
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		record, err := s.loadTensionRecord(ctx, tx, input.WorkspaceID, input.TensionID)
		if err != nil {
			return err
		}
		if _, err := s.loadTensionRecord(ctx, tx, input.WorkspaceID, input.DependsOnTensionID); err != nil {
			return err
		}
		edge, changed, err := s.removeTensionDependencyTx(ctx, tx, input.WorkspaceID, input.TensionID, input.DependsOnTensionID)
		if err != nil {
			return err
		}
		result = TensionDependencyMutationResult{Edge: edge, Changed: changed}
		if !changed {
			return nil
		}
		record.UpdatedAt = now
		event, err := s.appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx, tx, authority, record, nil, "tension.dependency.removed", "operator", input.ActorID, input.Reason, tensionRuntimePromptContext{
			Envelope:           input.PromptContextEnvelope,
			Surface:            tensionPromptContextSurface(input.PromptContextSurface, "tension.dependency.removed"),
			PrincipalType:      input.PromptContextPrincipalType,
			PrincipalID:        input.PromptContextPrincipalID,
			DependsOnTensionID: input.DependsOnTensionID,
			DependencyType:     edge.DependencyType,
		})
		if err != nil {
			return err
		}
		result.Event = event
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return TensionDependencyMutationResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return TensionDependencyMutationResult{}, fmt.Errorf("commit tension dependency remove tx: %w", err)
	}
	return result, nil
}

func (s *Store) AttachTensionAgentWithContext(ctx context.Context, input TensionCoalitionMemberMutationInput) (TensionCoalitionMemberMutationResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.TensionID = strings.TrimSpace(input.TensionID)
	input.AgentID = strings.TrimSpace(input.AgentID)
	input.ActorType = normalizeTensionCoalitionMemberActorType(input.ActorType, input.PromptContextPrincipalType)
	input.ActorID = strings.TrimSpace(input.ActorID)
	if input.WorkspaceID == "" || input.TensionID == "" || input.AgentID == "" {
		return TensionCoalitionMemberMutationResult{}, errors.New("workspace_id, tension_id, and agent_id are required")
	}
	if input.ActorID == "" {
		return TensionCoalitionMemberMutationResult{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return TensionCoalitionMemberMutationResult{}, errors.New("prompt_context_envelope is required")
	}
	if strings.TrimSpace(input.PromptContextPrincipalType) == "" || strings.TrimSpace(input.PromptContextPrincipalID) == "" {
		return TensionCoalitionMemberMutationResult{}, errors.New("prompt context principal binding is required")
	}
	surface := tensionCoalitionMemberMutationSurface(input.PromptContextSurface, input.PromptContextEnvelope)
	if err := validateTensionCoalitionMemberPromptPrincipal(surface, input.PromptContextPrincipalType, input.PromptContextPrincipalID, input.ActorType, input.ActorID, input.AgentID); err != nil {
		return TensionCoalitionMemberMutationResult{}, err
	}
	if err := validateTensionMutationPromptContextSurface(input.PromptContextEnvelope, input.PromptContextSurface, "tension.agent.attached", false); err != nil {
		return TensionCoalitionMemberMutationResult{}, err
	}
	if err := validateTensionCoalitionMemberAgentToolBinding(input.PromptContextSurface, input.PromptContextEnvelope, input.PromptContextPrincipalType, input.PromptContextPrincipalID, input.ActorType, input.ActorID, input.AgentID); err != nil {
		return TensionCoalitionMemberMutationResult{}, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return TensionCoalitionMemberMutationResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TensionCoalitionMemberMutationResult{}, fmt.Errorf("begin tension agent attach tx: %w", err)
	}
	defer tx.Rollback()

	var result TensionCoalitionMemberMutationResult
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		coalition, tension, _, err := s.createCoalitionTx(ctx, tx, input.WorkspaceID, input.TensionID, input.SuccessCriterion)
		if err != nil {
			return err
		}
		if expectedCoalitionID := strings.TrimSpace(input.CoalitionID); expectedCoalitionID != "" && coalition.CoalitionID != expectedCoalitionID {
			return ErrCoalitionExpired
		}
		if input.RequireActorMembership || coalitionSurfaceRequiresActorMembership(surface) {
			if _, ok := coalitionMemberRecord(&coalition, input.ActorID); !ok {
				return fmt.Errorf("%w: %s %s", ErrCoalitionActorNotMember, coalitionActorMemberErrorLabelForSurface(surface, input.CoalitionAction), input.ActorID)
			}
		}
		memberResult, err := s.addCoalitionMemberTx(ctx, tx, input.WorkspaceID, coalition.CoalitionID, input.AgentID, nil)
		if err != nil {
			return err
		}
		result = TensionCoalitionMemberMutationResult{
			Coalition: memberResult.Coalition,
			Tension:   tension,
			Factors:   memberResult.Factors,
			Changed:   memberResult.Changed,
		}
		if !memberResult.Changed {
			return nil
		}
		tension.UpdatedAt = now
		event, err := s.appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx, tx, authority, tension, nil, "tension.agent.attached", input.ActorType, input.ActorID, input.Reason, tensionRuntimePromptContext{
			Envelope:             input.PromptContextEnvelope,
			Surface:              tensionPromptContextSurface(input.PromptContextSurface, "tension.agent.attached"),
			PrincipalType:        input.PromptContextPrincipalType,
			PrincipalID:          input.PromptContextPrincipalID,
			CoalitionID:          memberResult.Coalition.CoalitionID,
			CoalitionAgentID:     input.AgentID,
			CoalitionRole:        memberResult.Member.Role,
			CoalitionStatus:      memberResult.Coalition.Status,
			CoalitionAction:      tensionCoalitionMemberAction(input.CoalitionAction, "attached"),
			CoalitionMemberCount: len(memberResult.Coalition.Members),
		})
		if err != nil {
			return err
		}
		result.Event = event
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return TensionCoalitionMemberMutationResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return TensionCoalitionMemberMutationResult{}, fmt.Errorf("commit tension agent attach tx: %w", err)
	}
	return result, nil
}

func (s *Store) DetachTensionAgentWithContext(ctx context.Context, input TensionCoalitionMemberMutationInput) (TensionCoalitionMemberMutationResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.CoalitionID = strings.TrimSpace(input.CoalitionID)
	input.AgentID = strings.TrimSpace(input.AgentID)
	input.ActorType = normalizeTensionCoalitionMemberActorType(input.ActorType, input.PromptContextPrincipalType)
	input.ActorID = strings.TrimSpace(input.ActorID)
	if input.WorkspaceID == "" || input.CoalitionID == "" || input.AgentID == "" {
		return TensionCoalitionMemberMutationResult{}, errors.New("workspace_id, coalition_id, and agent_id are required")
	}
	if input.ActorID == "" {
		return TensionCoalitionMemberMutationResult{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return TensionCoalitionMemberMutationResult{}, errors.New("prompt_context_envelope is required")
	}
	if strings.TrimSpace(input.PromptContextPrincipalType) == "" || strings.TrimSpace(input.PromptContextPrincipalID) == "" {
		return TensionCoalitionMemberMutationResult{}, errors.New("prompt context principal binding is required")
	}
	surface := tensionCoalitionMemberMutationSurface(input.PromptContextSurface, input.PromptContextEnvelope)
	if err := validateTensionCoalitionMemberPromptPrincipal(surface, input.PromptContextPrincipalType, input.PromptContextPrincipalID, input.ActorType, input.ActorID, input.AgentID); err != nil {
		return TensionCoalitionMemberMutationResult{}, err
	}
	if (input.RejectActorSelfMutation || coalitionSurfaceRejectsSelfMutation(surface)) && input.ActorID == input.AgentID {
		return TensionCoalitionMemberMutationResult{}, ErrCoalitionSelfKick
	}
	if err := validateTensionMutationPromptContextSurface(input.PromptContextEnvelope, input.PromptContextSurface, "tension.agent.detached", false); err != nil {
		return TensionCoalitionMemberMutationResult{}, err
	}
	if err := validateTensionCoalitionMemberAgentToolBinding(input.PromptContextSurface, input.PromptContextEnvelope, input.PromptContextPrincipalType, input.PromptContextPrincipalID, input.ActorType, input.ActorID, input.AgentID); err != nil {
		return TensionCoalitionMemberMutationResult{}, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return TensionCoalitionMemberMutationResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TensionCoalitionMemberMutationResult{}, fmt.Errorf("begin tension agent detach tx: %w", err)
	}
	defer tx.Rollback()

	var result TensionCoalitionMemberMutationResult
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if input.RequireActorMembership || coalitionSurfaceRequiresActorMembership(surface) {
			if err := s.requireCoalitionActorMemberTx(ctx, tx, input.WorkspaceID, input.CoalitionID, input.ActorID, coalitionActorMemberErrorLabelForSurface(surface, input.CoalitionAction)); err != nil {
				return err
			}
		}
		memberResult, err := s.removeCoalitionMemberTx(ctx, tx, input.WorkspaceID, input.CoalitionID, input.AgentID, true)
		if err != nil {
			return err
		}
		result = TensionCoalitionMemberMutationResult{
			Coalition: memberResult.Coalition,
			Tension:   memberResult.Tension,
			Changed:   memberResult.Changed,
		}
		if !memberResult.Changed {
			return nil
		}
		tension := memberResult.Tension
		tension.UpdatedAt = now
		event, err := s.appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx, tx, authority, tension, nil, "tension.agent.detached", input.ActorType, input.ActorID, input.Reason, tensionRuntimePromptContext{
			Envelope:             input.PromptContextEnvelope,
			Surface:              tensionPromptContextSurface(input.PromptContextSurface, "tension.agent.detached"),
			PrincipalType:        input.PromptContextPrincipalType,
			PrincipalID:          input.PromptContextPrincipalID,
			CoalitionID:          memberResult.Coalition.CoalitionID,
			CoalitionAgentID:     input.AgentID,
			CoalitionStatus:      memberResult.Coalition.Status,
			CoalitionAction:      tensionCoalitionMemberAction(input.CoalitionAction, "detached"),
			CoalitionMemberCount: len(memberResult.Coalition.Members),
		})
		if err != nil {
			return err
		}
		result.Event = event
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return TensionCoalitionMemberMutationResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return TensionCoalitionMemberMutationResult{}, fmt.Errorf("commit tension agent detach tx: %w", err)
	}
	return result, nil
}

func normalizeTensionCoalitionMemberActorType(actorType, principalType string) string {
	actorType = strings.TrimSpace(actorType)
	if actorType != "" {
		return actorType
	}
	if strings.EqualFold(strings.TrimSpace(principalType), "agent") {
		return "agent"
	}
	return "operator"
}

func tensionCoalitionMemberMutationSurface(surface string, envelope map[string]any) string {
	return firstNonEmpty(strings.TrimSpace(surface), executionPromptRawString(envelope, "surface"))
}

func tensionCoalitionMemberAction(action, fallback string) string {
	action = strings.TrimSpace(action)
	if action != "" {
		return action
	}
	return strings.TrimSpace(fallback)
}

func coalitionSurfaceAllowsAgentActorToMutateTarget(surface string) bool {
	switch strings.TrimSpace(surface) {
	case "coalition.invite", "coalition.kick":
		return true
	default:
		return false
	}
}

func coalitionSurfaceRequiresActorMembership(surface string) bool {
	switch strings.TrimSpace(surface) {
	case "coalition.invite", "coalition.kick":
		return true
	default:
		return false
	}
}

func coalitionSurfaceRejectsSelfMutation(surface string) bool {
	return strings.TrimSpace(surface) == "coalition.kick"
}

func validateTensionCoalitionMemberPromptPrincipal(surface, principalType, principalID, actorType, actorID, agentID string) error {
	principalType = strings.TrimSpace(principalType)
	principalID = strings.TrimSpace(principalID)
	actorType = strings.TrimSpace(actorType)
	actorID = strings.TrimSpace(actorID)
	agentID = strings.TrimSpace(agentID)
	allowAgentActorForOtherTarget := coalitionSurfaceAllowsAgentActorToMutateTarget(surface)
	if principalID == "" || actorID == "" {
		return errors.New("prompt context principal and actor binding is required")
	}
	if actorType == "" {
		return errors.New("actor_type is required")
	}
	if principalID != actorID {
		return fmt.Errorf("workspace_tension coalition prompt context principal_id %q does not match actor_id %q", principalID, actorID)
	}
	if strings.EqualFold(principalType, "agent") && principalID != agentID && !allowAgentActorForOtherTarget {
		return fmt.Errorf("workspace_tension coalition agent principal %q cannot mutate membership for agent %q", principalID, agentID)
	}
	if strings.EqualFold(actorType, "agent") && (!strings.EqualFold(principalType, "agent") || (actorID != agentID && !allowAgentActorForOtherTarget)) {
		return fmt.Errorf("workspace_tension coalition actor_type agent requires matching agent principal and agent_id")
	}
	return nil
}

func coalitionActorMemberErrorLabelForSurface(surface, action string) string {
	switch strings.TrimSpace(surface) {
	case "coalition.invite":
		return "inviter"
	case "coalition.kick":
		return "kicker"
	}
	switch strings.TrimSpace(action) {
	case "invited":
		return "inviter"
	case "kicked":
		return "kicker"
	default:
		return "actor"
	}
}

func (s *Store) requireCoalitionActorMemberTx(ctx context.Context, tx *sql.Tx, workspaceID, coalitionID, actorID, label string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	coalitionID = strings.TrimSpace(coalitionID)
	actorID = strings.TrimSpace(actorID)
	label = strings.TrimSpace(label)
	if label == "" {
		label = "actor"
	}
	coalition, err := s.loadCoalitionRecordByID(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return err
	}
	if coalition == nil {
		return fmt.Errorf("%w: coalition %s", ErrCoalitionTargetNotFound, coalitionID)
	}
	if !coalitionIsActive(coalition.Status) {
		return ErrCoalitionExpired
	}
	currentEpoch, err := s.currentControlEpoch(ctx, tx, workspaceID)
	if err != nil {
		return err
	}
	if expired, err := s.expireCoalitionIfNeeded(ctx, tx, coalition, currentEpoch); err != nil {
		return err
	} else if expired {
		return ErrCoalitionExpired
	}
	canonical, err := s.reconcileLiveCoalitionCandidateByTension(ctx, tx, workspaceID, coalition.TensionID, currentEpoch)
	if err != nil {
		return err
	}
	if canonical == nil || canonical.coalition.CoalitionID != coalitionID {
		return ErrCoalitionExpired
	}
	members, err := s.loadCoalitionMembers(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return err
	}
	for _, member := range members {
		if strings.TrimSpace(member.AgentID) == actorID {
			return nil
		}
	}
	return fmt.Errorf("%w: %s %s", ErrCoalitionActorNotMember, label, actorID)
}

func validateTensionCoalitionMemberAgentToolBinding(surface string, envelope map[string]any, principalType, principalID, actorType, actorID, agentID string) error {
	surfaces := []string{strings.TrimSpace(surface), executionPromptRawString(envelope, "surface")}
	for _, candidate := range surfaces {
		switch candidate {
		case "agent.tension.attach", "agent.tension.detach":
			if !strings.EqualFold(strings.TrimSpace(principalType), "agent") ||
				!strings.EqualFold(strings.TrimSpace(actorType), "agent") ||
				strings.TrimSpace(principalID) != strings.TrimSpace(agentID) ||
				strings.TrimSpace(actorID) != strings.TrimSpace(agentID) {
				return fmt.Errorf("workspace_tension agent-tool surface %q requires principal_id, actor_id, and agent_id to match the acting agent", candidate)
			}
		}
	}
	return nil
}

func normalizeTensionDependencyMutationFields(workspaceID, tensionID, dependsOnTensionID, dependencyType string) (string, string, string, string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	tensionID = strings.TrimSpace(tensionID)
	dependsOnTensionID = strings.TrimSpace(dependsOnTensionID)
	dependencyType = strings.TrimSpace(dependencyType)
	if dependencyType == "" {
		dependencyType = "BLOCKS"
	}
	if workspaceID == "" || tensionID == "" || dependsOnTensionID == "" {
		return "", "", "", "", errors.New("workspace_id, tension_id, and depends_on_tension_id are required")
	}
	if tensionID == dependsOnTensionID {
		return "", "", "", "", errors.New("tension dependency cannot reference itself")
	}
	return workspaceID, tensionID, dependsOnTensionID, dependencyType, nil
}

func (s *Store) addTensionDependencyTx(ctx context.Context, q coalitionQueryer, workspaceID, tensionID, dependsOnTensionID, dependencyType, now string) (TensionDependencyEdge, bool, error) {
	var existingDependencyType string
	err := q.QueryRowContext(ctx, `SELECT dependency_type FROM workspace_tension_dependencies WHERE workspace_id = ? AND tension_id = ? AND depends_on_tension_id = ?`, workspaceID, tensionID, dependsOnTensionID).Scan(&existingDependencyType)
	if err == nil {
		return TensionDependencyEdge{
			WorkspaceID:        workspaceID,
			TensionID:          tensionID,
			DependsOnTensionID: dependsOnTensionID,
			DependencyType:     strings.TrimSpace(existingDependencyType),
		}, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return TensionDependencyEdge{}, false, err
	}
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_tension_dependencies WHERE workspace_id = ? AND tension_id = ?`, workspaceID, tensionID).Scan(&count); err != nil {
		return TensionDependencyEdge{}, false, fmt.Errorf("check dependency bounds: %w", err)
	}
	if count >= 20 {
		return TensionDependencyEdge{}, false, errors.New("tension dependency threshold exceeded: cannot attach more dependencies")
	}
	query := `INSERT INTO workspace_tension_dependencies (workspace_id, tension_id, depends_on_tension_id, dependency_type, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT DO NOTHING`
	res, err := q.ExecContext(ctx, query, workspaceID, tensionID, dependsOnTensionID, dependencyType, now)
	if err != nil {
		return TensionDependencyEdge{}, false, err
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return TensionDependencyEdge{}, false, err
	}
	return TensionDependencyEdge{
		WorkspaceID:        workspaceID,
		TensionID:          tensionID,
		DependsOnTensionID: dependsOnTensionID,
		DependencyType:     dependencyType,
	}, changed > 0, nil
}

func (s *Store) removeTensionDependencyTx(ctx context.Context, q coalitionQueryer, workspaceID, tensionID, dependsOnTensionID string) (TensionDependencyEdge, bool, error) {
	var dependencyType string
	err := q.QueryRowContext(ctx, `SELECT dependency_type FROM workspace_tension_dependencies WHERE workspace_id = ? AND tension_id = ? AND depends_on_tension_id = ?`, workspaceID, tensionID, dependsOnTensionID).Scan(&dependencyType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TensionDependencyEdge{
				WorkspaceID:        workspaceID,
				TensionID:          tensionID,
				DependsOnTensionID: dependsOnTensionID,
				DependencyType:     "BLOCKS",
			}, false, nil
		}
		return TensionDependencyEdge{}, false, err
	}
	res, err := q.ExecContext(ctx, `DELETE FROM workspace_tension_dependencies WHERE workspace_id = ? AND tension_id = ? AND depends_on_tension_id = ?`, workspaceID, tensionID, dependsOnTensionID)
	if err != nil {
		return TensionDependencyEdge{}, false, err
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return TensionDependencyEdge{}, false, err
	}
	return TensionDependencyEdge{
		WorkspaceID:        workspaceID,
		TensionID:          tensionID,
		DependsOnTensionID: dependsOnTensionID,
		DependencyType:     strings.TrimSpace(dependencyType),
	}, changed > 0, nil
}

func (s *Store) GetActiveTensionGraph(ctx context.Context, workspaceID string) (map[string][]string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}

	query := `SELECT d.tension_id, d.depends_on_tension_id
		FROM workspace_tension_dependencies d
		JOIN workspace_tensions t1 ON d.workspace_id = t1.workspace_id AND d.tension_id = t1.tension_id
		JOIN workspace_tensions t2 ON d.workspace_id = t2.workspace_id AND d.depends_on_tension_id = t2.tension_id
		WHERE d.workspace_id = ?
		  AND t1.lifecycle_state IN ('ACTIVE', 'EMERGENT')
		  AND t2.lifecycle_state IN ('ACTIVE', 'EMERGENT')`

	rows, err := s.db.QueryContext(ctx, query, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("query tension graph: %w", err)
	}
	defer rows.Close()

	graph := make(map[string][]string)
	for rows.Next() {
		var src, dst string
		if err := rows.Scan(&src, &dst); err != nil {
			return nil, err
		}
		graph[src] = append(graph[src], dst)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return graph, nil
}

// Coalition Management Data Layer for RRP-1.2

type WorkspaceCoalition struct {
	CoalitionID      string                     `json:"coalition_id"`
	WorkspaceID      string                     `json:"workspace_id"`
	TensionID        string                     `json:"tension_id"`
	SuccessCriterion string                     `json:"success_criterion"`
	SynergyScore     float64                    `json:"synergy_score"`
	TTLEpochs        int                        `json:"ttl_epochs"`
	Status           string                     `json:"status"` // FORMING, ACTIVE, DISBANDED
	CreatedEpoch     int                        `json:"created_epoch"`
	CreatedAt        string                     `json:"created_at"`
	UpdatedAt        string                     `json:"updated_at"`
	Members          []WorkspaceCoalitionMember `json:"members"`
}

type WorkspaceCoalitionMember struct {
	CoalitionID       string  `json:"coalition_id"`
	WorkspaceID       string  `json:"workspace_id"`
	AgentID           string  `json:"agent_id"`
	Role              string  `json:"role"` // GENERATOR, NEAR_REVIEWER, FAR_REVIEWER
	FitScore          float64 `json:"fit_score"`
	NoveltyScore      float64 `json:"novelty_score"`
	MinStayUntilEpoch int     `json:"min_stay_until_epoch"`
	JoinedAt          string  `json:"joined_at"`
}

var (
	ErrCoalitionExpired             = errors.New("coalition expired")
	ErrCoalitionCapacityReached     = errors.New("coalition size cap reached")
	ErrCoalitionMinimumTenureNotMet = errors.New("coalition member minimum tenure not met")
	ErrCoalitionAttachmentRejected  = errors.New("coalition attachment rejected by admissibility policy")
)

type coalitionQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type coalitionRecordCandidate struct {
	coalition   WorkspaceCoalition
	memberCount int
}

func coalitionSizeCapForTensionType(tensionType string) int {
	switch strings.ToLower(strings.TrimSpace(tensionType)) {
	case "bottleneck", "meta-tension", "meta_tension":
		return 4
	default:
		return 3
	}
}

func coalitionStatusPriority(status string) int {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ACTIVE":
		return 2
	case "FORMING":
		return 1
	default:
		return 0
	}
}

func betterCoalitionAuthorityCandidate(left, right coalitionRecordCandidate) bool {
	if left.memberCount != right.memberCount {
		return left.memberCount > right.memberCount
	}
	if coalitionStatusPriority(left.coalition.Status) != coalitionStatusPriority(right.coalition.Status) {
		return coalitionStatusPriority(left.coalition.Status) > coalitionStatusPriority(right.coalition.Status)
	}
	if left.coalition.CreatedEpoch != right.coalition.CreatedEpoch {
		return left.coalition.CreatedEpoch > right.coalition.CreatedEpoch
	}
	if left.coalition.UpdatedAt != right.coalition.UpdatedAt {
		return left.coalition.UpdatedAt > right.coalition.UpdatedAt
	}
	if left.coalition.CreatedAt != right.coalition.CreatedAt {
		return left.coalition.CreatedAt > right.coalition.CreatedAt
	}
	return left.coalition.CoalitionID > right.coalition.CoalitionID
}

func coalitionExpiredAtEpoch(coalition WorkspaceCoalition, currentEpoch int) bool {
	ttl := coalitionEffectiveTTLEpochs(coalition.TTLEpochs)
	return currentEpoch >= coalition.CreatedEpoch+ttl
}

func isMetaTensionType(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "meta-tension", "meta_tension":
		return true
	default:
		return false
	}
}

func coalitionMinStayEpochsForRole(role string) int {
	return 1
}

func coalitionIsActive(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "FORMING", "ACTIVE":
		return true
	default:
		return false
	}
}

func coalitionEffectiveTTLEpochs(ttlEpochs int) int {
	if ttlEpochs <= 0 {
		return 3
	}
	return ttlEpochs
}

func (s *Store) currentControlEpoch(ctx context.Context, q coalitionQueryer, workspaceID string) (int, error) {
	var currentEpoch int
	err := q.QueryRowContext(ctx,
		`SELECT current_epoch
		 FROM workspace_control_epochs
		 WHERE workspace_id = ?`,
		workspaceID).Scan(&currentEpoch)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("query control epoch: %w", err)
	}
	return currentEpoch, nil
}

func (s *Store) loadCoalitionRecordByID(ctx context.Context, q coalitionQueryer, workspaceID, coalitionID string) (*WorkspaceCoalition, error) {
	query := `SELECT coalition_id, workspace_id, tension_id, success_criterion, synergy_score, ttl_epochs, status, created_epoch, created_at, updated_at
		FROM workspace_coalitions
		WHERE workspace_id = ? AND coalition_id = ?`
	var c WorkspaceCoalition
	err := q.QueryRowContext(ctx, query, workspaceID, coalitionID).Scan(
		&c.CoalitionID, &c.WorkspaceID, &c.TensionID, &c.SuccessCriterion, &c.SynergyScore, &c.TTLEpochs, &c.Status, &c.CreatedEpoch, &c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("load coalition: %w", err)
	}
	return &c, nil
}

func (s *Store) loadCoalitionRecordByTension(ctx context.Context, q coalitionQueryer, workspaceID, tensionID string) (*WorkspaceCoalition, error) {
	query := `SELECT coalition_id, workspace_id, tension_id, success_criterion, synergy_score, ttl_epochs, status, created_epoch, created_at, updated_at
		FROM workspace_coalitions
		WHERE workspace_id = ? AND tension_id = ? AND status IN ('FORMING', 'ACTIVE')
		ORDER BY created_epoch DESC, created_at DESC LIMIT 1`
	var c WorkspaceCoalition
	err := q.QueryRowContext(ctx, query, workspaceID, tensionID).Scan(
		&c.CoalitionID, &c.WorkspaceID, &c.TensionID, &c.SuccessCriterion, &c.SynergyScore, &c.TTLEpochs, &c.Status, &c.CreatedEpoch, &c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("load coalition by tension: %w", err)
	}
	return &c, nil
}

func (s *Store) loadLiveCoalitionCandidatesByTension(ctx context.Context, q coalitionQueryer, workspaceID, tensionID string) ([]coalitionRecordCandidate, error) {
	rows, err := q.QueryContext(ctx, `SELECT c.coalition_id, c.workspace_id, c.tension_id, c.success_criterion, c.synergy_score, c.ttl_epochs, c.status, c.created_epoch, c.created_at, c.updated_at,
		COUNT(DISTINCT m.agent_id) AS member_count
		FROM workspace_coalitions c
		LEFT JOIN workspace_coalition_members m
			ON m.workspace_id = c.workspace_id AND m.coalition_id = c.coalition_id
		WHERE c.workspace_id = ? AND c.tension_id = ? AND c.status IN ('FORMING', 'ACTIVE')
		GROUP BY c.coalition_id, c.workspace_id, c.tension_id, c.success_criterion, c.synergy_score, c.ttl_epochs, c.status, c.created_epoch, c.created_at, c.updated_at`,
		workspaceID,
		tensionID,
	)
	if err != nil {
		return nil, fmt.Errorf("query coalition candidates by tension: %w", err)
	}
	defer rows.Close()

	candidates := make([]coalitionRecordCandidate, 0)
	for rows.Next() {
		var candidate coalitionRecordCandidate
		if err := rows.Scan(
			&candidate.coalition.CoalitionID,
			&candidate.coalition.WorkspaceID,
			&candidate.coalition.TensionID,
			&candidate.coalition.SuccessCriterion,
			&candidate.coalition.SynergyScore,
			&candidate.coalition.TTLEpochs,
			&candidate.coalition.Status,
			&candidate.coalition.CreatedEpoch,
			&candidate.coalition.CreatedAt,
			&candidate.coalition.UpdatedAt,
			&candidate.memberCount,
		); err != nil {
			return nil, fmt.Errorf("scan coalition candidate by tension: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate coalition candidates by tension: %w", err)
	}
	return candidates, nil
}

func (s *Store) selectLiveCoalitionCandidateByTension(ctx context.Context, q coalitionQueryer, workspaceID, tensionID string, currentEpoch int) (*coalitionRecordCandidate, []coalitionRecordCandidate, []coalitionRecordCandidate, error) {
	candidates, err := s.loadLiveCoalitionCandidatesByTension(ctx, q, workspaceID, tensionID)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(candidates) == 0 {
		return nil, nil, nil, nil
	}

	live := make([]coalitionRecordCandidate, 0, len(candidates))
	expired := make([]coalitionRecordCandidate, 0)
	for _, candidate := range candidates {
		if coalitionExpiredAtEpoch(candidate.coalition, currentEpoch) {
			expired = append(expired, candidate)
			continue
		}
		live = append(live, candidate)
	}
	if len(live) == 0 {
		return nil, nil, expired, nil
	}

	canonicalIdx := 0
	for idx := 1; idx < len(live); idx++ {
		if betterCoalitionAuthorityCandidate(live[idx], live[canonicalIdx]) {
			canonicalIdx = idx
		}
	}
	return &live[canonicalIdx], live, expired, nil
}

func (s *Store) reconcileLiveCoalitionCandidateByTension(ctx context.Context, q coalitionQueryer, workspaceID, tensionID string, currentEpoch int) (*coalitionRecordCandidate, error) {
	canonical, live, expired, err := s.selectLiveCoalitionCandidateByTension(ctx, q, workspaceID, tensionID, currentEpoch)
	if err != nil {
		return nil, err
	}
	for _, candidate := range expired {
		if err := s.disbandCoalition(ctx, q, &candidate.coalition); err != nil {
			return nil, err
		}
	}
	if canonical == nil {
		for _, candidate := range live {
			if err := s.disbandCoalition(ctx, q, &candidate.coalition); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	for idx := range live {
		if live[idx].coalition.CoalitionID == canonical.coalition.CoalitionID {
			continue
		}
		if err := s.disbandCoalition(ctx, q, &live[idx].coalition); err != nil {
			return nil, err
		}
	}
	canonicalCopy := *canonical
	return &canonicalCopy, nil
}

func (s *Store) loadCoalitionMembers(ctx context.Context, q coalitionQueryer, workspaceID, coalitionID string) ([]WorkspaceCoalitionMember, error) {
	rows, err := q.QueryContext(ctx, `SELECT agent_id, role, fit_score, novelty_score, min_stay_until_epoch, joined_at
		FROM workspace_coalition_members
		WHERE coalition_id = ? AND workspace_id = ?
		ORDER BY joined_at ASC`, coalitionID, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("get coalition members: %w", err)
	}
	defer rows.Close()

	var members []WorkspaceCoalitionMember
	for rows.Next() {
		var m WorkspaceCoalitionMember
		m.CoalitionID = coalitionID
		m.WorkspaceID = workspaceID
		if err := rows.Scan(&m.AgentID, &m.Role, &m.FitScore, &m.NoveltyScore, &m.MinStayUntilEpoch, &m.JoinedAt); err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return members, nil
}

func compareCoalitionGeneratorCandidate(left, right WorkspaceCoalitionMember) bool {
	if left.FitScore != right.FitScore {
		return left.FitScore > right.FitScore
	}
	if left.NoveltyScore != right.NoveltyScore {
		return left.NoveltyScore > right.NoveltyScore
	}
	if left.AgentID != right.AgentID {
		return left.AgentID < right.AgentID
	}
	return left.JoinedAt < right.JoinedAt
}

func compareCoalitionFarReviewerCandidate(left WorkspaceCoalitionMember, leftDistance float64, right WorkspaceCoalitionMember, rightDistance float64) bool {
	if math.Abs(leftDistance-rightDistance) > 0.000001 {
		return leftDistance > rightDistance
	}
	if left.FitScore != right.FitScore {
		return left.FitScore > right.FitScore
	}
	if left.NoveltyScore != right.NoveltyScore {
		return left.NoveltyScore > right.NoveltyScore
	}
	if left.AgentID != right.AgentID {
		return left.AgentID < right.AgentID
	}
	return left.JoinedAt < right.JoinedAt
}

func normalizedCoalitionMinStayUntilEpoch(member WorkspaceCoalitionMember, role string, currentEpoch int) int {
	target := currentEpoch + coalitionMinStayEpochsForRole(role)
	if member.Role == role {
		if member.MinStayUntilEpoch > 0 {
			return member.MinStayUntilEpoch
		}
		return target
	}
	return target
}

func (s *Store) normalizeCoalitionMemberRolesTx(ctx context.Context, tx *sql.Tx, workspaceID, coalitionID string, currentEpoch int) error {
	members, err := s.loadCoalitionMembers(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return nil
	}
	generator := members[0]
	for _, member := range members[1:] {
		if compareCoalitionGeneratorCandidate(member, generator) {
			generator = member
		}
	}

	var farReviewer WorkspaceCoalitionMember
	farReviewerDistance := -1.0
	hasFarReviewer := false
	for _, member := range members {
		if member.AgentID == generator.AgentID {
			continue
		}
		distance, hasEvidence, err := s.calculateCoalitionPairwiseDistanceStats(ctx, tx, workspaceID, generator.AgentID, member.AgentID)
		if err != nil {
			return fmt.Errorf("calculate coalition reviewer distance: %w", err)
		}
		if !hasEvidence {
			continue
		}
		if distance <= 0.6 {
			continue
		}
		if !hasFarReviewer || compareCoalitionFarReviewerCandidate(member, distance, farReviewer, farReviewerDistance) {
			farReviewer = member
			farReviewerDistance = distance
			hasFarReviewer = true
		}
	}

	for _, member := range members {
		role := "NEAR_REVIEWER"
		switch {
		case member.AgentID == generator.AgentID:
			role = "GENERATOR"
		case hasFarReviewer && member.AgentID == farReviewer.AgentID:
			role = "FAR_REVIEWER"
		}
		minStayUntil := normalizedCoalitionMinStayUntilEpoch(member, role, currentEpoch)
		if member.Role == role && member.MinStayUntilEpoch == minStayUntil {
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE workspace_coalition_members
			 SET role = ?, min_stay_until_epoch = ?
			 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`,
			role,
			minStayUntil,
			workspaceID,
			coalitionID,
			member.AgentID,
		); err != nil {
			return fmt.Errorf("normalize coalition member role: %w", err)
		}
	}
	return nil
}

func (s *Store) expireCoalitionIfNeeded(ctx context.Context, q coalitionQueryer, coalition *WorkspaceCoalition, currentEpoch int) (bool, error) {
	if coalition == nil {
		return false, nil
	}
	ttl := coalitionEffectiveTTLEpochs(coalition.TTLEpochs)
	if currentEpoch < coalition.CreatedEpoch+ttl {
		return false, nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := q.ExecContext(ctx,
		`UPDATE workspace_coalitions
		 SET status = 'DISBANDED', updated_at = ?
		 WHERE coalition_id = ? AND workspace_id = ?`,
		now, coalition.CoalitionID, coalition.WorkspaceID)
	if err != nil {
		return false, fmt.Errorf("expire coalition: %w", err)
	}
	return true, nil
}

func (s *Store) disbandCoalition(ctx context.Context, q coalitionQueryer, coalition *WorkspaceCoalition) error {
	if coalition == nil {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := q.ExecContext(ctx,
		`UPDATE workspace_coalitions
		 SET status = 'DISBANDED', updated_at = ?
		 WHERE coalition_id = ? AND workspace_id = ?`,
		now, coalition.CoalitionID, coalition.WorkspaceID)
	if err != nil {
		return fmt.Errorf("disband coalition: %w", err)
	}
	return nil
}

func (s *Store) disbandLiveCoalitionsForTension(ctx context.Context, q coalitionQueryer, workspaceID, tensionID string) error {
	candidates, err := s.loadLiveCoalitionCandidatesByTension(ctx, q, workspaceID, tensionID)
	if err != nil {
		return err
	}
	for idx := range candidates {
		if err := s.disbandCoalition(ctx, q, &candidates[idx].coalition); err != nil {
			return err
		}
	}
	return nil
}

// CreateCoalition creates or gets a primary coalition for a specific tension.
func (s *Store) CreateCoalition(ctx context.Context, workspaceID, tensionID, successCriterion string) (*WorkspaceCoalition, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	tensionID = strings.TrimSpace(tensionID)
	if workspaceID == "" || tensionID == "" {
		return nil, errors.New("workspace_id and tension_id are required")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin create coalition: %w", err)
	}
	defer tx.Rollback()

	coalition, _, _, err := s.createCoalitionTx(ctx, tx, workspaceID, tensionID, successCriterion)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit create coalition: %w", err)
	}
	current, err := s.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil || current != nil {
		return current, err
	}
	return &coalition, nil
}

func (s *Store) createCoalitionTx(ctx context.Context, tx *sql.Tx, workspaceID, tensionID, successCriterion string) (WorkspaceCoalition, TensionRecord, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	tensionID = strings.TrimSpace(tensionID)
	if workspaceID == "" || tensionID == "" {
		return WorkspaceCoalition{}, TensionRecord{}, false, errors.New("workspace_id and tension_id are required")
	}

	currentEpoch, err := s.currentControlEpoch(ctx, tx, workspaceID)
	if err != nil {
		return WorkspaceCoalition{}, TensionRecord{}, false, err
	}
	tension, err := s.loadTensionRecord(ctx, tx, workspaceID, tensionID)
	if err != nil {
		return WorkspaceCoalition{}, TensionRecord{}, false, err
	}
	if !coalitionEligibleTension(tension) {
		return WorkspaceCoalition{}, TensionRecord{}, false, fmt.Errorf("tension %s is not coalition-eligible", tensionID)
	}

	existingCandidate, err := s.reconcileLiveCoalitionCandidateByTension(ctx, tx, workspaceID, tensionID, currentEpoch)
	if err != nil {
		return WorkspaceCoalition{}, TensionRecord{}, false, err
	}
	if existingCandidate != nil {
		existing := existingCandidate.coalition
		members, err := s.loadCoalitionMembers(ctx, tx, workspaceID, existing.CoalitionID)
		if err != nil {
			return WorkspaceCoalition{}, TensionRecord{}, false, err
		}
		existing.Members = members
		return existing, tension, false, nil
	}

	coalitionID := nextID("coalition")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	query := `INSERT INTO workspace_coalitions
		(coalition_id, workspace_id, tension_id, success_criterion, synergy_score, ttl_epochs, status, created_epoch, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if _, err := tx.ExecContext(ctx, query, coalitionID, workspaceID, tensionID, successCriterion, 0.0, 3, "FORMING", currentEpoch, now, now); err != nil {
		return WorkspaceCoalition{}, TensionRecord{}, false, fmt.Errorf("create coalition: %w", err)
	}
	return WorkspaceCoalition{
		CoalitionID:      coalitionID,
		WorkspaceID:      workspaceID,
		TensionID:        tensionID,
		SuccessCriterion: strings.TrimSpace(successCriterion),
		SynergyScore:     0,
		TTLEpochs:        3,
		Status:           "FORMING",
		CreatedEpoch:     currentEpoch,
		CreatedAt:        now,
		UpdatedAt:        now,
	}, tension, true, nil
}

// GetTensionCoalition returns the active/forming coalition for a tension, along with its members.
func (s *Store) GetTensionCoalition(ctx context.Context, workspaceID, tensionID string) (*WorkspaceCoalition, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	tensionID = strings.TrimSpace(tensionID)

	currentEpoch, err := s.currentControlEpoch(ctx, s.db, workspaceID)
	if err != nil {
		return nil, err
	}
	candidate, _, _, err := s.selectLiveCoalitionCandidateByTension(ctx, s.db, workspaceID, tensionID, currentEpoch)
	if err != nil || candidate == nil {
		return nil, err
	}
	c := candidate.coalition
	tension, err := s.loadTensionRecord(ctx, nil, workspaceID, tensionID)
	if err != nil {
		if isTensionNotFoundErr(err) {
			return nil, nil
		}
		return nil, err
	}
	if !coalitionEligibleTension(tension) {
		return nil, nil
	}

	members, err := s.loadCoalitionMembers(ctx, s.db, workspaceID, c.CoalitionID)
	if err != nil {
		return nil, err
	}
	c.Members = members

	return &c, nil
}

// calculateJaccardDistance measures cognitive distance between two agents based on memory graph overlap.
func (s *Store) calculateJaccardDistance(ctx context.Context, tx *sql.Tx, workspaceID, agentA, agentB string) (float64, error) {
	distance, hasEvidence, err := s.calculateCoalitionPairwiseDistanceStats(ctx, tx, workspaceID, agentA, agentB)
	if err != nil {
		return 0, err
	}
	if !hasEvidence {
		return 0, nil
	}
	return distance, nil
}

// calculateJaccardDistanceReadRef measures cognitive distance but doesn't require an active write transaction.
func (s *Store) calculateJaccardDistanceReadRef(ctx context.Context, workspaceID, agentA, agentB string) (float64, error) {
	distance, hasEvidence, err := s.calculateCoalitionPairwiseDistanceStatsReadRef(ctx, workspaceID, agentA, agentB)
	if err != nil {
		return 0, err
	}
	if !hasEvidence {
		return 0, nil
	}
	return distance, nil
}

func memoryOverlapKeySQL(alias string) string {
	prefix := ""
	if trimmed := strings.TrimSpace(alias); trimmed != "" {
		prefix = trimmed + "."
	}
	return `CASE
		WHEN COALESCE(` + prefix + `source_kind, '') <> '' OR COALESCE(` + prefix + `source_id, '') <> '' THEN 'source:' || COALESCE(` + prefix + `source_kind, '') || ':' || COALESCE(` + prefix + `source_id, '')
		WHEN COALESCE(` + prefix + `origin_kind, '') <> '' OR COALESCE(` + prefix + `origin_id, '') <> '' THEN 'origin:' || COALESCE(` + prefix + `origin_kind, '') || ':' || COALESCE(` + prefix + `origin_id, '')
		ELSE 'memory:' || ` + prefix + `memory_id
	END`
}

const coalitionHistoricalPriorScale = 6.0

func normalizeHistoricalCoalitionPrior(score float64) float64 {
	if score <= 0 {
		return 0
	}
	return clampCoalitionSignal(score / coalitionHistoricalPriorScale)
}

func coalitionHasFarReviewer(members []WorkspaceCoalitionMember) bool {
	for _, member := range members {
		if strings.EqualFold(strings.TrimSpace(member.Role), "FAR_REVIEWER") {
			return true
		}
	}
	return false
}

func coalitionHasNearReviewer(members []WorkspaceCoalitionMember) bool {
	for _, member := range members {
		if strings.EqualFold(strings.TrimSpace(member.Role), "NEAR_REVIEWER") {
			return true
		}
	}
	return false
}

func coalitionRoleDiversity(members []WorkspaceCoalitionMember) float64 {
	if len(members) == 0 {
		return 0
	}
	distinct := map[string]struct{}{}
	for _, member := range members {
		if role := strings.ToUpper(strings.TrimSpace(member.Role)); role != "" {
			distinct[role] = struct{}{}
		}
	}
	return clampCoalitionSignal(float64(len(distinct)) / 3.0)
}

func coalitionActiveRoleLockPressure(members []WorkspaceCoalitionMember, currentEpoch int) float64 {
	if len(members) < 3 {
		return 0
	}

	activeLockedMembers := 0
	maxLockedRoleCount := 0
	activeLockedByRole := map[string]int{}
	for _, member := range members {
		if member.MinStayUntilEpoch <= currentEpoch {
			continue
		}
		role := strings.ToUpper(strings.TrimSpace(member.Role))
		if role == "" || role == "GENERATOR" {
			continue
		}
		activeLockedMembers++
		activeLockedByRole[role]++
		if activeLockedByRole[role] > maxLockedRoleCount {
			maxLockedRoleCount = activeLockedByRole[role]
		}
	}
	if activeLockedMembers == 0 || maxLockedRoleCount < 2 {
		return 0
	}

	pressure := 0.20 * float64(activeLockedMembers) / float64(len(members))
	pressure += 0.35 * float64(maxLockedRoleCount-1) / float64(len(members)-1)
	if activeLockedByRole["NEAR_REVIEWER"] >= 2 {
		pressure += 0.15
	}
	return clampCoalitionSignal(pressure)
}

func coalitionActiveGeneratorLockPressure(members []WorkspaceCoalitionMember, currentEpoch int) float64 {
	if len(members) < 2 || coalitionHasFarReviewer(members) {
		return 0
	}

	for _, member := range members {
		if !strings.EqualFold(strings.TrimSpace(member.Role), "GENERATOR") {
			continue
		}
		if member.MinStayUntilEpoch <= currentEpoch {
			return 0
		}

		pressure := 0.25
		pressure += 0.15 * (1.0 - coalitionRoleDiversity(members))
		if len(members) >= 3 {
			pressure += 0.10
		}
		return clampCoalitionSignal(pressure)
	}
	return 0
}

func coalitionActiveFarReviewerLockPressure(members []WorkspaceCoalitionMember, currentEpoch int) float64 {
	if len(members) < 2 || coalitionHasNearReviewer(members) {
		return 0
	}

	for _, member := range members {
		if !strings.EqualFold(strings.TrimSpace(member.Role), "FAR_REVIEWER") {
			continue
		}
		if member.MinStayUntilEpoch <= currentEpoch {
			return 0
		}

		pressure := 0.25
		pressure += 0.15 * (1.0 - coalitionRoleDiversity(members))
		if len(members) >= 3 {
			pressure += 0.10
		}
		return clampCoalitionSignal(pressure)
	}
	return 0
}

func coalitionMeanMemberSignal(members []WorkspaceCoalitionMember, selector func(WorkspaceCoalitionMember) float64) float64 {
	if len(members) == 0 {
		return 0
	}
	sum := 0.0
	for _, member := range members {
		sum += clampCoalitionSignal(selector(member))
	}
	return clampCoalitionSignal(sum / float64(len(members)))
}

func coalitionGoalSignal(members []WorkspaceCoalitionMember) float64 {
	if len(members) == 0 {
		return 0
	}
	meanFit := coalitionMeanMemberSignal(members, func(member WorkspaceCoalitionMember) float64 {
		return member.FitScore
	})
	minFit := 1.0
	for _, member := range members {
		fit := clampCoalitionSignal(member.FitScore)
		if fit < minFit {
			minFit = fit
		}
	}
	return clampCoalitionSignal(0.75*meanFit + 0.25*minFit)
}

func coalitionGoalEvidenceRetention(evidenceCoverage float64, memberCount int) float64 {
	if memberCount < 3 {
		return 1.0
	}
	return clampCoalitionSignal(0.85 + 0.15*clampCoalitionSignal(evidenceCoverage))
}

func coalitionGoalTopologyRetention(overlapTopologyFactor float64, memberCount int) float64 {
	if memberCount < 4 {
		return 1.0
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(overlapTopologyFactor))
}

func coalitionGoalComplementarityRetention(pairwiseDistance float64, memberCount int) float64 {
	if memberCount < 3 {
		return 1.0
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(pairwiseDistance))
}

func coalitionGoalRoleDiversityRetention(roleDiversity float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 3 {
		return 1.0
	}
	if hasFarReviewer {
		return clampCoalitionSignal(0.92 + 0.08*clampCoalitionSignal(roleDiversity))
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(roleDiversity))
}

func coalitionGoalNoveltyRetention(baseNovelty float64, memberCount int) float64 {
	if memberCount < 3 {
		return 1.0
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(baseNovelty))
}

func coalitionGoalScoreSignal(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor float64, memberCount int) float64 {
	signal := clampCoalitionSignal(goal)
	signal *= coalitionGoalEvidenceRetention(evidenceCoverage, memberCount)
	signal *= coalitionGoalComplementarityRetention(pairwiseDistance, memberCount)
	signal *= coalitionGoalTopologyRetention(overlapTopologyFactor, memberCount)
	return clampCoalitionSignal(signal)
}

func coalitionGoalLockRetention(roleLockPressure, generatorLockPressure, farReviewerLockPressure float64) float64 {
	strongestLockPressure := math.Max(
		clampCoalitionSignal(roleLockPressure),
		math.Max(
			clampCoalitionSignal(generatorLockPressure),
			clampCoalitionSignal(farReviewerLockPressure),
		),
	)
	return clampCoalitionSignal(1.0 - 0.10*strongestLockPressure)
}

func coalitionBaseNoveltySignal(members []WorkspaceCoalitionMember) float64 {
	if len(members) == 0 {
		return 0
	}
	meanNovelty := coalitionMeanMemberSignal(members, func(member WorkspaceCoalitionMember) float64 {
		return member.NoveltyScore
	})
	minNovelty := 1.0
	for _, member := range members {
		novelty := clampCoalitionSignal(member.NoveltyScore)
		if novelty < minNovelty {
			minNovelty = novelty
		}
	}
	return clampCoalitionSignal(0.80*meanNovelty + 0.20*minNovelty)
}

func coalitionNoveltyComplementarityRetention(pairwiseDistance float64, memberCount int) float64 {
	if memberCount < 3 {
		return 1.0
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(pairwiseDistance))
}

func coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor float64, memberCount int) float64 {
	noveltySignal := clampCoalitionSignal(novelty)
	noveltySignal *= clampCoalitionSignal(0.5 + 0.5*goal)
	noveltySignal *= clampCoalitionSignal(0.6 + 0.4*evidenceCoverage)
	noveltySignal *= coalitionNoveltyComplementarityRetention(pairwiseDistance, memberCount)
	if memberCount >= 4 && overlapTopologyFactor < 1.0 {
		noveltySignal *= clampCoalitionSignal(1.0 - 0.15*(1.0-overlapTopologyFactor))
	}
	return clampCoalitionSignal(noveltySignal)
}

func coalitionNoveltyLockRetention(roleLockPressure, generatorLockPressure, farReviewerLockPressure float64) float64 {
	strongestLockPressure := math.Max(
		clampCoalitionSignal(roleLockPressure),
		math.Max(
			clampCoalitionSignal(generatorLockPressure),
			clampCoalitionSignal(farReviewerLockPressure),
		),
	)
	return clampCoalitionSignal(1.0 - 0.15*strongestLockPressure)
}

func coalitionNoveltyRoleDiversityRetention(roleDiversity float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 3 {
		return 1.0
	}
	if hasFarReviewer {
		return clampCoalitionSignal(0.92 + 0.08*clampCoalitionSignal(roleDiversity))
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(roleDiversity))
}

func coalitionGenericRoleDiversityNoveltyRetention(baseNovelty float64, memberCount int) float64 {
	if memberCount < 3 {
		return 1.0
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(baseNovelty))
}

func coalitionReviewerDiversityLockRetention(roleLockPressure, farReviewerLockPressure float64) float64 {
	strongestLockPressure := math.Max(
		clampCoalitionSignal(roleLockPressure),
		clampCoalitionSignal(farReviewerLockPressure),
	)
	return clampCoalitionSignal(1.0 - 0.10*strongestLockPressure)
}

func coalitionReviewerDiversityNoveltyRetention(baseNovelty float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 3 || !hasFarReviewer {
		return 1.0
	}
	return clampCoalitionSignal(0.92 + 0.08*clampCoalitionSignal(baseNovelty))
}

func coalitionGenericRoleDiversityLockRetention(roleLockPressure float64) float64 {
	return clampCoalitionSignal(1.0 - 0.10*clampCoalitionSignal(roleLockPressure))
}

func coalitionGenericRoleDiversityTopologyRetention(overlapTopologyFactor float64, memberCount int) float64 {
	if memberCount < 4 {
		return 1.0
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(overlapTopologyFactor))
}

func coalitionReviewerDiversityGoalRetention(goal float64) float64 {
	return clampCoalitionSignal(0.80 + 0.20*goal)
}

func coalitionRoleDiversityGoalRetention(goal float64, hasFarReviewer bool) float64 {
	if hasFarReviewer {
		return coalitionReviewerDiversityGoalRetention(goal)
	}
	return clampCoalitionSignal(0.85 + 0.15*goal)
}

func coalitionRoleDiversityEvidenceRetention(evidenceCoverage float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 3 {
		return 1.0
	}
	if hasFarReviewer {
		return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(evidenceCoverage))
	}
	return clampCoalitionSignal(0.85 + 0.15*clampCoalitionSignal(evidenceCoverage))
}

func coalitionRoleDiversityComplementarityRetention(pairwiseDistance float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 3 {
		return 1.0
	}
	if !hasFarReviewer {
		return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(pairwiseDistance))
	}
	return clampCoalitionSignal(0.85 + 0.15*clampCoalitionSignal(pairwiseDistance))
}

func coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, roleLockPressure, farReviewerLockPressure float64, memberCount int, hasFarReviewer bool) float64 {
	signal := clampCoalitionSignal(roleDiversity)
	signal *= coalitionRoleDiversityGoalRetention(goal, hasFarReviewer)
	signal *= coalitionRoleDiversityEvidenceRetention(evidenceCoverage, memberCount, hasFarReviewer)
	signal *= coalitionRoleDiversityComplementarityRetention(pairwiseDistance, memberCount, hasFarReviewer)
	if hasFarReviewer {
		if memberCount >= 4 {
			signal *= overlapTopologyFactor
		}
		signal *= coalitionReviewerDiversityLockRetention(roleLockPressure, farReviewerLockPressure)
	} else {
		signal *= coalitionGenericRoleDiversityTopologyRetention(overlapTopologyFactor, memberCount)
		signal *= coalitionGenericRoleDiversityLockRetention(roleLockPressure)
	}
	return clampCoalitionSignal(signal)
}

func coalitionFarReviewerBonusComplementarityRetention(pairwiseDistance float64, memberCount int) float64 {
	if memberCount < 3 {
		return 1.0
	}
	return clampCoalitionSignal(0.85 + 0.15*clampCoalitionSignal(pairwiseDistance))
}

func coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor float64, memberCount int) float64 {
	signal := 0.15 * clampCoalitionSignal(evidenceCoverage)
	signal *= coalitionFarReviewerBonusComplementarityRetention(pairwiseDistance, memberCount)
	signal *= clampCoalitionSignal(0.75 + 0.25*goal)
	if memberCount >= 4 {
		signal *= overlapTopologyFactor
	}
	return clampCoalitionSignal(signal)
}

func coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity float64) float64 {
	return clampCoalitionSignal(0.85 + 0.15*clampCoalitionSignal(roleDiversity))
}

func coalitionFarReviewerBonusNoveltyRetention(baseNovelty float64) float64 {
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(baseNovelty))
}

func coalitionFarReviewerBonusLockRetention(roleLockPressure, farReviewerLockPressure float64) float64 {
	strongestLockPressure := math.Max(
		clampCoalitionSignal(roleLockPressure),
		clampCoalitionSignal(farReviewerLockPressure),
	)
	return clampCoalitionSignal(1.0 - 0.10*strongestLockPressure)
}

func coalitionPairwiseDistanceLockRetention(roleLockPressure, generatorLockPressure, farReviewerLockPressure float64) float64 {
	strongestLockPressure := math.Max(
		clampCoalitionSignal(roleLockPressure),
		math.Max(
			clampCoalitionSignal(generatorLockPressure),
			clampCoalitionSignal(farReviewerLockPressure),
		),
	)
	return clampCoalitionSignal(1.0 - 0.10*strongestLockPressure)
}

func coalitionPairwiseDistanceRoleDiversityRetention(roleDiversity float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 3 {
		return 1.0
	}
	if hasFarReviewer {
		return clampCoalitionSignal(0.92 + 0.08*clampCoalitionSignal(roleDiversity))
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(roleDiversity))
}

func coalitionPairwiseDistanceNoveltyRetention(baseNovelty float64, memberCount int) float64 {
	if memberCount < 3 {
		return 1.0
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(baseNovelty))
}

func coalitionPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor float64) float64 {
	signal := clampCoalitionSignal(pairwiseDistance)
	signal *= clampCoalitionSignal(0.5 + 0.5*evidenceCoverage)
	signal *= clampCoalitionSignal(overlapTopologyFactor)
	signal *= clampCoalitionSignal(0.75 + 0.25*goal)
	return clampCoalitionSignal(signal)
}

func coalitionCoordinationGoalPenalty(goal float64, memberCount int) float64 {
	if memberCount < 3 {
		return 0
	}
	return 0.08 * (1.0 - clampCoalitionSignal(goal))
}

func coalitionCoordinationComplementarityPenalty(pairwiseDistance float64, memberCount int) float64 {
	if memberCount < 3 {
		return 0
	}
	return 0.05 * (1.0 - clampCoalitionSignal(pairwiseDistance))
}

func coalitionCoordinationRoleDiversityPenalty(roleDiversity float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 3 {
		return 0
	}
	if hasFarReviewer {
		return 0.02 * (1.0 - clampCoalitionSignal(roleDiversity))
	}
	return 0.04 * (1.0 - clampCoalitionSignal(roleDiversity))
}

func coalitionCoordinationNoveltyPenalty(baseNovelty float64, memberCount int) float64 {
	if memberCount < 3 {
		return 0
	}
	return 0.03 * (1.0 - clampCoalitionSignal(baseNovelty))
}

func coalitionCoordinationLockPenalty(roleLockPressure, generatorLockPressure, farReviewerLockPressure float64, memberCount int) float64 {
	if memberCount < 3 {
		return 0
	}
	strongestLockPressure := math.Max(
		clampCoalitionSignal(roleLockPressure),
		math.Max(
			clampCoalitionSignal(generatorLockPressure),
			clampCoalitionSignal(farReviewerLockPressure),
		),
	)
	return 0.06 * strongestLockPressure
}

func coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 3 {
		return 0
	}
	if hasFarReviewer {
		return 0.04 * (1.0 - clampCoalitionSignal(evidenceCoverage))
	}
	return 0.08 * (1.0 - clampCoalitionSignal(evidenceCoverage))
}

func coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 3 {
		return 0
	}
	if hasFarReviewer {
		return 0.03 * (1.0 - clampCoalitionSignal(pairwiseDistance))
	}
	return 0.06 * (1.0 - clampCoalitionSignal(pairwiseDistance))
}

func coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 4 {
		return 0
	}
	if hasFarReviewer {
		return 0.03 * (1.0 - clampCoalitionSignal(overlapTopologyFactor))
	}
	return 0.05 * (1.0 - clampCoalitionSignal(overlapTopologyFactor))
}

func coalitionLockPenaltyNoveltySurcharge(baseNovelty float64, memberCount int, hasFarReviewer bool) float64 {
	if memberCount < 3 {
		return 0
	}
	if hasFarReviewer {
		return 0.02 * (1.0 - clampCoalitionSignal(baseNovelty))
	}
	return 0.04 * (1.0 - clampCoalitionSignal(baseNovelty))
}

func coalitionHistoricalPriorLockRetention(roleLockPressure, generatorLockPressure, farReviewerLockPressure float64) float64 {
	strongestLockPressure := math.Max(
		clampCoalitionSignal(roleLockPressure),
		math.Max(
			clampCoalitionSignal(generatorLockPressure),
			clampCoalitionSignal(farReviewerLockPressure),
		),
	)
	return clampCoalitionSignal(1.0 - 0.25*strongestLockPressure)
}

func coalitionHistoricalPriorNoveltyRetention(baseNovelty float64) float64 {
	return clampCoalitionSignal(0.80 + 0.20*clampCoalitionSignal(baseNovelty))
}

func coalitionHistoricalPriorRoleDiversityRetention(roleDiversity float64, hasFarReviewer bool) float64 {
	if hasFarReviewer {
		return clampCoalitionSignal(0.88 + 0.12*clampCoalitionSignal(roleDiversity))
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(roleDiversity))
}

func coalitionHistoricalPriorRoleDiversityNoveltyRetention(baseNovelty float64, hasFarReviewer bool) float64 {
	if hasFarReviewer {
		return clampCoalitionSignal(0.88 + 0.12*clampCoalitionSignal(baseNovelty))
	}
	return clampCoalitionSignal(0.90 + 0.10*clampCoalitionSignal(baseNovelty))
}

func coalitionHistoricalPriorComplementarityRetention(pairwiseDistance float64) float64 {
	return clampCoalitionSignal(0.85 + 0.15*clampCoalitionSignal(pairwiseDistance))
}

func coalitionHistoricalPriorSignal(previousScore, baseNovelty, evidenceCoverage, overlapTopologyFactor, pairwiseDistance, goal, roleLockPressure, generatorLockPressure, farReviewerLockPressure float64) float64 {
	priorSignal := normalizeHistoricalCoalitionPrior(previousScore)
	priorSignal *= coalitionHistoricalPriorNoveltyRetention(baseNovelty)
	priorSignal *= clampCoalitionSignal(evidenceCoverage)
	priorSignal *= clampCoalitionSignal(overlapTopologyFactor)
	priorSignal *= coalitionHistoricalPriorComplementarityRetention(pairwiseDistance)
	priorSignal *= clampCoalitionSignal(0.5 + 0.5*goal)
	priorSignal *= coalitionHistoricalPriorLockRetention(roleLockPressure, generatorLockPressure, farReviewerLockPressure)
	return clampCoalitionSignal(priorSignal)
}

func coalitionOverlapConnectedComponentCount(adjacency [][]int) int {
	if len(adjacency) == 0 {
		return 0
	}
	visited := make([]bool, len(adjacency))
	components := 0
	stack := make([]int, 0, len(adjacency))
	for idx := range adjacency {
		if visited[idx] {
			continue
		}
		components++
		stack = append(stack[:0], idx)
		visited[idx] = true
		for len(stack) > 0 {
			last := len(stack) - 1
			current := stack[last]
			stack = stack[:last]
			for _, next := range adjacency[current] {
				if next < 0 || next >= len(adjacency) || visited[next] {
					continue
				}
				visited[next] = true
				stack = append(stack, next)
			}
		}
	}
	return components
}

func (s *Store) calculateCoalitionPairwiseFootprintStats(ctx context.Context, q coalitionQueryer, workspaceID, agentA, agentB string) (int, int, int, error) {
	queryCounts := `SELECT agent_id, COUNT(DISTINCT ` + memoryOverlapKeySQL("") + `)
		FROM memory_nodes
		WHERE workspace_id = ? AND agent_id IN (?, ?)
		GROUP BY agent_id`

	rows, err := q.QueryContext(ctx, queryCounts, workspaceID, agentA, agentB)
	if err != nil {
		return 0, 0, 0, err
	}
	defer rows.Close()

	footprints := map[string]int{}
	for rows.Next() {
		var currentAgentID string
		var count int
		if err := rows.Scan(&currentAgentID, &count); err != nil {
			return 0, 0, 0, err
		}
		footprints[currentAgentID] = count
	}
	if err := rows.Err(); err != nil {
		return 0, 0, 0, err
	}

	countA := footprints[agentA]
	countB := footprints[agentB]
	if countA == 0 || countB == 0 {
		return countA, countB, 0, nil
	}

	overlapKeyA := memoryOverlapKeySQL("a")
	overlapKeyB := memoryOverlapKeySQL("b")
	queryIntersect := `
		SELECT COUNT(DISTINCT ` + overlapKeyA + `)
		FROM memory_nodes a
		JOIN memory_nodes b ON ` + overlapKeyA + ` = ` + overlapKeyB + `
		WHERE a.workspace_id = ? AND b.workspace_id = ?
		  AND a.agent_id = ? AND b.agent_id = ?`

	var intersectCount int
	if err := q.QueryRowContext(ctx, queryIntersect, workspaceID, workspaceID, agentA, agentB).Scan(&intersectCount); err != nil {
		return 0, 0, 0, err
	}

	return countA, countB, intersectCount, nil
}

func (s *Store) calculateCoalitionPairwiseDistanceStats(ctx context.Context, q coalitionQueryer, workspaceID, agentA, agentB string) (float64, bool, error) {
	countA, countB, intersectCount, err := s.calculateCoalitionPairwiseFootprintStats(ctx, q, workspaceID, agentA, agentB)
	if err != nil {
		return 0, false, err
	}
	if countA == 0 || countB == 0 {
		return 0, false, nil
	}
	unionCount := countA + countB - intersectCount
	if unionCount <= 0 {
		return 0, false, nil
	}
	return 1.0 - float64(intersectCount)/float64(unionCount), true, nil
}

func (s *Store) calculateCoalitionPairwiseDistanceStatsReadRef(ctx context.Context, workspaceID, agentA, agentB string) (float64, bool, error) {
	return s.calculateCoalitionPairwiseDistanceStats(ctx, s.db, workspaceID, agentA, agentB)
}

func (s *Store) calculateCoalitionSynergyScore(ctx context.Context, q coalitionQueryer, workspaceID string, previousScore float64, members []WorkspaceCoalitionMember, currentEpoch int) (float64, error) {
	if len(members) < 2 {
		return 0.0, nil
	}

	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)

	overlapAdjacency := make([][]int, len(members))
	overlapEdgeCount := 0
	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := s.calculateCoalitionPairwiseDistanceStats(ctx, q, workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				return 0, fmt.Errorf("calculate coalition pair distance: %w", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdgeCount++
				overlapAdjacency[idx] = append(overlapAdjacency[idx], jdx)
				overlapAdjacency[jdx] = append(overlapAdjacency[jdx], idx)
			}
		}
	}

	evidenceCoverage := 0.0
	pairwiseDistance := 0.0
	if totalPairs > 0 {
		evidenceCoverage = float64(evidencePairs) / float64(totalPairs)
	}
	if evidencePairs > 0 {
		pairwiseDistance = distanceSum / float64(evidencePairs)
	}
	overlapComponents := coalitionOverlapConnectedComponentCount(overlapAdjacency)
	overlapTopologyFactor := 1.0
	if len(members) >= 4 {
		if overlapComponents > 1 {
			overlapTopologyFactor = clampCoalitionSignal(1.0 - float64(overlapComponents-1)/float64(len(members)-1))
		} else if overlapEdgeCount > 0 && overlapEdgeCount < len(members) {
			overlapTopologyFactor = clampCoalitionSignal(float64(overlapEdgeCount) / float64(len(members)))
		}
	}
	roleLockPressure := coalitionActiveRoleLockPressure(members, currentEpoch)
	generatorLockPressure := coalitionActiveGeneratorLockPressure(members, currentEpoch)
	farReviewerLockPressure := coalitionActiveFarReviewerLockPressure(members, currentEpoch)
	roleDiversity := coalitionRoleDiversity(members)
	hasFarReviewer := coalitionHasFarReviewer(members)
	goalScore := coalitionGoalScoreSignal(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))
	goalScore *= coalitionGoalRoleDiversityRetention(roleDiversity, len(members), hasFarReviewer)
	goalScore *= coalitionGoalNoveltyRetention(novelty, len(members))
	goalScore *= coalitionGoalLockRetention(roleLockPressure, generatorLockPressure, farReviewerLockPressure)
	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), hasFarReviewer)
	noveltySignal *= coalitionNoveltyLockRetention(roleLockPressure, generatorLockPressure, farReviewerLockPressure)
	pairwiseDistanceSignal := coalitionPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor)
	pairwiseDistanceSignal *= coalitionPairwiseDistanceRoleDiversityRetention(roleDiversity, len(members), hasFarReviewer)
	pairwiseDistanceSignal *= coalitionPairwiseDistanceNoveltyRetention(novelty, len(members))
	pairwiseDistanceSignal *= coalitionPairwiseDistanceLockRetention(roleLockPressure, generatorLockPressure, farReviewerLockPressure)

	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, roleLockPressure, farReviewerLockPressure, len(members), hasFarReviewer)
	if hasFarReviewer {
		roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	} else {
		roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	}
	farReviewerBonus := 0.0
	if hasFarReviewer {
		farReviewerBonus = coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, len(members))
		farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
		farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
		farReviewerBonus *= coalitionFarReviewerBonusLockRetention(roleLockPressure, farReviewerLockPressure)
	}
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)
	prior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, overlapTopologyFactor, pairwiseDistance, goal, roleLockPressure, generatorLockPressure, farReviewerLockPressure)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, hasFarReviewer)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, hasFarReviewer)

	memberCount := len(members)
	coord := 0.35 + 0.22*float64(memberCount) + 0.06*float64(memberCount*memberCount)
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, memberCount)
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, memberCount)
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, memberCount, hasFarReviewer)
	coord += coalitionCoordinationNoveltyPenalty(novelty, memberCount)
	coord += coalitionCoordinationLockPenalty(roleLockPressure, generatorLockPressure, farReviewerLockPressure, memberCount)
	if memberCount >= 3 {
		coord += 0.10 * (1.0 - evidenceCoverage)
		if !coalitionHasFarReviewer(members) {
			coord += 0.10
		}
	}
	if memberCount >= 4 {
		if overlapComponents > 1 {
			coord += 0.12 * float64(overlapComponents-1) / float64(memberCount-1)
		} else if overlapEdgeCount > 0 && overlapEdgeCount < memberCount {
			coord += 0.08 * (1.0 - overlapTopologyFactor)
		}
	}

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	if memberCount >= 3 && !coalitionHasFarReviewer(members) {
		lockPenalty += 0.35
	}
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, memberCount, coalitionHasFarReviewer(members))
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, memberCount, coalitionHasFarReviewer(members))
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, memberCount, coalitionHasFarReviewer(members))
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, memberCount, coalitionHasFarReviewer(members))
	lockPenalty += 0.50 * roleLockPressure
	lockPenalty += 0.45 * generatorLockPressure
	lockPenalty += 0.40 * farReviewerLockPressure

	return CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty), nil
}

type coalitionMemberTxResult struct {
	Factors       AgentAttachmentFactors
	Coalition     WorkspaceCoalition
	Tension       TensionRecord
	Member        WorkspaceCoalitionMember
	Changed       bool
	CommitOnError bool
}

// AddCoalitionMember attaches an agent to a coalition and recalculates the synergy hypergraph score.
func (s *Store) AddCoalitionMember(ctx context.Context, workspaceID, coalitionID, agentID string, fitScore, noveltyScore float64) error {
	_, err := s.addCoalitionMember(ctx, workspaceID, coalitionID, agentID, &AgentAttachmentFactors{
		Fit:     fitScore,
		Novelty: noveltyScore,
	})
	return err
}

func (s *Store) AddCoalitionMemberWithHeuristicFactors(ctx context.Context, workspaceID, coalitionID, agentID string) (AgentAttachmentFactors, error) {
	result, err := s.addCoalitionMember(ctx, workspaceID, coalitionID, agentID, nil)
	return result.Factors, err
}

func (s *Store) addCoalitionMember(ctx context.Context, workspaceID, coalitionID, agentID string, provided *AgentAttachmentFactors) (coalitionMemberTxResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	coalitionID = strings.TrimSpace(coalitionID)
	agentID = strings.TrimSpace(agentID)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return coalitionMemberTxResult{}, fmt.Errorf("begin add coalition member: %w", err)
	}
	defer tx.Rollback()

	result, err := s.addCoalitionMemberTx(ctx, tx, workspaceID, coalitionID, agentID, provided)
	if err != nil {
		if result.CommitOnError {
			if commitErr := tx.Commit(); commitErr != nil {
				return coalitionMemberTxResult{}, fmt.Errorf("commit coalition cleanup after add failure: %w", commitErr)
			}
		}
		return coalitionMemberTxResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return coalitionMemberTxResult{}, err
	}
	return result, nil
}

func (s *Store) addCoalitionMemberTx(ctx context.Context, tx *sql.Tx, workspaceID, coalitionID, agentID string, provided *AgentAttachmentFactors) (coalitionMemberTxResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	coalitionID = strings.TrimSpace(coalitionID)
	agentID = strings.TrimSpace(agentID)

	coalition, err := s.loadCoalitionRecordByID(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}
	if coalition == nil {
		return coalitionMemberTxResult{}, fmt.Errorf("coalition not found")
	}
	if !coalitionIsActive(coalition.Status) {
		return coalitionMemberTxResult{}, ErrCoalitionExpired
	}
	currentEpoch, err := s.currentControlEpoch(ctx, tx, workspaceID)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}
	if expired, err := s.expireCoalitionIfNeeded(ctx, tx, coalition, currentEpoch); err != nil {
		return coalitionMemberTxResult{}, err
	} else if expired {
		return coalitionMemberTxResult{CommitOnError: true}, ErrCoalitionExpired
	}
	canonical, err := s.reconcileLiveCoalitionCandidateByTension(ctx, tx, workspaceID, coalition.TensionID, currentEpoch)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}
	if canonical == nil || canonical.coalition.CoalitionID != coalition.CoalitionID {
		return coalitionMemberTxResult{CommitOnError: true}, ErrCoalitionExpired
	}

	tension, err := s.loadTensionRecord(ctx, tx, workspaceID, coalition.TensionID)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}
	if !coalitionEligibleTension(tension) {
		return coalitionMemberTxResult{}, fmt.Errorf("coalition tension %s is not coalition-eligible", coalition.TensionID)
	}
	capacity := coalitionSizeCapForTensionType(tension.TensionType)

	existingMembers, err := s.loadCoalitionMembers(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}
	assignedRole := "GENERATOR"
	for _, member := range existingMembers {
		if member.AgentID == agentID {
			coalition.Members = existingMembers
			result := coalitionMemberTxResult{
				Coalition: *coalition,
				Tension:   tension,
				Member:    member,
				Changed:   false,
			}
			if provided != nil {
				result.Factors = AgentAttachmentFactors{
					Fit:     clampCoalitionSignal(provided.Fit),
					Novelty: clampCoalitionSignal(provided.Novelty),
				}
				return result, nil
			}
			currentSession, err := s.latestActiveAgentSessionState(ctx, workspaceID, agentID)
			if err != nil {
				return coalitionMemberTxResult{}, fmt.Errorf("load agent attachment session context: %w", err)
			}
			historyMass, err := s.agentAttachmentHistoryMass(ctx, workspaceID, agentID)
			if err != nil {
				return coalitionMemberTxResult{}, fmt.Errorf("load agent attachment history: %w", err)
			}
			factors, err := s.buildAgentAttachmentFactors(ctx, workspaceID, agentID, tension, coalition, currentSession, agentExplorationPrior(historyMass))
			if err != nil {
				return coalitionMemberTxResult{}, err
			}
			result.Factors = factors
			return result, nil
		}
	}
	if len(existingMembers) > 0 {
		assignedRole = "NEAR_REVIEWER"
	}
	if len(existingMembers) >= capacity {
		return coalitionMemberTxResult{}, ErrCoalitionCapacityReached
	}

	factors := AgentAttachmentFactors{}
	currentSession, err := s.latestActiveAgentSessionState(ctx, workspaceID, agentID)
	if err != nil {
		return coalitionMemberTxResult{}, fmt.Errorf("load agent attachment session context: %w", err)
	}
	historyMass, err := s.agentAttachmentHistoryMass(ctx, workspaceID, agentID)
	if err != nil {
		return coalitionMemberTxResult{}, fmt.Errorf("load agent attachment history: %w", err)
	}
	coalitionSnapshot := &WorkspaceCoalition{
		CoalitionID: coalitionID,
		WorkspaceID: workspaceID,
		TensionID:   coalition.TensionID,
		Members:     existingMembers,
	}
	factors, err = s.buildAgentAttachmentFactors(ctx, workspaceID, agentID, tension, coalitionSnapshot, currentSession, agentExplorationPrior(historyMass))
	if err != nil {
		return coalitionMemberTxResult{}, err
	}
	decision := evaluateAttachmentDecision(tension, factors)
	if decision.State == AttachmentDecisionRejected {
		return coalitionMemberTxResult{}, fmt.Errorf("%w: %s", ErrCoalitionAttachmentRejected, strings.Join(decision.Reasons, ", "))
	}
	if provided != nil {
		factors.Fit = clampCoalitionSignal(provided.Fit)
		factors.Novelty = clampCoalitionSignal(provided.Novelty)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	minStayUntil := currentEpoch + coalitionMinStayEpochsForRole(assignedRole)
	query := `INSERT INTO workspace_coalition_members
		(coalition_id, workspace_id, agent_id, role, fit_score, novelty_score, min_stay_until_epoch, joined_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err = tx.ExecContext(ctx, query, coalitionID, workspaceID, agentID, assignedRole, factors.Fit, factors.Novelty, minStayUntil, now)
	if err != nil {
		return coalitionMemberTxResult{}, fmt.Errorf("insert coalition member: %w", err)
	}
	if err := s.normalizeCoalitionMemberRolesTx(ctx, tx, workspaceID, coalitionID, currentEpoch); err != nil {
		return coalitionMemberTxResult{}, err
	}
	currentMembers, err := s.loadCoalitionMembers(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return coalitionMemberTxResult{}, fmt.Errorf("load coalition members after attach: %w", err)
	}

	// Calculate new total members
	count := len(currentMembers)

	// RRP-05 member-aware coalition scoring from existing fit/diversity signals.
	synScore, err := s.calculateCoalitionSynergyScore(ctx, tx, workspaceID, coalition.SynergyScore, currentMembers, currentEpoch)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}

	// Update Synergy Score & Status (if >= 2, consider it ACTIVE not FORMING)
	status := "FORMING"
	if count >= 2 {
		status = "ACTIVE"
	}
	updQuery := `UPDATE workspace_coalitions SET synergy_score = ?, status = ?, updated_at = ? WHERE coalition_id = ?`
	if _, err := tx.ExecContext(ctx, updQuery, synScore, status, now, coalitionID); err != nil {
		return coalitionMemberTxResult{}, fmt.Errorf("update synergy score: %w", err)
	}
	coalition.SynergyScore = synScore
	coalition.Status = status
	coalition.UpdatedAt = now
	coalition.Members = currentMembers
	var joinedMember WorkspaceCoalitionMember
	for _, member := range currentMembers {
		if member.AgentID == agentID {
			joinedMember = member
			break
		}
	}
	return coalitionMemberTxResult{
		Factors:   factors,
		Coalition: *coalition,
		Tension:   tension,
		Member:    joinedMember,
		Changed:   true,
	}, nil
}

// RemoveCoalitionMember detaches an agent from a coalition.
func (s *Store) RemoveCoalitionMember(ctx context.Context, workspaceID, coalitionID, agentID string) error {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin remove coalition member: %w", err)
	}
	defer tx.Rollback()

	result, err := s.removeCoalitionMemberTx(ctx, tx, workspaceID, coalitionID, agentID, false)
	if err != nil {
		if result.CommitOnError {
			if commitErr := tx.Commit(); commitErr != nil {
				return fmt.Errorf("commit coalition cleanup after remove failure: %w", commitErr)
			}
		}
		return err
	}
	return tx.Commit()
}

func (s *Store) removeCoalitionMemberTx(ctx context.Context, tx *sql.Tx, workspaceID, coalitionID, agentID string, strict bool) (coalitionMemberTxResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	coalitionID = strings.TrimSpace(coalitionID)
	agentID = strings.TrimSpace(agentID)

	coalition, err := s.loadCoalitionRecordByID(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}
	if coalition == nil {
		if strict {
			return coalitionMemberTxResult{}, fmt.Errorf("%w: coalition %s", ErrCoalitionTargetNotFound, coalitionID)
		}
		return coalitionMemberTxResult{}, nil
	}
	if !coalitionIsActive(coalition.Status) {
		return coalitionMemberTxResult{}, ErrCoalitionExpired
	}

	currentEpoch, err := s.currentControlEpoch(ctx, tx, workspaceID)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}
	if expired, err := s.expireCoalitionIfNeeded(ctx, tx, coalition, currentEpoch); err != nil {
		return coalitionMemberTxResult{}, err
	} else if expired {
		return coalitionMemberTxResult{CommitOnError: true}, ErrCoalitionExpired
	}
	canonical, err := s.reconcileLiveCoalitionCandidateByTension(ctx, tx, workspaceID, coalition.TensionID, currentEpoch)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}
	if canonical == nil || canonical.coalition.CoalitionID != coalition.CoalitionID {
		return coalitionMemberTxResult{CommitOnError: true}, ErrCoalitionExpired
	}

	tension, err := s.loadTensionRecord(ctx, tx, workspaceID, coalition.TensionID)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}

	var minStayUntil int
	err = tx.QueryRowContext(ctx, `SELECT min_stay_until_epoch FROM workspace_coalition_members WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`, workspaceID, coalitionID, agentID).Scan(&minStayUntil)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if strict {
				return coalitionMemberTxResult{}, fmt.Errorf("%w: agent %s", ErrCoalitionActorNotMember, agentID)
			}
			members, loadErr := s.loadCoalitionMembers(ctx, tx, workspaceID, coalitionID)
			if loadErr != nil {
				return coalitionMemberTxResult{}, loadErr
			}
			coalition.Members = members
			return coalitionMemberTxResult{Coalition: *coalition, Tension: tension, Changed: false}, nil
		}
		return coalitionMemberTxResult{}, fmt.Errorf("load coalition member: %w", err)
	}
	if currentEpoch < minStayUntil {
		return coalitionMemberTxResult{}, ErrCoalitionMinimumTenureNotMet
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM workspace_coalition_members WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`, workspaceID, coalitionID, agentID); err != nil {
		return coalitionMemberTxResult{}, err
	}
	if err := s.normalizeCoalitionMemberRolesTx(ctx, tx, workspaceID, coalitionID, currentEpoch); err != nil {
		return coalitionMemberTxResult{}, err
	}
	currentMembers, err := s.loadCoalitionMembers(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return coalitionMemberTxResult{}, fmt.Errorf("load coalition members after remove: %w", err)
	}
	count := len(currentMembers)

	status := "FORMING"
	switch {
	case count == 0:
		status = "DISBANDED"
	case count >= 2:
		status = "ACTIVE"
	}
	synScore, err := s.calculateCoalitionSynergyScore(ctx, tx, workspaceID, coalition.SynergyScore, currentMembers, currentEpoch)
	if err != nil {
		return coalitionMemberTxResult{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `UPDATE workspace_coalitions SET synergy_score = ?, status = ?, updated_at = ? WHERE coalition_id = ?`, synScore, status, now, coalitionID); err != nil {
		return coalitionMemberTxResult{}, fmt.Errorf("update coalition after remove: %w", err)
	}

	coalition.SynergyScore = synScore
	coalition.Status = status
	coalition.UpdatedAt = now
	coalition.Members = currentMembers
	return coalitionMemberTxResult{
		Coalition: *coalition,
		Tension:   tension,
		Member: WorkspaceCoalitionMember{
			CoalitionID: coalitionID,
			WorkspaceID: workspaceID,
			AgentID:     agentID,
		},
		Changed: true,
	}, nil
}

type ScoredTension struct {
	TensionRecord
	AttachScore    float64                `json:"attach_score"`
	AttachProb     float64                `json:"attach_prob"`
	AttachFactors  AgentAttachmentFactors `json:"attach_factors,omitempty"`
	AttachDecision AttachmentDecision     `json:"attach_decision,omitempty"`
}

type AgentAttachmentFactors struct {
	Fit                   float64 `json:"fit,omitempty"`
	Novelty               float64 `json:"novelty,omitempty"`
	CrowdingRatio         float64 `json:"crowding_ratio,omitempty"`
	ArchivePropensity     float64 `json:"archive_propensity,omitempty"`
	RecoveryRisk          float64 `json:"recovery_risk,omitempty"`
	LeaseSensitive        bool    `json:"lease_sensitive,omitempty"`
	FarReviewerRelief     float64 `json:"far_reviewer_relief,omitempty"`
	StayBonus             float64 `json:"stay_bonus,omitempty"`
	SwitchPenalty         float64 `json:"switch_penalty,omitempty"`
	ContextLossPenalty    float64 `json:"context_loss_penalty,omitempty"`
	ExplorationPrior      float64 `json:"exploration_prior,omitempty"`
	PersonalizationJitter float64 `json:"personalization_jitter,omitempty"`
}

func (s *Store) buildAgentAttachmentFactors(ctx context.Context, workspaceID, agentID string, tension TensionRecord, coalition *WorkspaceCoalition, currentSession *AgentSessionStateRecord, explorationPrior float64) (AgentAttachmentFactors, error) {
	var occupierIds []string
	var generatorID string
	roleDiversity := 0.0
	hasFarReviewer := false
	if coalition != nil {
		for _, m := range coalition.Members {
			occupierIds = append(occupierIds, m.AgentID)
			if m.Role == "GENERATOR" {
				generatorID = m.AgentID
			}
		}
		roleDiversity = coalitionRoleDiversity(coalition.Members)
		hasFarReviewer = coalitionHasFarReviewer(coalition.Members)
	}

	crowdingRatio := crowdingRatioForAttachment(tension, occupierIds)
	noveltySignals := AttachmentNoveltySignals{
		OccupierCount:  len(occupierIds),
		CrowdingRatio:  crowdingRatio,
		RoleDiversity:  roleDiversity,
		EvidenceSignal: attachmentEvidenceSignal(tension),
		HasFarReviewer: hasFarReviewer,
	}
	if generatorID != "" {
		dist, hasEvidence, err := s.calculateCoalitionPairwiseDistanceStatsReadRef(ctx, workspaceID, generatorID, agentID)
		if err != nil {
			return AgentAttachmentFactors{}, fmt.Errorf("calculate attach generator distance: %w", err)
		}
		noveltySignals.GeneratorDistance = dist
		noveltySignals.HasGeneratorEvidence = hasEvidence
	}
	farReviewerRelief := attachmentFarReviewerRelief(noveltySignals)

	return AgentAttachmentFactors{
		Fit:                   heuristicAttachmentFit(agentID, tension, occupierIds, currentSession),
		Novelty:               calculateAttachmentNoveltyFromSignals(noveltySignals),
		CrowdingRatio:         crowdingRatio,
		ArchivePropensity:     clampCoalitionSignal(tension.ArchivePropensity),
		RecoveryRisk:          clampCoalitionSignal(tension.RecoveryRisk),
		LeaseSensitive:        tension.LeaseSensitive,
		FarReviewerRelief:     farReviewerRelief,
		StayBonus:             attachmentStayBonus(agentID, tension, occupierIds, currentSession),
		SwitchPenalty:         attachmentSwitchPenalty(agentID, tension, occupierIds, currentSession),
		ContextLossPenalty:    attachmentContextLossPenalty(tension, currentSession),
		ExplorationPrior:      attachmentExplorationPrior(explorationPrior, tension, currentSession),
		PersonalizationJitter: attachmentPersonalizationJitter(agentID, tension.TensionID),
	}, nil
}

// ListAgentAvailableTensionsScored lists Active/Emergent tensions, calculating Softmax attachment probabilities based on current Coalition occupiers.
func (s *Store) ListAgentAvailableTensionsScored(ctx context.Context, workspaceID, agentID string) ([]ScoredTension, error) {
	filterActive := TensionFilter{
		WorkspaceID:    workspaceID,
		LifecycleState: "ACTIVE",
		Limit:          100,
	}
	tensionsActive, err := s.ListTensions(ctx, filterActive)
	if err != nil {
		return nil, fmt.Errorf("list active tensions: %w", err)
	}

	filterEmergent := TensionFilter{
		WorkspaceID:    workspaceID,
		LifecycleState: "EMERGENT",
		Limit:          100,
	}
	tensionsEmergent, err := s.ListTensions(ctx, filterEmergent)
	if err != nil {
		return nil, fmt.Errorf("list emergent tensions: %w", err)
	}

	tensions := append(tensionsActive, tensionsEmergent...)

	if len(tensions) == 0 {
		return nil, nil
	}
	currentSession, err := s.latestActiveAgentSessionState(ctx, workspaceID, agentID)
	if err != nil {
		return nil, fmt.Errorf("load agent attachment session context: %w", err)
	}
	historyMass, err := s.agentAttachmentHistoryMass(ctx, workspaceID, agentID)
	if err != nil {
		return nil, fmt.Errorf("load agent attachment history: %w", err)
	}
	explorationPrior := agentExplorationPrior(historyMass)

	// Fetch current Stabilize mode to enforce Policy Engine coalition-only routing
	var controlMode string
	_ = s.db.QueryRowContext(ctx, `SELECT current_mode FROM cluster_control_states WHERE workspace_id = ? ORDER BY created_at DESC LIMIT 1`, workspaceID).Scan(&controlMode)
	isStabilize := (controlMode == "STABILIZE")

	// 1. Calculate raw Log-Odds scores for each tension
	scored := make([]ScoredTension, 0, len(tensions))
	rawScores := make([]float64, 0, len(tensions))

	for _, t := range tensions {
		coalition, err := s.GetTensionCoalition(ctx, workspaceID, t.TensionID)
		var occupierIds []string
		if err == nil && coalition != nil {
			for _, m := range coalition.Members {
				occupierIds = append(occupierIds, m.AgentID)
			}
		}
		if len(occupierIds) >= coalitionSizeCapForTensionType(t.TensionType) {
			continue
		}

		// STABILIZE only allows solo starts on canonical meta or fork follow-up tensions.
		tensionType := normalizeTensionType(t.TensionType)
		if isStabilize && t.LifecycleState == tensionLifecycleEmergent && !isMetaTensionType(tensionType) && tensionType != "fork_candidate" {
			if len(occupierIds) == 0 {
				continue // Block solo starts of pure emergent tasks
			}
		}

		factors, err := s.buildAgentAttachmentFactors(ctx, workspaceID, agentID, t, coalition, currentSession, explorationPrior)
		if err != nil {
			return nil, fmt.Errorf("build attachment factors: %w", err)
		}
		attachScore := calculateAttachScoreWithFactors(clampAttachmentSurfacePriority(t.SurfaceScore), factors)

		scored = append(scored, ScoredTension{
			TensionRecord:  t,
			AttachScore:    attachScore,
			AttachFactors:  factors,
			AttachDecision: evaluateAttachmentDecision(t, factors),
		})
		rawScores = append(rawScores, attachScore)
	}

	// 2. Compute Softmax probabilities
	probs := CalculateSoftmaxDistribution(rawScores, ParamBaseTemp)

	for i := range scored {
		scored[i].AttachProb = probs[i]
	}

	// 3. Sort by attach probability descending
	sort.Slice(scored, func(i, j int) bool {
		leftDecision := attachmentDecisionPriority(scored[i].AttachDecision.State)
		rightDecision := attachmentDecisionPriority(scored[j].AttachDecision.State)
		if leftDecision != rightDecision {
			return leftDecision < rightDecision
		}
		if scored[i].AttachProb != scored[j].AttachProb {
			return scored[i].AttachProb > scored[j].AttachProb
		}
		if scored[i].AttachScore != scored[j].AttachScore {
			return scored[i].AttachScore > scored[j].AttachScore
		}
		return scored[i].TensionID < scored[j].TensionID
	})

	scored = injectExplorationCandidate(scored, explorationPrior)

	return scored, nil
}

func injectExplorationCandidate(scored []ScoredTension, explorationPrior float64) []ScoredTension {
	if explorationPrior < 0.35 || len(scored) < 4 {
		return scored
	}
	insertAt := 2
	if insertAt >= len(scored) {
		return scored
	}
	start := len(scored) / 2
	if start <= insertAt {
		start = insertAt + 1
	}
	if start >= len(scored) {
		return scored
	}
	candidateIdx := -1
	candidateWeight := -1.0
	for idx := start; idx < len(scored); idx++ {
		if scored[idx].AttachDecision.State == AttachmentDecisionRejected {
			continue
		}
		weight := attachmentExplorationCandidateWeight(scored[idx].AttachFactors)
		if weight > candidateWeight {
			candidateWeight = weight
			candidateIdx = idx
		}
	}
	if candidateIdx <= insertAt {
		return scored
	}
	candidate := scored[candidateIdx]
	copy(scored[insertAt+1:candidateIdx+1], scored[insertAt:candidateIdx])
	scored[insertAt] = candidate
	return scored
}

func clampAttachmentSurfacePriority(surfaceScore int) float64 {
	piSurf := float64(surfaceScore) / 100.0
	if piSurf < 0 {
		return 0
	}
	if piSurf > 1 {
		return 1
	}
	return piSurf
}

func heuristicAttachmentFit(agentID string, tension TensionRecord, occupierIds []string, currentSession *AgentSessionStateRecord) float64 {
	fit := ParamFitMinAttach
	if containsString(occupierIds, agentID) {
		fit += 0.20
	}
	fit += attachmentAgentAffinityBonus(agentID, tension)
	fit += attachmentSessionAffinityBonus(tension, currentSession)

	matchedWeight, totalWeight := attachmentRequirementCoverage(agentID, tension, currentSession)
	if totalWeight > 0 {
		fit += 0.45 * (matchedWeight / totalWeight)
	} else if !attachmentHasExplicitRequirementAnchors(tension) {
		// Generic tensions without explicit requirement anchors stay moderately attachable.
		fit += 0.20
	}

	if fit > 1 {
		fit = 1
	}
	return fit
}

func attachmentHasExplicitRequirementAnchors(tension TensionRecord) bool {
	return len(tension.AgentIDs) > 0 ||
		len(tension.TaskIDs) > 0 ||
		len(tension.SessionIDs) > 0 ||
		len(tension.DocKeys) > 0 ||
		len(tension.ArtifactRefs) > 0
}

func attachmentHasStructuredRequirementAnchors(tension TensionRecord) bool {
	return len(tension.TaskIDs) > 0 || len(tension.DocKeys) > 0 || len(tension.ArtifactRefs) > 0
}

func attachmentRequirementCoverage(agentID string, tension TensionRecord, currentSession *AgentSessionStateRecord) (matchedWeight, totalWeight float64) {
	hasStructuredAnchors := attachmentHasStructuredRequirementAnchors(tension)

	if hasStructuredAnchors && len(tension.AgentIDs) > 0 {
		totalWeight += 0.12
		if containsString(tension.AgentIDs, agentID) {
			if currentSession == nil {
				matchedWeight += 0.06
			} else {
				matchedWeight += 0.12
			}
		}
	}
	if currentSession == nil {
		return matchedWeight, totalWeight
	}
	if hasStructuredAnchors && len(tension.SessionIDs) > 0 {
		totalWeight += 0.08
		if sessionID := strings.TrimSpace(currentSession.SessionID); sessionID != "" && containsString(tension.SessionIDs, sessionID) {
			matchedWeight += 0.08
		} else {
			matchedWeight += 0.08 * attachmentSessionAnchorRetention(tension, currentSession)
		}
	}

	if len(tension.TaskIDs) > 0 {
		totalWeight += 0.60
		if taskID := strings.TrimSpace(currentSession.TaskID); taskID != "" && containsString(tension.TaskIDs, taskID) {
			matchedWeight += 0.60
		}
	}
	if len(tension.DocKeys) > 0 {
		totalWeight += 0.24
		matchedWeight += 0.24 * trimmedStringCoverageRatio(tension.DocKeys, currentSession.RelatedDocKeys)
	}
	if len(tension.ArtifactRefs) > 0 {
		totalWeight += 0.16
		matchedWeight += 0.16 * artifactRefCoverageRatio(tension.ArtifactRefs, currentSession.RelatedArtifactRefs)
	}

	return matchedWeight, totalWeight
}

func attachmentAgentAffinityBonus(agentID string, tension TensionRecord) float64 {
	if containsString(tension.AgentIDs, agentID) {
		if attachmentHasStructuredRequirementAnchors(tension) {
			return 0.06
		}
		return 0.12
	}
	return 0
}

func attachmentSessionAffinityBonus(tension TensionRecord, currentSession *AgentSessionStateRecord) float64 {
	if currentSession == nil {
		return 0
	}
	if sessionID := strings.TrimSpace(currentSession.SessionID); sessionID != "" && containsString(tension.SessionIDs, sessionID) {
		return 0.08
	}
	if attachmentHasStructuredRequirementAnchors(tension) && len(tension.SessionIDs) > 0 {
		return 0
	}
	return 0.04 * attachmentSessionAnchorRetention(tension, currentSession)
}

func attachmentEvidenceSignal(tension TensionRecord) float64 {
	if tension.EvidenceCount <= 0 {
		if strings.TrimSpace(tension.LastRefreshedAt) == "" && strings.TrimSpace(tension.LastSeenAt) == "" {
			return 0
		}
		return 0.2
	}
	if tension.EvidenceCount >= 4 {
		return 1.0
	}
	return float64(tension.EvidenceCount) / 4.0
}

func attachmentStayBonus(agentID string, tension TensionRecord, occupierIds []string, currentSession *AgentSessionStateRecord) float64 {
	if containsString(occupierIds, agentID) {
		return 1.0
	}
	if currentSession != nil {
		if taskID := strings.TrimSpace(currentSession.TaskID); taskID != "" && containsString(tension.TaskIDs, taskID) {
			return 0.6
		}
		return 0.30 * attachmentSessionAnchorRetention(tension, currentSession)
	}
	return 0
}

func attachmentSwitchPenalty(agentID string, tension TensionRecord, occupierIds []string, currentSession *AgentSessionStateRecord) float64 {
	if containsString(occupierIds, agentID) {
		return 0
	}
	if currentSession == nil {
		return 0
	}
	if taskID := strings.TrimSpace(currentSession.TaskID); taskID != "" && containsString(tension.TaskIDs, taskID) {
		return 0
	}
	return 0.75 * (1.0 - attachmentSessionAnchorRetention(tension, currentSession))
}

func attachmentContextLossPenalty(tension TensionRecord, currentSession *AgentSessionStateRecord) float64 {
	if currentSession == nil {
		return 0
	}
	if taskID := strings.TrimSpace(currentSession.TaskID); taskID != "" && containsString(tension.TaskIDs, taskID) {
		return 0
	}
	return 0.35 * (1.0 - attachmentSessionAnchorRetention(tension, currentSession))
}

func attachmentSessionAnchorRetention(tension TensionRecord, currentSession *AgentSessionStateRecord) float64 {
	if currentSession == nil {
		return 0
	}

	retention := 0.0
	if sessionID := strings.TrimSpace(currentSession.SessionID); sessionID != "" && containsString(tension.SessionIDs, sessionID) {
		retention += 0.30
	}
	retention += 0.40 * trimmedStringCoverageRatio(tension.DocKeys, currentSession.RelatedDocKeys)
	retention += 0.30 * artifactRefCoverageRatio(tension.ArtifactRefs, currentSession.RelatedArtifactRefs)
	return clampCoalitionSignal(retention)
}

func (s *Store) latestActiveAgentSessionState(ctx context.Context, workspaceID, agentID string) (*AgentSessionStateRecord, error) {
	sessions, err := s.ListWorkspaceSessionStates(ctx, workspaceID, true, 64)
	if err != nil {
		return nil, err
	}
	for idx := range sessions {
		if strings.TrimSpace(sessions[idx].AgentID) == strings.TrimSpace(agentID) {
			session := sessions[idx]
			return &session, nil
		}
	}
	return nil, nil
}

func (s *Store) agentAttachmentHistoryMass(ctx context.Context, workspaceID, agentID string) (float64, error) {
	var eventCount int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM runtime_events
		  WHERE workspace_id = ?
		    AND agent_id = ?
		    AND (
		      event_type IN (?, ?, ?, ?, ?, ?)
		    )`,
		workspaceID,
		agentID,
		model.SessionEventStart,
		model.SessionEventStatus,
		model.SessionEventBlocked,
		model.SessionEventDecisionNeeded,
		model.SessionEventEnd,
		"session.takeover",
	).Scan(&eventCount); err != nil {
		return 0, err
	}
	mass := float64(eventCount) / 8.0
	if mass > 4 {
		mass = 4
	}
	return mass, nil
}

func agentExplorationPrior(historyMass float64) float64 {
	if historyMass <= 0 {
		return 1
	}
	return math.Exp(-0.7 * historyMass)
}

func attachmentExplorationPrior(basePrior float64, tension TensionRecord, currentSession *AgentSessionStateRecord) float64 {
	if basePrior <= 0 || isMetaTensionType(tension.TensionType) {
		return 0
	}
	prior := basePrior * (1 - clampAttachmentSurfacePriority(tension.SurfaceScore))
	if len(tension.AgentIDs) > 0 || len(tension.SessionIDs) > 0 {
		if currentSession != nil {
			if taskID := strings.TrimSpace(currentSession.TaskID); taskID != "" && containsString(tension.TaskIDs, taskID) {
				return prior
			}
		}
		return 0
	}
	if currentSession == nil {
		return prior
	}
	if taskID := strings.TrimSpace(currentSession.TaskID); taskID != "" && containsString(tension.TaskIDs, taskID) {
		return prior
	}
	retention := attachmentSessionAnchorRetention(tension, currentSession)
	if retention <= 0 {
		return prior
	}
	return prior * (1 - 0.5*retention)
}

func attachmentPersonalizationJitter(agentID, tensionID string) float64 {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(tensionID) == "" {
		return 0
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(agentID) + "|" + strings.TrimSpace(tensionID)))
	return float64(sum[0]) / 2550.0
}

func intersectsTrimmedStrings(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	index := make(map[string]struct{}, len(left))
	for _, item := range left {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			index[trimmed] = struct{}{}
		}
	}
	for _, item := range right {
		if _, ok := index[strings.TrimSpace(item)]; ok {
			return true
		}
	}
	return false
}

func trimmedStringCoverageRatio(requirements, observed []string) float64 {
	requirementSet := make(map[string]struct{}, len(requirements))
	for _, item := range requirements {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			requirementSet[trimmed] = struct{}{}
		}
	}
	if len(requirementSet) == 0 {
		return 0
	}

	observedSet := make(map[string]struct{}, len(observed))
	for _, item := range observed {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			observedSet[trimmed] = struct{}{}
		}
	}

	matched := 0
	for item := range requirementSet {
		if _, ok := observedSet[item]; ok {
			matched++
		}
	}
	return clampCoalitionSignal(float64(matched) / float64(len(requirementSet)))
}

func tensionArtifactsOverlapSession(tensionRefs []string, sessionRefs []model.AgentUpdateArtifactRef) bool {
	if len(tensionRefs) == 0 || len(sessionRefs) == 0 {
		return false
	}
	index := make(map[string]struct{}, len(tensionRefs))
	for _, ref := range tensionRefs {
		if trimmed := strings.TrimSpace(ref); trimmed != "" {
			index[trimmed] = struct{}{}
		}
	}
	for _, ref := range sessionRefs {
		if _, ok := index[strings.TrimSpace(ref.Ref)]; ok {
			return true
		}
	}
	return false
}

func artifactRefCoverageRatio(requirements []string, observed []model.AgentUpdateArtifactRef) float64 {
	requirementSet := make(map[string]struct{}, len(requirements))
	for _, ref := range requirements {
		if trimmed := strings.TrimSpace(ref); trimmed != "" {
			requirementSet[trimmed] = struct{}{}
		}
	}
	if len(requirementSet) == 0 {
		return 0
	}

	observedSet := make(map[string]struct{}, len(observed))
	for _, ref := range observed {
		if trimmed := strings.TrimSpace(ref.Ref); trimmed != "" {
			observedSet[trimmed] = struct{}{}
		}
	}

	matched := 0
	for ref := range requirementSet {
		if _, ok := observedSet[ref]; ok {
			matched++
		}
	}
	return clampCoalitionSignal(float64(matched) / float64(len(requirementSet)))
}

func (s *Store) RebootStalledSessions(ctx context.Context, minSurfaceScore int) ([]string, error) {
	referenceAt := time.Now().UTC()
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT tension_id, workspace_id, session_ids_json
		 FROM workspace_tensions
		 WHERE surface_score >= ? AND lifecycle_state IN (?, ?)`,
		minSurfaceScore, tensionLifecycleActive, tensionLifecycleEmergent,
	)
	if err != nil {
		return nil, fmt.Errorf("query stalled tensions: %w", err)
	}
	defer rows.Close()

	var targets []struct {
		TensionID   string
		WorkspaceID string
		SessionIDs  []string
	}

	for rows.Next() {
		var tensionID, workspaceID, sessionIDsJSON string
		if err := rows.Scan(&tensionID, &workspaceID, &sessionIDsJSON); err != nil {
			return nil, err
		}
		var sessionIDs []string
		if err := json.Unmarshal([]byte(sessionIDsJSON), &sessionIDs); err != nil {
			continue
		}
		if len(sessionIDs) > 0 {
			targets = append(targets, struct {
				TensionID   string
				WorkspaceID string
				SessionIDs  []string
			}{TensionID: tensionID, WorkspaceID: workspaceID, SessionIDs: sessionIDs})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(targets) == 0 {
		return nil, nil // Nothing to do
	}

	var rebooted []string
	rebootedSet := map[string]struct{}{}

	for _, t := range targets {
		tensionRebooted := false
		for _, sessionID := range t.SessionIDs {
			sessionID = strings.TrimSpace(sessionID)
			if sessionID == "" {
				continue
			}
			if _, done := rebootedSet[sessionID]; done {
				continue
			}
			state, err := s.GetAgentSessionState(ctx, t.WorkspaceID, sessionID)
			if err != nil {
				fmt.Printf("DEBUG GetAgentSessionState err: %v\n", err)
				continue
			}
			if !model.IsSessionStatusActive(state.Status) {
				fmt.Printf("DEBUG IsSessionStatusActive false: %s\n", state.Status)
				continue
			}
			agentLastSeenAt, err := s.lookupAgentLastSeenAt(ctx, t.WorkspaceID, state.AgentID)
			if err != nil && !errors.Is(err, ErrAgentNotFound) {
				fmt.Printf("DEBUG lookupAgentLastSeenAt err: %v\n", err)
				continue
			}
			abandoned, err := localSessionOwnershipAbandoned(state.UpdatedAt, agentLastSeenAt, referenceAt)
			if err != nil {
				fmt.Printf("DEBUG localSessionOwnershipAbandoned err: %v\n", err)
				continue
			}
			if !abandoned {
				continue
			}
			keep := false
			_, err = s.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
				EventType:         model.SessionEventEnd,
				WorkspaceID:       t.WorkspaceID,
				SessionID:         sessionID,
				AgentID:           state.AgentID,
				TaskID:            state.TaskID,
				Summary:           "System intervention - Tension Anti-Stall Reboot",
				Status:            model.SessionStatusEnded,
				KeepSessionActive: &keep,
				UpdatedAt:         referenceAt.Format(time.RFC3339Nano),
			})
			if err != nil {
				fmt.Printf("DEBUG RecordAgentSessionCoordination err: %v\n", err)
				continue
			}

			if state.TaskID != "" {
				_ = s.ReleaseTaskClaim(ctx, TaskReleaseInput{
					WorkspaceID: t.WorkspaceID,
					TaskID:      state.TaskID,
					AgentID:     "system_anti_stall",
					Reason:      "Tension Anti-Stall Reboot",
				})
			}

			rebootedSet[sessionID] = struct{}{}
			rebooted = append(rebooted, sessionID)
			tensionRebooted = true
		}

		if tensionRebooted {
			_, _ = s.DiscardTension(ctx, TensionMutationInput{
				WorkspaceID: t.WorkspaceID,
				TensionID:   t.TensionID,
				ActorID:     "system_anti_stall",
				Reason:      "Anti-Stall Reboot",
			})
		}
	}

	return rebooted, nil
}

func (s *Store) lookupAgentLastSeenAt(ctx context.Context, workspaceID, agentID string) (string, error) {
	var lastSeenAt sql.NullString
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT last_seen_at FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(agentID),
	).Scan(&lastSeenAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrAgentNotFound
		}
		return "", fmt.Errorf("query agent last_seen_at: %w", err)
	}
	return strings.TrimSpace(lastSeenAt.String), nil
}
