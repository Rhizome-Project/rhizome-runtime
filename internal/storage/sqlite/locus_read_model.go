package sqlite

import (
	"context"
	"errors"
	"sort"
	"strings"
)

const instrumentationLocusClusterLimit = 128

type InstrumentationLocusFilter struct {
	WorkspaceID    string              `json:"workspace_id"`
	ProtoClusterID string              `json:"proto_cluster_id,omitempty"`
	AgentID        string              `json:"agent_id,omitempty"`
	TaskID         string              `json:"task_id,omitempty"`
	SessionID      string              `json:"session_id,omitempty"`
	DocKeys        []string            `json:"doc_keys,omitempty"`
	ArtifactRefs   []string            `json:"artifact_refs,omitempty"`
	FrontierLimit  int                 `json:"frontier_limit,omitempty"`
	MemoryBudget   *MemoryPacketBudget `json:"memory_budget,omitempty"`
}

type InstrumentationLocusBundle struct {
	WorkspaceID        string                          `json:"workspace_id"`
	TimeAuthority      WorkspaceTimeAuthority          `json:"time_authority"`
	GeneratedAt        string                          `json:"generated_at"`
	Resolved           bool                            `json:"resolved"`
	ResolvedFrom       string                          `json:"resolved_from,omitempty"`
	MatchScore         int                             `json:"match_score,omitempty"`
	ProtoClusterID     string                          `json:"proto_cluster_id,omitempty"`
	Control            *ControlClusterDetail           `json:"control,omitempty"`
	ControlState       *ClusterControlStateDetail      `json:"control_state,omitempty"`
	MemoryCoherence    *MemoryCoherenceScopeReport     `json:"memory_coherence,omitempty"`
	Corridor           *CorridorClusterDetail          `json:"corridor,omitempty"`
	CorridorOwnership  *CorridorOwnershipClusterDetail `json:"corridor_ownership,omitempty"`
	CorridorFit        *CorridorFitClusterDetail       `json:"corridor_fit,omitempty"`
	CorridorBoundary   *CorridorBoundaryClusterDetail  `json:"corridor_boundary,omitempty"`
	CorridorAuthority  *CorridorAuthorityTaskDetail    `json:"corridor_authority,omitempty"`
	SegmentReport      *WorkspaceSegmentReport         `json:"segment_report,omitempty"`
	RelatedSegmentRefs []string                        `json:"related_segment_refs,omitempty"`
	Frontier           []TensionFrontierItem           `json:"frontier,omitempty"`
	DominantTension    *TensionDetail                  `json:"dominant_tension,omitempty"`
	MemoryPacket       *MemoryKernelPacket             `json:"memory_packet,omitempty"`
}

type instrumentationLocusClusterMatch struct {
	clusterID    string
	resolvedFrom string
	score        int
}

func normalizeInstrumentationLocusFilter(filter InstrumentationLocusFilter) InstrumentationLocusFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	filter.DocKeys = uniqueTrimmedLocusStrings(filter.DocKeys)
	filter.ArtifactRefs = uniqueTrimmedLocusStrings(filter.ArtifactRefs)
	if filter.FrontierLimit <= 0 {
		filter.FrontierLimit = 3
	}
	return filter
}

func (s *Store) BuildInstrumentationLocusBundle(ctx context.Context, filter InstrumentationLocusFilter) (InstrumentationLocusBundle, error) {
	filter = normalizeInstrumentationLocusFilter(filter)
	if filter.WorkspaceID == "" {
		return InstrumentationLocusBundle{}, errors.New("workspace_id is required")
	}

	var err error
	bundle := InstrumentationLocusBundle{
		WorkspaceID: filter.WorkspaceID,
	}
	bundle.TimeAuthority, err = s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return InstrumentationLocusBundle{}, err
	}
	bundle.GeneratedAt = generatedAtFromWorkspaceTimeAuthority(bundle.TimeAuthority)

	match, err := s.resolveInstrumentationLocusCluster(ctx, filter)
	if err != nil {
		return InstrumentationLocusBundle{}, err
	}
	if strings.TrimSpace(match.clusterID) == "" {
		return bundle, nil
	}

	bundle.Resolved = true
	bundle.ResolvedFrom = match.resolvedFrom
	bundle.MatchScore = match.score
	bundle.ProtoClusterID = match.clusterID

	controlDetail, err := s.BuildControlClusterDetail(ctx, filter.WorkspaceID, match.clusterID)
	if err != nil {
		return InstrumentationLocusBundle{}, err
	}
	bundle.Control = &controlDetail

	if detail, err := s.BuildClusterControlStateDetail(ctx, filter.WorkspaceID, match.clusterID); err == nil {
		bundle.ControlState = &detail
	}
	if detail, err := s.buildInstrumentationLocusMemoryCoherence(ctx, filter, bundle); err == nil {
		bundle.MemoryCoherence = detail
	}
	if detail, err := s.BuildCorridorClusterDetail(ctx, filter.WorkspaceID, match.clusterID); err == nil {
		bundle.Corridor = &detail
	}
	if detail, err := s.BuildCorridorOwnershipClusterDetail(ctx, filter.WorkspaceID, match.clusterID); err == nil {
		bundle.CorridorOwnership = &detail
	}
	if detail, err := s.BuildCorridorFitClusterDetail(ctx, filter.WorkspaceID, match.clusterID); err == nil {
		bundle.CorridorFit = &detail
	}
	if detail, err := s.BuildCorridorBoundaryClusterDetail(ctx, filter.WorkspaceID, match.clusterID); err == nil {
		bundle.CorridorBoundary = &detail
	}
	if items, err := s.ListTensionFrontier(ctx, TensionFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: match.clusterID,
		LifecycleState: tensionLifecycleActive,
		Limit:          filter.FrontierLimit,
	}); err == nil {
		bundle.Frontier = items
	}

	dominant := selectInstrumentationLocusDominantTension(controlDetail.Tensions, bundle.Frontier, filter.TaskID, filter.SessionID)
	if dominant != nil {
		if detail, err := s.GetTension(ctx, filter.WorkspaceID, strings.TrimSpace(dominant.TensionID)); err == nil {
			bundle.DominantTension = &detail
		} else {
			bundle.DominantTension = &TensionDetail{Tension: *dominant}
		}
	}
	if authorityTaskID := instrumentationLocusAuthorityTaskID(filter, bundle); authorityTaskID != "" {
		if detail, err := s.BuildCorridorAuthorityTaskDetail(ctx, filter.WorkspaceID, authorityTaskID); err == nil {
			bundle.CorridorAuthority = &detail
		}
	}
	if segmentReport, relatedSegmentRefs, err := s.buildInstrumentationLocusSegmentReport(ctx, filter, bundle); err == nil {
		bundle.SegmentReport = segmentReport
		bundle.RelatedSegmentRefs = relatedSegmentRefs
	}

	return bundle, nil
}

func (s *Store) buildInstrumentationLocusMemoryCoherence(ctx context.Context, filter InstrumentationLocusFilter, bundle InstrumentationLocusBundle) (*MemoryCoherenceScopeReport, error) {
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" && bundle.Control != nil && len(bundle.Control.Cluster.AgentIDs) == 1 {
		agentID = strings.TrimSpace(bundle.Control.Cluster.AgentIDs[0])
	}
	if agentID == "" {
		return nil, nil
	}
	reportScope := "AGENT"
	if strings.TrimSpace(filter.SessionID) != "" {
		reportScope = "SESSION"
	}
	detail, err := s.GetMemoryCoherenceScope(ctx, filter.WorkspaceID, agentID, filter.SessionID, reportScope)
	if err != nil {
		return nil, err
	}
	return &detail, nil
}

func (s *Store) buildInstrumentationLocusSegmentReport(ctx context.Context, filter InstrumentationLocusFilter, bundle InstrumentationLocusBundle) (*WorkspaceSegmentReport, []string, error) {
	docKeys := uniqueTrimmedLocusStrings(append([]string{}, filter.DocKeys...))
	artifactRefs := uniqueTrimmedLocusStrings(append([]string{}, filter.ArtifactRefs...))
	relatedSegmentRefs := []string{}
	if bundle.DominantTension != nil {
		docKeys = uniqueTrimmedLocusStrings(append(docKeys, bundle.DominantTension.Tension.DocKeys...))
		artifactRefs = uniqueTrimmedLocusStrings(append(artifactRefs, bundle.DominantTension.Tension.ArtifactRefs...))
		relatedSegmentRefs = uniqueTrimmedLocusStrings(append(relatedSegmentRefs, bundle.DominantTension.Tension.SegmentRefs...))
	}
	if len(docKeys) == 0 && len(artifactRefs) == 0 && len(relatedSegmentRefs) == 0 {
		return nil, nil, nil
	}
	segments, err := s.collectWorkspaceSegmentsBestEffort(ctx, filter.WorkspaceID, docKeys, artifactRefs)
	if err != nil {
		return nil, nil, err
	}
	if len(segments) == 0 {
		return nil, relatedSegmentRefs, nil
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].SourceKind != segments[j].SourceKind {
			return segments[i].SourceKind < segments[j].SourceKind
		}
		if segments[i].SourceRef != segments[j].SourceRef {
			return segments[i].SourceRef < segments[j].SourceRef
		}
		if segments[i].StartLine != segments[j].StartLine {
			return segments[i].StartLine < segments[j].StartLine
		}
		return segments[i].SegmentRef < segments[j].SegmentRef
	})
	generatedAt := generatedAtFromWorkspaceTimeAuthority(bundle.TimeAuthority)
	for idx := range segments {
		segments[idx].GeneratedAt = generatedAt
	}
	report := &WorkspaceSegmentReport{
		WorkspaceID:   filter.WorkspaceID,
		TimeAuthority: bundle.TimeAuthority,
		GeneratedAt:   generatedAt,
		Filter: WorkspaceSegmentFilter{
			WorkspaceID: filter.WorkspaceID,
			Limit:       len(segments),
		},
		Sources:  buildWorkspaceSegmentSources(segments),
		Segments: segments,
	}
	return report, relatedSegmentRefs, nil
}

func instrumentationLocusAuthorityTaskID(filter InstrumentationLocusFilter, bundle InstrumentationLocusBundle) string {
	if taskID := strings.TrimSpace(filter.TaskID); taskID != "" {
		return taskID
	}
	if bundle.CorridorOwnership != nil {
		if taskID := strings.TrimSpace(bundle.CorridorOwnership.Cluster.Ownership.OwnerTaskID); taskID != "" {
			return taskID
		}
		if len(bundle.CorridorOwnership.Cluster.Ownership.OwnerTaskIDs) == 1 {
			return strings.TrimSpace(bundle.CorridorOwnership.Cluster.Ownership.OwnerTaskIDs[0])
		}
	}
	if bundle.DominantTension != nil && len(bundle.DominantTension.Tension.TaskIDs) == 1 {
		return strings.TrimSpace(bundle.DominantTension.Tension.TaskIDs[0])
	}
	if bundle.Control != nil && len(bundle.Control.Cluster.TaskIDs) == 1 {
		return strings.TrimSpace(bundle.Control.Cluster.TaskIDs[0])
	}
	return ""
}

func (s *Store) resolveInstrumentationLocusCluster(ctx context.Context, filter InstrumentationLocusFilter) (instrumentationLocusClusterMatch, error) {
	if filter.ProtoClusterID != "" {
		return instrumentationLocusClusterMatch{
			clusterID:    filter.ProtoClusterID,
			resolvedFrom: "proto_cluster_id",
			score:        100,
		}, nil
	}

	report, err := s.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: filter.WorkspaceID,
		Limit:       instrumentationLocusClusterLimit,
	})
	if err != nil {
		return instrumentationLocusClusterMatch{}, err
	}

	best := instrumentationLocusClusterMatch{}
	for _, cluster := range report.Clusters {
		match := scoreInstrumentationLocusCluster(cluster, filter)
		if match.score == 0 {
			continue
		}
		if match.score > best.score || (match.score == best.score && (best.clusterID == "" || strings.Compare(match.clusterID, best.clusterID) < 0)) {
			best = match
		}
	}
	return best, nil
}

func scoreInstrumentationLocusCluster(cluster ControlClusterReport, filter InstrumentationLocusFilter) instrumentationLocusClusterMatch {
	match := instrumentationLocusClusterMatch{
		clusterID: strings.TrimSpace(cluster.ProtoClusterID),
	}
	taskMatched := containsLocusString(cluster.TaskIDs, filter.TaskID)
	sessionMatched := containsLocusString(cluster.SessionIDs, filter.SessionID)
	docMatched := intersectsLocusStrings(cluster.DocKeys, filter.DocKeys)
	artifactMatched := intersectsLocusStrings(cluster.ArtifactRefs, filter.ArtifactRefs)
	agentMatched := containsLocusString(cluster.AgentIDs, filter.AgentID)

	switch {
	case taskMatched:
		match.resolvedFrom = "task_id"
	case sessionMatched:
		match.resolvedFrom = "session_id"
	case docMatched:
		match.resolvedFrom = "doc_key"
	case artifactMatched:
		match.resolvedFrom = "artifact_ref"
	case agentMatched:
		match.resolvedFrom = "agent_id"
	}

	if taskMatched {
		match.score += 8
	}
	if sessionMatched {
		match.score += 7
	}
	if docMatched {
		match.score += 4
	}
	if artifactMatched {
		match.score += 4
	}
	if agentMatched {
		match.score += 2
	}
	if cluster.ConfirmedTensionCount > 0 || cluster.PendingTensionCount > 0 {
		match.score++
	}
	return match
}

func selectInstrumentationLocusDominantTension(items []TensionRecord, frontier []TensionFrontierItem, taskID, sessionID string) *TensionRecord {
	taskID = strings.TrimSpace(taskID)
	sessionID = strings.TrimSpace(sessionID)
	if len(items) == 0 {
		if len(frontier) == 0 {
			return nil
		}
		first := frontier[0]
		return &TensionRecord{
			TensionID:      strings.TrimSpace(first.TensionID),
			ProtoClusterID: strings.TrimSpace(first.ProtoClusterID),
			TensionType:    strings.TrimSpace(first.TensionType),
			ReviewStatus:   strings.TrimSpace(first.ReviewStatus),
			Title:          strings.TrimSpace(first.Title),
			Summary:        strings.TrimSpace(first.Summary),
			SurfaceScore:   first.SurfaceScore,
		}
	}

	bestIdx := -1
	bestScore := -1
	for i, item := range items {
		score := 0
		if containsLocusString(item.TaskIDs, taskID) {
			score += 5
		}
		if containsLocusString(item.SessionIDs, sessionID) {
			score += 4
		}
		if strings.EqualFold(strings.TrimSpace(item.ReviewStatus), tensionReviewConfirmed) {
			score++
		}
		if bestIdx < 0 || score > bestScore || (score == bestScore && (item.SurfaceScore > items[bestIdx].SurfaceScore || (item.SurfaceScore == items[bestIdx].SurfaceScore && strings.Compare(item.TensionID, items[bestIdx].TensionID) < 0))) {
			bestIdx = i
			bestScore = score
		}
	}
	if bestIdx < 0 {
		return nil
	}
	record := items[bestIdx]
	return &record
}

func uniqueTrimmedLocusStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func containsLocusString(values []string, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func intersectsLocusStrings(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	index := map[string]struct{}{}
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
