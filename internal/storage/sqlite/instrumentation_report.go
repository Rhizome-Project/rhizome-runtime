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

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

type InstrumentationReportFilter struct {
	WorkspaceID  string `json:"workspace_id"`
	AgentID      string `json:"agent_id,omitempty"`
	SessionID    string `json:"session_id,omitempty"`
	TaskID       string `json:"task_id,omitempty"`
	Limit        int    `json:"limit"`
	ClusterLimit int    `json:"cluster_limit,omitempty"`
}

type ProtoClusterMetrics struct {
	EventCount                  int                         `json:"event_count"`
	EventTypeCounts             map[string]int              `json:"event_type_counts,omitempty"`
	ActiveSessionCount          int                         `json:"active_session_count"`
	OpenQueueCount              int                         `json:"open_queue_count"`
	BlockerSignalCount          int                         `json:"blocker_signal_count"`
	BlockerDensity              float64                     `json:"blocker_density"`
	ActivityCountsByAgent       map[string]int              `json:"activity_counts_by_agent,omitempty"`
	ActivityShareByAgent        map[string]float64          `json:"activity_share_by_agent,omitempty"`
	MaxAgentActivityShare       float64                     `json:"max_agent_activity_share"`
	CommunicationInByAgent      map[string]int              `json:"communication_in_by_agent,omitempty"`
	CommunicationOutByAgent     map[string]int              `json:"communication_out_by_agent,omitempty"`
	CommunicationCentralization float64                     `json:"communication_centralization"`
	DuplicationSignalCount      int                         `json:"duplication_signal_count"`
	DuplicationIndex            float64                     `json:"duplication_index"`
	RoleLock                    ProtoClusterRoleLockMetrics `json:"role_lock"`
	LastEventAt                 string                      `json:"last_event_at,omitempty"`
}

type ProtoClusterRoleLockMetrics struct {
	Index               float64  `json:"index"`
	Partial             bool     `json:"partial,omitempty"`
	StewardHHI          float64  `json:"steward_hhi"`
	AcceptedBuilderHHI  float64  `json:"accepted_builder_hhi"`
	DefaultReviewerHHI  float64  `json:"default_reviewer_hhi"`
	MotifReuseHHI       float64  `json:"motif_reuse_hhi"`
	ActiveStewardCount  int      `json:"active_steward_count"`
	ActiveClaimCount    int      `json:"active_claim_count"`
	BlockingReviewCount int      `json:"blocking_review_count"`
	MissingComponents   []string `json:"missing_components,omitempty"`
}

type ProtoClusterReport struct {
	ProtoClusterID string              `json:"proto_cluster_id"`
	ResolutionKind string              `json:"resolution_kind"`
	TaskIDs        []string            `json:"task_ids,omitempty"`
	SessionIDs     []string            `json:"session_ids,omitempty"`
	DocKeys        []string            `json:"doc_keys,omitempty"`
	ArtifactRefs   []string            `json:"artifact_refs,omitempty"`
	AgentIDs       []string            `json:"agent_ids,omitempty"`
	Metrics        ProtoClusterMetrics `json:"metrics"`
}

type InstrumentationWorkspaceMetrics struct {
	TotalClusters                        int            `json:"total_clusters"`
	BlockedClusterCount                  int            `json:"blocked_cluster_count"`
	DuplicateProneClusterCount           int            `json:"duplicate_prone_cluster_count"`
	TopAgentByActivity                   string         `json:"top_agent_by_activity,omitempty"`
	TopAgentActivityShare                float64        `json:"top_agent_activity_share"`
	WorkspaceCommunicationCentralization float64        `json:"workspace_communication_centralization"`
	WorkspaceActivityCountsByAgent       map[string]int `json:"workspace_activity_counts_by_agent,omitempty"`
}

type InstrumentationReport struct {
	WorkspaceID       string                          `json:"workspace_id"`
	TimeAuthority     WorkspaceTimeAuthority          `json:"time_authority"`
	GeneratedAt       string                          `json:"generated_at"`
	Filter            InstrumentationReportFilter     `json:"filter"`
	Truncated         bool                            `json:"truncated"`
	Replay            RuntimeReplayMetrics            `json:"replay"`
	Workspace         InstrumentationWorkspaceMetrics `json:"workspace"`
	Clusters          []ProtoClusterReport            `json:"clusters,omitempty"`
	readSurfacePolicy ReadSurfacePolicy               `json:"-"`
}

type InstrumentationSnapshotInput struct {
	ActorID string
	Limit   int
}

func normalizeInstrumentationReportFilter(filter InstrumentationReportFilter) InstrumentationReportFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	filter.Limit = clampReadSurfaceLimit(filter.Limit, readSurfaceReplayLimitDefault, readSurfaceReplayLimitMax)
	filter.ClusterLimit = clampReadSurfaceLimit(filter.ClusterLimit, readSurfaceClusterLimitDefault, readSurfaceClusterLimitMax)
	return filter
}

type instrumentationClusterAccumulator struct {
	protoClusterID            string
	resolutionKind            string
	taskIDs                   map[string]struct{}
	sessionIDs                map[string]struct{}
	docKeys                   map[string]struct{}
	artifactRefs              map[string]struct{}
	agentIDs                  map[string]struct{}
	eventTypeCounts           map[string]int
	activityCounts            map[string]int
	communicationIn           map[string]int
	communicationOut          map[string]int
	eventCount                int
	activeSessions            int
	openQueues                int
	blockerSignals            int
	knowledgeSurfaceMutations int
	lastEventAt               string
}

type instrumentationResolution struct {
	protoClusterID string
	resolutionKind string
	taskIDs        []string
	sessionIDs     []string
	docKeys        []string
	artifactRefs   []string
	agentIDs       []string
}

type instrumentationEventEvidence struct {
	taskIDs      []string
	sessionIDs   []string
	docKeys      []string
	artifactRefs []string
	agentIDs     []string
}

type instrumentationAgentUpdateLinks struct {
	taskIDs      []string
	docKeys      []string
	artifactRefs []string
}

type instrumentationSnapshotPayload struct {
	GeneratedAt   string                          `json:"generated_at"`
	WorkspaceID   string                          `json:"workspace_id"`
	TimeAuthority WorkspaceTimeAuthority          `json:"time_authority"`
	Filter        InstrumentationReportFilter     `json:"filter"`
	Replay        RuntimeReplayMetrics            `json:"replay"`
	Workspace     InstrumentationWorkspaceMetrics `json:"workspace"`
	Clusters      []ProtoClusterReport            `json:"clusters,omitempty"`
}

func (s *Store) BuildInstrumentationReport(ctx context.Context, filter InstrumentationReportFilter) (InstrumentationReport, error) {
	filter = normalizeInstrumentationReportFilter(filter)
	if filter.WorkspaceID == "" {
		return InstrumentationReport{}, errors.New("workspace_id is required")
	}

	replay, err := s.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID:      filter.WorkspaceID,
		AgentID:          filter.AgentID,
		SessionID:        filter.SessionID,
		TaskID:           filter.TaskID,
		ExcludeSynthetic: true,
		Limit:            filter.Limit,
	})
	if err != nil {
		return InstrumentationReport{}, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return InstrumentationReport{}, err
	}

	report := InstrumentationReport{
		WorkspaceID:   filter.WorkspaceID,
		TimeAuthority: authority,
		GeneratedAt:   generatedAtFromWorkspaceTimeAuthority(authority),
		Filter:        filter,
		Truncated:     replay.Truncated,
		Replay:        replay.Metrics,
		Workspace: InstrumentationWorkspaceMetrics{
			WorkspaceActivityCountsByAgent: map[string]int{},
		},
		readSurfacePolicy: instrumentationReadSurfacePolicy(filter),
	}

	taskHydrationCache := map[string]TaskHydrationBundle{}
	updateLinkCache := map[string]instrumentationAgentUpdateLinks{}
	clusterAccumulators := map[string]*instrumentationClusterAccumulator{}
	globalActivityCounts := map[string]int{}
	globalCommunicationCounts := map[string]int{}

	events := append([]RuntimeEventRecord(nil), replay.Events...)
	for left, right := 0, len(events)-1; left < right; left, right = left+1, right-1 {
		events[left], events[right] = events[right], events[left]
	}

	for _, event := range events {
		if isSyntheticInstrumentationEvent(event) {
			continue
		}
		resolution, err := s.resolveProtoClusterForEvent(ctx, filter.WorkspaceID, event, taskHydrationCache, updateLinkCache)
		if err != nil {
			return InstrumentationReport{}, err
		}
		cluster := ensureInstrumentationCluster(clusterAccumulators, resolution.protoClusterID, resolution.resolutionKind)
		cluster.addResolution(resolution)
		cluster.eventCount++
		cluster.eventTypeCounts[event.EventType]++
		cluster.lastEventAt = firstNonEmpty(strings.TrimSpace(event.CreatedAt), cluster.lastEventAt)

		activityAgent := firstNonEmpty(strings.TrimSpace(event.AgentID), strings.TrimSpace(event.ActorID))
		if activityAgent != "" {
			cluster.activityCounts[activityAgent]++
			globalActivityCounts[activityAgent]++
			cluster.agentIDs[activityAgent] = struct{}{}
		}
		for _, agentID := range resolution.agentIDs {
			if agentID != "" {
				cluster.agentIDs[agentID] = struct{}{}
			}
		}

		if event.EventType == "session.blocked" || event.EventType == "task.blocked" {
			cluster.blockerSignals++
		}
		switch event.EventType {
		case "workspace_doc.upserted", "workspace_doc.archived", "workspace_doc.deleted", "workspace_artifact.created":
			cluster.knowledgeSurfaceMutations++
		}

		fromAgent, toAgent := instrumentationCommunicationPair(event)
		if fromAgent != "" {
			cluster.communicationOut[fromAgent]++
			globalCommunicationCounts[fromAgent]++
			cluster.agentIDs[fromAgent] = struct{}{}
		}
		if toAgent != "" {
			cluster.communicationIn[toAgent]++
			globalCommunicationCounts[toAgent]++
			cluster.agentIDs[toAgent] = struct{}{}
		}
	}

	for _, session := range replay.Sessions {
		resolution := resolveProtoClusterFromSession(filter.WorkspaceID, session)
		cluster := ensureInstrumentationCluster(clusterAccumulators, resolution.protoClusterID, resolution.resolutionKind)
		cluster.addResolution(resolution)
		if model.IsSessionStatusActive(session.Status) {
			cluster.activeSessions++
		}
	}
	for _, queue := range replay.Queues {
		if normalizeOperatorQueueStatus(queue.Status) != "OPEN" {
			continue
		}
		resolution := resolveProtoClusterFromQueue(filter.WorkspaceID, queue)
		cluster := ensureInstrumentationCluster(clusterAccumulators, resolution.protoClusterID, resolution.resolutionKind)
		cluster.addResolution(resolution)
		cluster.openQueues++
		cluster.blockerSignals++
	}

	report.Clusters = make([]ProtoClusterReport, 0, len(clusterAccumulators))
	for _, cluster := range clusterAccumulators {
		finalized, err := s.finalizeInstrumentationCluster(ctx, filter.WorkspaceID, cluster)
		if err != nil {
			return InstrumentationReport{}, err
		}
		report.Clusters = append(report.Clusters, finalized)
		if finalized.Metrics.BlockerSignalCount > 0 || finalized.Metrics.OpenQueueCount > 0 {
			report.Workspace.BlockedClusterCount++
		}
		if finalized.Metrics.DuplicationSignalCount > 0 {
			report.Workspace.DuplicateProneClusterCount++
		}
	}
	sort.Slice(report.Clusters, func(i, j int) bool {
		left := report.Clusters[i]
		right := report.Clusters[j]
		if left.Metrics.BlockerDensity != right.Metrics.BlockerDensity {
			return left.Metrics.BlockerDensity > right.Metrics.BlockerDensity
		}
		if left.Metrics.EventCount != right.Metrics.EventCount {
			return left.Metrics.EventCount > right.Metrics.EventCount
		}
		return left.ProtoClusterID < right.ProtoClusterID
	})

	report.Workspace.TotalClusters = len(report.Clusters)
	report.Workspace.WorkspaceActivityCountsByAgent = cloneIntMap(globalActivityCounts)
	report.Workspace.TopAgentByActivity, report.Workspace.TopAgentActivityShare = instrumentationTopShare(globalActivityCounts)
	_, report.Workspace.WorkspaceCommunicationCentralization = instrumentationTopShare(globalCommunicationCounts)
	if len(report.Clusters) > filter.ClusterLimit {
		report.Clusters = append([]ProtoClusterReport(nil), report.Clusters[:filter.ClusterLimit]...)
	}
	return report, nil
}

func (s *Store) ListProtoClusters(ctx context.Context, filter InstrumentationReportFilter) ([]ProtoClusterReport, error) {
	report, err := s.BuildInstrumentationReport(ctx, filter)
	if err != nil {
		return nil, err
	}
	return append([]ProtoClusterReport(nil), report.Clusters...), nil
}

func (s *Store) RecordInstrumentationMetricSnapshot(ctx context.Context, report InstrumentationReport, input InstrumentationSnapshotInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(report.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, referenceAt)
	if err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = "instrumentation.report"
	}
	clusterLimit := input.Limit
	if clusterLimit <= 0 {
		clusterLimit = 10
	}
	clusters := report.Clusters
	if len(clusters) > clusterLimit {
		clusters = append([]ProtoClusterReport(nil), clusters[:clusterLimit]...)
	} else {
		clusters = append([]ProtoClusterReport(nil), clusters...)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin instrumentation snapshot tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "cluster.metric_snapshot",
			EntityType:  "workspace",
			EntityID:    workspaceID,
			ActorType:   "system",
			ActorID:     actorID,
			PayloadJSON: mustJSON(instrumentationSnapshotPayload{
				GeneratedAt:   report.GeneratedAt,
				WorkspaceID:   report.WorkspaceID,
				TimeAuthority: report.TimeAuthority,
				Filter:        report.Filter,
				Replay:        report.Replay,
				Workspace:     report.Workspace,
				Clusters:      clusters,
			}),
			CreatedAt: referenceAt,
		})
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit instrumentation snapshot tx: %w", err)
	}
	return record, nil
}

func ensureInstrumentationCluster(clusters map[string]*instrumentationClusterAccumulator, protoClusterID, resolutionKind string) *instrumentationClusterAccumulator {
	protoClusterID = strings.TrimSpace(protoClusterID)
	if protoClusterID == "" {
		protoClusterID = "workspace:unknown"
	}
	if existing, ok := clusters[protoClusterID]; ok {
		if existing.resolutionKind == "" {
			existing.resolutionKind = resolutionKind
		}
		return existing
	}
	cluster := &instrumentationClusterAccumulator{
		protoClusterID:   protoClusterID,
		resolutionKind:   strings.TrimSpace(resolutionKind),
		taskIDs:          map[string]struct{}{},
		sessionIDs:       map[string]struct{}{},
		docKeys:          map[string]struct{}{},
		artifactRefs:     map[string]struct{}{},
		agentIDs:         map[string]struct{}{},
		eventTypeCounts:  map[string]int{},
		activityCounts:   map[string]int{},
		communicationIn:  map[string]int{},
		communicationOut: map[string]int{},
	}
	clusters[protoClusterID] = cluster
	return cluster
}

func (cluster *instrumentationClusterAccumulator) addResolution(resolution instrumentationResolution) {
	for _, taskID := range resolution.taskIDs {
		if trimmed := strings.TrimSpace(taskID); trimmed != "" {
			cluster.taskIDs[trimmed] = struct{}{}
		}
	}
	for _, sessionID := range resolution.sessionIDs {
		if trimmed := strings.TrimSpace(sessionID); trimmed != "" {
			cluster.sessionIDs[trimmed] = struct{}{}
		}
	}
	for _, docKey := range resolution.docKeys {
		if trimmed := strings.TrimSpace(docKey); trimmed != "" {
			cluster.docKeys[trimmed] = struct{}{}
		}
	}
	for _, artifactRef := range resolution.artifactRefs {
		if trimmed := strings.TrimSpace(artifactRef); trimmed != "" {
			cluster.artifactRefs[trimmed] = struct{}{}
		}
	}
	for _, agentID := range resolution.agentIDs {
		if trimmed := strings.TrimSpace(agentID); trimmed != "" {
			cluster.agentIDs[trimmed] = struct{}{}
		}
	}
}

func (s *Store) finalizeInstrumentationCluster(ctx context.Context, workspaceID string, cluster *instrumentationClusterAccumulator) (ProtoClusterReport, error) {
	finalized := cluster.finalize()
	roleLock, err := s.instrumentationRoleLockMetrics(ctx, workspaceID, finalized)
	if err != nil {
		return ProtoClusterReport{}, err
	}
	finalized.Metrics.RoleLock = roleLock
	return finalized, nil
}

func (cluster *instrumentationClusterAccumulator) finalize() ProtoClusterReport {
	activityShares := map[string]float64{}
	totalActivity := 0
	for _, count := range cluster.activityCounts {
		totalActivity += count
	}
	maxActivityShare := 0.0
	for agentID, count := range cluster.activityCounts {
		if totalActivity > 0 {
			activityShares[agentID] = float64(count) / float64(totalActivity)
			if activityShares[agentID] > maxActivityShare {
				maxActivityShare = activityShares[agentID]
			}
		}
	}

	communicationTotals := map[string]int{}
	for agentID, count := range cluster.communicationOut {
		communicationTotals[agentID] += count
	}
	for agentID, count := range cluster.communicationIn {
		communicationTotals[agentID] += count
	}
	_, communicationCentralization := instrumentationTopShare(communicationTotals)

	duplicationSignals := 0
	if count := len(cluster.sessionIDs); count > 1 {
		duplicationSignals += count - 1
	}
	if count := len(cluster.agentIDs); count > 1 {
		duplicationSignals += count - 1
	}
	if cluster.knowledgeSurfaceMutations > 1 {
		duplicationSignals++
	}

	blockerDensity := 0.0
	duplicationIndex := 0.0
	if cluster.eventCount > 0 {
		blockerDensity = float64(cluster.blockerSignals) / float64(cluster.eventCount)
		duplicationIndex = float64(duplicationSignals) / float64(cluster.eventCount)
	}

	return ProtoClusterReport{
		ProtoClusterID: cluster.protoClusterID,
		ResolutionKind: cluster.resolutionKind,
		TaskIDs:        instrumentationSortedKeys(cluster.taskIDs),
		SessionIDs:     instrumentationSortedKeys(cluster.sessionIDs),
		DocKeys:        instrumentationSortedKeys(cluster.docKeys),
		ArtifactRefs:   instrumentationSortedKeys(cluster.artifactRefs),
		AgentIDs:       instrumentationSortedKeys(cluster.agentIDs),
		Metrics: ProtoClusterMetrics{
			EventCount:                  cluster.eventCount,
			EventTypeCounts:             cloneIntMap(cluster.eventTypeCounts),
			ActiveSessionCount:          cluster.activeSessions,
			OpenQueueCount:              cluster.openQueues,
			BlockerSignalCount:          cluster.blockerSignals,
			BlockerDensity:              blockerDensity,
			ActivityCountsByAgent:       cloneIntMap(cluster.activityCounts),
			ActivityShareByAgent:        activityShares,
			MaxAgentActivityShare:       maxActivityShare,
			CommunicationInByAgent:      cloneIntMap(cluster.communicationIn),
			CommunicationOutByAgent:     cloneIntMap(cluster.communicationOut),
			CommunicationCentralization: communicationCentralization,
			DuplicationSignalCount:      duplicationSignals,
			DuplicationIndex:            duplicationIndex,
			LastEventAt:                 cluster.lastEventAt,
		},
	}
}

func (s *Store) instrumentationRoleLockMetrics(ctx context.Context, workspaceID string, cluster ProtoClusterReport) (ProtoClusterRoleLockMetrics, error) {
	metrics := ProtoClusterRoleLockMetrics{}

	stewardCounts, activeStewardCount, err := s.instrumentationStewardCounts(ctx, cluster.ProtoClusterID)
	if err != nil {
		return ProtoClusterRoleLockMetrics{}, err
	}
	metrics.StewardHHI = instrumentationHHIFromCounts(stewardCounts)
	metrics.ActiveStewardCount = activeStewardCount

	claimCounts, activeClaimCount, err := s.instrumentationTaskClaimCounts(ctx, workspaceID, cluster.TaskIDs)
	if err != nil {
		return ProtoClusterRoleLockMetrics{}, err
	}
	metrics.AcceptedBuilderHHI = instrumentationHHIFromCounts(claimCounts)
	metrics.ActiveClaimCount = activeClaimCount

	reviewerCounts, blockingReviewCount, err := s.instrumentationBlockingReviewerCounts(ctx, workspaceID, cluster.TaskIDs)
	if err != nil {
		return ProtoClusterRoleLockMetrics{}, err
	}
	metrics.DefaultReviewerHHI = instrumentationHHIFromCounts(reviewerCounts)
	metrics.BlockingReviewCount = blockingReviewCount

	metrics.MotifReuseHHI = instrumentationHHIFromCounts(
		instrumentationMergeCounts(stewardCounts, claimCounts, reviewerCounts),
	)
	metrics.Partial = len(metrics.MissingComponents) > 0
	observed := []float64{
		metrics.StewardHHI,
		metrics.AcceptedBuilderHHI,
		metrics.DefaultReviewerHHI,
		metrics.MotifReuseHHI,
	}
	metrics.Index = instrumentationAverageObservedHHI(observed...)
	return metrics, nil
}

func (s *Store) instrumentationStewardCounts(ctx context.Context, clusterID string) (map[string]int, int, error) {
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return map[string]int{}, 0, nil
	}
	referenceAt, err := s.clusterStewardReferenceTime(ctx, clusterID, time.Now().UTC())
	if err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT steward_agent_id, status, expires_at FROM cluster_stewards WHERE cluster_id = ?`, clusterID)
	if err != nil {
		return nil, 0, fmt.Errorf("query cluster stewards: %w", err)
	}
	defer rows.Close()

	counts := map[string]int{}
	activeCount := 0
	for rows.Next() {
		var stewardAgentID string
		var status string
		var expiresAt time.Time
		if err := rows.Scan(&stewardAgentID, &status, &expiresAt); err != nil {
			return nil, 0, fmt.Errorf("scan cluster steward: %w", err)
		}
		stewardAgentID = strings.TrimSpace(stewardAgentID)
		if stewardAgentID == "" {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(status), "ACTIVE") && expiresAt.After(referenceAt) {
			counts[stewardAgentID]++
			activeCount++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate cluster stewards: %w", err)
	}
	return counts, activeCount, nil
}

func (s *Store) instrumentationTaskClaimCounts(ctx context.Context, workspaceID string, taskIDs []string) (map[string]int, int, error) {
	taskIDs = uniqueSortedStrings(taskIDs)
	if len(taskIDs) == 0 {
		return map[string]int{}, 0, nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(taskIDs)), ",")
	args := make([]any, 0, len(taskIDs)+1)
	args = append(args, strings.TrimSpace(workspaceID))
	for _, taskID := range taskIDs {
		args = append(args, taskID)
	}
	query := fmt.Sprintf(`SELECT agent_id FROM task_claims WHERE workspace_id = ? AND task_id IN (%s) AND claim_status IN ('CLAIMED', 'BLOCKED')`, placeholders)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query task claims for role lock: %w", err)
	}
	defer rows.Close()

	counts := map[string]int{}
	total := 0
	for rows.Next() {
		var agentID string
		if err := rows.Scan(&agentID); err != nil {
			return nil, 0, fmt.Errorf("scan task claim for role lock: %w", err)
		}
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		counts[agentID]++
		total++
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate task claims for role lock: %w", err)
	}
	return counts, total, nil
}

func (s *Store) instrumentationBlockingReviewerCounts(ctx context.Context, workspaceID string, taskIDs []string) (map[string]int, int, error) {
	taskIDs = uniqueSortedStrings(taskIDs)
	if len(taskIDs) == 0 {
		return map[string]int{}, 0, nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(taskIDs)), ",")
	args := make([]any, 0, len(taskIDs)+1)
	args = append(args, strings.TrimSpace(workspaceID))
	for _, taskID := range taskIDs {
		args = append(args, taskID)
	}
	query := fmt.Sprintf(`SELECT COALESCE(NULLIF(TRIM(assigned_to), ''), NULLIF(TRIM(agent_id), '')) FROM human_actions WHERE workspace_id = ? AND task_id IN (%s) AND blocking = 1 AND status = 'PENDING'`, placeholders)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query blocking reviewers for role lock: %w", err)
	}
	defer rows.Close()

	counts := map[string]int{}
	total := 0
	for rows.Next() {
		var reviewer sql.NullString
		if err := rows.Scan(&reviewer); err != nil {
			return nil, 0, fmt.Errorf("scan blocking reviewer for role lock: %w", err)
		}
		normalized := strings.TrimSpace(reviewer.String)
		if !reviewer.Valid || normalized == "" {
			continue
		}
		counts[normalized]++
		total++
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate blocking reviewers for role lock: %w", err)
	}
	return counts, total, nil
}

func instrumentationHHIFromCounts(counts map[string]int) float64 {
	total := 0
	for _, count := range counts {
		if count > 0 {
			total += count
		}
	}
	if total == 0 {
		return 0
	}
	score := 0.0
	for _, count := range counts {
		if count <= 0 {
			continue
		}
		share := float64(count) / float64(total)
		score += share * share
	}
	return instrumentationClampUnit(score)
}

func instrumentationMergeCounts(groups ...map[string]int) map[string]int {
	merged := map[string]int{}
	for _, group := range groups {
		for key, count := range group {
			if count <= 0 {
				continue
			}
			merged[key] += count
		}
	}
	return merged
}

func instrumentationAverageObservedHHI(values ...float64) float64 {
	sum := 0.0
	observed := 0
	for _, value := range values {
		if value < 0 {
			continue
		}
		sum += value
		observed++
	}
	if observed == 0 {
		return 0
	}
	return instrumentationClampUnit(sum / float64(observed))
}

func instrumentationClampUnit(value float64) float64 {
	switch {
	case value < 0:
		return 0
	case value > 1:
		return 1
	default:
		return value
	}
}

func (s *Store) resolveProtoClusterForEvent(ctx context.Context, workspaceID string, event RuntimeEventRecord, taskHydrationCache map[string]TaskHydrationBundle, updateLinkCache map[string]instrumentationAgentUpdateLinks) (instrumentationResolution, error) {
	refs, err := s.instrumentationEvidenceForEvent(ctx, workspaceID, event, taskHydrationCache, updateLinkCache)
	if err != nil {
		return instrumentationResolution{}, err
	}

	resolution := instrumentationResolution{
		taskIDs:      uniqueSortedStrings(refs.taskIDs),
		sessionIDs:   uniqueSortedStrings(refs.sessionIDs),
		docKeys:      uniqueSortedStrings(refs.docKeys),
		artifactRefs: uniqueSortedStrings(refs.artifactRefs),
		agentIDs:     uniqueSortedStrings(refs.agentIDs),
	}
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
	default:
		entityID := firstNonEmpty(strings.TrimSpace(event.EntityID), strings.TrimSpace(event.EventID))
		resolution.protoClusterID = "entity:" + workspaceID + "/" + strings.TrimSpace(event.EntityType) + "/" + entityID
		resolution.resolutionKind = "entity"
	}
	return resolution, nil
}

func (s *Store) instrumentationEvidenceForEvent(ctx context.Context, workspaceID string, event RuntimeEventRecord, taskHydrationCache map[string]TaskHydrationBundle, updateLinkCache map[string]instrumentationAgentUpdateLinks) (instrumentationEventEvidence, error) {
	payload := instrumentationDecodePayloadMap(event.PayloadJSON)
	evidence := instrumentationEventEvidence{
		taskIDs:      collectNonEmpty(strings.TrimSpace(event.TaskID), instrumentationPayloadString(payload, "task_id")),
		sessionIDs:   collectNonEmpty(strings.TrimSpace(event.SessionID), instrumentationPayloadString(payload, "session_id")),
		docKeys:      []string{},
		artifactRefs: []string{},
		agentIDs: collectNonEmpty(
			strings.TrimSpace(event.AgentID),
			strings.TrimSpace(event.ActorID),
			instrumentationPayloadString(payload, "agent_id"),
			instrumentationPayloadString(payload, "from_agent_id"),
			instrumentationPayloadString(payload, "to_agent_id"),
			instrumentationPayloadString(payload, "from"),
		),
	}

	evidence.taskIDs = append(evidence.taskIDs, instrumentationPayloadStringSlice(payload, "task_ids")...)
	evidence.sessionIDs = append(evidence.sessionIDs, instrumentationPayloadStringSlice(payload, "session_ids")...)
	evidence.docKeys = append(evidence.docKeys, instrumentationPayloadString(payload, "doc_key"))
	evidence.docKeys = append(evidence.docKeys, instrumentationPayloadStringSlice(payload, "doc_keys")...)
	evidence.docKeys = append(evidence.docKeys, instrumentationPayloadStringSlice(payload, "related_doc_keys")...)
	evidence.artifactRefs = append(evidence.artifactRefs, instrumentationPayloadString(payload, "artifact_ref"))
	evidence.artifactRefs = append(evidence.artifactRefs, instrumentationPayloadStringSlice(payload, "artifact_refs")...)
	evidence.artifactRefs = append(evidence.artifactRefs, instrumentationPayloadArtifactRefs(payload, "related_artifact_refs")...)

	switch strings.TrimSpace(event.EntityType) {
	case "workspace_doc":
		evidence.docKeys = append(evidence.docKeys, strings.TrimSpace(event.EntityID))
	case "workspace_artifact":
		if ref := instrumentationPayloadString(payload, "artifact_ref"); ref != "" {
			evidence.artifactRefs = append(evidence.artifactRefs, ref)
		} else if trimmed := strings.TrimSpace(event.EntityID); trimmed != "" {
			evidence.artifactRefs = append(evidence.artifactRefs, trimmed)
		}
	case "agent_session":
		if strings.TrimSpace(event.EventType) == "session.takeover" {
			var takeover struct {
				SourceState    AgentSessionStateRecord `json:"source_state"`
				SuccessorState AgentSessionStateRecord `json:"successor_state"`
			}
			if decodeReplayPayload(event, &takeover) {
				evidence.taskIDs = append(evidence.taskIDs, strings.TrimSpace(takeover.SourceState.TaskID), strings.TrimSpace(takeover.SuccessorState.TaskID))
				evidence.sessionIDs = append(evidence.sessionIDs, strings.TrimSpace(takeover.SourceState.SessionID), strings.TrimSpace(takeover.SuccessorState.SessionID))
				evidence.docKeys = append(evidence.docKeys, takeover.SourceState.RelatedDocKeys...)
				evidence.docKeys = append(evidence.docKeys, takeover.SuccessorState.RelatedDocKeys...)
				for _, artifact := range takeover.SourceState.RelatedArtifactRefs {
					evidence.artifactRefs = append(evidence.artifactRefs, strings.TrimSpace(artifact.Ref))
				}
				for _, artifact := range takeover.SuccessorState.RelatedArtifactRefs {
					evidence.artifactRefs = append(evidence.artifactRefs, strings.TrimSpace(artifact.Ref))
				}
			}
		} else {
			var state AgentSessionStateRecord
			if decodeReplayPayload(event, &state) {
				evidence.taskIDs = append(evidence.taskIDs, strings.TrimSpace(state.TaskID))
				evidence.sessionIDs = append(evidence.sessionIDs, strings.TrimSpace(state.SessionID))
				evidence.docKeys = append(evidence.docKeys, state.RelatedDocKeys...)
				for _, artifact := range state.RelatedArtifactRefs {
					evidence.artifactRefs = append(evidence.artifactRefs, strings.TrimSpace(artifact.Ref))
				}
			}
		}
	case "agent_update":
		updateID := firstNonEmpty(strings.TrimSpace(event.EntityID), instrumentationPayloadString(payload, "update_id"))
		if updateID != "" {
			links, err := s.loadInstrumentationAgentUpdateLinks(ctx, workspaceID, updateID, updateLinkCache)
			if err != nil {
				return instrumentationEventEvidence{}, err
			}
			evidence.taskIDs = append(evidence.taskIDs, links.taskIDs...)
			evidence.docKeys = append(evidence.docKeys, links.docKeys...)
			evidence.artifactRefs = append(evidence.artifactRefs, links.artifactRefs...)
		}
	}

	if len(evidence.taskIDs) > 0 {
		validTaskIDs := []string{}
		for _, taskID := range uniqueSortedStrings(evidence.taskIDs) {
			bundle, err := s.instrumentationTaskHydration(ctx, workspaceID, taskID, taskHydrationCache)
			if err != nil {
				if errors.Is(err, ErrTaskNotFound) || errors.Is(err, ErrWorkspaceTaskAbsent) {
					continue
				}
				return instrumentationEventEvidence{}, err
			}
			validTaskIDs = append(validTaskIDs, taskID)
			for _, doc := range bundle.Docs {
				evidence.docKeys = append(evidence.docKeys, strings.TrimSpace(doc.DocKey))
			}
			for _, artifact := range bundle.Artifacts {
				evidence.artifactRefs = append(evidence.artifactRefs, strings.TrimSpace(artifact.ArtifactRef))
			}
			for _, task := range bundle.RelatedTasks {
				validTaskIDs = append(validTaskIDs, strings.TrimSpace(task.TaskID))
			}
		}
		evidence.taskIDs = validTaskIDs
	}

	evidence.taskIDs = uniqueSortedStrings(evidence.taskIDs)
	evidence.sessionIDs = uniqueSortedStrings(evidence.sessionIDs)
	evidence.docKeys = uniqueSortedStrings(evidence.docKeys)
	evidence.artifactRefs = uniqueSortedStrings(evidence.artifactRefs)
	evidence.agentIDs = uniqueSortedStrings(evidence.agentIDs)
	return evidence, nil
}

func (s *Store) instrumentationTaskHydration(ctx context.Context, workspaceID, taskID string, cache map[string]TaskHydrationBundle) (TaskHydrationBundle, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return TaskHydrationBundle{}, nil
	}
	if bundle, ok := cache[taskID]; ok {
		return bundle, nil
	}
	bundle, err := s.GetTaskHydrationBundle(ctx, TaskHydrationFilter{
		TaskID:           taskID,
		WorkspaceID:      workspaceID,
		UpdatesLimit:     20,
		ArtifactLimit:    20,
		RelatedTaskLimit: 20,
	})
	if err != nil {
		return TaskHydrationBundle{}, fmt.Errorf("load task hydration bundle for %s: %w", taskID, err)
	}
	cache[taskID] = bundle
	return bundle, nil
}

func (s *Store) loadInstrumentationAgentUpdateLinks(ctx context.Context, workspaceID, updateID string, cache map[string]instrumentationAgentUpdateLinks) (instrumentationAgentUpdateLinks, error) {
	updateID = strings.TrimSpace(updateID)
	if updateID == "" {
		return instrumentationAgentUpdateLinks{}, nil
	}
	if cached, ok := cache[updateID]; ok {
		return cached, nil
	}
	var payloadJSON string
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT payload_json FROM agent_updates WHERE workspace_id = ? AND update_id = ?`,
		workspaceID,
		updateID,
	).Scan(&payloadJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cache[updateID] = instrumentationAgentUpdateLinks{}
			return instrumentationAgentUpdateLinks{}, nil
		}
		return instrumentationAgentUpdateLinks{}, fmt.Errorf("query agent update payload %s: %w", updateID, err)
	}
	normalized, err := model.ParseAndNormalizeAgentUpdatePayloadV1(payloadJSON)
	if err != nil {
		return instrumentationAgentUpdateLinks{}, nil
	}
	if strings.TrimSpace(normalized) == "" {
		cache[updateID] = instrumentationAgentUpdateLinks{}
		return instrumentationAgentUpdateLinks{}, nil
	}
	var payload model.AgentUpdatePayloadV1
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		cache[updateID] = instrumentationAgentUpdateLinks{}
		return instrumentationAgentUpdateLinks{}, nil
	}
	links := instrumentationAgentUpdateLinks{
		taskIDs: uniqueSortedStrings(payload.TaskIDs),
		docKeys: uniqueSortedStrings(payload.DocKeys),
	}
	for _, artifact := range payload.Artifacts {
		if trimmed := strings.TrimSpace(artifact.Ref); trimmed != "" {
			links.artifactRefs = append(links.artifactRefs, trimmed)
		}
	}
	links.artifactRefs = uniqueSortedStrings(links.artifactRefs)
	cache[updateID] = links
	return links, nil
}

func instrumentationCommunicationPair(event RuntimeEventRecord) (string, string) {
	payload := instrumentationDecodePayloadMap(event.PayloadJSON)
	fromAgent := firstNonEmpty(
		instrumentationPayloadString(payload, "from_agent_id"),
		instrumentationPayloadString(payload, "from"),
	)
	toAgent := instrumentationPayloadString(payload, "to_agent_id")
	if strings.TrimSpace(event.EventType) == "agent_response.recorded" && fromAgent == "" {
		fromAgent = strings.TrimSpace(event.AgentID)
	}
	return strings.TrimSpace(fromAgent), strings.TrimSpace(toAgent)
}

func isSyntheticInstrumentationEvent(event RuntimeEventRecord) bool {
	return isSyntheticOperationalEvent(event)
}

func resolveProtoClusterFromSession(workspaceID string, session RuntimeReplaySession) instrumentationResolution {
	taskID := strings.TrimSpace(session.TaskID)
	sessionID := strings.TrimSpace(session.SessionID)
	resolution := instrumentationResolution{
		taskIDs:    collectNonEmpty(taskID),
		sessionIDs: collectNonEmpty(sessionID),
		docKeys:    append([]string{}, session.RelatedDocKeys...),
		agentIDs:   collectNonEmpty(strings.TrimSpace(session.AgentID)),
	}
	if taskID != "" {
		resolution.protoClusterID = "task:" + workspaceID + "/" + taskID
		resolution.resolutionKind = "task"
	} else {
		resolution.protoClusterID = "session:" + workspaceID + "/" + sessionID
		resolution.resolutionKind = "session"
	}
	return resolution
}

func resolveProtoClusterFromQueue(workspaceID string, queue RuntimeReplayQueue) instrumentationResolution {
	taskID := strings.TrimSpace(queue.TaskID)
	sessionID := strings.TrimSpace(queue.SessionID)
	resolution := instrumentationResolution{
		taskIDs:    collectNonEmpty(taskID),
		sessionIDs: collectNonEmpty(sessionID),
		agentIDs:   collectNonEmpty(strings.TrimSpace(queue.AgentID)),
	}
	switch {
	case taskID != "":
		resolution.protoClusterID = "task:" + workspaceID + "/" + taskID
		resolution.resolutionKind = "task"
	case sessionID != "":
		resolution.protoClusterID = "session:" + workspaceID + "/" + sessionID
		resolution.resolutionKind = "session"
	case strings.TrimSpace(queue.SourceID) != "":
		resolution.protoClusterID = "source:" + workspaceID + "/" + strings.TrimSpace(queue.SourceKind) + "/" + strings.TrimSpace(queue.SourceID)
		resolution.resolutionKind = "source"
	default:
		resolution.protoClusterID = "queue:" + workspaceID + "/" + strings.TrimSpace(queue.QueueID)
		resolution.resolutionKind = "queue"
	}
	return resolution
}

func instrumentationTopShare(counts map[string]int) (string, float64) {
	total := 0
	topAgent := ""
	topCount := 0
	for agentID, count := range counts {
		total += count
		if count > topCount || (count == topCount && topAgent != "" && agentID < topAgent) {
			topAgent = agentID
			topCount = count
		}
		if topAgent == "" {
			topAgent = agentID
			topCount = count
		}
	}
	if total == 0 || topAgent == "" {
		return "", 0
	}
	return topAgent, float64(topCount) / float64(total)
}

func instrumentationDecodePayloadMap(payloadJSON string) map[string]any {
	payloadJSON = strings.TrimSpace(payloadJSON)
	if payloadJSON == "" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return nil
	}
	return payload
}

func instrumentationPayloadString(payload map[string]any, key string) string {
	if len(payload) == 0 {
		return ""
	}
	value, ok := payload[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}

func instrumentationPayloadStringSlice(payload map[string]any, key string) []string {
	if len(payload) == 0 {
		return nil
	}
	raw, ok := payload[key]
	if !ok {
		return nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		switch typed := item.(type) {
		case string:
			if trimmed := strings.TrimSpace(typed); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}

func instrumentationPayloadArtifactRefs(payload map[string]any, key string) []string {
	if len(payload) == 0 {
		return nil
	}
	raw, ok := payload[key]
	if !ok {
		return nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if ref := instrumentationPayloadString(mapped, "ref"); ref != "" {
			out = append(out, ref)
		}
	}
	return out
}

func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
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
	sort.Strings(out)
	return out
}

func instrumentationSortedKeys(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func cloneIntMap(values map[string]int) map[string]int {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]int, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func collectNonEmpty(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
