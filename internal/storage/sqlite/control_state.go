package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	clusterControlModeSteady         = "STEADY"
	clusterControlModeAntiCollapse   = "ANTI_COLLAPSE"
	clusterControlModeCoherence      = "COHERENCE"
	clusterControlModeDecentralize   = "DECENTRALIZE"
	clusterControlModeSynergySeeking = "SYNERGY_SEEKING"
	clusterControlModeUnfreeze       = "UNFREEZE"
	clusterControlModeStabilize      = "STABILIZE"

	clusterControlTickClusterLimit    = 128
	clusterControlTickHysteresisEpoch = 2
)

type ClusterControlStateFilter struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Mode           string `json:"mode,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type ClusterControlTickInput struct {
	WorkspaceID                string
	ProtoClusterID             string
	ActorID                    string
	PromptContextEnvelope      map[string]any
	PromptContextSurface       string
	PromptContextPrincipalType string
	PromptContextPrincipalID   string
}

type ClusterControlSnapshotInput struct {
	ActorID                    string
	Limit                      int
	PromptContextEnvelope      map[string]any
	PromptContextSurface       string
	PromptContextPrincipalType string
	PromptContextPrincipalID   string
}

type ClusterControlHeuristicProfileContext struct {
	Profile           string  `json:"profile"`
	ThroughputMin     float64 `json:"throughput_min"`
	ThroughputMax     float64 `json:"throughput_max"`
	ReviewMin         float64 `json:"review_min"`
	ReviewMax         float64 `json:"review_max"`
	CoordinationMin   float64 `json:"coordination_min"`
	CoordinationMax   float64 `json:"coordination_max"`
	CentralizationMax float64 `json:"centralization_max"`
	BlockerDensityMax float64 `json:"blocker_density_max"`
	DuplicationMin    float64 `json:"duplication_min"`
	DuplicationMax    float64 `json:"duplication_max"`
}

type ClusterControlSignalDeviationVector struct {
	Throughput     float64 `json:"throughput"`
	Review         float64 `json:"review"`
	Coordination   float64 `json:"coordination"`
	Centralization float64 `json:"centralization"`
	BlockerDensity float64 `json:"blocker_density"`
	Duplication    float64 `json:"duplication"`
	NoveltyGap     float64 `json:"novelty_gap"`
	SynergyGap     float64 `json:"synergy_gap"`
}

type ClusterControlStateRecord struct {
	WorkspaceID            string                              `json:"workspace_id"`
	ProtoClusterID         string                              `json:"proto_cluster_id"`
	ResolutionKind         string                              `json:"resolution_kind"`
	CorridorProfile        string                              `json:"heuristic_profile"`
	Epoch                  int                                 `json:"epoch"`
	CurrentMode            string                              `json:"stabilized_mode_hint"`
	CandidateMode          string                              `json:"candidate_mode_hint"`
	CandidateStreak        int                                 `json:"stability_streak"`
	DominantViolationKind  string                              `json:"dominant_signal_kind,omitempty"`
	DominantViolationScore float64                             `json:"dominant_signal_score"`
	AttentionBand          string                              `json:"attention_band"`
	PressureScore          int                                 `json:"pressure_score"`
	ConfirmedTensionCount  int                                 `json:"confirmed_tension_count"`
	PendingTensionCount    int                                 `json:"pending_tension_count"`
	TaskIDs                []string                            `json:"task_ids,omitempty"`
	SessionIDs             []string                            `json:"session_ids,omitempty"`
	DocKeys                []string                            `json:"doc_keys,omitempty"`
	ArtifactRefs           []string                            `json:"artifact_refs,omitempty"`
	AgentIDs               []string                            `json:"agent_ids,omitempty"`
	ConfirmedTensionIDs    []string                            `json:"confirmed_tension_ids,omitempty"`
	PendingTensionIDs      []string                            `json:"pending_tension_ids,omitempty"`
	OperatorHints          ControlSuggestedControls            `json:"operator_hints"`
	ViolationVector        ClusterControlSignalDeviationVector `json:"signal_deviation_vector"`
	Summary                string                              `json:"summary,omitempty"`
	LastBasisAt            string                              `json:"last_basis_at,omitempty"`
	LastTickEventID        string                              `json:"last_tick_event_id,omitempty"`
	LastTickAt             string                              `json:"last_tick_at,omitempty"`
	LastTransitionAt       string                              `json:"last_stabilized_at,omitempty"`
	CreatedAt              string                              `json:"created_at"`
	UpdatedAt              string                              `json:"updated_at"`
}

type ClusterControlStateCluster struct {
	ProtoClusterID        string                                `json:"proto_cluster_id"`
	ResolutionKind        string                                `json:"resolution_kind"`
	ProfileContext        ClusterControlHeuristicProfileContext `json:"heuristic_profile_context"`
	State                 ClusterControlStateRecord             `json:"state"`
	Metrics               ProtoClusterMetrics                   `json:"metrics"`
	Signals               ControlSignalVector                   `json:"signals"`
	SuggestedControls     ControlSuggestedControls              `json:"suggested_controls"`
	MetricsMissing        bool                                  `json:"metrics_missing,omitempty"`
	BasisStale            bool                                  `json:"basis_stale,omitempty"`
	LastTensionBasisAt    string                                `json:"last_tension_basis_at,omitempty"`
	ConfirmedCountsByType map[string]int                        `json:"confirmed_counts_by_type,omitempty"`
	PendingCountsByType   map[string]int                        `json:"pending_counts_by_type,omitempty"`
	Summary               string                                `json:"summary,omitempty"`
}

type ClusterControlStateWorkspaceMetrics struct {
	TotalClusters            int            `json:"total_clusters"`
	HotClusterCount          int            `json:"hot_cluster_count"`
	ConfirmedTensionCount    int            `json:"confirmed_tension_count"`
	PendingTensionCount      int            `json:"pending_tension_count"`
	NonSteadyCount           int            `json:"non_steady_hint_count"`
	TransitioningCount       int            `json:"stabilizing_count"`
	HighestPressureClusterID string         `json:"highest_pressure_cluster_id,omitempty"`
	HighestPressureScore     int            `json:"highest_pressure_score"`
	ModeCounts               map[string]int `json:"stabilized_hint_counts,omitempty"`
	CandidateCounts          map[string]int `json:"candidate_hint_counts,omitempty"`
}

type ClusterControlStateReport struct {
	WorkspaceID       string                              `json:"workspace_id"`
	TimeAuthority     WorkspaceTimeAuthority              `json:"time_authority"`
	GeneratedAt       string                              `json:"generated_at"`
	Filter            ClusterControlStateFilter           `json:"filter"`
	Workspace         ClusterControlStateWorkspaceMetrics `json:"workspace"`
	Clusters          []ClusterControlStateCluster        `json:"clusters,omitempty"`
	readSurfacePolicy ReadSurfacePolicy                   `json:"-"`
}

type ClusterControlStateDetail struct {
	TimeAuthority WorkspaceTimeAuthority     `json:"time_authority"`
	Cluster       ControlClusterReport       `json:"cluster"`
	State         ClusterControlStateCluster `json:"state"`
	Tensions      []TensionRecord            `json:"tensions,omitempty"`
	Events        []RuntimeEventRecord       `json:"events,omitempty"`
}

type ClusterControlTickResult struct {
	WorkspaceID       string                    `json:"workspace_id"`
	ProtoClusterID    string                    `json:"proto_cluster_id,omitempty"`
	TickedAt          string                    `json:"ticked_at"`
	EvaluatedClusters int                       `json:"evaluated_clusters"`
	UpdatedCount      int                       `json:"updated_count"`
	TransitionedCount int                       `json:"transitioned_count"`
	Events            []RuntimeEventRecord      `json:"events,omitempty"`
	Report            ClusterControlStateReport `json:"report"`
}

type clusterControlModeCandidate struct {
	Mode          string
	ViolationKind string
	Score         float64
}

func normalizeClusterControlStateFilter(filter ClusterControlStateFilter) ClusterControlStateFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	filter.Mode = strings.ToUpper(strings.TrimSpace(filter.Mode))
	filter.Limit = clampReadSurfaceLimit(filter.Limit, readSurfaceReportLimitDefault, readSurfaceReportLimitMax)
	return filter
}

func isKnownClusterControlMode(mode string) bool {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case clusterControlModeSteady,
		clusterControlModeAntiCollapse,
		clusterControlModeCoherence,
		clusterControlModeDecentralize,
		clusterControlModeSynergySeeking,
		clusterControlModeUnfreeze,
		clusterControlModeStabilize:
		return true
	default:
		return false
	}
}

func normalizeClusterControlMode(mode string) string {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case clusterControlModeAntiCollapse:
		return clusterControlModeAntiCollapse
	case clusterControlModeCoherence:
		return clusterControlModeCoherence
	case clusterControlModeDecentralize:
		return clusterControlModeDecentralize
	case clusterControlModeSynergySeeking:
		return clusterControlModeSynergySeeking
	case clusterControlModeUnfreeze:
		return clusterControlModeUnfreeze
	case clusterControlModeStabilize:
		return clusterControlModeStabilize
	default:
		return clusterControlModeSteady
	}
}

func (s *Store) BuildClusterControlStateReport(ctx context.Context, filter ClusterControlStateFilter) (ClusterControlStateReport, error) {
	if trimmed := strings.TrimSpace(filter.Mode); trimmed != "" && !isKnownClusterControlMode(trimmed) {
		return ClusterControlStateReport{}, fmt.Errorf("mode is invalid: %s", trimmed)
	}
	filter = normalizeClusterControlStateFilter(filter)
	if filter.WorkspaceID == "" {
		return ClusterControlStateReport{}, errors.New("workspace_id is required")
	}
	advisory, err := s.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		Limit:          clusterControlReportBasisLimit(filter.ProtoClusterID),
	})
	if err != nil {
		return ClusterControlStateReport{}, err
	}
	rows, err := s.listClusterControlStateRows(ctx, filter.WorkspaceID, filter.ProtoClusterID)
	if err != nil {
		return ClusterControlStateReport{}, err
	}
	rowByCluster := make(map[string]ClusterControlStateRecord, len(rows))
	for _, row := range rows {
		rowByCluster[row.ProtoClusterID] = row
	}
	report := ClusterControlStateReport{
		WorkspaceID:   filter.WorkspaceID,
		TimeAuthority: advisory.TimeAuthority,
		GeneratedAt:   generatedAtFromWorkspaceTimeAuthority(advisory.TimeAuthority),
		Filter:        filter,
		Workspace: ClusterControlStateWorkspaceMetrics{
			ModeCounts:      map[string]int{},
			CandidateCounts: map[string]int{},
		},
		readSurfacePolicy: clusterControlStatePolicy(filter),
	}
	clusters := make([]ClusterControlStateCluster, 0, len(advisory.Clusters)+len(rows))
	seenClusters := make(map[string]struct{}, len(advisory.Clusters))
	for _, cluster := range advisory.Clusters {
		clusterID := strings.TrimSpace(cluster.ProtoClusterID)
		row, ok := rowByCluster[clusterID]
		item := clusterControlStateClusterFromReport(filter.WorkspaceID, cluster, row, ok)
		if filter.Mode != "" && normalizeClusterControlMode(item.State.CurrentMode) != filter.Mode {
			continue
		}
		seenClusters[clusterID] = struct{}{}
		clusters = append(clusters, item)
	}
	for _, row := range rows {
		clusterID := strings.TrimSpace(row.ProtoClusterID)
		if _, ok := seenClusters[clusterID]; ok {
			continue
		}
		item := clusterControlStateClusterFromStoredState(row)
		if filter.Mode != "" && normalizeClusterControlMode(item.State.CurrentMode) != filter.Mode {
			continue
		}
		clusters = append(clusters, item)
	}
	sort.Slice(clusters, func(i, j int) bool {
		left := clusters[i]
		right := clusters[j]
		if left.State.PressureScore != right.State.PressureScore {
			return left.State.PressureScore > right.State.PressureScore
		}
		if left.State.CurrentMode != right.State.CurrentMode {
			return left.State.CurrentMode < right.State.CurrentMode
		}
		return left.ProtoClusterID < right.ProtoClusterID
	})
	fullClusters := append([]ClusterControlStateCluster(nil), clusters...)
	report.Clusters = fullClusters
	for _, cluster := range fullClusters {
		report.Workspace.TotalClusters++
		report.Workspace.ModeCounts[cluster.State.CurrentMode]++
		report.Workspace.CandidateCounts[cluster.State.CandidateMode]++
		report.Workspace.ConfirmedTensionCount += cluster.State.ConfirmedTensionCount
		report.Workspace.PendingTensionCount += cluster.State.PendingTensionCount
		if cluster.Signals.AttentionBand == "HOT" {
			report.Workspace.HotClusterCount++
		}
		if cluster.State.CurrentMode != clusterControlModeSteady {
			report.Workspace.NonSteadyCount++
		}
		if cluster.State.CandidateMode != cluster.State.CurrentMode && cluster.State.CandidateStreak > 0 {
			report.Workspace.TransitioningCount++
		}
		if cluster.State.PressureScore > report.Workspace.HighestPressureScore {
			report.Workspace.HighestPressureScore = cluster.State.PressureScore
			report.Workspace.HighestPressureClusterID = cluster.ProtoClusterID
		}
	}
	if filter.Limit > 0 && len(fullClusters) > filter.Limit {
		report.Clusters = append([]ClusterControlStateCluster(nil), fullClusters[:filter.Limit]...)
	}
	return report, nil
}

func (s *Store) BuildClusterControlStateDetail(ctx context.Context, workspaceID, protoClusterID string) (ClusterControlStateDetail, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	protoClusterID = strings.TrimSpace(protoClusterID)
	if workspaceID == "" {
		return ClusterControlStateDetail{}, errors.New("workspace_id is required")
	}
	if protoClusterID == "" {
		return ClusterControlStateDetail{}, errors.New("proto_cluster_id is required")
	}
	report, err := s.BuildClusterControlStateReport(ctx, ClusterControlStateFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: protoClusterID,
		Limit:          1,
	})
	if err != nil {
		return ClusterControlStateDetail{}, err
	}
	if len(report.Clusters) == 0 {
		return ClusterControlStateDetail{}, fmt.Errorf("cluster control state not found: %s/%s", workspaceID, protoClusterID)
	}
	advisory, err := s.BuildControlClusterDetail(ctx, workspaceID, protoClusterID)
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "not found") {
			return ClusterControlStateDetail{}, err
		}
		advisory = ControlClusterDetail{
			Cluster: synthesizeControlClusterFromState(report.Clusters[0].State),
		}
	}
	events, err := s.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "instrumentation_control_state",
		EntityID:    protoClusterID,
		Limit:       10,
	})
	if err != nil {
		return ClusterControlStateDetail{}, err
	}
	return ClusterControlStateDetail{
		TimeAuthority: report.TimeAuthority,
		Cluster:       advisory.Cluster,
		State:         report.Clusters[0],
		Tensions:      advisory.Tensions,
		Events:        events,
	}, nil
}

func (s *Store) TickClusterControlState(ctx context.Context, input ClusterControlTickInput) (ClusterControlTickResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.ProtoClusterID = strings.TrimSpace(input.ProtoClusterID)
	input.ActorID = strings.TrimSpace(input.ActorID)
	if input.WorkspaceID == "" {
		return ClusterControlTickResult{}, errors.New("workspace_id is required")
	}
	if input.ActorID == "" {
		return ClusterControlTickResult{}, errors.New("actor_id is required")
	}
	advisory, err := s.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID:    input.WorkspaceID,
		ProtoClusterID: input.ProtoClusterID,
		Limit:          clusterControlTickLimit(input.ProtoClusterID),
	})
	if err != nil {
		return ClusterControlTickResult{}, err
	}
	if input.ProtoClusterID != "" && len(advisory.Clusters) == 0 {
		return ClusterControlTickResult{}, fmt.Errorf("cluster control state not found: %s/%s", input.WorkspaceID, input.ProtoClusterID)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ClusterControlTickResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ClusterControlTickResult{}, fmt.Errorf("begin control state tick tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	events := make([]RuntimeEventRecord, 0, len(advisory.Clusters))
	updatedCount := 0
	transitionedCount := 0
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		currentRows, err := s.listClusterControlStateRowsTx(ctx, tx, input.WorkspaceID, input.ProtoClusterID)
		if err != nil {
			return err
		}
		rowByCluster := make(map[string]ClusterControlStateRecord, len(currentRows))
		for _, row := range currentRows {
			rowByCluster[row.ProtoClusterID] = row
		}
		for _, cluster := range advisory.Clusters {
			clusterID := strings.TrimSpace(cluster.ProtoClusterID)
			previous, ok := rowByCluster[clusterID]
			next := evaluateClusterControlState(input.WorkspaceID, cluster, clusterControlPreviousPointer(previous, ok), now)
			eventType := "cluster.control_state_ticked"
			if ok && normalizeClusterControlMode(previous.CurrentMode) != normalizeClusterControlMode(next.CurrentMode) {
				eventType = "cluster.control_state_stabilized"
				transitionedCount++
			}
			if !ok || clusterControlStateChanged(previous, next) {
				updatedCount++
			}
			eventID := nextID("rtev")
			next.LastTickEventID = eventID
			next.LastTickAt = now
			if err := s.upsertClusterControlStateTx(ctx, tx, next); err != nil {
				return err
			}
			agentID, sessionID, taskID, err := s.clusterControlEventRefsTx(ctx, tx, next)
			if err != nil {
				return err
			}
			payload := clusterControlRuntimeEventPayload(next, cluster, eventType)
			payload["event_type"] = eventType
			payload["entity_type"] = "instrumentation_control_state"
			payload["entity_id"] = clusterID
			payload["actor_type"] = "operator"
			payload["actor_id"] = input.ActorID
			promptFields := map[string]string{
				"workspace_id":         input.WorkspaceID,
				"actor_id":             input.ActorID,
				"proto_cluster_id":     clusterID,
				"event_type":           eventType,
				"entity_type":          "instrumentation_control_state",
				"entity_id":            clusterID,
				"actor_type":           "operator",
				"epoch":                fmt.Sprintf("%d", next.Epoch),
				"last_tick_event_id":   eventID,
				"last_tick_at":         now,
				"candidate_mode_hint":  next.CandidateMode,
				"stabilized_mode_hint": next.CurrentMode,
				"event_kind":           eventType,
				"typed_event_type":     "CONTROL_STATE_INTERPRETATION",
			}
			addControlStatePromptContextPrincipalFields(promptFields, input.PromptContextPrincipalType, input.PromptContextPrincipalID)
			payload, err = attachControlStatePromptContextEnvelope(
				payload,
				input.PromptContextEnvelope,
				controlStatePromptContextSurface(input.PromptContextSurface, "workspace.instrumentation.control.state.tick"),
				promptFields,
			)
			if err != nil {
				return err
			}
			event, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
				EventID:     eventID,
				WorkspaceID: input.WorkspaceID,
				EventType:   eventType,
				EntityType:  "instrumentation_control_state",
				EntityID:    clusterID,
				ActorType:   "operator",
				ActorID:     input.ActorID,
				AgentID:     agentID,
				SessionID:   sessionID,
				TaskID:      taskID,
				PayloadJSON: mustJSON(payload),
				CreatedAt:   now,
			})
			if err != nil {
				return err
			}
			events = append(events, event)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return ClusterControlTickResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ClusterControlTickResult{}, fmt.Errorf("commit control state tick tx: %w", err)
	}
	report, err := s.BuildClusterControlStateReport(ctx, ClusterControlStateFilter{
		WorkspaceID:    input.WorkspaceID,
		ProtoClusterID: input.ProtoClusterID,
		Limit:          clusterControlTickLimit(input.ProtoClusterID),
	})
	if err != nil {
		return ClusterControlTickResult{}, err
	}
	return ClusterControlTickResult{
		WorkspaceID:       input.WorkspaceID,
		ProtoClusterID:    input.ProtoClusterID,
		TickedAt:          now,
		EvaluatedClusters: len(advisory.Clusters),
		UpdatedCount:      updatedCount,
		TransitionedCount: transitionedCount,
		Events:            events,
		Report:            report,
	}, nil
}

func (s *Store) clusterControlEventRefsTx(ctx context.Context, tx *sql.Tx, record ClusterControlStateRecord) (string, string, string, error) {
	agentID := clusterControlPrimaryAgentID(record)
	if agentID != "" {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, record.WorkspaceID, agentID); err != nil {
			if !errors.Is(err, ErrAgentNotFound) {
				return "", "", "", err
			}
			agentID = ""
		}
	}
	sessionID := clusterControlPrimarySessionID(record)
	if sessionID != "" {
		if err := s.ensureAgentSessionInWorkspaceTx(ctx, tx, record.WorkspaceID, sessionID); err != nil {
			if !errors.Is(err, ErrSessionNotFound) {
				return "", "", "", err
			}
			sessionID = ""
		}
	}
	taskID := clusterControlPrimaryTaskID(record)
	if taskID != "" {
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, record.WorkspaceID, taskID); err != nil {
			if !errors.Is(err, ErrWorkspaceTaskAbsent) {
				return "", "", "", err
			}
			taskID = ""
		}
	}
	return agentID, sessionID, taskID, nil
}

func (s *Store) RecordClusterControlStateSnapshot(ctx context.Context, report ClusterControlStateReport, input ClusterControlSnapshotInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(report.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = "control.state.snapshot"
	}
	clusters := append([]ClusterControlStateCluster(nil), report.Clusters...)
	if input.Limit > 0 && len(clusters) > input.Limit {
		clusters = append([]ClusterControlStateCluster(nil), clusters[:input.Limit]...)
	}
	if strings.TrimSpace(report.Filter.ProtoClusterID) != "" && len(clusters) == 0 {
		return RuntimeEventRecord{}, fmt.Errorf("cluster control state not found: %s/%s", workspaceID, strings.TrimSpace(report.Filter.ProtoClusterID))
	}
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, referenceAt)
	if err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	entityID := clusterControlSnapshotEntityID(report.Filter)
	snapshotTruncated := input.Limit > 0 && len(report.Clusters) > input.Limit
	capturedClusterIDs := clusterControlSnapshotClusterIDs(clusters)
	capturedClusterIDsHash := clusterControlSnapshotClusterIDsHash(capturedClusterIDs)
	snapshotScope := clusterControlSnapshotScope(report.Filter)
	payload := map[string]any{
		"generated_at":                    report.GeneratedAt,
		"workspace_id":                    report.WorkspaceID,
		"filter":                          report.Filter,
		"workspace":                       report.Workspace,
		"clusters":                        clusters,
		"summary":                         clusterControlSnapshotSummary(report, clusters),
		"source_cluster_count":            len(report.Clusters),
		"captured_cluster_count":          len(clusters),
		"snapshot_limit":                  input.Limit,
		"snapshot_truncated":              snapshotTruncated,
		"typed_event_type":                "CONTROL_STATE_SNAPSHOT",
		"event_kind":                      "cluster.control_state_snapshot",
		"event_type":                      "cluster.control_state_snapshot",
		"entity_type":                     "instrumentation_control_state",
		"entity_id":                       entityID,
		"actor_type":                      "operator",
		"actor_id":                        actorID,
		"filter_proto_cluster_id":         strings.TrimSpace(report.Filter.ProtoClusterID),
		"snapshot_scope":                  snapshotScope,
		"captured_proto_cluster_ids":      capturedClusterIDs,
		"captured_proto_cluster_ids_hash": capturedClusterIDsHash,
	}
	promptFields := map[string]string{
		"workspace_id":                    workspaceID,
		"actor_id":                        actorID,
		"filter_proto_cluster_id":         strings.TrimSpace(report.Filter.ProtoClusterID),
		"event_type":                      "cluster.control_state_snapshot",
		"entity_type":                     "instrumentation_control_state",
		"entity_id":                       entityID,
		"actor_type":                      "operator",
		"captured_cluster_count":          fmt.Sprintf("%d", len(clusters)),
		"source_cluster_count":            fmt.Sprintf("%d", len(report.Clusters)),
		"snapshot_limit":                  fmt.Sprintf("%d", input.Limit),
		"snapshot_truncated":              fmt.Sprintf("%t", snapshotTruncated),
		"snapshot_scope":                  snapshotScope,
		"captured_proto_cluster_ids_hash": capturedClusterIDsHash,
		"event_kind":                      "cluster.control_state_snapshot",
		"typed_event_type":                "CONTROL_STATE_SNAPSHOT",
	}
	addControlStatePromptContextPrincipalFields(promptFields, input.PromptContextPrincipalType, input.PromptContextPrincipalID)
	payload, err = attachControlStatePromptContextEnvelope(
		payload,
		input.PromptContextEnvelope,
		controlStatePromptContextSurface(input.PromptContextSurface, "workspace.instrumentation.control.state.snapshot"),
		promptFields,
	)
	if err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin control state snapshot tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: workspaceID,
			EventType:   "cluster.control_state_snapshot",
			EntityType:  "instrumentation_control_state",
			EntityID:    entityID,
			ActorType:   "operator",
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   referenceAt,
		})
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit control state snapshot tx: %w", err)
	}
	return record, nil
}

func clusterControlTickLimit(protoClusterID string) int {
	if strings.TrimSpace(protoClusterID) != "" {
		return 1
	}
	return clusterControlTickClusterLimit
}

func clusterControlReportBasisLimit(protoClusterID string) int {
	return clusterControlTickLimit(protoClusterID)
}

func clusterControlStateClusterFromReport(workspaceID string, cluster ControlClusterReport, row ClusterControlStateRecord, hasRow bool) ClusterControlStateCluster {
	var state ClusterControlStateRecord
	if hasRow {
		state = row
	} else {
		state = previewClusterControlState(workspaceID, cluster)
	}
	return ClusterControlStateCluster{
		ProtoClusterID:        cluster.ProtoClusterID,
		ResolutionKind:        firstNonEmpty(strings.TrimSpace(cluster.ResolutionKind), strings.TrimSpace(state.ResolutionKind), "proto_cluster"),
		ProfileContext:        clusterControlHeuristicProfileForCluster(cluster),
		State:                 state,
		Metrics:               cluster.Metrics,
		Signals:               cluster.Signals,
		SuggestedControls:     cluster.SuggestedControls,
		MetricsMissing:        cluster.MetricsMissing,
		BasisStale:            cluster.BasisStale,
		LastTensionBasisAt:    cluster.LastTensionBasisAt,
		ConfirmedCountsByType: cloneIntMap(cluster.ConfirmedCountsByType),
		PendingCountsByType:   cloneIntMap(cluster.PendingCountsByType),
		Summary:               firstNonEmpty(strings.TrimSpace(state.Summary), strings.TrimSpace(cluster.Summary)),
	}
}

func clusterControlStateClusterFromStoredState(row ClusterControlStateRecord) ClusterControlStateCluster {
	return ClusterControlStateCluster{
		ProtoClusterID: row.ProtoClusterID,
		ResolutionKind: row.ResolutionKind,
		ProfileContext: clusterControlHeuristicProfileForName(row.CorridorProfile),
		State:          row,
		Summary:        row.Summary,
	}
}

func synthesizeControlClusterFromState(row ClusterControlStateRecord) ControlClusterReport {
	return ControlClusterReport{
		ProtoClusterID:        row.ProtoClusterID,
		ResolutionKind:        row.ResolutionKind,
		TaskIDs:               append([]string{}, row.TaskIDs...),
		SessionIDs:            append([]string{}, row.SessionIDs...),
		DocKeys:               append([]string{}, row.DocKeys...),
		ArtifactRefs:          append([]string{}, row.ArtifactRefs...),
		AgentIDs:              append([]string{}, row.AgentIDs...),
		Signals:               ControlSignalVector{PressureScore: row.PressureScore, AttentionBand: row.AttentionBand},
		ConfirmedTensionCount: row.ConfirmedTensionCount,
		PendingTensionCount:   row.PendingTensionCount,
		ConfirmedTensionIDs:   append([]string{}, row.ConfirmedTensionIDs...),
		PendingTensionIDs:     append([]string{}, row.PendingTensionIDs...),
		Summary:               row.Summary,
	}
}

func previewClusterControlState(workspaceID string, cluster ControlClusterReport) ClusterControlStateRecord {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	profile := clusterControlHeuristicProfileForCluster(cluster)
	violation := clusterControlSignalDeviationForCluster(cluster, profile)
	candidate := clusterControlModeHintForCluster(cluster, profile, violation)
	return ClusterControlStateRecord{
		WorkspaceID:            strings.TrimSpace(workspaceID),
		ProtoClusterID:         strings.TrimSpace(cluster.ProtoClusterID),
		ResolutionKind:         firstNonEmpty(strings.TrimSpace(cluster.ResolutionKind), "proto_cluster"),
		CorridorProfile:        profile.Profile,
		Epoch:                  0,
		CurrentMode:            clusterControlModeSteady,
		CandidateMode:          candidate.Mode,
		CandidateStreak:        0,
		DominantViolationKind:  candidate.ViolationKind,
		DominantViolationScore: candidate.Score,
		AttentionBand:          firstNonEmpty(strings.TrimSpace(cluster.Signals.AttentionBand), "STEADY"),
		PressureScore:          cluster.Signals.PressureScore,
		ConfirmedTensionCount:  cluster.ConfirmedTensionCount,
		PendingTensionCount:    cluster.PendingTensionCount,
		TaskIDs:                uniqueSortedStrings(cluster.TaskIDs),
		SessionIDs:             uniqueSortedStrings(cluster.SessionIDs),
		DocKeys:                uniqueSortedStrings(cluster.DocKeys),
		ArtifactRefs:           uniqueSortedStrings(cluster.ArtifactRefs),
		AgentIDs:               uniqueSortedStrings(cluster.AgentIDs),
		ConfirmedTensionIDs:    uniqueSortedStrings(cluster.ConfirmedTensionIDs),
		PendingTensionIDs:      uniqueSortedStrings(cluster.PendingTensionIDs),
		OperatorHints:          cluster.SuggestedControls,
		ViolationVector:        violation,
		Summary:                clusterControlStateSummary(cluster, clusterControlModeSteady, candidate.Mode, 0, candidate),
		LastBasisAt:            firstNonEmpty(strings.TrimSpace(cluster.LastTensionBasisAt), strings.TrimSpace(cluster.Metrics.LastEventAt)),
		CreatedAt:              now,
		UpdatedAt:              now,
	}
}

func evaluateClusterControlState(workspaceID string, cluster ControlClusterReport, previous *ClusterControlStateRecord, now string) ClusterControlStateRecord {
	profile := clusterControlHeuristicProfileForCluster(cluster)
	violation := clusterControlSignalDeviationForCluster(cluster, profile)
	candidate := clusterControlModeHintForCluster(cluster, profile, violation)
	record := ClusterControlStateRecord{
		WorkspaceID:            strings.TrimSpace(workspaceID),
		ProtoClusterID:         strings.TrimSpace(cluster.ProtoClusterID),
		ResolutionKind:         firstNonEmpty(strings.TrimSpace(cluster.ResolutionKind), "proto_cluster"),
		CorridorProfile:        profile.Profile,
		Epoch:                  1,
		CurrentMode:            clusterControlModeSteady,
		CandidateMode:          candidate.Mode,
		CandidateStreak:        1,
		DominantViolationKind:  candidate.ViolationKind,
		DominantViolationScore: candidate.Score,
		AttentionBand:          firstNonEmpty(strings.TrimSpace(cluster.Signals.AttentionBand), "STEADY"),
		PressureScore:          cluster.Signals.PressureScore,
		ConfirmedTensionCount:  cluster.ConfirmedTensionCount,
		PendingTensionCount:    cluster.PendingTensionCount,
		TaskIDs:                uniqueSortedStrings(cluster.TaskIDs),
		SessionIDs:             uniqueSortedStrings(cluster.SessionIDs),
		DocKeys:                uniqueSortedStrings(cluster.DocKeys),
		ArtifactRefs:           uniqueSortedStrings(cluster.ArtifactRefs),
		AgentIDs:               uniqueSortedStrings(cluster.AgentIDs),
		ConfirmedTensionIDs:    uniqueSortedStrings(cluster.ConfirmedTensionIDs),
		PendingTensionIDs:      uniqueSortedStrings(cluster.PendingTensionIDs),
		OperatorHints:          cluster.SuggestedControls,
		ViolationVector:        violation,
		LastBasisAt:            firstNonEmpty(strings.TrimSpace(cluster.LastTensionBasisAt), strings.TrimSpace(cluster.Metrics.LastEventAt)),
		LastTickAt:             now,
		CreatedAt:              now,
		UpdatedAt:              now,
	}
	if previous != nil {
		record.Epoch = previous.Epoch + 1
		record.CurrentMode = normalizeClusterControlMode(previous.CurrentMode)
		record.CreatedAt = firstNonEmpty(strings.TrimSpace(previous.CreatedAt), now)
		record.LastTransitionAt = strings.TrimSpace(previous.LastTransitionAt)
		record.LastTickAt = strings.TrimSpace(previous.LastTickAt)
		if normalizeClusterControlMode(previous.CandidateMode) == candidate.Mode {
			record.CandidateStreak = previous.CandidateStreak + 1
		} else {
			record.CandidateStreak = 1
		}
	}
	if candidate.Mode == record.CurrentMode {
		record.CandidateStreak = 0
	}
	if candidate.Mode != record.CurrentMode && record.CandidateStreak >= clusterControlTickHysteresisEpoch {
		record.CurrentMode = candidate.Mode
		record.CandidateStreak = 0
		record.LastTransitionAt = now
	}
	record.LastTickAt = now
	record.Summary = clusterControlStateSummary(cluster, record.CurrentMode, candidate.Mode, record.CandidateStreak, candidate)
	return record
}

func clusterControlPreviousPointer(previous ClusterControlStateRecord, ok bool) *ClusterControlStateRecord {
	if !ok {
		return nil
	}
	copy := previous
	return &copy
}

func clusterControlStateChanged(previous, next ClusterControlStateRecord) bool {
	if !equalStringSlices(previous.TaskIDs, next.TaskIDs) ||
		!equalStringSlices(previous.SessionIDs, next.SessionIDs) ||
		!equalStringSlices(previous.DocKeys, next.DocKeys) ||
		!equalStringSlices(previous.ArtifactRefs, next.ArtifactRefs) ||
		!equalStringSlices(previous.AgentIDs, next.AgentIDs) ||
		!equalStringSlices(previous.ConfirmedTensionIDs, next.ConfirmedTensionIDs) ||
		!equalStringSlices(previous.PendingTensionIDs, next.PendingTensionIDs) {
		return true
	}
	if previous.ResolutionKind != next.ResolutionKind ||
		previous.CorridorProfile != next.CorridorProfile ||
		previous.Epoch != next.Epoch ||
		previous.CurrentMode != next.CurrentMode ||
		previous.CandidateMode != next.CandidateMode ||
		previous.CandidateStreak != next.CandidateStreak ||
		previous.DominantViolationKind != next.DominantViolationKind ||
		previous.AttentionBand != next.AttentionBand ||
		previous.PressureScore != next.PressureScore ||
		previous.ConfirmedTensionCount != next.ConfirmedTensionCount ||
		previous.PendingTensionCount != next.PendingTensionCount ||
		previous.Summary != next.Summary ||
		previous.LastBasisAt != next.LastBasisAt ||
		previous.LastTransitionAt != next.LastTransitionAt {
		return true
	}
	return !clusterControlOperatorHintsEqual(previous.OperatorHints, next.OperatorHints) ||
		!clusterControlViolationsEqual(previous.ViolationVector, next.ViolationVector)
}

func (s *Store) listClusterControlStateRows(ctx context.Context, workspaceID, protoClusterID string) ([]ClusterControlStateRecord, error) {
	return s.listClusterControlStateRowsTx(ctx, nil, workspaceID, protoClusterID)
}

func (s *Store) listClusterControlStateRowsTx(ctx context.Context, tx *sql.Tx, workspaceID, protoClusterID string) ([]ClusterControlStateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	protoClusterID = strings.TrimSpace(protoClusterID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	query := `SELECT
		workspace_id, proto_cluster_id, resolution_kind, corridor_profile, epoch,
		current_mode, candidate_mode, candidate_streak, dominant_violation_kind,
		dominant_violation_score, attention_band, pressure_score, confirmed_tension_count,
		pending_tension_count, task_ids_json, session_ids_json, doc_keys_json,
		artifact_refs_json, agent_ids_json, confirmed_tension_ids_json, pending_tension_ids_json,
		control_vector_json, violation_vector_json, summary, last_basis_at, last_tick_event_id,
		last_tick_at, last_transition_at, created_at, updated_at
	FROM workspace_cluster_control_state
	WHERE workspace_id = ?`
	args := []any{workspaceID}
	if protoClusterID != "" {
		query += ` AND proto_cluster_id = ?`
		args = append(args, protoClusterID)
	}
	query += ` ORDER BY pressure_score DESC, updated_at DESC, proto_cluster_id ASC`
	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, args...)
	} else {
		rows, err = s.db.QueryContext(ctx, query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("list cluster control state rows: %w", err)
	}
	defer rows.Close()
	out := []ClusterControlStateRecord{}
	for rows.Next() {
		var record ClusterControlStateRecord
		var taskIDsJSON, sessionIDsJSON, docKeysJSON, artifactRefsJSON, agentIDsJSON string
		var confirmedIDsJSON, pendingIDsJSON, controlJSON, violationJSON string
		if err := rows.Scan(
			&record.WorkspaceID,
			&record.ProtoClusterID,
			&record.ResolutionKind,
			&record.CorridorProfile,
			&record.Epoch,
			&record.CurrentMode,
			&record.CandidateMode,
			&record.CandidateStreak,
			&record.DominantViolationKind,
			&record.DominantViolationScore,
			&record.AttentionBand,
			&record.PressureScore,
			&record.ConfirmedTensionCount,
			&record.PendingTensionCount,
			&taskIDsJSON,
			&sessionIDsJSON,
			&docKeysJSON,
			&artifactRefsJSON,
			&agentIDsJSON,
			&confirmedIDsJSON,
			&pendingIDsJSON,
			&controlJSON,
			&violationJSON,
			&record.Summary,
			&record.LastBasisAt,
			&record.LastTickEventID,
			&record.LastTickAt,
			&record.LastTransitionAt,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan cluster control state row: %w", err)
		}
		record.CurrentMode = normalizeClusterControlMode(record.CurrentMode)
		record.CandidateMode = normalizeClusterControlMode(record.CandidateMode)
		record.TaskIDs = decodeStringJSONArray(taskIDsJSON)
		record.SessionIDs = decodeStringJSONArray(sessionIDsJSON)
		record.DocKeys = decodeStringJSONArray(docKeysJSON)
		record.ArtifactRefs = decodeStringJSONArray(artifactRefsJSON)
		record.AgentIDs = decodeStringJSONArray(agentIDsJSON)
		record.ConfirmedTensionIDs = decodeStringJSONArray(confirmedIDsJSON)
		record.PendingTensionIDs = decodeStringJSONArray(pendingIDsJSON)
		_ = json.Unmarshal([]byte(strings.TrimSpace(controlJSON)), &record.OperatorHints)
		_ = json.Unmarshal([]byte(strings.TrimSpace(violationJSON)), &record.ViolationVector)
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *Store) upsertClusterControlStateTx(ctx context.Context, tx *sql.Tx, record ClusterControlStateRecord) error {
	record.WorkspaceID = strings.TrimSpace(record.WorkspaceID)
	record.ProtoClusterID = strings.TrimSpace(record.ProtoClusterID)
	if record.WorkspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if record.ProtoClusterID == "" {
		return errors.New("proto_cluster_id is required")
	}
	record.ResolutionKind = firstNonEmpty(strings.TrimSpace(record.ResolutionKind), "proto_cluster")
	record.CorridorProfile = firstNonEmpty(strings.TrimSpace(record.CorridorProfile), "integration")
	record.CurrentMode = normalizeClusterControlMode(record.CurrentMode)
	record.CandidateMode = normalizeClusterControlMode(record.CandidateMode)
	record.AttentionBand = firstNonEmpty(strings.TrimSpace(record.AttentionBand), "STEADY")
	record.TaskIDs = uniqueSortedStrings(record.TaskIDs)
	record.SessionIDs = uniqueSortedStrings(record.SessionIDs)
	record.DocKeys = uniqueSortedStrings(record.DocKeys)
	record.ArtifactRefs = uniqueSortedStrings(record.ArtifactRefs)
	record.AgentIDs = uniqueSortedStrings(record.AgentIDs)
	record.ConfirmedTensionIDs = uniqueSortedStrings(record.ConfirmedTensionIDs)
	record.PendingTensionIDs = uniqueSortedStrings(record.PendingTensionIDs)
	record.CreatedAt = firstNonEmpty(strings.TrimSpace(record.CreatedAt), time.Now().UTC().Format(time.RFC3339Nano))
	record.UpdatedAt = firstNonEmpty(strings.TrimSpace(record.UpdatedAt), record.CreatedAt)
	_, err := tx.ExecContext(ctx, `INSERT INTO workspace_cluster_control_state(
		workspace_id, proto_cluster_id, resolution_kind, corridor_profile, epoch,
		current_mode, candidate_mode, candidate_streak, dominant_violation_kind,
		dominant_violation_score, attention_band, pressure_score, confirmed_tension_count,
		pending_tension_count, task_ids_json, session_ids_json, doc_keys_json,
		artifact_refs_json, agent_ids_json, confirmed_tension_ids_json, pending_tension_ids_json,
		control_vector_json, violation_vector_json, summary, last_basis_at, last_tick_event_id,
		last_tick_at, last_transition_at, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(workspace_id, proto_cluster_id) DO UPDATE SET
		resolution_kind=excluded.resolution_kind,
		corridor_profile=excluded.corridor_profile,
		epoch=excluded.epoch,
		current_mode=excluded.current_mode,
		candidate_mode=excluded.candidate_mode,
		candidate_streak=excluded.candidate_streak,
		dominant_violation_kind=excluded.dominant_violation_kind,
		dominant_violation_score=excluded.dominant_violation_score,
		attention_band=excluded.attention_band,
		pressure_score=excluded.pressure_score,
		confirmed_tension_count=excluded.confirmed_tension_count,
		pending_tension_count=excluded.pending_tension_count,
		task_ids_json=excluded.task_ids_json,
		session_ids_json=excluded.session_ids_json,
		doc_keys_json=excluded.doc_keys_json,
		artifact_refs_json=excluded.artifact_refs_json,
		agent_ids_json=excluded.agent_ids_json,
		confirmed_tension_ids_json=excluded.confirmed_tension_ids_json,
		pending_tension_ids_json=excluded.pending_tension_ids_json,
		control_vector_json=excluded.control_vector_json,
		violation_vector_json=excluded.violation_vector_json,
		summary=excluded.summary,
		last_basis_at=excluded.last_basis_at,
		last_tick_event_id=excluded.last_tick_event_id,
		last_tick_at=excluded.last_tick_at,
		last_transition_at=excluded.last_transition_at,
		updated_at=excluded.updated_at`,
		record.WorkspaceID,
		record.ProtoClusterID,
		record.ResolutionKind,
		record.CorridorProfile,
		record.Epoch,
		record.CurrentMode,
		record.CandidateMode,
		record.CandidateStreak,
		record.DominantViolationKind,
		record.DominantViolationScore,
		record.AttentionBand,
		record.PressureScore,
		record.ConfirmedTensionCount,
		record.PendingTensionCount,
		mustJSON(record.TaskIDs),
		mustJSON(record.SessionIDs),
		mustJSON(record.DocKeys),
		mustJSON(record.ArtifactRefs),
		mustJSON(record.AgentIDs),
		mustJSON(record.ConfirmedTensionIDs),
		mustJSON(record.PendingTensionIDs),
		mustJSON(record.OperatorHints),
		mustJSON(record.ViolationVector),
		record.Summary,
		record.LastBasisAt,
		record.LastTickEventID,
		record.LastTickAt,
		record.LastTransitionAt,
		record.CreatedAt,
		record.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert cluster control state: %w", err)
	}
	return nil
}

func clusterControlHeuristicProfileForCluster(cluster ControlClusterReport) ClusterControlHeuristicProfileContext {
	confirmed := cluster.ConfirmedCountsByType
	switch {
	case confirmed["bottleneck"] > 0 || cluster.Metrics.OpenQueueCount > 0:
		return clusterControlHeuristicProfileForName("incident")
	case confirmed["contradiction"] > 0:
		return clusterControlHeuristicProfileForName("proof")
	case confirmed["bridge"] > 0:
		return clusterControlHeuristicProfileForName("exploration")
	default:
		return clusterControlHeuristicProfileForName("integration")
	}
}

func clusterControlHeuristicProfileForName(profile string) ClusterControlHeuristicProfileContext {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "proof":
		return ClusterControlHeuristicProfileContext{Profile: "proof", ThroughputMin: 0.25, ThroughputMax: 0.60, ReviewMin: 0.45, ReviewMax: 0.80, CoordinationMin: 0.20, CoordinationMax: 0.55, CentralizationMax: 0.45, BlockerDensityMax: 0.30, DuplicationMin: 0.10, DuplicationMax: 0.35}
	case "exploration":
		return ClusterControlHeuristicProfileContext{Profile: "exploration", ThroughputMin: 0.20, ThroughputMax: 0.55, ReviewMin: 0.20, ReviewMax: 0.45, CoordinationMin: 0.30, CoordinationMax: 0.70, CentralizationMax: 0.40, BlockerDensityMax: 0.35, DuplicationMin: 0.35, DuplicationMax: 0.75}
	case "incident":
		return ClusterControlHeuristicProfileContext{Profile: "incident", ThroughputMin: 0.55, ThroughputMax: 0.85, ReviewMin: 0.15, ReviewMax: 0.45, CoordinationMin: 0.20, CoordinationMax: 0.50, CentralizationMax: 0.50, BlockerDensityMax: 0.50, DuplicationMin: 0.05, DuplicationMax: 0.25}
	default:
		return ClusterControlHeuristicProfileContext{Profile: "integration", ThroughputMin: 0.35, ThroughputMax: 0.70, ReviewMin: 0.25, ReviewMax: 0.55, CoordinationMin: 0.25, CoordinationMax: 0.55, CentralizationMax: 0.45, BlockerDensityMax: 0.30, DuplicationMin: 0.15, DuplicationMax: 0.45}
	}
}

func clusterControlSignalDeviationForCluster(cluster ControlClusterReport, profile ClusterControlHeuristicProfileContext) ClusterControlSignalDeviationVector {
	throughput := clusterControlDeviationFromRange(float64(cluster.Signals.ThroughputPressure)/100, profile.ThroughputMin, profile.ThroughputMax)
	review := clusterControlDeviationFromRange(float64(cluster.Signals.ReviewPressure)/100, profile.ReviewMin, profile.ReviewMax)
	coordination := clusterControlDeviationFromRange(float64(cluster.Signals.CoordinationPressure)/100, profile.CoordinationMin, profile.CoordinationMax)
	centralization := clusterControlDeviationAbove(cluster.Metrics.CommunicationCentralization, profile.CentralizationMax)
	blockerDensity := clusterControlDeviationAbove(cluster.Metrics.BlockerDensity, profile.BlockerDensityMax)
	duplication := clusterControlDeviationFromRange(cluster.Metrics.DuplicationIndex, profile.DuplicationMin, profile.DuplicationMax)
	noveltyGap := clusterControlDeviationBelow(cluster.Metrics.DuplicationIndex, profile.DuplicationMin)
	synergyGap := 0.0
	if cluster.ConfirmedCountsByType["bridge"] > 0 {
		synergyGap = clusterControlDeviationBelow(float64(cluster.Signals.CoordinationPressure)/100, profile.CoordinationMin)
	}
	return ClusterControlSignalDeviationVector{
		Throughput:     throughput,
		Review:         review,
		Coordination:   coordination,
		Centralization: centralization,
		BlockerDensity: blockerDensity,
		Duplication:    duplication,
		NoveltyGap:     noveltyGap,
		SynergyGap:     synergyGap,
	}
}

func clusterControlModeHintForCluster(cluster ControlClusterReport, profile ClusterControlHeuristicProfileContext, deviation ClusterControlSignalDeviationVector) clusterControlModeCandidate {
	confirmed := cluster.ConfirmedCountsByType
	switch {
	case cluster.Signals.RSPDominantState == "THRASHING" || cluster.Signals.RSPRiskScore >= 0.75:
		return clusterControlModeCandidate{Mode: clusterControlModeStabilize, ViolationKind: "thrashing_risk", Score: maxFloat(cluster.Signals.RSPRiskScore, 0.75)}
	case deviation.Centralization > 0 && cluster.Metrics.DuplicationIndex < profile.DuplicationMin:
		return clusterControlModeCandidate{Mode: clusterControlModeAntiCollapse, ViolationKind: "centralization", Score: maxFloat(deviation.Centralization, deviation.NoveltyGap)}
	case confirmed["bridge"] > 0 && deviation.SynergyGap > 0:
		return clusterControlModeCandidate{Mode: clusterControlModeSynergySeeking, ViolationKind: "synergy_gap", Score: deviation.SynergyGap}
	case clusterControlDominantDeviation(deviation) == "centralization" || cluster.Metrics.MaxAgentActivityShare >= 0.70:
		return clusterControlModeCandidate{Mode: clusterControlModeDecentralize, ViolationKind: "centralization", Score: maxFloat(deviation.Centralization, clusterControlDeviationAbove(cluster.Metrics.MaxAgentActivityShare, 0.70))}
	case confirmed["contradiction"] > 0 || clusterControlDominantDeviation(deviation) == "review":
		return clusterControlModeCandidate{Mode: clusterControlModeCoherence, ViolationKind: firstNonEmpty(clusterControlDominantDeviation(deviation), "review"), Score: maxFloat(deviation.Review, float64(minInt(confirmed["contradiction"], 1))*0.30)}
	case confirmed["bottleneck"] > 0 || cluster.Metrics.OpenQueueCount > 0 || clusterControlDominantDeviation(deviation) == "throughput" || clusterControlDominantDeviation(deviation) == "blocker_density":
		return clusterControlModeCandidate{Mode: clusterControlModeStabilize, ViolationKind: firstNonEmpty(clusterControlDominantDeviation(deviation), "throughput"), Score: maxFloat(deviation.Throughput, deviation.BlockerDensity)}
	case (confirmed["ambiguity"] > 0 || confirmed["gap"] > 0) && deviation.NoveltyGap > 0:
		return clusterControlModeCandidate{Mode: clusterControlModeUnfreeze, ViolationKind: "novelty_gap", Score: maxFloat(deviation.NoveltyGap, deviation.Duplication)}
	default:
		return clusterControlModeCandidate{Mode: clusterControlModeSteady, ViolationKind: "", Score: 0}
	}
}

func clusterControlDominantDeviation(violation ClusterControlSignalDeviationVector) string {
	values := []struct {
		kind  string
		score float64
	}{
		{"throughput", violation.Throughput},
		{"review", violation.Review},
		{"coordination", violation.Coordination},
		{"centralization", violation.Centralization},
		{"blocker_density", violation.BlockerDensity},
		{"duplication", violation.Duplication},
		{"novelty_gap", violation.NoveltyGap},
		{"synergy_gap", violation.SynergyGap},
	}
	bestKind := ""
	bestScore := 0.0
	for _, item := range values {
		if item.score > bestScore {
			bestScore = item.score
			bestKind = item.kind
		}
	}
	return bestKind
}

func clusterControlStateSummary(cluster ControlClusterReport, currentMode, candidateMode string, candidateStreak int, candidate clusterControlModeCandidate) string {
	parts := []string{
		"stabilized_hint=" + normalizeClusterControlMode(currentMode),
		"candidate_hint=" + normalizeClusterControlMode(candidateMode),
		fmt.Sprintf("stability_streak=%d", candidateStreak),
		fmt.Sprintf("pressure=%d", cluster.Signals.PressureScore),
		fmt.Sprintf("band=%s", firstNonEmpty(strings.TrimSpace(cluster.Signals.AttentionBand), "STEADY")),
	}
	if candidate.ViolationKind != "" {
		parts = append(parts, fmt.Sprintf("signal=%s", candidate.ViolationKind))
	}
	if cluster.SuggestedControls.PriorityFocus != "" {
		parts = append(parts, fmt.Sprintf("focus=%s", cluster.SuggestedControls.PriorityFocus))
	}
	return strings.Join(parts, " | ")
}

func clusterControlRuntimeEventPayload(record ClusterControlStateRecord, cluster ControlClusterReport, eventType string) map[string]any {
	return map[string]any{
		"workspace_id":              record.WorkspaceID,
		"proto_cluster_id":          record.ProtoClusterID,
		"resolution_kind":           record.ResolutionKind,
		"heuristic_profile":         record.CorridorProfile,
		"epoch":                     record.Epoch,
		"stabilized_mode_hint":      record.CurrentMode,
		"candidate_mode_hint":       record.CandidateMode,
		"stability_streak":          record.CandidateStreak,
		"dominant_signal_kind":      record.DominantViolationKind,
		"dominant_signal_score":     record.DominantViolationScore,
		"attention_band":            record.AttentionBand,
		"pressure_score":            record.PressureScore,
		"confirmed_tension_count":   record.ConfirmedTensionCount,
		"pending_tension_count":     record.PendingTensionCount,
		"task_ids":                  append([]string{}, record.TaskIDs...),
		"session_ids":               append([]string{}, record.SessionIDs...),
		"doc_keys":                  append([]string{}, record.DocKeys...),
		"artifact_refs":             append([]string{}, record.ArtifactRefs...),
		"agent_ids":                 append([]string{}, record.AgentIDs...),
		"confirmed_tension_ids":     append([]string{}, record.ConfirmedTensionIDs...),
		"pending_tension_ids":       append([]string{}, record.PendingTensionIDs...),
		"operator_hints":            record.OperatorHints,
		"signal_deviation_vector":   record.ViolationVector,
		"heuristic_profile_context": clusterControlHeuristicProfileForCluster(cluster),
		"metrics":                   cluster.Metrics,
		"signals":                   cluster.Signals,
		"suggested_controls":        cluster.SuggestedControls,
		"metrics_missing":           cluster.MetricsMissing,
		"basis_stale":               cluster.BasisStale,
		"last_tension_basis_at":     cluster.LastTensionBasisAt,
		"confirmed_counts_by_type":  cloneIntMap(cluster.ConfirmedCountsByType),
		"pending_counts_by_type":    cloneIntMap(cluster.PendingCountsByType),
		"summary":                   record.Summary,
		"last_basis_at":             record.LastBasisAt,
		"last_tick_event_id":        record.LastTickEventID,
		"last_tick_at":              record.LastTickAt,
		"last_stabilized_at":        record.LastTransitionAt,
		"typed_event_type":          "CONTROL_STATE_INTERPRETATION",
		"event_kind":                eventType,
	}
}

func clusterControlSnapshotEntityID(filter ClusterControlStateFilter) string {
	if clusterID := strings.TrimSpace(filter.ProtoClusterID); clusterID != "" {
		return clusterID
	}
	return strings.TrimSpace(filter.WorkspaceID)
}

func controlStatePromptContextSurface(surface, fallback string) string {
	if trimmed := strings.TrimSpace(surface); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(fallback)
}

func addControlStatePromptContextPrincipalFields(fields map[string]string, principalType, principalID string) {
	if fields == nil {
		return
	}
	if principalType = strings.TrimSpace(principalType); principalType != "" {
		fields["principal_type"] = principalType
	}
	if principalID = strings.TrimSpace(principalID); principalID != "" {
		fields["principal_id"] = principalID
	}
}

func clusterControlSnapshotScope(filter ClusterControlStateFilter) string {
	if strings.TrimSpace(filter.ProtoClusterID) != "" {
		return "proto_cluster"
	}
	return "workspace"
}

func clusterControlSnapshotClusterIDs(clusters []ClusterControlStateCluster) []string {
	out := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		if clusterID := strings.TrimSpace(cluster.ProtoClusterID); clusterID != "" {
			out = append(out, clusterID)
		}
	}
	sort.Strings(out)
	return out
}

func clusterControlSnapshotClusterIDsHash(clusterIDs []string) string {
	sum := sha256.Sum256([]byte(strings.Join(clusterIDs, "\n")))
	return fmt.Sprintf("sha256:%x", sum[:])
}

func clusterControlSnapshotSummary(report ClusterControlStateReport, clusters []ClusterControlStateCluster) string {
	if clusterID := strings.TrimSpace(report.Filter.ProtoClusterID); clusterID != "" {
		if len(clusters) > 0 {
			return fmt.Sprintf("Control-state snapshot: %s stabilized_hint=%s", clusterID, clusters[0].State.CurrentMode)
		}
		return "Control-state snapshot: " + clusterID
	}
	if len(clusters) == 0 {
		return fmt.Sprintf("Control-state snapshot for %s captured with no eligible clusters", firstNonEmpty(strings.TrimSpace(report.WorkspaceID), "workspace"))
	}
	if sourceCount := len(report.Clusters); sourceCount > len(clusters) {
		return fmt.Sprintf(
			"Control-state snapshot: captured %d/%d clusters; %d non-steady hints total",
			len(clusters),
			sourceCount,
			report.Workspace.NonSteadyCount,
		)
	}
	return fmt.Sprintf("Control-state snapshot: %d non-steady hints / %d clusters", report.Workspace.NonSteadyCount, report.Workspace.TotalClusters)
}

func clusterControlPrimaryTaskID(record ClusterControlStateRecord) string {
	for _, value := range record.TaskIDs {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func clusterControlPrimarySessionID(record ClusterControlStateRecord) string {
	for _, value := range record.SessionIDs {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func clusterControlPrimaryAgentID(record ClusterControlStateRecord) string {
	for _, value := range record.AgentIDs {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func clusterControlDeviationFromRange(value, minValue, maxValue float64) float64 {
	switch {
	case value < minValue:
		return clusterControlClampFloat(minValue-value, 0, 1)
	case value > maxValue:
		return clusterControlClampFloat(value-maxValue, 0, 1)
	default:
		return 0
	}
}

func clusterControlDeviationAbove(value, maxValue float64) float64 {
	return clusterControlClampFloat(value-maxValue, 0, 1)
}

func clusterControlDeviationBelow(value, minValue float64) float64 {
	return clusterControlClampFloat(minValue-value, 0, 1)
}

func clusterControlClampFloat(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func clusterControlOperatorHintsEqual(left, right ControlSuggestedControls) bool {
	return left.FanoutCap == right.FanoutCap &&
		left.ReviewDepth == right.ReviewDepth &&
		left.ContextCap == right.ContextCap &&
		left.BridgeQuota == right.BridgeQuota &&
		clusterControlFloatEqual(left.MergeThreshold, right.MergeThreshold) &&
		left.PriorityFocus == right.PriorityFocus
}

func clusterControlViolationsEqual(left, right ClusterControlSignalDeviationVector) bool {
	return clusterControlFloatEqual(left.Throughput, right.Throughput) &&
		clusterControlFloatEqual(left.Review, right.Review) &&
		clusterControlFloatEqual(left.Coordination, right.Coordination) &&
		clusterControlFloatEqual(left.Centralization, right.Centralization) &&
		clusterControlFloatEqual(left.BlockerDensity, right.BlockerDensity) &&
		clusterControlFloatEqual(left.Duplication, right.Duplication) &&
		clusterControlFloatEqual(left.NoveltyGap, right.NoveltyGap) &&
		clusterControlFloatEqual(left.SynergyGap, right.SynergyGap)
}

func clusterControlFloatEqual(left, right float64) bool {
	diff := left - right
	if diff < 0 {
		diff = -diff
	}
	return diff < 0.00001
}

func maxFloat(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}
