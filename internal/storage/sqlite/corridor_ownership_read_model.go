package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	corridorOwnershipStateOwnedExplicit      = "OWNED_EXPLICIT"
	corridorOwnershipStateOwnedExplicitStale = "OWNED_EXPLICIT_STALE"
	corridorOwnershipStateSeededTemplate     = "SEEDED_TEMPLATE"
	corridorOwnershipStateDerivedCluster     = "DERIVED_CLUSTER"
	corridorOwnershipStateContested          = "CONTESTED"
	corridorOwnershipStateUnresolved         = "UNRESOLVED"
)

type CorridorOwnershipFilter struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type CorridorOwnershipDigest struct {
	OwnershipState       string   `json:"ownership_state"`
	BasisTaskClass       string   `json:"basis_task_class,omitempty"`
	BasisTaskClassSource string   `json:"basis_task_class_source,omitempty"`
	BasisUpdatedAt       string   `json:"basis_updated_at,omitempty"`
	BasisFresh           bool     `json:"basis_fresh"`
	BasisAuthoritative   bool     `json:"basis_authoritative"`
	OwnerTaskID          string   `json:"owner_task_id,omitempty"`
	OwnerTaskIDs         []string `json:"owner_task_ids,omitempty"`
	SupportingTaskIDs    []string `json:"supporting_task_ids,omitempty"`
	ConflictingTaskIDs   []string `json:"conflicting_task_ids,omitempty"`
	Summary              string   `json:"summary,omitempty"`
}

type CorridorStewardLeaseDigest struct {
	ClusterID      string `json:"cluster_id"`
	EpochID        string `json:"epoch_id"`
	StewardAgentID string `json:"steward_agent_id"`
	GrantedAt      string `json:"granted_at"`
	ExpiresAt      string `json:"expires_at"`
	Status         string `json:"status"`
}

type CorridorOwnershipClusterReport struct {
	ProtoClusterID      string                      `json:"proto_cluster_id"`
	ResolutionKind      string                      `json:"resolution_kind"`
	TaskIDs             []string                    `json:"task_ids,omitempty"`
	SessionIDs          []string                    `json:"session_ids,omitempty"`
	DocKeys             []string                    `json:"doc_keys,omitempty"`
	ArtifactRefs        []string                    `json:"artifact_refs,omitempty"`
	AgentIDs            []string                    `json:"agent_ids,omitempty"`
	TaskClassHint       string                      `json:"task_class_hint"`
	CorridorCatalogHint string                      `json:"corridor_catalog_hint,omitempty"`
	CorridorLookup      CorridorLookupRecord        `json:"corridor_lookup,omitempty"`
	CorridorReadiness   string                      `json:"corridor_readiness"`
	ReadinessConfidence float64                     `json:"readiness_confidence"`
	MixedTaskClasses    bool                        `json:"mixed_task_classes,omitempty"`
	BasisStale          bool                        `json:"basis_stale,omitempty"`
	LastBasisEventAt    string                      `json:"last_basis_event_at,omitempty"`
	UnknownTaskCount    int                         `json:"unknown_task_count"`
	TaskClassCounts     map[string]int              `json:"task_class_counts,omitempty"`
	Ownership           CorridorOwnershipDigest     `json:"ownership"`
	Steward             *CorridorStewardLeaseDigest `json:"steward,omitempty"`
	Summary             string                      `json:"summary,omitempty"`
}

type CorridorOwnershipWorkspaceMetrics struct {
	TotalClusters           int            `json:"total_clusters"`
	OwnedExplicitCount      int            `json:"owned_explicit_count"`
	OwnedExplicitStaleCount int            `json:"owned_explicit_stale_count"`
	SeededTemplateCount     int            `json:"seeded_template_count"`
	DerivedClusterCount     int            `json:"derived_cluster_count"`
	ContestedCount          int            `json:"contested_count"`
	UnresolvedCount         int            `json:"unresolved_count"`
	ActiveStewardCount      int            `json:"active_steward_count"`
	OwnershipStateCounts    map[string]int `json:"ownership_state_counts,omitempty"`
}

type CorridorOwnershipReport struct {
	WorkspaceID   string                            `json:"workspace_id"`
	TimeAuthority WorkspaceTimeAuthority            `json:"time_authority"`
	GeneratedAt   string                            `json:"generated_at"`
	Filter        CorridorOwnershipFilter           `json:"filter"`
	Workspace     CorridorOwnershipWorkspaceMetrics `json:"workspace"`
	Clusters      []CorridorOwnershipClusterReport  `json:"clusters,omitempty"`
}

type CorridorOwnershipClusterDetail struct {
	TimeAuthority WorkspaceTimeAuthority         `json:"time_authority"`
	Cluster       CorridorOwnershipClusterReport `json:"cluster"`
	Tasks         []TaskClassHintRecord          `json:"tasks,omitempty"`
}

type CorridorOwnershipSnapshotInput struct {
	ActorID string
	Limit   int
}

type corridorOwnershipBucket struct {
	taskIDs    []string
	updatedAts []string
}

func normalizeCorridorOwnershipFilter(filter CorridorOwnershipFilter) CorridorOwnershipFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	return filter
}

func (s *Store) BuildCorridorOwnershipReport(ctx context.Context, filter CorridorOwnershipFilter) (CorridorOwnershipReport, error) {
	filter = normalizeCorridorOwnershipFilter(filter)
	if filter.WorkspaceID == "" {
		return CorridorOwnershipReport{}, errors.New("workspace_id is required")
	}
	fullLimit := corridorReadClusterWindow
	if filter.Limit > fullLimit {
		fullLimit = filter.Limit
	}
	if filter.ProtoClusterID != "" {
		fullLimit = 1
	}
	readiness, err := s.BuildCorridorReadinessReport(ctx, CorridorReadinessFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		Limit:          fullLimit,
	})
	if err != nil {
		return CorridorOwnershipReport{}, err
	}
	report := CorridorOwnershipReport{
		WorkspaceID:   filter.WorkspaceID,
		TimeAuthority: readiness.TimeAuthority,
		GeneratedAt:   generatedAtFromWorkspaceTimeAuthority(readiness.TimeAuthority),
		Filter:        filter,
		Workspace: CorridorOwnershipWorkspaceMetrics{
			OwnershipStateCounts: map[string]int{},
		},
	}
	taskCache := map[string]TaskClassHintRecord{}
	for _, cluster := range readiness.Clusters {
		tasks, err := s.corridorOwnershipTasksForCluster(ctx, cluster, taskCache)
		if err != nil {
			return CorridorOwnershipReport{}, err
		}
		item := buildCorridorOwnershipClusterReport(cluster, tasks)
		item.Steward, err = s.activeCorridorStewardLeaseDigest(ctx, item.ProtoClusterID)
		if err != nil {
			return CorridorOwnershipReport{}, err
		}
		report.Clusters = append(report.Clusters, item)
		report.Workspace.OwnershipStateCounts[item.Ownership.OwnershipState]++
		if item.Steward != nil {
			report.Workspace.ActiveStewardCount++
		}
		switch item.Ownership.OwnershipState {
		case corridorOwnershipStateOwnedExplicit:
			report.Workspace.OwnedExplicitCount++
		case corridorOwnershipStateOwnedExplicitStale:
			report.Workspace.OwnedExplicitStaleCount++
		case corridorOwnershipStateSeededTemplate:
			report.Workspace.SeededTemplateCount++
		case corridorOwnershipStateDerivedCluster:
			report.Workspace.DerivedClusterCount++
		case corridorOwnershipStateContested:
			report.Workspace.ContestedCount++
		default:
			report.Workspace.UnresolvedCount++
		}
	}
	report.Workspace.TotalClusters = len(report.Clusters)
	sort.Slice(report.Clusters, func(i, j int) bool {
		left := report.Clusters[i]
		right := report.Clusters[j]
		if corridorOwnershipStateRank(left.Ownership.OwnershipState) != corridorOwnershipStateRank(right.Ownership.OwnershipState) {
			return corridorOwnershipStateRank(left.Ownership.OwnershipState) > corridorOwnershipStateRank(right.Ownership.OwnershipState)
		}
		if left.Ownership.BasisAuthoritative != right.Ownership.BasisAuthoritative {
			return left.Ownership.BasisAuthoritative
		}
		if left.Ownership.BasisFresh != right.Ownership.BasisFresh {
			return left.Ownership.BasisFresh
		}
		if left.Ownership.BasisUpdatedAt != right.Ownership.BasisUpdatedAt {
			return left.Ownership.BasisUpdatedAt > right.Ownership.BasisUpdatedAt
		}
		return left.ProtoClusterID < right.ProtoClusterID
	})
	if filter.Limit > 0 && len(report.Clusters) > filter.Limit {
		report.Clusters = append([]CorridorOwnershipClusterReport(nil), report.Clusters[:filter.Limit]...)
	}
	return report, nil
}

func (s *Store) corridorOwnershipTasksForCluster(ctx context.Context, cluster CorridorClusterReport, taskCache map[string]TaskClassHintRecord) ([]TaskClassHintRecord, error) {
	if taskCache == nil {
		taskCache = map[string]TaskClassHintRecord{}
	}
	tasks := make([]TaskClassHintRecord, 0, len(cluster.TaskIDs))
	for _, taskID := range cluster.TaskIDs {
		task, err := s.corridorTaskClassification(ctx, taskID, taskCache)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool {
		left := tasks[i]
		right := tasks[j]
		if left.TaskClassHint != right.TaskClassHint {
			return left.TaskClassHint < right.TaskClassHint
		}
		if left.HintConfidence != right.HintConfidence {
			return left.HintConfidence > right.HintConfidence
		}
		return left.TaskID < right.TaskID
	})
	return tasks, nil
}

func (s *Store) BuildCorridorOwnershipClusterDetail(ctx context.Context, workspaceID, protoClusterID string) (CorridorOwnershipClusterDetail, error) {
	detail, err := s.BuildCorridorClusterDetail(ctx, workspaceID, protoClusterID)
	if err != nil {
		return CorridorOwnershipClusterDetail{}, err
	}
	cluster := buildCorridorOwnershipClusterReport(detail.Cluster, detail.Tasks)
	cluster.Steward, err = s.activeCorridorStewardLeaseDigest(ctx, cluster.ProtoClusterID)
	if err != nil {
		return CorridorOwnershipClusterDetail{}, err
	}
	return CorridorOwnershipClusterDetail{
		TimeAuthority: detail.TimeAuthority,
		Cluster:       cluster,
		Tasks:         append([]TaskClassHintRecord(nil), detail.Tasks...),
	}, nil
}

func (s *Store) RecordCorridorOwnershipSnapshot(ctx context.Context, report CorridorOwnershipReport, input CorridorOwnershipSnapshotInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(report.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = "corridor.ownership.snapshot"
	}
	clusters := append([]CorridorOwnershipClusterReport(nil), report.Clusters...)
	if input.Limit > 0 && len(clusters) > input.Limit {
		clusters = append([]CorridorOwnershipClusterReport(nil), clusters[:input.Limit]...)
	}
	if strings.TrimSpace(report.Filter.ProtoClusterID) != "" && len(clusters) == 0 {
		return RuntimeEventRecord{}, fmt.Errorf("corridor ownership cluster not found: %s/%s", workspaceID, strings.TrimSpace(report.Filter.ProtoClusterID))
	}
	payload := map[string]any{
		"generated_at":           report.GeneratedAt,
		"workspace_id":           report.WorkspaceID,
		"filter":                 report.Filter,
		"workspace":              report.Workspace,
		"clusters":               clusters,
		"summary":                corridorOwnershipSnapshotSummary(report, clusters),
		"source_cluster_count":   len(report.Clusters),
		"captured_cluster_count": len(clusters),
		"snapshot_limit":         input.Limit,
		"snapshot_truncated":     input.Limit > 0 && len(report.Clusters) > input.Limit,
		"typed_event_type":       "CORRIDOR_OWNERSHIP_SNAPSHOT",
		"event_kind":             "cluster.corridor_ownership_snapshot",
	}
	return s.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "cluster.corridor_ownership_snapshot",
		EntityType:  "instrumentation_corridor_ownership",
		EntityID:    corridorOwnershipSnapshotEntityID(report.Filter),
		ActorType:   "operator",
		ActorID:     actorID,
		SessionID:   corridorOwnershipSnapshotSessionID(report),
		TaskID:      corridorOwnershipSnapshotTaskID(report),
		PayloadJSON: mustJSON(payload),
		CreatedAt:   referenceAt,
	})
}

func buildCorridorOwnershipClusterReport(cluster CorridorClusterReport, tasks []TaskClassHintRecord) CorridorOwnershipClusterReport {
	ownership := corridorOwnershipDigest(cluster, tasks)
	return CorridorOwnershipClusterReport{
		ProtoClusterID:      cluster.ProtoClusterID,
		ResolutionKind:      cluster.ResolutionKind,
		TaskIDs:             append([]string{}, cluster.TaskIDs...),
		SessionIDs:          append([]string{}, cluster.SessionIDs...),
		DocKeys:             append([]string{}, cluster.DocKeys...),
		ArtifactRefs:        append([]string{}, cluster.ArtifactRefs...),
		AgentIDs:            append([]string{}, cluster.AgentIDs...),
		TaskClassHint:       cluster.TaskClassHint,
		CorridorCatalogHint: cluster.CorridorCatalogHint,
		CorridorLookup:      cluster.CorridorLookup,
		CorridorReadiness:   cluster.CorridorReadiness,
		ReadinessConfidence: cluster.ReadinessConfidence,
		MixedTaskClasses:    cluster.MixedTaskClasses,
		BasisStale:          cluster.BasisStale,
		LastBasisEventAt:    cluster.LastBasisEventAt,
		UnknownTaskCount:    cluster.UnknownTaskCount,
		TaskClassCounts:     cloneIntMap(cluster.TaskClassCounts),
		Ownership:           ownership,
		Summary:             corridorOwnershipClusterSummary(cluster, ownership),
	}
}

func (s *Store) activeCorridorStewardLeaseDigest(ctx context.Context, protoClusterID string) (*CorridorStewardLeaseDigest, error) {
	protoClusterID = strings.TrimSpace(protoClusterID)
	if protoClusterID == "" {
		return nil, nil
	}
	steward, err := s.GetActiveSteward(ctx, protoClusterID)
	if err != nil {
		if errors.Is(err, ErrStewardNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &CorridorStewardLeaseDigest{
		ClusterID:      steward.ClusterID,
		EpochID:        steward.EpochID,
		StewardAgentID: steward.StewardAgentID,
		GrantedAt:      steward.GrantedAt.UTC().Format(time.RFC3339Nano),
		ExpiresAt:      steward.ExpiresAt.UTC().Format(time.RFC3339Nano),
		Status:         strings.TrimSpace(steward.Status),
	}, nil
}

func corridorOwnershipDigest(cluster CorridorClusterReport, tasks []TaskClassHintRecord) CorridorOwnershipDigest {
	referenceEventAt := firstNonEmpty(strings.TrimSpace(cluster.Metrics.LastEventAt), strings.TrimSpace(cluster.LastBasisEventAt))
	explicitFresh := map[string]*corridorOwnershipBucket{}
	explicitStale := map[string]*corridorOwnershipBucket{}
	templateSeeded := map[string]*corridorOwnershipBucket{}
	derived := map[string]*corridorOwnershipBucket{}

	addBucketTask := func(group map[string]*corridorOwnershipBucket, classHint, taskID, updatedAt string) {
		classHint = normalizeTaskClassHint(classHint)
		if classHint == taskClassHintUnknown || strings.TrimSpace(taskID) == "" {
			return
		}
		entry := group[classHint]
		if entry == nil {
			entry = &corridorOwnershipBucket{}
			group[classHint] = entry
		}
		entry.taskIDs = append(entry.taskIDs, strings.TrimSpace(taskID))
		if strings.TrimSpace(updatedAt) != "" {
			entry.updatedAts = append(entry.updatedAts, strings.TrimSpace(updatedAt))
		}
	}

	for _, task := range tasks {
		taskID := strings.TrimSpace(task.TaskID)
		authoredClass := normalizeTaskClassHint(task.TaskClass)
		source := normalizeAuthoredTaskClassSource(task.TaskClassSource)
		switch source {
		case model.TaskClassSourceExplicit:
			updatedAt := strings.TrimSpace(task.TaskClassUpdatedAt)
			if updatedAt == "" || corridorBasisIsStale(updatedAt, referenceEventAt) {
				addBucketTask(explicitStale, authoredClass, taskID, updatedAt)
			} else {
				addBucketTask(explicitFresh, authoredClass, taskID, updatedAt)
			}
			continue
		case model.TaskClassSourceTemplateDefault:
			addBucketTask(templateSeeded, authoredClass, taskID, strings.TrimSpace(task.TaskClassUpdatedAt))
			continue
		}
		activeClass, _, _, basisUpdatedAt, _ := corridorActiveTaskClassification(task)
		addBucketTask(derived, activeClass, taskID, basisUpdatedAt)
	}

	if digest, ok := corridorOwnershipChooseDigest(explicitFresh, corridorOwnershipStateOwnedExplicit, model.TaskClassSourceExplicit, true, referenceEventAt); ok {
		digest.SupportingTaskIDs = uniqueSortedStrings(append(digest.SupportingTaskIDs, corridorOwnershipBucketTaskIDs(explicitStale[digest.BasisTaskClass])...))
		digest.SupportingTaskIDs = uniqueSortedStrings(append(digest.SupportingTaskIDs, corridorOwnershipBucketTaskIDs(templateSeeded[digest.BasisTaskClass])...))
		digest.SupportingTaskIDs = uniqueSortedStrings(append(digest.SupportingTaskIDs, corridorOwnershipBucketTaskIDs(derived[digest.BasisTaskClass])...))
		digest.Summary = corridorOwnershipDigestSummary(digest)
		return digest
	}
	if digest, ok := corridorOwnershipChooseDigest(explicitStale, corridorOwnershipStateOwnedExplicitStale, model.TaskClassSourceExplicit, true, referenceEventAt); ok {
		digest.SupportingTaskIDs = uniqueSortedStrings(append(digest.SupportingTaskIDs, corridorOwnershipBucketTaskIDs(templateSeeded[digest.BasisTaskClass])...))
		digest.SupportingTaskIDs = uniqueSortedStrings(append(digest.SupportingTaskIDs, corridorOwnershipBucketTaskIDs(derived[digest.BasisTaskClass])...))
		digest.Summary = corridorOwnershipDigestSummary(digest)
		return digest
	}
	if corridorOwnershipDistinctClassCount(explicitFresh)+corridorOwnershipDistinctClassCount(explicitStale) > 1 {
		return corridorOwnershipContestedDigest("explicit task-owned corridor basis is contested across multiple authored classes", explicitFresh, explicitStale)
	}
	if digest, ok := corridorOwnershipChooseDigest(templateSeeded, corridorOwnershipStateSeededTemplate, model.TaskClassSourceTemplateDefault, false, referenceEventAt); ok {
		digest.SupportingTaskIDs = uniqueSortedStrings(append(digest.SupportingTaskIDs, corridorOwnershipBucketTaskIDs(derived[digest.BasisTaskClass])...))
		digest.Summary = corridorOwnershipDigestSummary(digest)
		return digest
	}
	if corridorOwnershipDistinctClassCount(templateSeeded) > 1 {
		return corridorOwnershipContestedDigest("seeded template defaults disagree across multiple corridor classes", templateSeeded)
	}
	if cluster.MixedTaskClasses || strings.TrimSpace(cluster.CorridorLookup.LookupStatus) == corridorLookupStatusAmbiguous {
		conflictingTaskIDs := []string{}
		for classHint, entry := range derived {
			if normalizeTaskClassHint(classHint) == taskClassHintUnknown {
				continue
			}
			conflictingTaskIDs = append(conflictingTaskIDs, entry.taskIDs...)
		}
		return CorridorOwnershipDigest{
			OwnershipState:     corridorOwnershipStateContested,
			ConflictingTaskIDs: uniqueSortedStrings(conflictingTaskIDs),
			Summary:            "cluster-level corridor basis remains contested across multiple derived task-class signals",
		}
	}
	if digest, ok := corridorOwnershipChooseDigest(derived, corridorOwnershipStateDerivedCluster, "", false, referenceEventAt); ok {
		digest.BasisTaskClassSource = model.TaskClassSourceHeuristicFallback
		digest.Summary = corridorOwnershipDigestSummary(digest)
		return digest
	}
	return CorridorOwnershipDigest{
		OwnershipState: corridorOwnershipStateUnresolved,
		Summary:        "cluster does not yet have a stable task-owned corridor basis",
	}
}

func corridorOwnershipChooseDigest(group map[string]*corridorOwnershipBucket, state, source string, authoritative bool, referenceEventAt string) (CorridorOwnershipDigest, bool) {
	if len(group) != 1 {
		return CorridorOwnershipDigest{}, false
	}
	for classHint, entry := range group {
		taskIDs := uniqueSortedStrings(entry.taskIDs)
		updatedAt := corridorNewestTimestamp(entry.updatedAts)
		fresh := updatedAt != "" && !corridorBasisIsStale(updatedAt, referenceEventAt)
		if state == corridorOwnershipStateOwnedExplicitStale {
			fresh = false
		}
		digest := CorridorOwnershipDigest{
			OwnershipState:       state,
			BasisTaskClass:       normalizeTaskClassHint(classHint),
			BasisTaskClassSource: strings.TrimSpace(source),
			BasisUpdatedAt:       strings.TrimSpace(updatedAt),
			BasisFresh:           fresh,
			BasisAuthoritative:   authoritative,
			OwnerTaskIDs:         taskIDs,
			SupportingTaskIDs:    []string{},
		}
		if len(taskIDs) == 1 {
			digest.OwnerTaskID = taskIDs[0]
		}
		return digest, true
	}
	return CorridorOwnershipDigest{}, false
}

func corridorOwnershipContestedDigest(summary string, groups ...map[string]*corridorOwnershipBucket) CorridorOwnershipDigest {
	conflicting := []string{}
	for _, group := range groups {
		for _, entry := range group {
			conflicting = append(conflicting, entry.taskIDs...)
		}
	}
	return CorridorOwnershipDigest{
		OwnershipState:     corridorOwnershipStateContested,
		ConflictingTaskIDs: uniqueSortedStrings(conflicting),
		Summary:            summary,
	}
}

func corridorOwnershipBucketTaskIDs(entry *corridorOwnershipBucket) []string {
	if entry == nil {
		return nil
	}
	return append([]string{}, entry.taskIDs...)
}

func corridorOwnershipDistinctClassCount(group map[string]*corridorOwnershipBucket) int {
	count := 0
	for classHint := range group {
		if normalizeTaskClassHint(classHint) == taskClassHintUnknown {
			continue
		}
		count++
	}
	return count
}

func corridorOwnershipDigestSummary(digest CorridorOwnershipDigest) string {
	label := strings.ToLower(firstNonEmpty(corridorCatalogHint(digest.BasisTaskClass), digest.BasisTaskClass, "corridor"))
	switch digest.OwnershipState {
	case corridorOwnershipStateOwnedExplicit:
		return "task-owned explicit corridor basis anchors " + label
	case corridorOwnershipStateOwnedExplicitStale:
		return "stale explicit task-owned corridor basis still anchors " + label
	case corridorOwnershipStateSeededTemplate:
		return "seeded template defaults currently anchor " + label + " without authoritative ownership"
	case corridorOwnershipStateDerivedCluster:
		return "cluster currently leans on derived corridor basis " + label
	default:
		return firstNonEmpty(digest.Summary, "cluster does not yet have a stable task-owned corridor basis")
	}
}

func corridorOwnershipClusterSummary(cluster CorridorClusterReport, ownership CorridorOwnershipDigest) string {
	switch strings.TrimSpace(ownership.OwnershipState) {
	case corridorOwnershipStateContested, corridorOwnershipStateUnresolved:
		return ownership.Summary
	default:
		if strings.TrimSpace(ownership.Summary) != "" {
			return ownership.Summary
		}
		return corridorClusterSummary(cluster.ProtoClusterID, cluster.TaskClassHint, cluster.CorridorReadiness, cluster.MixedTaskClasses, cluster.UnknownTaskCount, cluster.BasisStale)
	}
}

func corridorOwnershipStateRank(state string) int {
	switch strings.TrimSpace(state) {
	case corridorOwnershipStateOwnedExplicit:
		return 6
	case corridorOwnershipStateOwnedExplicitStale:
		return 5
	case corridorOwnershipStateSeededTemplate:
		return 4
	case corridorOwnershipStateDerivedCluster:
		return 3
	case corridorOwnershipStateContested:
		return 2
	case corridorOwnershipStateUnresolved:
		return 1
	default:
		return 0
	}
}

func corridorOwnershipSnapshotEntityID(filter CorridorOwnershipFilter) string {
	if clusterID := strings.TrimSpace(filter.ProtoClusterID); clusterID != "" {
		return clusterID
	}
	return strings.TrimSpace(filter.WorkspaceID)
}

func corridorOwnershipSnapshotTaskID(report CorridorOwnershipReport) string {
	if strings.TrimSpace(report.Filter.ProtoClusterID) == "" {
		return ""
	}
	for _, cluster := range report.Clusters {
		if len(cluster.TaskIDs) == 1 {
			return strings.TrimSpace(cluster.TaskIDs[0])
		}
	}
	return ""
}

func corridorOwnershipSnapshotSessionID(report CorridorOwnershipReport) string {
	if strings.TrimSpace(report.Filter.ProtoClusterID) == "" {
		return ""
	}
	for _, cluster := range report.Clusters {
		if len(cluster.SessionIDs) == 1 {
			return strings.TrimSpace(cluster.SessionIDs[0])
		}
	}
	return ""
}

func corridorOwnershipSnapshotSummary(report CorridorOwnershipReport, clusters []CorridorOwnershipClusterReport) string {
	if clusterID := strings.TrimSpace(report.Filter.ProtoClusterID); clusterID != "" {
		if len(clusters) > 0 {
			return fmt.Sprintf(
				"Corridor ownership snapshot: %s state=%s basis=%s",
				clusterID,
				strings.ToLower(firstNonEmpty(clusters[0].Ownership.OwnershipState, corridorOwnershipStateUnresolved)),
				strings.ToLower(firstNonEmpty(clusters[0].Ownership.BasisTaskClass, taskClassHintUnknown)),
			)
		}
		return "Corridor ownership snapshot: " + clusterID
	}
	return fmt.Sprintf(
		"Corridor ownership snapshot: %d explicit, %d stale-explicit, %d contested, %d unresolved",
		report.Workspace.OwnedExplicitCount,
		report.Workspace.OwnedExplicitStaleCount,
		report.Workspace.ContestedCount,
		report.Workspace.UnresolvedCount,
	)
}
