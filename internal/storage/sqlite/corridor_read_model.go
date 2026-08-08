package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	corridorReadRuntimeEventWindow = 500
	corridorReadClusterWindow      = 128
	corridorReadinessStaleAfter    = 72 * time.Hour

	taskClassHintUnknown     = "UNKNOWN"
	taskClassHintProof       = "PROOF"
	taskClassHintExploration = "EXPLORATION"
	taskClassHintIntegration = "INTEGRATION"
	taskClassHintIncident    = "INCIDENT"

	corridorReadinessReady          = "READY"
	corridorReadinessBorderline     = "BORDERLINE"
	corridorReadinessUnderEvidenced = "UNDER_EVIDENCED"
	corridorReadinessMixed          = "MIXED"
	corridorReadinessStaleBasis     = "STALE_BASIS"

	corridorLookupStatusNoMatch       = "NO_MATCH"
	corridorLookupStatusClassMatch    = "CLASS_MATCH"
	corridorLookupStatusTemplateMatch = "TEMPLATE_MATCH"
	corridorLookupStatusAmbiguous     = "AMBIGUOUS"
)

type CorridorReadinessFilter struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type CorridorCatalogEntry struct {
	CatalogKey             string   `json:"catalog_key"`
	DisplayName            string   `json:"display_name"`
	Summary                string   `json:"summary"`
	TaskClassHint          string   `json:"task_class_hint"`
	PreferredTaskTemplates []string `json:"preferred_task_templates,omitempty"`
}

type CorridorLookupRecord struct {
	LookupStatus    string   `json:"lookup_status"`
	CatalogKey      string   `json:"catalog_key,omitempty"`
	DisplayName     string   `json:"display_name,omitempty"`
	MatchSource     string   `json:"match_source,omitempty"`
	MatchConfidence float64  `json:"match_confidence"`
	MatchBasis      []string `json:"match_basis,omitempty"`
	Summary         string   `json:"summary,omitempty"`
}

type TaskClassHintRecord struct {
	TaskID             string               `json:"task_id"`
	Title              string               `json:"title,omitempty"`
	Status             string               `json:"status,omitempty"`
	TaskKind           string               `json:"task_kind"`
	TaskTemplate       string               `json:"task_template"`
	TaskClass          string               `json:"task_class,omitempty"`
	TaskClassSource    string               `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt string               `json:"task_class_updated_at,omitempty"`
	UpdatedAt          string               `json:"updated_at,omitempty"`
	BasisUpdatedAt     string               `json:"basis_updated_at,omitempty"`
	Tags               []string             `json:"tags,omitempty"`
	TaskClassHint      string               `json:"task_class_hint"`
	HintConfidence     float64              `json:"hint_confidence"`
	TaskClassBasis     []string             `json:"task_class_basis,omitempty"`
	CorridorHint       string               `json:"corridor_catalog_hint,omitempty"`
	CorridorLookup     CorridorLookupRecord `json:"corridor_lookup,omitempty"`
	Summary            string               `json:"summary,omitempty"`
}

type CorridorClusterReport struct {
	ProtoClusterID      string               `json:"proto_cluster_id"`
	ResolutionKind      string               `json:"resolution_kind"`
	TaskIDs             []string             `json:"task_ids,omitempty"`
	SessionIDs          []string             `json:"session_ids,omitempty"`
	DocKeys             []string             `json:"doc_keys,omitempty"`
	ArtifactRefs        []string             `json:"artifact_refs,omitempty"`
	AgentIDs            []string             `json:"agent_ids,omitempty"`
	TaskClass           string               `json:"task_class,omitempty"`
	TaskClassSource     string               `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt  string               `json:"task_class_updated_at,omitempty"`
	TaskClassHint       string               `json:"task_class_hint"`
	CorridorCatalogHint string               `json:"corridor_catalog_hint,omitempty"`
	CorridorLookup      CorridorLookupRecord `json:"corridor_lookup,omitempty"`
	TaskClassConfidence float64              `json:"task_class_confidence"`
	CorridorReadiness   string               `json:"corridor_readiness"`
	ReadinessConfidence float64              `json:"readiness_confidence"`
	TaskClassCounts     map[string]int       `json:"task_class_counts,omitempty"`
	UnknownTaskCount    int                  `json:"unknown_task_count"`
	MixedTaskClasses    bool                 `json:"mixed_task_classes,omitempty"`
	BasisStale          bool                 `json:"basis_stale,omitempty"`
	LastBasisEventAt    string               `json:"last_basis_event_at,omitempty"`
	TaskClassBasis      []string             `json:"task_class_basis,omitempty"`
	Metrics             ProtoClusterMetrics  `json:"metrics"`
	Summary             string               `json:"summary,omitempty"`
}

type CorridorWorkspaceMetrics struct {
	TotalClusters         int            `json:"total_clusters"`
	ReadyCount            int            `json:"ready_count"`
	BorderlineCount       int            `json:"borderline_count"`
	UnderEvidencedCount   int            `json:"under_evidenced_count"`
	MixedCount            int            `json:"mixed_count"`
	StaleBasisCount       int            `json:"stale_basis_count"`
	DominantTaskClass     string         `json:"dominant_task_class,omitempty"`
	DominantTaskClassHits int            `json:"dominant_task_class_hits"`
	TaskClassCounts       map[string]int `json:"task_class_hint_counts,omitempty"`
	LookupStatusCounts    map[string]int `json:"lookup_status_counts,omitempty"`
}

type CorridorReadinessReport struct {
	WorkspaceID       string                   `json:"workspace_id"`
	TimeAuthority     WorkspaceTimeAuthority   `json:"time_authority"`
	GeneratedAt       string                   `json:"generated_at"`
	Filter            CorridorReadinessFilter  `json:"filter"`
	Catalog           []CorridorCatalogEntry   `json:"catalog,omitempty"`
	Workspace         CorridorWorkspaceMetrics `json:"workspace"`
	Clusters          []CorridorClusterReport  `json:"clusters,omitempty"`
	readSurfacePolicy ReadSurfacePolicy        `json:"-"`
}

type CorridorClusterDetail struct {
	TimeAuthority WorkspaceTimeAuthority `json:"time_authority"`
	Cluster       CorridorClusterReport  `json:"cluster"`
	Tasks         []TaskClassHintRecord  `json:"tasks,omitempty"`
}

type CorridorSnapshotInput struct {
	ActorID string
	Limit   int
}

type corridorTaskContext struct {
	TaskID             string
	Title              string
	Description        string
	Status             string
	TaskKind           string
	TaskTemplate       string
	TaskClass          string
	TaskClassSource    string
	TaskClassUpdatedAt string
	UpdatedAt          string
	Tags               []string
}

func normalizeCorridorReadinessFilter(filter CorridorReadinessFilter) CorridorReadinessFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	filter.Limit = clampReadSurfaceLimit(filter.Limit, readSurfaceReportLimitDefault, readSurfaceReportLimitMax)
	return filter
}

func (s *Store) BuildCorridorReadinessReport(ctx context.Context, filter CorridorReadinessFilter) (CorridorReadinessReport, error) {
	filter = normalizeCorridorReadinessFilter(filter)
	if filter.WorkspaceID == "" {
		return CorridorReadinessReport{}, errors.New("workspace_id is required")
	}
	clusterLimit := corridorReadClusterWindow
	if filter.ProtoClusterID != "" {
		clusterLimit = corridorReadRuntimeEventWindow
	}
	instrumentation, err := s.BuildInstrumentationReport(ctx, InstrumentationReportFilter{
		WorkspaceID:  filter.WorkspaceID,
		Limit:        corridorReadRuntimeEventWindow,
		ClusterLimit: clusterLimit,
	})
	if err != nil {
		return CorridorReadinessReport{}, err
	}
	epochAnchorAt, err := s.persistedControlEpochAnchor(ctx, filter.WorkspaceID)
	if err != nil {
		return CorridorReadinessReport{}, err
	}
	workspaceObservedAt := corridorWorkspaceObservedAtForProtoClusters(epochAnchorAt, instrumentation.Clusters)
	authority, err := s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return CorridorReadinessReport{}, err
	}

	taskCache := map[string]TaskClassHintRecord{}
	taskDetails := map[string]TaskClassHintRecord{}
	report := CorridorReadinessReport{
		WorkspaceID:   filter.WorkspaceID,
		TimeAuthority: authority,
		GeneratedAt:   generatedAtFromWorkspaceTimeAuthority(authority),
		Filter:        filter,
		Catalog:       corridorCatalogEntriesCopy(),
		Workspace: CorridorWorkspaceMetrics{
			TaskClassCounts:    map[string]int{},
			LookupStatusCounts: map[string]int{},
		},
		readSurfacePolicy: corridorReadinessPolicy(filter, clusterLimit),
	}
	for _, cluster := range instrumentation.Clusters {
		if filter.ProtoClusterID != "" && strings.TrimSpace(cluster.ProtoClusterID) != filter.ProtoClusterID {
			continue
		}
		item, detailTasks, err := s.buildCorridorClusterReport(ctx, cluster, taskCache, workspaceObservedAt)
		if err != nil {
			return CorridorReadinessReport{}, err
		}
		report.Clusters = append(report.Clusters, item)
		for _, task := range detailTasks {
			taskDetails[task.TaskID] = task
		}
	}

	sort.Slice(report.Clusters, func(i, j int) bool {
		left := report.Clusters[i]
		right := report.Clusters[j]
		if corridorReadinessRank(left.CorridorReadiness) != corridorReadinessRank(right.CorridorReadiness) {
			return corridorReadinessRank(left.CorridorReadiness) > corridorReadinessRank(right.CorridorReadiness)
		}
		if left.ReadinessConfidence != right.ReadinessConfidence {
			return left.ReadinessConfidence > right.ReadinessConfidence
		}
		return left.ProtoClusterID < right.ProtoClusterID
	})

	report.Workspace.TotalClusters = len(report.Clusters)
	for _, cluster := range report.Clusters {
		switch strings.TrimSpace(cluster.CorridorReadiness) {
		case corridorReadinessReady:
			report.Workspace.ReadyCount++
		case corridorReadinessBorderline:
			report.Workspace.BorderlineCount++
		case corridorReadinessMixed:
			report.Workspace.MixedCount++
		case corridorReadinessStaleBasis:
			report.Workspace.StaleBasisCount++
		default:
			report.Workspace.UnderEvidencedCount++
		}
		if !cluster.MixedTaskClasses && strings.TrimSpace(cluster.CorridorLookup.LookupStatus) != corridorLookupStatusAmbiguous {
			if hint := normalizeTaskClassHint(firstNonEmpty(cluster.TaskClass, cluster.TaskClassHint)); hint != "" && hint != taskClassHintUnknown {
				report.Workspace.TaskClassCounts[hint]++
			}
		}
		if status := strings.TrimSpace(cluster.CorridorLookup.LookupStatus); status != "" {
			report.Workspace.LookupStatusCounts[status]++
		}
	}
	report.Workspace.DominantTaskClass, report.Workspace.DominantTaskClassHits = corridorDominantClass(report.Workspace.TaskClassCounts)
	if filter.Limit > 0 && len(report.Clusters) > filter.Limit {
		report.Clusters = append([]CorridorClusterReport(nil), report.Clusters[:filter.Limit]...)
	}
	return report, nil
}

func (s *Store) BuildCorridorClusterDetail(ctx context.Context, workspaceID, protoClusterID string) (CorridorClusterDetail, error) {
	report, err := s.BuildCorridorReadinessReport(ctx, CorridorReadinessFilter{
		WorkspaceID:    strings.TrimSpace(workspaceID),
		ProtoClusterID: strings.TrimSpace(protoClusterID),
		Limit:          1,
	})
	if err != nil {
		return CorridorClusterDetail{}, err
	}
	if len(report.Clusters) == 0 {
		return CorridorClusterDetail{}, fmt.Errorf("corridor cluster not found: %s/%s", strings.TrimSpace(workspaceID), strings.TrimSpace(protoClusterID))
	}
	cluster := report.Clusters[0]
	taskCache := map[string]TaskClassHintRecord{}
	tasks := make([]TaskClassHintRecord, 0, len(cluster.TaskIDs))
	for _, taskID := range cluster.TaskIDs {
		task, err := s.corridorTaskClassification(ctx, taskID, taskCache)
		if err != nil {
			return CorridorClusterDetail{}, err
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
	return CorridorClusterDetail{
		TimeAuthority: report.TimeAuthority,
		Cluster:       cluster,
		Tasks:         tasks,
	}, nil
}

func (s *Store) RecordCorridorReadinessSnapshot(ctx context.Context, report CorridorReadinessReport, input CorridorSnapshotInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(report.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = "corridor.snapshot"
	}
	clusters := append([]CorridorClusterReport(nil), report.Clusters...)
	if input.Limit > 0 && len(clusters) > input.Limit {
		clusters = append([]CorridorClusterReport(nil), clusters[:input.Limit]...)
	}
	if strings.TrimSpace(report.Filter.ProtoClusterID) != "" && len(clusters) == 0 {
		return RuntimeEventRecord{}, fmt.Errorf("corridor cluster not found: %s/%s", workspaceID, strings.TrimSpace(report.Filter.ProtoClusterID))
	}
	payload := map[string]any{
		"generated_at":           report.GeneratedAt,
		"workspace_id":           report.WorkspaceID,
		"filter":                 report.Filter,
		"workspace":              report.Workspace,
		"clusters":               clusters,
		"summary":                corridorSnapshotSummary(report, clusters),
		"source_cluster_count":   len(report.Clusters),
		"captured_cluster_count": len(clusters),
		"snapshot_limit":         input.Limit,
		"snapshot_truncated":     input.Limit > 0 && len(report.Clusters) > input.Limit,
		"typed_event_type":       "CORRIDOR_READINESS_SNAPSHOT",
		"event_kind":             "cluster.corridor_readiness_snapshot",
	}
	return s.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "cluster.corridor_readiness_snapshot",
		EntityType:  "instrumentation_corridor",
		EntityID:    corridorSnapshotEntityID(report.Filter),
		ActorType:   "operator",
		ActorID:     actorID,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   referenceAt,
	})
}

func (s *Store) buildCorridorClusterReport(ctx context.Context, cluster ProtoClusterReport, taskCache map[string]TaskClassHintRecord, workspaceObservedAt string) (CorridorClusterReport, []TaskClassHintRecord, error) {
	tasks := make([]TaskClassHintRecord, 0, len(cluster.TaskIDs))
	for _, taskID := range cluster.TaskIDs {
		task, err := s.corridorTaskClassification(ctx, taskID, taskCache)
		if err != nil {
			return CorridorClusterReport{}, nil, err
		}
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].TaskID < tasks[j].TaskID
	})

	classCounts := map[string]int{}
	concreteCounts := map[string]int{}
	basisByClass := map[string][]string{}
	confidenceByClass := map[string][]float64{}
	basisUpdatedAtByClass := map[string][]string{}
	authoredSourcesByClass := map[string][]string{}
	authoredUpdatedAtByClass := map[string][]string{}
	authoredCountsByClass := map[string]int{}
	heuristicConcreteCounts := map[string]int{}
	basisMissingTimestampByClass := map[string]bool{}
	unknownCount := 0
	for _, task := range tasks {
		heuristicHint := normalizeTaskClassHint(task.TaskClassHint)
		if heuristicHint != taskClassHintUnknown {
			heuristicConcreteCounts[heuristicHint]++
		}
		activeClass, activeConfidence, activeBasis, activeUpdatedAt, activeSource := corridorActiveTaskClassification(task)
		classCounts[activeClass]++
		basisByClass[activeClass] = append(basisByClass[activeClass], activeBasis...)
		confidenceByClass[activeClass] = append(confidenceByClass[activeClass], activeConfidence)
		if strings.TrimSpace(activeUpdatedAt) != "" && len(activeBasis) > 0 {
			basisUpdatedAtByClass[activeClass] = append(basisUpdatedAtByClass[activeClass], strings.TrimSpace(activeUpdatedAt))
		} else if len(activeBasis) > 0 {
			basisMissingTimestampByClass[activeClass] = true
		}
		if activeClass == taskClassHintUnknown {
			unknownCount++
			continue
		}
		concreteCounts[activeClass]++
		if activeSource != "" {
			authoredCountsByClass[activeClass]++
			authoredSourcesByClass[activeClass] = append(authoredSourcesByClass[activeClass], activeSource)
			if strings.TrimSpace(task.TaskClassUpdatedAt) != "" {
				authoredUpdatedAtByClass[activeClass] = append(authoredUpdatedAtByClass[activeClass], strings.TrimSpace(task.TaskClassUpdatedAt))
			}
		}
	}

	taskClassHint, _ := corridorDominantClass(heuristicConcreteCounts)
	if taskClassHint == "" {
		taskClassHint = taskClassHintUnknown
	}
	dominantClass, classHits := corridorDominantClass(concreteCounts)
	mixed := len(concreteCounts) > 1
	if mixed {
		taskClassHint = taskClassHintUnknown
	}
	if dominantClass == "" {
		dominantClass = taskClassHintUnknown
	}
	taskClassConfidence := 0.0
	if values := confidenceByClass[dominantClass]; len(values) > 0 {
		for _, value := range values {
			taskClassConfidence += value
		}
		taskClassConfidence /= float64(len(values))
		taskClassConfidence = corridorClampUnit(taskClassConfidence)
	}

	lastBasisEventAt := corridorNewestTimestamp(basisUpdatedAtByClass[dominantClass])
	basisStale := corridorBasisIsStale(lastBasisEventAt, controlLaterTimestamp(workspaceObservedAt, strings.TrimSpace(cluster.Metrics.LastEventAt)))
	if !basisStale && dominantClass != taskClassHintUnknown && (lastBasisEventAt == "" || basisMissingTimestampByClass[dominantClass]) && len(basisByClass[dominantClass]) > 0 {
		basisStale = true
	}
	readiness := corridorReadinessUnderEvidenced
	readinessConfidence := taskClassConfidence
	switch {
	case len(cluster.TaskIDs) == 0 || len(concreteCounts) == 0:
		readiness = corridorReadinessUnderEvidenced
		readinessConfidence = 0
	case mixed:
		readiness = corridorReadinessMixed
		dominantShare := 0.0
		if concreteTotal := corridorTotalCount(concreteCounts); concreteTotal > 0 {
			dominantShare = float64(classHits) / float64(concreteTotal)
		}
		readinessConfidence = corridorClampUnit((taskClassConfidence * dominantShare) - 0.1)
	case basisStale:
		readiness = corridorReadinessStaleBasis
		readinessConfidence = corridorClampUnit(taskClassConfidence - 0.15)
	case unknownCount > 0 || taskClassConfidence < 0.78:
		readiness = corridorReadinessBorderline
		readinessConfidence = corridorClampUnit(taskClassConfidence - 0.1)
	default:
		readiness = corridorReadinessReady
		readinessConfidence = taskClassConfidence
	}

	surfaceTaskClassHint := taskClassHint
	lookupTaskClassHint := dominantClass
	if lookupTaskClassHint == "" {
		lookupTaskClassHint = taskClassHint
	}
	surfaceTaskClassConfidence := taskClassConfidence
	surfaceTaskClassBasis := uniqueSortedStrings(basisByClass[dominantClass])
	surfaceLastBasisEventAt := lastBasisEventAt
	if mixed {
		surfaceTaskClassHint = taskClassHintUnknown
		lookupTaskClassHint = taskClassHintUnknown
		surfaceTaskClassConfidence = 0
		surfaceTaskClassBasis = []string{}
		surfaceLastBasisEventAt = corridorNewestTimestampForClasses(basisUpdatedAtByClass)
	} else if readiness == corridorReadinessUnderEvidenced {
		surfaceTaskClassHint = taskClassHintUnknown
		lookupTaskClassHint = taskClassHintUnknown
		surfaceTaskClassConfidence = 0
		surfaceTaskClassBasis = []string{}
		dominantClass = taskClassHintUnknown
	}
	corridorLookup := corridorLookupForCluster(lookupTaskClassHint, surfaceTaskClassBasis, tasks, surfaceTaskClassConfidence, mixed)
	corridorHint := strings.TrimSpace(corridorLookup.CatalogKey)
	clusterTaskClass := ""
	clusterTaskClassSource := ""
	clusterTaskClassUpdatedAt := ""
	if dominantClass != taskClassHintUnknown && !mixed && authoredCountsByClass[dominantClass] > 0 {
		clusterTaskClass = dominantClass
		clusterTaskClassSource = corridorSingularAuthoredSource(authoredSourcesByClass[dominantClass])
		clusterTaskClassUpdatedAt = corridorNewestTimestamp(authoredUpdatedAtByClass[dominantClass])
	}
	return CorridorClusterReport{
		ProtoClusterID:      cluster.ProtoClusterID,
		ResolutionKind:      cluster.ResolutionKind,
		TaskIDs:             append([]string{}, cluster.TaskIDs...),
		SessionIDs:          append([]string{}, cluster.SessionIDs...),
		DocKeys:             append([]string{}, cluster.DocKeys...),
		ArtifactRefs:        append([]string{}, cluster.ArtifactRefs...),
		AgentIDs:            append([]string{}, cluster.AgentIDs...),
		TaskClass:           clusterTaskClass,
		TaskClassSource:     clusterTaskClassSource,
		TaskClassUpdatedAt:  clusterTaskClassUpdatedAt,
		TaskClassHint:       surfaceTaskClassHint,
		CorridorCatalogHint: corridorHint,
		CorridorLookup:      corridorLookup,
		TaskClassConfidence: surfaceTaskClassConfidence,
		CorridorReadiness:   readiness,
		ReadinessConfidence: readinessConfidence,
		TaskClassCounts:     cloneIntMap(classCounts),
		UnknownTaskCount:    unknownCount,
		MixedTaskClasses:    mixed,
		BasisStale:          basisStale,
		LastBasisEventAt:    surfaceLastBasisEventAt,
		TaskClassBasis:      surfaceTaskClassBasis,
		Metrics:             cluster.Metrics,
		Summary:             corridorClusterSummary(cluster.ProtoClusterID, lookupTaskClassHint, readiness, mixed, unknownCount, basisStale),
	}, tasks, nil
}

func (s *Store) corridorTaskClassification(ctx context.Context, taskID string, cache map[string]TaskClassHintRecord) (TaskClassHintRecord, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return TaskClassHintRecord{}, nil
	}
	if record, ok := cache[taskID]; ok {
		return record, nil
	}
	contextRecord, err := s.loadCorridorTaskContext(ctx, taskID)
	if err != nil {
		return TaskClassHintRecord{}, err
	}
	record := classifyCorridorTask(contextRecord)
	cache[taskID] = record
	return record, nil
}

func (s *Store) loadCorridorTaskContext(ctx context.Context, taskID string) (corridorTaskContext, error) {
	var record corridorTaskContext
	var tagsJSON string
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT task_id, title, description, status, task_kind, task_template, COALESCE(task_class, ''), COALESCE(task_class_source, ''), COALESCE(task_class_updated_at, ''), updated_at, COALESCE(tags_json, '[]')
		 FROM tasks
		 WHERE task_id = ?`,
		strings.TrimSpace(taskID),
	).Scan(
		&record.TaskID,
		&record.Title,
		&record.Description,
		&record.Status,
		&record.TaskKind,
		&record.TaskTemplate,
		&record.TaskClass,
		&record.TaskClassSource,
		&record.TaskClassUpdatedAt,
		&record.UpdatedAt,
		&tagsJSON,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return corridorTaskContext{}, ErrTaskNotFound
		}
		return corridorTaskContext{}, fmt.Errorf("query corridor task context: %w", err)
	}
	record.Tags = decodeCapabilities(tagsJSON)
	if record.Tags == nil {
		record.Tags = []string{}
	}
	return record, nil
}

func classifyCorridorTask(task corridorTaskContext) TaskClassHintRecord {
	scores := map[string]float64{}
	basisByClass := map[string][]string{}
	addScore := func(classHint string, score float64, basis string) {
		classHint = normalizeTaskClassHint(classHint)
		if classHint == taskClassHintUnknown || score <= 0 {
			return
		}
		scores[classHint] += score
		if trimmed := strings.TrimSpace(basis); trimmed != "" {
			basisByClass[classHint] = append(basisByClass[classHint], trimmed)
		}
	}

	hasTemplateSignal := false
	switch strings.ToLower(strings.TrimSpace(task.TaskTemplate)) {
	case model.TaskTemplateResearch:
		addScore(taskClassHintExploration, 0.85, "task_template:research")
		hasTemplateSignal = true
	case model.TaskTemplateIntegration:
		addScore(taskClassHintIntegration, 0.85, "task_template:integration")
		hasTemplateSignal = true
	case model.TaskTemplateTooling:
		addScore(taskClassHintIntegration, 0.78, "task_template:tooling")
		hasTemplateSignal = true
	case model.TaskTemplateBugfix:
		addScore(taskClassHintIncident, 0.88, "task_template:bugfix")
		hasTemplateSignal = true
	case model.TaskTemplateDeploy:
		addScore(taskClassHintIncident, 0.82, "task_template:deploy")
		hasTemplateSignal = true
	case model.TaskTemplateOps:
		addScore(taskClassHintIncident, 0.84, "task_template:ops")
		hasTemplateSignal = true
	}

	if !hasTemplateSignal {
		switch strings.ToUpper(strings.TrimSpace(task.TaskKind)) {
		case model.TaskKindCoordination:
			addScore(taskClassHintExploration, 0.04, "task_kind:coordination")
			addScore(taskClassHintProof, 0.03, "task_kind:coordination")
		case model.TaskKindExecution:
			addScore(taskClassHintIntegration, 0.03, "task_kind:execution")
			addScore(taskClassHintIncident, 0.03, "task_kind:execution")
		}
	}

	for _, tag := range normalizeStringSlice(task.Tags) {
		corridorKeywordScores(strings.ToLower(tag), 0.45, "tag:"+strings.ToLower(tag), addScore)
	}
	corridorKeywordScores(strings.ToLower(strings.TrimSpace(task.Title)), 0.45, "title", addScore)
	corridorKeywordScores(strings.ToLower(strings.TrimSpace(task.Description)), 0.20, "description", addScore)

	bestClass := taskClassHintUnknown
	bestScore := 0.0
	secondScore := 0.0
	for _, classHint := range []string{taskClassHintProof, taskClassHintExploration, taskClassHintIntegration, taskClassHintIncident} {
		score := scores[classHint]
		if score > bestScore {
			secondScore = bestScore
			bestScore = score
			bestClass = classHint
			continue
		}
		if score > secondScore {
			secondScore = score
		}
	}

	confidence := corridorClampUnit(bestScore)
	if bestScore < 0.45 || (bestScore < 0.60 && bestScore-secondScore < 0.15) {
		bestClass = taskClassHintUnknown
		confidence = 0
	}
	heuristicBasis := uniqueSortedStrings(basisByClass[bestClass])
	record := TaskClassHintRecord{
		TaskID:             strings.TrimSpace(task.TaskID),
		Title:              strings.TrimSpace(task.Title),
		Status:             strings.TrimSpace(task.Status),
		TaskKind:           strings.TrimSpace(task.TaskKind),
		TaskTemplate:       strings.TrimSpace(task.TaskTemplate),
		TaskClass:          normalizeTaskClassHint(task.TaskClass),
		TaskClassSource:    normalizeAuthoredTaskClassSource(task.TaskClassSource),
		TaskClassUpdatedAt: strings.TrimSpace(task.TaskClassUpdatedAt),
		UpdatedAt:          strings.TrimSpace(task.UpdatedAt),
		Tags:               append([]string{}, normalizeStringSlice(task.Tags)...),
		TaskClassHint:      bestClass,
		HintConfidence:     confidence,
	}
	if record.TaskClass == taskClassHintUnknown || record.TaskClassSource == "" {
		record.TaskClass = ""
		record.TaskClassSource = ""
		record.TaskClassUpdatedAt = ""
	}
	if record.TaskClass != "" {
		record.BasisUpdatedAt = record.TaskClassUpdatedAt
		record.TaskClassBasis = uniqueSortedStrings([]string{
			"task_class:" + strings.ToLower(record.TaskClass),
			"task_class_source:" + strings.ToLower(record.TaskClassSource),
		})
		record.CorridorLookup = corridorLookupForTask(task, record)
		record.CorridorHint = strings.TrimSpace(record.CorridorLookup.CatalogKey)
		if record.TaskClassHint != taskClassHintUnknown && record.TaskClassHint != record.TaskClass {
			record.Summary = fmt.Sprintf(
				"Explicit task_class %s (%s) currently drives corridor lookup; heuristic comparison still leans %s",
				strings.ToLower(record.TaskClass),
				strings.ToLower(record.TaskClassSource),
				strings.ToLower(record.TaskClassHint),
			)
		} else {
			record.Summary = fmt.Sprintf(
				"Explicit task_class %s (%s) currently drives corridor lookup",
				strings.ToLower(record.TaskClass),
				strings.ToLower(record.TaskClassSource),
			)
		}
		return record
	}
	record.TaskClassBasis = heuristicBasis
	if record.TaskClassHint != taskClassHintUnknown && len(heuristicBasis) > 0 {
		record.BasisUpdatedAt = strings.TrimSpace(task.UpdatedAt)
	}
	record.CorridorLookup = corridorLookupForTask(task, record)
	record.CorridorHint = strings.TrimSpace(record.CorridorLookup.CatalogKey)
	record.Summary = corridorTaskSummary(bestClass, confidence, heuristicBasis)
	return record
}

func corridorKeywordScores(text string, weight float64, basisPrefix string, addScore func(string, float64, string)) {
	text = strings.TrimSpace(strings.ToLower(text))
	if text == "" {
		return
	}
	check := func(classHint string, keywords []string) {
		for _, keyword := range keywords {
			if strings.Contains(text, keyword) {
				basis := basisPrefix
				if basisPrefix == "title" || basisPrefix == "description" {
					basis = basisPrefix + ":" + keyword
				}
				addScore(classHint, weight, basis)
				return
			}
		}
	}
	check(taskClassHintProof, []string{"review", "audit", "verify", "validation", "validate", "proof", "formal", "test", "acceptance", "compliance"})
	check(taskClassHintExploration, []string{"research", "explore", "exploration", "discovery", "ideation", "brainstorm", "analy", "investigat", "mapping", "compare", "option"})
	check(taskClassHintIntegration, []string{"integrat", "refactor", "migration", "migrate", "bridge", "hookup", "connect", "adapter", "protocol", "align", "wire", "tooling"})
	check(taskClassHintIncident, []string{"incident", "repair", "bugfix", "bug", "fix", "hotfix", "deploy", "rollback", "outage", "regression", "restore", "recovery", "maintenance", "ops", "runtime"})
}

func corridorReadinessRank(status string) int {
	switch strings.TrimSpace(status) {
	case corridorReadinessReady:
		return 5
	case corridorReadinessBorderline:
		return 4
	case corridorReadinessMixed:
		return 3
	case corridorReadinessStaleBasis:
		return 2
	case corridorReadinessUnderEvidenced:
		return 1
	default:
		return 0
	}
}

func corridorDominantClass(counts map[string]int) (string, int) {
	bestClass := ""
	bestCount := 0
	for _, classHint := range []string{taskClassHintProof, taskClassHintExploration, taskClassHintIntegration, taskClassHintIncident} {
		count := counts[classHint]
		if count > bestCount {
			bestClass = classHint
			bestCount = count
		}
	}
	return bestClass, bestCount
}

func corridorTotalCount(counts map[string]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

func corridorBasisIsStale(lastBasisEventAt, referenceEventAt string) bool {
	lastBasisEventAt = strings.TrimSpace(lastBasisEventAt)
	referenceEventAt = strings.TrimSpace(referenceEventAt)
	if lastBasisEventAt == "" || referenceEventAt == "" {
		return false
	}
	parsedBasis, err := time.Parse(time.RFC3339Nano, lastBasisEventAt)
	if err != nil {
		return false
	}
	parsedReference, err := time.Parse(time.RFC3339Nano, referenceEventAt)
	if err != nil || parsedReference.Before(parsedBasis) {
		return false
	}
	return parsedReference.Sub(parsedBasis) > corridorReadinessStaleAfter
}

func corridorWorkspaceObservedAtForProtoClusters(current string, clusters []ProtoClusterReport) string {
	for _, cluster := range clusters {
		current = controlLaterTimestamp(current, strings.TrimSpace(cluster.Metrics.LastEventAt))
	}
	return current
}

func corridorWorkspaceObservedAtForReadinessClusters(current string, clusters []CorridorClusterReport) string {
	for _, cluster := range clusters {
		current = controlLaterTimestamp(current, strings.TrimSpace(cluster.Metrics.LastEventAt))
	}
	return current
}

func normalizeTaskClassHint(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case taskClassHintProof:
		return taskClassHintProof
	case taskClassHintExploration:
		return taskClassHintExploration
	case taskClassHintIntegration:
		return taskClassHintIntegration
	case taskClassHintIncident:
		return taskClassHintIncident
	default:
		return taskClassHintUnknown
	}
}

func normalizeAuthoredTaskClassSource(value string) string {
	value = strings.TrimSpace(value)
	switch strings.ToUpper(value) {
	case model.TaskClassSourceExplicit:
		return model.TaskClassSourceExplicit
	case model.TaskClassSourceTemplateDefault:
		return model.TaskClassSourceTemplateDefault
	case "", model.TaskClassSourceUnset, model.TaskClassSourceHeuristicFallback:
		return ""
	default:
		// Preserve non-empty authored provenance already stored in the DB even if
		// the current write-path enum is narrower than legacy/manual sources.
		return value
	}
}

func corridorActiveTaskClassification(task TaskClassHintRecord) (string, float64, []string, string, string) {
	if authoredClass := normalizeTaskClassHint(task.TaskClass); authoredClass != taskClassHintUnknown && normalizeAuthoredTaskClassSource(task.TaskClassSource) != "" {
		confidence := 1.0
		if strings.EqualFold(strings.TrimSpace(task.TaskClassSource), model.TaskClassSourceTemplateDefault) {
			confidence = 0.9
		}
		updatedAt := strings.TrimSpace(task.TaskClassUpdatedAt)
		return authoredClass, confidence, append([]string{}, task.TaskClassBasis...), updatedAt, normalizeAuthoredTaskClassSource(task.TaskClassSource)
	}
	return normalizeTaskClassHint(task.TaskClassHint), corridorClampUnit(task.HintConfidence), append([]string{}, task.TaskClassBasis...), strings.TrimSpace(task.BasisUpdatedAt), ""
}

func corridorSingularAuthoredSource(values []string) string {
	unique := uniqueSortedStrings(values)
	if len(unique) != 1 {
		return ""
	}
	return unique[0]
}

func corridorCatalogHint(taskClassHint string) string {
	entry, ok := corridorCatalogEntryForTaskClass(taskClassHint)
	if !ok {
		return ""
	}
	return entry.CatalogKey
}

func corridorClampUnit(value float64) float64 {
	switch {
	case value < 0:
		return 0
	case value > 1:
		return 1
	default:
		return value
	}
}

func corridorTaskSummary(taskClassHint string, confidence float64, basis []string) string {
	taskClassHint = normalizeTaskClassHint(taskClassHint)
	switch taskClassHint {
	case taskClassHintProof:
		return fmt.Sprintf("Proof/formal-reasoning hint %.0f%% from %s", confidence*100, firstNonEmpty(strings.Join(basis, ", "), "task metadata"))
	case taskClassHintExploration:
		return fmt.Sprintf("Exploration/ideation hint %.0f%% from %s", confidence*100, firstNonEmpty(strings.Join(basis, ", "), "task metadata"))
	case taskClassHintIntegration:
		return fmt.Sprintf("Integration/refactoring hint %.0f%% from %s", confidence*100, firstNonEmpty(strings.Join(basis, ", "), "task metadata"))
	case taskClassHintIncident:
		return fmt.Sprintf("Incident/repair hint %.0f%% from %s", confidence*100, firstNonEmpty(strings.Join(basis, ", "), "task metadata"))
	default:
		return "Not enough task metadata to classify this task into a corridor basis yet"
	}
}

var corridorCatalogEntries = []CorridorCatalogEntry{
	{
		CatalogKey:             "proof",
		DisplayName:            "Proof Corridor",
		Summary:                "Validation, review, audit, acceptance, and compliance-heavy work.",
		TaskClassHint:          taskClassHintProof,
		PreferredTaskTemplates: []string{},
	},
	{
		CatalogKey:             "exploration",
		DisplayName:            "Exploration Corridor",
		Summary:                "Discovery, analysis, option-mapping, and early synthesis work.",
		TaskClassHint:          taskClassHintExploration,
		PreferredTaskTemplates: []string{model.TaskTemplateResearch},
	},
	{
		CatalogKey:             "integration",
		DisplayName:            "Integration Corridor",
		Summary:                "Bridge, migration, refactor, tooling, and protocol-alignment work.",
		TaskClassHint:          taskClassHintIntegration,
		PreferredTaskTemplates: []string{model.TaskTemplateIntegration, model.TaskTemplateTooling},
	},
	{
		CatalogKey:             "incident",
		DisplayName:            "Incident Corridor",
		Summary:                "Bugfix, deploy, rollback, ops, and runtime repair work.",
		TaskClassHint:          taskClassHintIncident,
		PreferredTaskTemplates: []string{model.TaskTemplateBugfix, model.TaskTemplateDeploy, model.TaskTemplateOps},
	},
}

func corridorCatalogEntriesCopy() []CorridorCatalogEntry {
	out := make([]CorridorCatalogEntry, 0, len(corridorCatalogEntries))
	for _, entry := range corridorCatalogEntries {
		cloned := entry
		cloned.PreferredTaskTemplates = append([]string{}, entry.PreferredTaskTemplates...)
		out = append(out, cloned)
	}
	return out
}

func corridorCatalogEntryForTaskClass(taskClassHint string) (CorridorCatalogEntry, bool) {
	normalized := normalizeTaskClassHint(taskClassHint)
	for _, entry := range corridorCatalogEntries {
		if entry.TaskClassHint == normalized {
			return entry, true
		}
	}
	return CorridorCatalogEntry{}, false
}

func corridorCatalogEntryForTemplate(taskTemplate string) (CorridorCatalogEntry, bool) {
	normalizedTemplate := strings.ToLower(strings.TrimSpace(taskTemplate))
	for _, entry := range corridorCatalogEntries {
		for _, template := range entry.PreferredTaskTemplates {
			if normalizedTemplate == strings.ToLower(strings.TrimSpace(template)) {
				return entry, true
			}
		}
	}
	return CorridorCatalogEntry{}, false
}

func corridorLookupForTask(task corridorTaskContext, record TaskClassHintRecord) CorridorLookupRecord {
	classHint := normalizeTaskClassHint(record.TaskClassHint)
	if authoredClass := normalizeTaskClassHint(record.TaskClass); authoredClass != taskClassHintUnknown && normalizeAuthoredTaskClassSource(record.TaskClassSource) != "" {
		entry, ok := corridorCatalogEntryForTaskClass(authoredClass)
		if !ok {
			return CorridorLookupRecord{
				LookupStatus: corridorLookupStatusNoMatch,
				Summary:      "No catalog entry is available for the explicit task_class evidence",
			}
		}
		lookup := CorridorLookupRecord{
			LookupStatus:    corridorLookupStatusClassMatch,
			CatalogKey:      entry.CatalogKey,
			DisplayName:     entry.DisplayName,
			MatchSource:     "task_class_authored",
			MatchConfidence: 1.0,
			MatchBasis:      append([]string{}, record.TaskClassBasis...),
			Summary:         fmt.Sprintf("%s matched from explicit task_class evidence", entry.DisplayName),
		}
		if strings.EqualFold(strings.TrimSpace(record.TaskClassSource), model.TaskClassSourceTemplateDefault) {
			lookup.MatchConfidence = 0.9
			lookup.MatchSource = "task_class_template_default"
		}
		if templateEntry, ok := corridorCatalogEntryForTemplate(task.TaskTemplate); ok && templateEntry.CatalogKey == entry.CatalogKey {
			lookup.MatchBasis = uniqueSortedStrings(append([]string{"catalog_template:" + strings.ToLower(strings.TrimSpace(task.TaskTemplate))}, lookup.MatchBasis...))
			lookup.Summary = fmt.Sprintf("%s matched from explicit task_class evidence; task_template=%s is supporting evidence only", entry.DisplayName, strings.ToLower(strings.TrimSpace(task.TaskTemplate)))
		}
		return lookup
	}
	if classHint == taskClassHintUnknown {
		return CorridorLookupRecord{
			LookupStatus: corridorLookupStatusNoMatch,
			Summary:      "No explicit corridor lookup is possible until task metadata becomes stronger",
		}
	}
	entry, ok := corridorCatalogEntryForTaskClass(classHint)
	if !ok {
		return CorridorLookupRecord{
			LookupStatus: corridorLookupStatusNoMatch,
			Summary:      "No catalog entry is available for the current task-class hint",
		}
	}
	lookup := CorridorLookupRecord{
		LookupStatus:    corridorLookupStatusClassMatch,
		CatalogKey:      entry.CatalogKey,
		DisplayName:     entry.DisplayName,
		MatchSource:     "task_class_hint",
		MatchConfidence: corridorClampUnit(record.HintConfidence),
		MatchBasis:      append([]string{}, record.TaskClassBasis...),
		Summary:         fmt.Sprintf("%s matched from task-class evidence", entry.DisplayName),
	}
	if templateEntry, ok := corridorCatalogEntryForTemplate(task.TaskTemplate); ok && templateEntry.CatalogKey == entry.CatalogKey {
		lookup.LookupStatus = corridorLookupStatusTemplateMatch
		lookup.MatchSource = "task_template"
		lookup.MatchConfidence = corridorClampUnit(corridorMaxFloat(record.HintConfidence, 0.85))
		lookup.MatchBasis = uniqueSortedStrings(append([]string{"catalog_template:" + strings.ToLower(strings.TrimSpace(task.TaskTemplate))}, lookup.MatchBasis...))
		lookup.Summary = fmt.Sprintf("%s matched directly from task_template=%s", entry.DisplayName, strings.ToLower(strings.TrimSpace(task.TaskTemplate)))
	}
	return lookup
}

func corridorLookupForCluster(taskClassHint string, taskClassBasis []string, tasks []TaskClassHintRecord, confidence float64, mixed bool) CorridorLookupRecord {
	if mixed {
		return CorridorLookupRecord{
			LookupStatus: corridorLookupStatusAmbiguous,
			Summary:      "Multiple concrete task-class hints are present, so no single corridor lookup is stable yet",
		}
	}
	if normalizeTaskClassHint(taskClassHint) == taskClassHintUnknown {
		return CorridorLookupRecord{
			LookupStatus: corridorLookupStatusNoMatch,
			Summary:      "No corridor lookup is available because the dominant task-class hint is unknown",
		}
	}
	entry, ok := corridorCatalogEntryForTaskClass(taskClassHint)
	if !ok {
		return CorridorLookupRecord{
			LookupStatus: corridorLookupStatusNoMatch,
			Summary:      "No catalog entry is available for the dominant task-class hint",
		}
	}
	lookup := CorridorLookupRecord{
		LookupStatus:    corridorLookupStatusClassMatch,
		CatalogKey:      entry.CatalogKey,
		DisplayName:     entry.DisplayName,
		MatchSource:     "dominant_task_class",
		MatchConfidence: corridorClampUnit(confidence),
		MatchBasis:      append([]string{}, taskClassBasis...),
		Summary:         fmt.Sprintf("%s is the dominant corridor lookup for this proto-cluster", entry.DisplayName),
	}
	authoredDominance := false
	for _, task := range tasks {
		if normalizeTaskClassHint(task.TaskClass) == normalizeTaskClassHint(taskClassHint) && normalizeAuthoredTaskClassSource(task.TaskClassSource) != "" {
			authoredDominance = true
			break
		}
	}
	if authoredDominance {
		lookup.MatchSource = "authored_task_class_dominant"
		lookup.MatchConfidence = corridorClampUnit(corridorMaxFloat(confidence, 0.9))
	}
	templateMatches := []string{}
	for _, task := range tasks {
		activeClass, _, _, _, _ := corridorActiveTaskClassification(task)
		if normalizeTaskClassHint(activeClass) != normalizeTaskClassHint(taskClassHint) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(task.CorridorLookup.LookupStatus), corridorLookupStatusTemplateMatch) {
			templateMatches = append(templateMatches, strings.ToLower(strings.TrimSpace(task.TaskTemplate)))
		}
	}
	if len(templateMatches) > 0 {
		if authoredDominance {
			lookup.MatchSource = "authored_task_class_with_template_support"
			lookup.Summary = fmt.Sprintf("%s is anchored by explicit task_class evidence and supported by matching task_template metadata", entry.DisplayName)
		} else {
			lookup.LookupStatus = corridorLookupStatusTemplateMatch
			lookup.MatchSource = "task_template"
			lookup.Summary = fmt.Sprintf("%s is supported by one or more exact task_template matches in the cluster", entry.DisplayName)
		}
		lookup.MatchConfidence = corridorClampUnit(corridorMaxFloat(confidence, 0.85))
		prefixed := make([]string, 0, len(templateMatches))
		for _, template := range uniqueSortedStrings(templateMatches) {
			prefixed = append(prefixed, "catalog_template:"+template)
		}
		lookup.MatchBasis = uniqueSortedStrings(append(prefixed, lookup.MatchBasis...))
	}
	return lookup
}

func corridorNewestTimestamp(values []string) string {
	var newest time.Time
	for _, value := range values {
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
		if err != nil {
			continue
		}
		if newest.IsZero() || parsed.After(newest) {
			newest = parsed
		}
	}
	if newest.IsZero() {
		return ""
	}
	return newest.UTC().Format(time.RFC3339Nano)
}

func corridorNewestTimestampForClasses(valuesByClass map[string][]string) string {
	all := make([]string, 0, len(valuesByClass))
	for _, values := range valuesByClass {
		all = append(all, values...)
	}
	return corridorNewestTimestamp(all)
}

func corridorMaxFloat(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}

func corridorClusterSummary(clusterID, taskClassHint, readiness string, mixed bool, unknownCount int, basisStale bool) string {
	taskClassLabel := strings.ToLower(strings.TrimSpace(corridorCatalogHint(taskClassHint)))
	switch strings.TrimSpace(readiness) {
	case corridorReadinessReady:
		return fmt.Sprintf("%s has an explicit %s catalog lookup backed by recent class evidence", firstNonEmpty(clusterID, "cluster"), firstNonEmpty(taskClassLabel, "task-class"))
	case corridorReadinessBorderline:
		return fmt.Sprintf("%s leans %s but still needs cleaner class evidence before the catalog lookup looks stable", firstNonEmpty(clusterID, "cluster"), firstNonEmpty(taskClassLabel, "toward a task-class"))
	case corridorReadinessMixed:
		return fmt.Sprintf("%s spans mixed task-class hints and cannot pick a single catalog lookup yet", firstNonEmpty(clusterID, "cluster"))
	case corridorReadinessStaleBasis:
		return fmt.Sprintf("%s still maps to %s, but the class-evidence basis is stale and should be refreshed", firstNonEmpty(clusterID, "cluster"), firstNonEmpty(taskClassLabel, "the current task-class"))
	default:
		if mixed {
			return fmt.Sprintf("%s has conflicting task-class hints and remains under-evidenced", firstNonEmpty(clusterID, "cluster"))
		}
		if unknownCount > 0 {
			return fmt.Sprintf("%s is missing enough task metadata to derive a catalog lookup for %d task anchors", firstNonEmpty(clusterID, "cluster"), unknownCount)
		}
		return fmt.Sprintf("%s has no reliable task-class basis yet", firstNonEmpty(clusterID, "cluster"))
	}
}

func corridorSnapshotSummary(report CorridorReadinessReport, clusters []CorridorClusterReport) string {
	if len(clusters) == 0 {
		return fmt.Sprintf("Corridor readiness snapshot for %s captured with no eligible clusters", firstNonEmpty(strings.TrimSpace(report.WorkspaceID), "workspace"))
	}
	if sourceCount := len(report.Clusters); sourceCount > len(clusters) {
		return fmt.Sprintf(
			"Corridor readiness snapshot: captured %d/%d clusters; %d ready, %d borderline, %d mixed, %d under-evidenced total",
			len(clusters),
			sourceCount,
			report.Workspace.ReadyCount,
			report.Workspace.BorderlineCount,
			report.Workspace.MixedCount,
			report.Workspace.UnderEvidencedCount,
		)
	}
	return fmt.Sprintf(
		"Corridor readiness snapshot: %d ready, %d borderline, %d mixed, %d under-evidenced",
		report.Workspace.ReadyCount,
		report.Workspace.BorderlineCount,
		report.Workspace.MixedCount,
		report.Workspace.UnderEvidencedCount,
	)
}

func corridorSnapshotEntityID(filter CorridorReadinessFilter) string {
	if clusterID := strings.TrimSpace(filter.ProtoClusterID); clusterID != "" {
		return clusterID
	}
	return strings.TrimSpace(filter.WorkspaceID)
}
