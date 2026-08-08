package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	corridorAuthorityBasisAuthoredFresh = "AUTHORED_FRESH"
	corridorAuthorityBasisAuthoredStale = "AUTHORED_STALE"
	corridorAuthorityBasisDerivedOnly   = "DERIVED_ONLY"
	corridorAuthorityBasisNoBasis       = "NO_BASIS"
)

type CorridorAuthorityFilter struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type CorridorAuthorityTaskRecord struct {
	TaskID                   string               `json:"task_id"`
	Title                    string               `json:"title,omitempty"`
	Status                   string               `json:"status,omitempty"`
	TaskKind                 string               `json:"task_kind,omitempty"`
	TaskTemplate             string               `json:"task_template,omitempty"`
	TaskClass                string               `json:"task_class,omitempty"`
	TaskClassSource          string               `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt       string               `json:"task_class_updated_at,omitempty"`
	TaskClassHint            string               `json:"task_class_hint"`
	HintConfidence           float64              `json:"hint_confidence"`
	TaskClassBasis           []string             `json:"task_class_basis,omitempty"`
	BasisUpdatedAt           string               `json:"basis_updated_at,omitempty"`
	BasisState               string               `json:"basis_state"`
	BasisFresh               bool                 `json:"basis_fresh"`
	BasisAuthoritative       bool                 `json:"basis_authoritative"`
	AuthorityClass           string               `json:"authority_class,omitempty"`
	CorridorLookup           CorridorLookupRecord `json:"corridor_lookup,omitempty"`
	VisibleInInstrumentation bool                 `json:"visible_in_instrumentation"`
	ActiveProtoClusterIDs    []string             `json:"active_proto_cluster_ids,omitempty"`
	LastActivityAt           string               `json:"last_activity_at,omitempty"`
	Summary                  string               `json:"summary,omitempty"`
}

type CorridorAuthorityWorkspaceMetrics struct {
	TotalTasks                int            `json:"total_tasks"`
	AuthoredFreshCount        int            `json:"authored_fresh_count"`
	AuthoredStaleCount        int            `json:"authored_stale_count"`
	DerivedOnlyCount          int            `json:"derived_only_count"`
	NoBasisCount              int            `json:"no_basis_count"`
	VisibleTaskCount          int            `json:"visible_task_count"`
	InactiveAuthoredTaskCount int            `json:"inactive_authored_task_count"`
	TaskClassCounts           map[string]int `json:"task_class_counts,omitempty"`
	TaskClassSourceCounts     map[string]int `json:"task_class_source_counts,omitempty"`
	BasisStateCounts          map[string]int `json:"basis_state_counts,omitempty"`
}

type CorridorAuthorityReport struct {
	WorkspaceID   string                            `json:"workspace_id"`
	TimeAuthority WorkspaceTimeAuthority            `json:"time_authority"`
	GeneratedAt   string                            `json:"generated_at"`
	Filter        CorridorAuthorityFilter           `json:"filter"`
	Workspace     CorridorAuthorityWorkspaceMetrics `json:"workspace"`
	Tasks         []CorridorAuthorityTaskRecord     `json:"tasks,omitempty"`
}

type CorridorAuthorityTaskDetail struct {
	TimeAuthority WorkspaceTimeAuthority      `json:"time_authority"`
	Task          CorridorAuthorityTaskRecord `json:"task"`
	Clusters      []ProtoClusterReport        `json:"clusters,omitempty"`
}

func normalizeCorridorAuthorityFilter(filter CorridorAuthorityFilter) CorridorAuthorityFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	return filter
}

func (s *Store) BuildCorridorAuthorityReport(ctx context.Context, filter CorridorAuthorityFilter) (CorridorAuthorityReport, error) {
	filter = normalizeCorridorAuthorityFilter(filter)
	if filter.WorkspaceID == "" {
		return CorridorAuthorityReport{}, errors.New("workspace_id is required")
	}
	tasks, err := s.listCorridorAuthorityTaskContexts(ctx, filter.WorkspaceID, filter.TaskID)
	if err != nil {
		return CorridorAuthorityReport{}, err
	}
	clusterIndex, clusterList, err := s.corridorAuthorityClusterIndex(ctx, filter.WorkspaceID)
	if err != nil {
		return CorridorAuthorityReport{}, err
	}
	epochAnchorAt, err := s.persistedControlEpochAnchor(ctx, filter.WorkspaceID)
	if err != nil {
		return CorridorAuthorityReport{}, err
	}
	workspaceObservedAt := corridorWorkspaceObservedAtForProtoClusters(epochAnchorAt, clusterList)
	authority, err := s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return CorridorAuthorityReport{}, err
	}
	report := CorridorAuthorityReport{
		WorkspaceID:   filter.WorkspaceID,
		TimeAuthority: authority,
		GeneratedAt:   generatedAtFromWorkspaceTimeAuthority(authority),
		Filter:        filter,
		Workspace: CorridorAuthorityWorkspaceMetrics{
			TaskClassCounts:       map[string]int{},
			TaskClassSourceCounts: map[string]int{},
			BasisStateCounts:      map[string]int{},
		},
	}
	for _, task := range tasks {
		record := buildCorridorAuthorityTaskRecord(task, clusterIndex[strings.TrimSpace(task.TaskID)], workspaceObservedAt)
		report.Tasks = append(report.Tasks, record)
		report.Workspace.TotalTasks++
		report.Workspace.BasisStateCounts[record.BasisState]++
		switch record.BasisState {
		case corridorAuthorityBasisAuthoredFresh:
			report.Workspace.AuthoredFreshCount++
		case corridorAuthorityBasisAuthoredStale:
			report.Workspace.AuthoredStaleCount++
		case corridorAuthorityBasisDerivedOnly:
			report.Workspace.DerivedOnlyCount++
		default:
			report.Workspace.NoBasisCount++
		}
		if record.VisibleInInstrumentation {
			report.Workspace.VisibleTaskCount++
		}
		if record.BasisAuthoritative && !record.VisibleInInstrumentation {
			report.Workspace.InactiveAuthoredTaskCount++
		}
		if record.TaskClass != "" {
			report.Workspace.TaskClassCounts[record.TaskClass]++
		}
		if record.TaskClassSource != "" {
			report.Workspace.TaskClassSourceCounts[record.TaskClassSource]++
		}
	}
	sort.Slice(report.Tasks, func(i, j int) bool {
		left := report.Tasks[i]
		right := report.Tasks[j]
		if corridorAuthorityBasisRank(left.BasisState) != corridorAuthorityBasisRank(right.BasisState) {
			return corridorAuthorityBasisRank(left.BasisState) > corridorAuthorityBasisRank(right.BasisState)
		}
		if left.BasisAuthoritative != right.BasisAuthoritative {
			return left.BasisAuthoritative
		}
		if left.BasisFresh != right.BasisFresh {
			return left.BasisFresh
		}
		if left.BasisUpdatedAt != right.BasisUpdatedAt {
			return left.BasisUpdatedAt > right.BasisUpdatedAt
		}
		if left.LastActivityAt != right.LastActivityAt {
			return left.LastActivityAt > right.LastActivityAt
		}
		return left.TaskID < right.TaskID
	})
	if filter.Limit > 0 && len(report.Tasks) > filter.Limit {
		report.Tasks = append([]CorridorAuthorityTaskRecord(nil), report.Tasks[:filter.Limit]...)
	}
	_ = clusterList
	return report, nil
}

func (s *Store) BuildCorridorAuthorityTaskDetail(ctx context.Context, workspaceID, taskID string) (CorridorAuthorityTaskDetail, error) {
	report, err := s.BuildCorridorAuthorityReport(ctx, CorridorAuthorityFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       1,
	})
	if err != nil {
		return CorridorAuthorityTaskDetail{}, err
	}
	if len(report.Tasks) == 0 {
		return CorridorAuthorityTaskDetail{}, ErrTaskNotFound
	}
	_, clusterList, err := s.corridorAuthorityClusterIndex(ctx, strings.TrimSpace(workspaceID))
	if err != nil {
		return CorridorAuthorityTaskDetail{}, err
	}
	taskClusters := make([]ProtoClusterReport, 0, 2)
	for _, cluster := range clusterList {
		for _, clusterTaskID := range cluster.TaskIDs {
			if strings.TrimSpace(clusterTaskID) == strings.TrimSpace(taskID) {
				taskClusters = append(taskClusters, cluster)
				break
			}
		}
	}
	sort.Slice(taskClusters, func(i, j int) bool {
		return taskClusters[i].ProtoClusterID < taskClusters[j].ProtoClusterID
	})
	return CorridorAuthorityTaskDetail{
		TimeAuthority: report.TimeAuthority,
		Task:          report.Tasks[0],
		Clusters:      taskClusters,
	}, nil
}

func (s *Store) listCorridorAuthorityTaskContexts(ctx context.Context, workspaceID, taskID string) ([]corridorTaskContext, error) {
	query := `SELECT task_id, title, description, status, task_kind, task_template, COALESCE(task_class, ''), COALESCE(task_class_source, ''), COALESCE(task_class_updated_at, ''), updated_at, COALESCE(tags_json, '[]')
		FROM tasks`
	args := []any{}
	clauses := []string{}
	if strings.TrimSpace(workspaceID) != "" {
		query = `SELECT t.task_id, t.title, t.description, t.status, t.task_kind, t.task_template, COALESCE(t.task_class, ''), COALESCE(t.task_class_source, ''), COALESCE(t.task_class_updated_at, ''), t.updated_at, COALESCE(t.tags_json, '[]')
			FROM tasks t
			JOIN workspace_tasks wt ON wt.task_id = t.task_id`
		clauses = append(clauses, "wt.workspace_id = ?")
		args = append(args, strings.TrimSpace(workspaceID))
	}
	if strings.TrimSpace(taskID) != "" {
		clauses = append(clauses, "t.task_id = ?")
		args = append(args, strings.TrimSpace(taskID))
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY t.updated_at DESC, t.task_id DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query corridor authority tasks: %w", err)
	}
	defer rows.Close()
	out := []corridorTaskContext{}
	for rows.Next() {
		var record corridorTaskContext
		var tagsJSON string
		if err := rows.Scan(
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
			return nil, fmt.Errorf("scan corridor authority task: %w", err)
		}
		record.Tags = decodeCapabilities(tagsJSON)
		if record.Tags == nil {
			record.Tags = []string{}
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate corridor authority tasks: %w", err)
	}
	return out, nil
}

func (s *Store) corridorAuthorityClusterIndex(ctx context.Context, workspaceID string) (map[string][]ProtoClusterReport, []ProtoClusterReport, error) {
	report, err := s.BuildInstrumentationReport(ctx, InstrumentationReportFilter{
		WorkspaceID:  strings.TrimSpace(workspaceID),
		Limit:        200,
		ClusterLimit: 1000,
	})
	if err != nil {
		return nil, nil, err
	}
	index := map[string][]ProtoClusterReport{}
	for _, cluster := range report.Clusters {
		for _, taskID := range cluster.TaskIDs {
			taskID = strings.TrimSpace(taskID)
			if taskID == "" {
				continue
			}
			index[taskID] = append(index[taskID], cluster)
		}
	}
	return index, report.Clusters, nil
}

func buildCorridorAuthorityTaskRecord(task corridorTaskContext, clusters []ProtoClusterReport, workspaceObservedAt string) CorridorAuthorityTaskRecord {
	classified := classifyCorridorTask(task)
	activeClass, _, basis, basisUpdatedAt, _ := corridorActiveTaskClassification(classified)
	authoritativeSource := normalizeAuthoritativeTaskClassSource(classified.TaskClassSource)
	activeClusterIDs := make([]string, 0, len(clusters))
	lastActivityAt := ""
	for _, cluster := range clusters {
		activeClusterIDs = append(activeClusterIDs, strings.TrimSpace(cluster.ProtoClusterID))
		lastActivityAt = corridorNewestTimestamp([]string{lastActivityAt, strings.TrimSpace(cluster.Metrics.LastEventAt)})
	}
	referenceEventAt := controlLaterTimestamp(workspaceObservedAt, lastActivityAt)
	basisState := corridorAuthorityBasisNoBasis
	basisFresh := false
	switch {
	case activeClass == taskClassHintUnknown:
		basisState = corridorAuthorityBasisNoBasis
	case authoritativeSource != "":
		if basisUpdatedAt == "" || corridorBasisIsStale(basisUpdatedAt, referenceEventAt) {
			basisState = corridorAuthorityBasisAuthoredStale
		} else {
			basisState = corridorAuthorityBasisAuthoredFresh
			basisFresh = true
		}
	default:
		basisState = corridorAuthorityBasisDerivedOnly
		basisFresh = basisUpdatedAt != "" && !corridorBasisIsStale(basisUpdatedAt, referenceEventAt)
	}
	summary := classified.Summary
	if authoritativeSource != "" && !basisFresh {
		summary = firstNonEmpty(summary, "Authored task_class evidence exists but its freshness is stale or missing.")
	}
	return CorridorAuthorityTaskRecord{
		TaskID:                   strings.TrimSpace(task.TaskID),
		Title:                    strings.TrimSpace(task.Title),
		Status:                   strings.TrimSpace(task.Status),
		TaskKind:                 strings.TrimSpace(task.TaskKind),
		TaskTemplate:             strings.TrimSpace(task.TaskTemplate),
		TaskClass:                strings.TrimSpace(classified.TaskClass),
		TaskClassSource:          strings.TrimSpace(classified.TaskClassSource),
		TaskClassUpdatedAt:       strings.TrimSpace(classified.TaskClassUpdatedAt),
		TaskClassHint:            strings.TrimSpace(classified.TaskClassHint),
		HintConfidence:           classified.HintConfidence,
		TaskClassBasis:           append([]string{}, basis...),
		BasisUpdatedAt:           strings.TrimSpace(basisUpdatedAt),
		BasisState:               basisState,
		BasisFresh:               basisFresh,
		BasisAuthoritative:       authoritativeSource != "",
		AuthorityClass:           authoritativeClassForTask(classified),
		CorridorLookup:           classified.CorridorLookup,
		VisibleInInstrumentation: len(activeClusterIDs) > 0,
		ActiveProtoClusterIDs:    uniqueSortedStrings(activeClusterIDs),
		LastActivityAt:           lastActivityAt,
		Summary:                  summary,
	}
}

func corridorAuthorityBasisRank(state string) int {
	switch strings.TrimSpace(state) {
	case corridorAuthorityBasisAuthoredFresh:
		return 4
	case corridorAuthorityBasisAuthoredStale:
		return 3
	case corridorAuthorityBasisDerivedOnly:
		return 2
	case corridorAuthorityBasisNoBasis:
		return 1
	default:
		return 0
	}
}

func normalizeAuthoritativeTaskClassSource(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case model.TaskClassSourceExplicit:
		return model.TaskClassSourceExplicit
	default:
		return ""
	}
}

func authoritativeClassForTask(record TaskClassHintRecord) string {
	if normalizeAuthoritativeTaskClassSource(record.TaskClassSource) == "" {
		return ""
	}
	return normalizeTaskClassHint(record.TaskClass)
}
