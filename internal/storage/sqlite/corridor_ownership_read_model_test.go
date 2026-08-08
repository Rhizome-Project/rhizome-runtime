package sqlite

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestCorridorOwnershipStateMatrix(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	stale := time.Now().UTC().Add(-96 * time.Hour).Format(time.RFC3339Nano)

	cases := []struct {
		name              string
		cluster           CorridorClusterReport
		tasks             []TaskClassHintRecord
		wantState         string
		wantClass         string
		wantSource        string
		wantFresh         bool
		wantAuthoritative bool
		wantOwnerTaskID   string
		wantOwnerTaskIDs  []string
		wantSupporting    []string
		wantConflicting   []string
		summaryContains   string
	}{
		{
			name:    "owned explicit",
			cluster: corridorOwnershipBaseCluster("task:ws-ownership/explicit", []string{"task-explicit", "task-template", "task-derived"}, taskClassHintIncident),
			tasks: []TaskClassHintRecord{
				corridorOwnershipAuthoredTask("task-explicit", model.TaskClassIncident, model.TaskClassSourceExplicit, now),
				corridorOwnershipAuthoredTask("task-template", model.TaskClassIncident, model.TaskClassSourceTemplateDefault, now),
				corridorOwnershipDerivedTask("task-derived", model.TaskClassIncident, now),
			},
			wantState:         corridorOwnershipStateOwnedExplicit,
			wantClass:         model.TaskClassIncident,
			wantSource:        model.TaskClassSourceExplicit,
			wantFresh:         true,
			wantAuthoritative: true,
			wantOwnerTaskID:   "task-explicit",
			wantOwnerTaskIDs:  []string{"task-explicit"},
			wantSupporting:    []string{"task-derived", "task-template"},
			summaryContains:   "task-owned explicit corridor basis anchors incident",
		},
		{
			name:    "owned explicit stale",
			cluster: corridorOwnershipBaseCluster("task:ws-ownership/stale", []string{"task-stale", "task-stale-support"}, taskClassHintProof),
			tasks: []TaskClassHintRecord{
				corridorOwnershipAuthoredTask("task-stale", model.TaskClassProof, model.TaskClassSourceExplicit, stale),
				corridorOwnershipDerivedTask("task-stale-support", model.TaskClassProof, now),
			},
			wantState:         corridorOwnershipStateOwnedExplicitStale,
			wantClass:         model.TaskClassProof,
			wantSource:        model.TaskClassSourceExplicit,
			wantFresh:         false,
			wantAuthoritative: true,
			wantOwnerTaskID:   "task-stale",
			wantOwnerTaskIDs:  []string{"task-stale"},
			wantSupporting:    []string{"task-stale-support"},
			summaryContains:   "stale explicit task-owned corridor basis still anchors proof",
		},
		{
			name:    "seeded template",
			cluster: corridorOwnershipBaseCluster("task:ws-ownership/template", []string{"task-template-owner", "task-template-support"}, taskClassHintIncident),
			tasks: []TaskClassHintRecord{
				corridorOwnershipAuthoredTask("task-template-owner", model.TaskClassIncident, model.TaskClassSourceTemplateDefault, now),
				corridorOwnershipDerivedTask("task-template-support", model.TaskClassIncident, now),
			},
			wantState:         corridorOwnershipStateSeededTemplate,
			wantClass:         model.TaskClassIncident,
			wantSource:        model.TaskClassSourceTemplateDefault,
			wantFresh:         true,
			wantAuthoritative: false,
			wantOwnerTaskID:   "task-template-owner",
			wantOwnerTaskIDs:  []string{"task-template-owner"},
			wantSupporting:    []string{"task-template-support"},
			summaryContains:   "seeded template defaults currently anchor incident without authoritative ownership",
		},
		{
			name:    "derived cluster",
			cluster: corridorOwnershipBaseCluster("task:ws-ownership/derived", []string{"task-derived-owner"}, taskClassHintExploration),
			tasks: []TaskClassHintRecord{
				corridorOwnershipDerivedTask("task-derived-owner", model.TaskClassExploration, now),
			},
			wantState:         corridorOwnershipStateDerivedCluster,
			wantClass:         model.TaskClassExploration,
			wantSource:        model.TaskClassSourceHeuristicFallback,
			wantFresh:         true,
			wantAuthoritative: false,
			wantOwnerTaskID:   "task-derived-owner",
			wantOwnerTaskIDs:  []string{"task-derived-owner"},
			summaryContains:   "cluster currently leans on derived corridor basis exploration",
		},
		{
			name:    "contested explicit classes",
			cluster: corridorOwnershipBaseCluster("task:ws-ownership/contested", []string{"task-explicit-incident", "task-explicit-proof"}, taskClassHintUnknown),
			tasks: []TaskClassHintRecord{
				corridorOwnershipAuthoredTask("task-explicit-incident", model.TaskClassIncident, model.TaskClassSourceExplicit, now),
				corridorOwnershipAuthoredTask("task-explicit-proof", model.TaskClassProof, model.TaskClassSourceExplicit, now),
			},
			wantState:         corridorOwnershipStateContested,
			wantFresh:         false,
			wantAuthoritative: false,
			wantConflicting:   []string{"task-explicit-incident", "task-explicit-proof"},
			summaryContains:   "contested across multiple authored classes",
		},
		{
			name:    "unresolved",
			cluster: corridorOwnershipBaseCluster("task:ws-ownership/unresolved", []string{"task-unresolved"}, taskClassHintUnknown),
			tasks: []TaskClassHintRecord{
				corridorOwnershipUnknownTask("task-unresolved"),
			},
			wantState:         corridorOwnershipStateUnresolved,
			wantFresh:         false,
			wantAuthoritative: false,
			summaryContains:   "stable task-owned corridor basis",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			report := buildCorridorOwnershipClusterReport(tc.cluster, tc.tasks)
			digest := report.Ownership

			if digest.OwnershipState != tc.wantState {
				t.Fatalf("expected ownership state %s, got %+v", tc.wantState, digest)
			}
			if digest.BasisTaskClass != tc.wantClass || digest.BasisTaskClassSource != tc.wantSource {
				t.Fatalf("expected ownership class/source %s/%s, got %+v", tc.wantClass, tc.wantSource, digest)
			}
			if digest.BasisFresh != tc.wantFresh || digest.BasisAuthoritative != tc.wantAuthoritative {
				t.Fatalf("expected fresh=%t authoritative=%t, got %+v", tc.wantFresh, tc.wantAuthoritative, digest)
			}
			if digest.OwnerTaskID != tc.wantOwnerTaskID {
				t.Fatalf("expected owner task id %q, got %+v", tc.wantOwnerTaskID, digest)
			}
			if !corridorOwnershipEqualStrings(digest.OwnerTaskIDs, tc.wantOwnerTaskIDs) {
				t.Fatalf("expected owner task ids %v, got %+v", tc.wantOwnerTaskIDs, digest)
			}
			if !corridorOwnershipEqualStrings(digest.SupportingTaskIDs, tc.wantSupporting) {
				t.Fatalf("expected supporting task ids %v, got %+v", tc.wantSupporting, digest)
			}
			if !corridorOwnershipEqualStrings(digest.ConflictingTaskIDs, tc.wantConflicting) {
				t.Fatalf("expected conflicting task ids %v, got %+v", tc.wantConflicting, digest)
			}
			if !strings.Contains(report.Summary, tc.summaryContains) || !strings.Contains(digest.Summary, tc.summaryContains) {
				t.Fatalf("expected ownership summary to contain %q, report=%+v digest=%+v", tc.summaryContains, report, digest)
			}
		})
	}
}

func TestBuildCorridorOwnershipReportAndDetailPreserveExplicitOwner(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID    = "ws-corridor-ownership-report"
		ownerTaskID    = "task-corridor-ownership-owner"
		supportTaskID  = "task-corridor-ownership-support"
		protoClusterID = "task:" + workspaceID + "/" + ownerTaskID
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, ownerTaskID, "node-corridor-ownership-owner")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, supportTaskID, "node-corridor-ownership-support")

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE tasks
		 SET title = ?, description = ?, task_kind = ?, task_template = ?, task_class = ?, task_class_source = ?, task_class_updated_at = ?, updated_at = ?, tags_json = ?
		 WHERE task_id = ?`,
		"Repair corridor ownership",
		"Explicit task-owned incident basis should stay authoritative inside the proto-cluster.",
		model.TaskKindExecution,
		model.TaskTemplateBugfix,
		model.TaskClassIncident,
		model.TaskClassSourceExplicit,
		now,
		now,
		`["incident","ownership"]`,
		ownerTaskID,
	); err != nil {
		t.Fatalf("update owner task metadata: %v", err)
	}
	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE tasks
		 SET title = ?, description = ?, task_kind = ?, task_template = ?, updated_at = ?, tags_json = ?
		 WHERE task_id = ?`,
		"Support corridor ownership",
		"Fix the deploy regression while the explicit owner remains authoritative.",
		model.TaskKindExecution,
		model.TaskTemplateBugfix,
		now,
		`["incident","support"]`,
		supportTaskID,
	); err != nil {
		t.Fatalf("update support task metadata: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    ownerTaskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		TaskID:      ownerTaskID,
		PayloadJSON: mustJSONString(t, map[string]any{
			"task_id":  ownerTaskID,
			"task_ids": []string{ownerTaskID, supportTaskID},
		}),
	}); err != nil {
		t.Fatalf("record corridor ownership runtime event: %v", err)
	}
	if _, err := store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   protoClusterID,
		EpochID:     "epoch-corridor-ownership",
		CandidateID: "agent-a",
		TTLSeconds:  300,
	}); err != nil {
		t.Fatalf("elect corridor ownership steward: %v", err)
	}

	report, err := store.BuildCorridorOwnershipReport(ctx, CorridorOwnershipFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build corridor ownership report: %v", err)
	}
	authority, err := store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFields(t, report.TimeAuthority, authority)
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor ownership report generated_at %q to anchor to report time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if report.Workspace.TotalClusters != 1 || report.Workspace.OwnedExplicitCount != 1 || report.Workspace.ActiveStewardCount != 1 {
		t.Fatalf("expected one explicit ownership cluster, got %+v", report.Workspace)
	}
	cluster := requireCorridorOwnershipCluster(t, report.Clusters, protoClusterID)
	if cluster.Ownership.OwnershipState != corridorOwnershipStateOwnedExplicit {
		t.Fatalf("expected explicit ownership cluster, got %+v", cluster)
	}
	if cluster.Ownership.OwnerTaskID != ownerTaskID || !reflect.DeepEqual(cluster.Ownership.OwnerTaskIDs, []string{ownerTaskID}) {
		t.Fatalf("expected explicit owner task id %s, got %+v", ownerTaskID, cluster.Ownership)
	}
	if !reflect.DeepEqual(cluster.Ownership.SupportingTaskIDs, []string{supportTaskID}) {
		t.Fatalf("expected supporting task ids [%s], got %+v", supportTaskID, cluster.Ownership)
	}
	if cluster.Ownership.BasisTaskClass != model.TaskClassIncident || cluster.Ownership.BasisTaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected incident explicit ownership digest, got %+v", cluster.Ownership)
	}
	if !cluster.Ownership.BasisAuthoritative || !cluster.Ownership.BasisFresh {
		t.Fatalf("expected explicit ownership digest to stay fresh and authoritative, got %+v", cluster.Ownership)
	}
	if cluster.Steward == nil || cluster.Steward.StewardAgentID != "agent-a" || cluster.Steward.EpochID != "epoch-corridor-ownership" || cluster.Steward.Status != "ACTIVE" {
		t.Fatalf("expected active corridor ownership steward lease, got %+v", cluster.Steward)
	}

	baseDetail, err := store.BuildCorridorClusterDetail(ctx, workspaceID, protoClusterID)
	if err != nil {
		t.Fatalf("build base corridor cluster detail: %v", err)
	}
	if cluster.TaskClassHint != baseDetail.Cluster.TaskClassHint || cluster.CorridorReadiness != baseDetail.Cluster.CorridorReadiness || !reflect.DeepEqual(cluster.CorridorLookup, baseDetail.Cluster.CorridorLookup) {
		t.Fatalf("expected ownership cluster to preserve upstream corridor parity, ownership=%+v corridor=%+v", cluster, baseDetail.Cluster)
	}

	detail, err := store.BuildCorridorOwnershipClusterDetail(ctx, workspaceID, protoClusterID)
	if err != nil {
		t.Fatalf("build corridor ownership detail: %v", err)
	}
	if detail.Cluster.ProtoClusterID != protoClusterID {
		t.Fatalf("expected scoped ownership detail for %s, got %+v", protoClusterID, detail)
	}
	requireSameWorkspaceTimeAuthorityFields(t, detail.TimeAuthority, authority)
	if !reflect.DeepEqual(detail.Cluster.Ownership, cluster.Ownership) {
		t.Fatalf("expected ownership detail/report parity, report=%+v detail=%+v", cluster.Ownership, detail.Cluster.Ownership)
	}
	if detail.Cluster.Steward == nil || !reflect.DeepEqual(detail.Cluster.Steward, cluster.Steward) {
		t.Fatalf("expected ownership detail/report steward parity, report=%+v detail=%+v", cluster.Steward, detail.Cluster.Steward)
	}
	if !reflect.DeepEqual(corridorOwnershipTaskIDs(detail.Tasks), []string{ownerTaskID, supportTaskID}) {
		t.Fatalf("expected ownership detail task coverage for both task anchors, got %+v", detail.Tasks)
	}

	scopedReport, err := store.BuildCorridorOwnershipReport(ctx, CorridorOwnershipFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: protoClusterID,
		Limit:          20,
	})
	if err != nil {
		t.Fatalf("build scoped corridor ownership report: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFields(t, scopedReport.TimeAuthority, authority)
	event, err := store.RecordCorridorOwnershipSnapshot(ctx, scopedReport, CorridorOwnershipSnapshotInput{
		ActorID: "dashboard",
		Limit:   20,
	})
	if err != nil {
		t.Fatalf("record corridor ownership snapshot: %v", err)
	}
	if event.EventType != "cluster.corridor_ownership_snapshot" || event.EntityType != "instrumentation_corridor_ownership" {
		t.Fatalf("unexpected corridor ownership snapshot event %+v", event)
	}
	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "cluster.corridor_ownership_snapshot",
		EntityType:  "instrumentation_corridor_ownership",
		EntityID:    protoClusterID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list persisted corridor ownership snapshots: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one persisted corridor ownership snapshot, got %+v", events)
	}
	if !strings.Contains(event.PayloadJSON, "\"generated_at\":\""+scopedReport.GeneratedAt+"\"") {
		t.Fatalf("expected corridor ownership snapshot payload to mirror report generated_at %q, got %s", scopedReport.GeneratedAt, event.PayloadJSON)
	}
	if !strings.Contains(event.PayloadJSON, "\"typed_event_type\":\"CORRIDOR_OWNERSHIP_SNAPSHOT\"") {
		t.Fatalf("expected corridor ownership snapshot payload to carry typed_event_type, got %s", event.PayloadJSON)
	}
}

func TestCorridorOwnershipAndFitReportsKeepFullClusterMetricsBeforeTruncation(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID         = "ws-corridor-ownership-limit"
		staleTaskID         = "task-corridor-ownership-stale"
		templateTaskID      = "task-corridor-ownership-template"
		staleProtoClusterID = "task:" + workspaceID + "/" + staleTaskID
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, staleTaskID, "node-corridor-ownership-stale")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, templateTaskID, "node-corridor-ownership-template")

	now := time.Now().UTC()
	staleBasis := now.Add(-96 * time.Hour).Format(time.RFC3339Nano)
	freshBasis := now.Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE tasks
		 SET task_kind = ?, task_template = ?, task_class = ?, task_class_source = ?, task_class_updated_at = ?, title = ?, description = ?, updated_at = ?
		 WHERE task_id = ?`,
		model.TaskKindExecution,
		model.TaskTemplateBugfix,
		model.TaskClassIncident,
		model.TaskClassSourceExplicit,
		staleBasis,
		"Repair stale corridor basis",
		"Keep explicit ownership visible even when the authored basis has gone stale.",
		freshBasis,
		staleTaskID,
	); err != nil {
		t.Fatalf("update stale ownership task: %v", err)
	}
	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE tasks
		 SET task_kind = ?, task_template = ?, task_class = ?, task_class_source = ?, task_class_updated_at = ?, title = ?, description = ?, updated_at = ?
		 WHERE task_id = ?`,
		model.TaskKindExecution,
		model.TaskTemplateBugfix,
		model.TaskClassIncident,
		model.TaskClassSourceTemplateDefault,
		freshBasis,
		"Seed corridor ownership defaults",
		"Template-default incident evidence should stay visible in workspace ownership metrics.",
		freshBasis,
		templateTaskID,
	); err != nil {
		t.Fatalf("update template ownership task: %v", err)
	}

	for _, taskID := range []string{staleTaskID, templateTaskID} {
		if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "agent.update.posted",
			EntityType:  "task",
			EntityID:    taskID,
			ActorType:   "agent",
			ActorID:     "agent-a",
			AgentID:     "agent-a",
			TaskID:      taskID,
			PayloadJSON: mustJSONString(t, map[string]any{
				"task_id": taskID,
			}),
		}); err != nil {
			t.Fatalf("record corridor ownership runtime event for %s: %v", taskID, err)
		}
	}

	ownership, err := store.BuildCorridorOwnershipReport(ctx, CorridorOwnershipFilter{
		WorkspaceID: workspaceID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("build limited corridor ownership report: %v", err)
	}
	if ownership.Workspace.TotalClusters != 2 {
		t.Fatalf("expected ownership metrics to cover both clusters before truncation, got %+v", ownership.Workspace)
	}
	if ownership.Workspace.OwnedExplicitStaleCount != 1 || ownership.Workspace.SeededTemplateCount != 1 {
		t.Fatalf("expected ownership metrics to keep stale-explicit and seeded-template counts, got %+v", ownership.Workspace)
	}
	if len(ownership.Clusters) != 1 {
		t.Fatalf("expected ownership clusters to stay truncated to one item, got %+v", ownership.Clusters)
	}
	if ownership.Clusters[0].ProtoClusterID != staleProtoClusterID || ownership.Clusters[0].Ownership.OwnershipState != corridorOwnershipStateOwnedExplicitStale {
		t.Fatalf("expected ownership ranking to retain the stale explicit cluster after full-window evaluation, got %+v", ownership.Clusters[0])
	}

	fit, err := store.BuildCorridorFitReport(ctx, CorridorFitFilter{
		WorkspaceID: workspaceID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("build limited corridor fit report: %v", err)
	}
	if fit.Workspace.TotalClusters != 2 {
		t.Fatalf("expected fit metrics to cover both clusters before truncation, got %+v", fit.Workspace)
	}
	if fit.Workspace.StaleBasisCount != 1 {
		t.Fatalf("expected fit metrics to retain the stale-basis cluster, got %+v", fit.Workspace)
	}
	if len(fit.Clusters) != 1 {
		t.Fatalf("expected fit clusters to stay truncated to one item, got %+v", fit.Clusters)
	}
}

func corridorOwnershipBaseCluster(protoClusterID string, taskIDs []string, hint string) CorridorClusterReport {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	lookup := CorridorLookupRecord{
		LookupStatus: corridorLookupStatusNoMatch,
	}
	if catalogKey := corridorCatalogHint(hint); catalogKey != "" {
		lookup = CorridorLookupRecord{
			LookupStatus:    corridorLookupStatusClassMatch,
			CatalogKey:      catalogKey,
			DisplayName:     strings.Title(catalogKey) + " Corridor",
			MatchSource:     "ownership-test",
			MatchConfidence: 0.9,
		}
	}
	return CorridorClusterReport{
		ProtoClusterID:      protoClusterID,
		ResolutionKind:      "task",
		TaskIDs:             append([]string{}, taskIDs...),
		TaskClassHint:       normalizeTaskClassHint(hint),
		CorridorCatalogHint: corridorCatalogHint(hint),
		CorridorLookup:      lookup,
		CorridorReadiness:   corridorReadinessReady,
		ReadinessConfidence: 0.9,
		TaskClassCounts:     map[string]int{normalizeTaskClassHint(hint): len(taskIDs)},
		Metrics: ProtoClusterMetrics{
			LastEventAt: now,
		},
	}
}

func corridorOwnershipAuthoredTask(taskID, classHint, source, updatedAt string) TaskClassHintRecord {
	record := TaskClassHintRecord{
		TaskID:             taskID,
		TaskClass:          normalizeTaskClassHint(classHint),
		TaskClassSource:    source,
		TaskClassUpdatedAt: strings.TrimSpace(updatedAt),
		TaskClassHint:      normalizeTaskClassHint(classHint),
		HintConfidence:     1.0,
		TaskClassBasis: []string{
			"task_class:" + strings.ToLower(normalizeTaskClassHint(classHint)),
			"task_class_source:" + strings.ToLower(strings.TrimSpace(source)),
		},
		BasisUpdatedAt: strings.TrimSpace(updatedAt),
	}
	record.CorridorLookup = CorridorLookupRecord{
		LookupStatus:    corridorLookupStatusClassMatch,
		CatalogKey:      corridorCatalogHint(classHint),
		MatchSource:     "ownership-test-authored",
		MatchConfidence: 1.0,
	}
	return record
}

func corridorOwnershipDerivedTask(taskID, classHint, basisUpdatedAt string) TaskClassHintRecord {
	record := TaskClassHintRecord{
		TaskID:         taskID,
		TaskClassHint:  normalizeTaskClassHint(classHint),
		HintConfidence: 0.88,
		TaskClassBasis: []string{
			"task_template:" + strings.ToLower(strings.TrimSpace(corridorCatalogHint(classHint))),
		},
		BasisUpdatedAt: strings.TrimSpace(basisUpdatedAt),
	}
	record.CorridorLookup = CorridorLookupRecord{
		LookupStatus:    corridorLookupStatusClassMatch,
		CatalogKey:      corridorCatalogHint(classHint),
		MatchSource:     "ownership-test-derived",
		MatchConfidence: 0.88,
	}
	return record
}

func corridorOwnershipUnknownTask(taskID string) TaskClassHintRecord {
	return TaskClassHintRecord{
		TaskID:         taskID,
		TaskClassHint:  taskClassHintUnknown,
		HintConfidence: 0,
	}
}

func requireCorridorOwnershipCluster(t *testing.T, items []CorridorOwnershipClusterReport, protoClusterID string) CorridorOwnershipClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == protoClusterID {
			return item
		}
	}
	t.Fatalf("corridor ownership cluster %s not found in %+v", protoClusterID, items)
	return CorridorOwnershipClusterReport{}
}

func corridorOwnershipTaskIDs(items []TaskClassHintRecord) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.TaskID)
	}
	sort.Strings(out)
	return out
}

func corridorOwnershipEqualStrings(left, right []string) bool {
	if len(left) == 0 && len(right) == 0 {
		return true
	}
	return reflect.DeepEqual(left, right)
}
