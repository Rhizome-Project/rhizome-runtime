package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestBuildTensionReportCountsAndFrontierFromStoredOverlay(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tension-storage-report"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	ensureTensionOverlayTables(t, ctx, store)

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:task:" + workspaceID + "/task-a",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "task:" + workspaceID + "/task-a",
		TensionType:    "bottleneck",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Primary blocker",
		AnchorKind:     "task",
		AnchorRef:      "task-a",
		TaskIDs:        []string{"task-a"},
		AgentIDs:       []string{"agent-a"},
		BaseScore:      70,
		SurfaceScore:   95,
		EvidenceCount:  3,
		CreatedAt:      "2026-03-23T00:00:00Z",
		UpdatedAt:      "2026-03-23T00:05:00Z",
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:task:" + workspaceID + "/task-b",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "task:" + workspaceID + "/task-b",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleArchived,
		ReviewStatus:   tensionReviewPending,
		Title:          "Archived gap",
		AnchorKind:     "task",
		AnchorRef:      "task-b",
		TaskIDs:        []string{"task-b"},
		AgentIDs:       []string{"agent-b"},
		BaseScore:      15,
		SurfaceScore:   25,
		EvidenceCount:  1,
		CreatedAt:      "2026-03-23T00:01:00Z",
		UpdatedAt:      "2026-03-23T00:02:00Z",
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:doc:" + workspaceID + "/doc-1",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "workspace_doc:" + workspaceID + "/doc-1",
		TensionType:    "contradiction",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewDiscarded,
		Title:          "Dismissed contradiction",
		AnchorKind:     "doc",
		AnchorRef:      "doc-1",
		DocKeys:        []string{"doc-1"},
		AgentIDs:       []string{"agent-a"},
		BaseScore:      20,
		SurfaceScore:   40,
		EvidenceCount:  2,
		CreatedAt:      "2026-03-23T00:03:00Z",
		UpdatedAt:      "2026-03-23T00:04:00Z",
	})

	report, err := store.BuildTensionReport(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build tension report: %v", err)
	}
	if report.TotalCount != 3 || report.ActiveCount != 2 || report.ArchivedCount != 1 || report.PendingCount != 2 {
		t.Fatalf("unexpected tension report counts %+v", report)
	}
	if report.FrontierCapacity != 5 || report.FreeAgentCount != 2 {
		t.Fatalf("expected derived frontier capacity/free-agent count, got %+v", report)
	}
	if report.CountsByType["bottleneck"] != 1 || report.CountsByType["gap"] != 1 || report.CountsByType["contradiction"] != 1 {
		t.Fatalf("unexpected counts by type %+v", report.CountsByType)
	}
	if report.CountsByReviewStatus[tensionReviewPending] != 2 || report.CountsByReviewStatus[tensionReviewDiscarded] != 1 {
		t.Fatalf("unexpected counts by review status %+v", report.CountsByReviewStatus)
	}
	if len(report.Frontier) != 1 || report.Frontier[0].TensionID != "tension:task:"+workspaceID+"/task-a" {
		t.Fatalf("expected only active non-dismissed tension in frontier, got %+v", report.Frontier)
	}
	if report.Frontier[0].Kind != "atomic" || report.Frontier[0].SurfacedPriority <= 0 || report.Frontier[0].BaseImportance <= 0 {
		t.Fatalf("expected enriched frontier structural fields, got %+v", report.Frontier[0])
	}
	if report.Frontier[0].RecoveryRisk <= 0 || report.Frontier[0].ArchivePropensity < 0 {
		t.Fatalf("expected frontier to expose advisory archive/recovery fields, got %+v", report.Frontier[0])
	}
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected report time authority, got %+v", report.TimeAuthority)
	}

	filtered, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		TaskID:      "task-a",
		AgentID:     "agent-a",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions with task/agent filter: %v", err)
	}
	if len(filtered) != 1 || filtered[0].TensionID != "tension:task:"+workspaceID+"/task-a" {
		t.Fatalf("expected filtered tensions to keep only primary tension, got %+v", filtered)
	}
	if filtered[0].Kind != "atomic" || filtered[0].SurfacedPriority <= 0 || filtered[0].VisibilityScore <= 0 {
		t.Fatalf("expected list tensions to expose structural fields, got %+v", filtered[0])
	}
}

func TestListTensionFrontierUsesDerivedFreeAgentCapacity(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tension-frontier-capacity"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b", "agent-c", "agent-d")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-frontier-capacity", "node-frontier-capacity")
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-frontier-capacity", "agent-a")

	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-frontier-capacity",
		AgentID:     "agent-a",
		TaskID:      "task-frontier-capacity",
		Summary:     "keep one agent busy so frontier capacity depends on N_free",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record active session for frontier capacity: %v", err)
	}

	for idx := 0; idx < 8; idx++ {
		insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
			TensionID:      "tension:frontier:" + workspaceID + "/" + string(rune('a'+idx)),
			WorkspaceID:    workspaceID,
			ProtoClusterID: "cluster/frontier-capacity",
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			Title:          "Frontier tension",
			AnchorKind:     "task",
			AnchorRef:      "task-frontier-capacity",
			BaseScore:      50 - idx,
			SurfaceScore:   98 - idx,
			EvidenceCount:  idx + 1,
			CreatedAt:      "2026-03-24T00:00:00Z",
			UpdatedAt:      "2026-03-24T00:00:00Z",
		})
	}

	frontier, err := store.ListTensionFrontier(ctx, TensionFilter{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("list frontier with derived capacity: %v", err)
	}
	if len(frontier) != 7 {
		t.Fatalf("expected frontier size 7 from N_free-derived capacity, got %+v", frontier)
	}
	gotIDs := make(map[string]struct{}, len(frontier))
	for _, item := range frontier {
		gotIDs[item.TensionID] = struct{}{}
	}
	for _, suffix := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		id := "tension:frontier:" + workspaceID + "/" + suffix
		if _, ok := gotIDs[id]; !ok {
			t.Fatalf("expected frontier to keep top capacity-bounded tension set, missing %s in %+v", id, frontier)
		}
	}
	if _, ok := gotIDs["tension:frontier:"+workspaceID+"/h"]; ok {
		t.Fatalf("expected frontier capacity to exclude lowest-ranked tension, got %+v", frontier)
	}

	report, err := store.BuildTensionReport(ctx, TensionFilter{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("build tension report with derived frontier capacity: %v", err)
	}
	if report.FreeAgentCount != 3 || report.FrontierCapacity != 7 {
		t.Fatalf("expected report to surface N_free-derived frontier capacity, got %+v", report)
	}
	if len(report.Frontier) != 7 {
		t.Fatalf("expected report frontier to respect capacity, got %+v", report.Frontier)
	}
}

func TestTensionFrontierIncludesEmergentMetaTensionsByDefault(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tension-frontier-meta"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:task:" + workspaceID + "/task-active",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "task:" + workspaceID + "/task-active",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Active gap",
		AnchorKind:     "task",
		AnchorRef:      "task-active",
		BaseScore:      60,
		SurfaceScore:   80,
		CreatedAt:      "2026-03-24T00:00:00Z",
		UpdatedAt:      "2026-03-24T00:02:00Z",
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "ten_meta_frontier",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "cluster:" + workspaceID + "/meta",
		TensionType:    "meta-tension",
		LifecycleState: tensionLifecycleEmergent,
		ReviewStatus:   tensionReviewPending,
		Title:          "Emergent meta tension",
		AnchorKind:     "scc_condensation",
		AnchorRef:      "2_members",
		BaseScore:      75,
		SurfaceScore:   95,
		CreatedAt:      "2026-03-24T00:01:00Z",
		UpdatedAt:      "2026-03-24T00:03:00Z",
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:task:" + workspaceID + "/task-emergent",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "task:" + workspaceID + "/task-emergent",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleEmergent,
		ReviewStatus:   tensionReviewPending,
		Title:          "Emergent atomic gap",
		AnchorKind:     "task",
		AnchorRef:      "task-emergent",
		BaseScore:      90,
		SurfaceScore:   99,
		CreatedAt:      "2026-03-24T00:04:00Z",
		UpdatedAt:      "2026-03-24T00:05:00Z",
	})

	frontier, err := store.ListTensionFrontier(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list tension frontier: %v", err)
	}
	if len(frontier) != 2 {
		t.Fatalf("expected active plus emergent meta tensions on default frontier, got %+v", frontier)
	}
	if frontier[0].TensionID != "ten_meta_frontier" || frontier[1].TensionID != "tension:task:"+workspaceID+"/task-active" {
		t.Fatalf("unexpected default frontier order %+v", frontier)
	}

	activeOnly, err := store.ListTensionFrontier(ctx, TensionFilter{
		WorkspaceID:    workspaceID,
		LifecycleState: tensionLifecycleActive,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("list active-only frontier: %v", err)
	}
	if len(activeOnly) != 1 || activeOnly[0].TensionID != "tension:task:"+workspaceID+"/task-active" {
		t.Fatalf("expected explicit ACTIVE filter to exclude emergent meta tensions, got %+v", activeOnly)
	}

	metaOnly, err := store.ListTensionFrontier(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		TensionType: "meta-tension",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list meta-only frontier: %v", err)
	}
	if len(metaOnly) != 1 || metaOnly[0].TensionID != "ten_meta_frontier" {
		t.Fatalf("expected meta-only frontier to surface emergent meta tension, got %+v", metaOnly)
	}

	report, err := store.BuildTensionReport(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build tension report: %v", err)
	}
	if len(report.Frontier) != 2 || report.Frontier[0].TensionID != "ten_meta_frontier" {
		t.Fatalf("expected report frontier to include emergent meta tension, got %+v", report.Frontier)
	}
}

func TestListTensionFrontierAppliesAdvisoryPressureFieldsAndFairness(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tension-frontier-fairness"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b", "agent-c")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC()
	recent := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	stale := now.Add(-10 * 24 * time.Hour).Format(time.RFC3339Nano)

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "ten-balanced",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "cluster:" + workspaceID + "/balanced",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Balanced frontier candidate",
		AnchorKind:     "task",
		AnchorRef:      "task-balanced",
		BaseScore:      70,
		SurfaceScore:   80,
		EvidenceCount:  2,
		LastSeenAt:     recent,
		CreatedAt:      recent,
		UpdatedAt:      recent,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "ten-crowded",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "cluster:" + workspaceID + "/crowded",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Crowded frontier candidate",
		AnchorKind:     "task",
		AnchorRef:      "task-crowded",
		SessionIDs:     []string{"sess-a", "sess-b", "sess-c"},
		AgentIDs:       []string{"agent-a", "agent-b", "agent-c"},
		SegmentRefs:    []string{"workspace_doc:" + workspaceID + "/runbook#root"},
		ConstraintRefs: []string{"constraint://coordination-lock"},
		BaseScore:      70,
		SurfaceScore:   80,
		EvidenceCount:  2,
		LastSeenAt:     recent,
		CreatedAt:      recent,
		UpdatedAt:      recent,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "ten-archival",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "cluster:" + workspaceID + "/archival",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Archival frontier candidate",
		AnchorKind:     "task",
		AnchorRef:      "task-archival",
		BaseScore:      70,
		SurfaceScore:   80,
		EvidenceCount:  1,
		LastSeenAt:     stale,
		CreatedAt:      stale,
		UpdatedAt:      stale,
	})

	frontier, err := store.ListTensionFrontier(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list tension frontier with advisory pressure fields: %v", err)
	}
	if len(frontier) != 3 {
		t.Fatalf("expected three frontier items, got %+v", frontier)
	}
	if frontier[0].TensionID != "ten-balanced" || frontier[1].TensionID != "ten-archival" || frontier[2].TensionID != "ten-crowded" {
		t.Fatalf("expected fairness-aware frontier ordering, got %+v", frontier)
	}
	if frontier[0].CrowdingRatio >= frontier[2].CrowdingRatio {
		t.Fatalf("expected balanced candidate to stay less crowded than crowded candidate, got %+v", frontier)
	}
	if !frontier[2].LeaseSensitive || frontier[2].CrowdingRatio <= 0 {
		t.Fatalf("expected crowded candidate to surface lease/crowding advisory fields, got %+v", frontier[2])
	}
	if frontier[1].ArchivePropensity <= frontier[0].ArchivePropensity || frontier[1].RecoveryRisk >= frontier[0].RecoveryRisk {
		t.Fatalf("expected stale archival candidate to surface higher archive propensity and lower recovery risk, got %+v", frontier)
	}

	detail, err := store.GetTension(ctx, workspaceID, "ten-crowded")
	if err != nil {
		t.Fatalf("get crowded tension detail: %v", err)
	}
	if !detail.Tension.LeaseSensitive || detail.Tension.CrowdingRatio <= 0 || detail.Tension.RecoveryRisk <= 0 {
		t.Fatalf("expected tension detail to expose advisory pressure fields, got %+v", detail.Tension)
	}
}

func TestTensionFrontierSurfacesAdvisoryRiskFieldsWhenAvailable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tension-frontier-advisory-fields"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-rookie", "agent-busy-a", "agent-busy-b")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-lease", "node-lease")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-secondary", "node-secondary")
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-lease", "agent-rookie")

	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-lease",
		AgentID:     "agent-rookie",
		TaskID:      "task-lease",
		Summary:     "active lease-sensitive task",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record lease-sensitive session: %v", err)
	}

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:lease-sensitive",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "cluster:lease-sensitive",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Lease-sensitive crowded tension",
		AnchorKind:     "task",
		AnchorRef:      "task-lease",
		TaskIDs:        []string{"task-lease"},
		BaseScore:      82,
		SurfaceScore:   92,
		CreatedAt:      "2026-03-28T00:00:00Z",
		UpdatedAt:      "2026-03-28T00:05:00Z",
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:secondary",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "cluster:secondary",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Secondary comparison tension",
		AnchorKind:     "task",
		AnchorRef:      "task-secondary",
		TaskIDs:        []string{"task-secondary"},
		BaseScore:      60,
		SurfaceScore:   80,
		CreatedAt:      "2026-03-28T00:00:00Z",
		UpdatedAt:      "2026-03-28T00:04:00Z",
	})

	coalition, err := store.CreateCoalition(ctx, workspaceID, "tension:lease-sensitive", "lease-sensitive crowded coalition")
	if err != nil {
		t.Fatalf("create lease-sensitive coalition: %v", err)
	}
	for _, agentID := range []string{"agent-busy-a", "agent-busy-b"} {
		if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, agentID, 0.7, 0.5); err != nil {
			t.Fatalf("add coalition member %s: %v", agentID, err)
		}
	}

	frontier, err := store.ListTensionFrontier(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list tension frontier with advisory fields: %v", err)
	}
	if len(frontier) == 0 {
		t.Fatalf("expected frontier items, got %+v", frontier)
	}

	item := requireFrontierAdvisoryFields(t, frontier, "tension:lease-sensitive")
	if item.CrowdingRatio <= 0 {
		t.Fatalf("expected crowded frontier item to expose crowding_ratio, got %+v", item)
	}
	if !item.LeaseSensitive {
		t.Fatalf("expected task-bound active lease to mark frontier item as lease_sensitive, got %+v", item)
	}
	if item.ArchivePropensity < 0 || item.ArchivePropensity > 1 || item.RecoveryRisk < 0 || item.RecoveryRisk > 1 {
		t.Fatalf("expected advisory risks to stay normalized, got %+v", item)
	}
}

func TestBuildTensionReportFrontierMirrorsAdvisoryRiskFieldsWhenAvailable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tension-report-advisory-fields"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-rookie", "agent-busy")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-report", "node-report")
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-report", "agent-rookie")

	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-report",
		AgentID:     "agent-rookie",
		TaskID:      "task-report",
		Summary:     "report lease-sensitive task",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record report session: %v", err)
	}

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:report",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "cluster:report",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Report advisory tension",
		AnchorKind:     "task",
		AnchorRef:      "task-report",
		TaskIDs:        []string{"task-report"},
		BaseScore:      78,
		SurfaceScore:   88,
		CreatedAt:      "2026-03-28T00:00:00Z",
		UpdatedAt:      "2026-03-28T00:03:00Z",
	})

	coalition, err := store.CreateCoalition(ctx, workspaceID, "tension:report", "report crowded coalition")
	if err != nil {
		t.Fatalf("create report coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-busy", 0.7, 0.5); err != nil {
		t.Fatalf("add report coalition member: %v", err)
	}

	frontier, err := store.ListTensionFrontier(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list report frontier: %v", err)
	}
	report, err := store.BuildTensionReport(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build report with advisory fields: %v", err)
	}
	if len(frontier) == 0 || len(report.Frontier) == 0 {
		t.Fatalf("expected non-empty list/report frontier, list=%+v report=%+v", frontier, report.Frontier)
	}

	listItem := requireFrontierAdvisoryFields(t, frontier, "tension:report")
	reportItem := requireFrontierAdvisoryFields(t, report.Frontier, "tension:report")
	if reportItem.CrowdingRatio != listItem.CrowdingRatio ||
		reportItem.ArchivePropensity != listItem.ArchivePropensity ||
		reportItem.RecoveryRisk != listItem.RecoveryRisk ||
		reportItem.LeaseSensitive != listItem.LeaseSensitive {
		t.Fatalf("expected report frontier to mirror advisory risk fields from list frontier, list=%+v report=%+v", listItem, reportItem)
	}
}

func TestTensionFrontierPrefersLessCrowdedPeerWhenScoresMatchAndFairnessOrderingLands(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tension-frontier-fairness-order"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-busy-a", "agent-busy-b")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-crowded", "node-crowded")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-open", "node-open")

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:fairness:crowded",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "cluster:fairness",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Crowded fairness peer",
		AnchorKind:     "task",
		AnchorRef:      "task-crowded",
		TaskIDs:        []string{"task-crowded"},
		BaseScore:      85,
		SurfaceScore:   90,
		CreatedAt:      "2026-03-28T00:00:00Z",
		UpdatedAt:      "2026-03-28T00:05:00Z",
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:fairness:open",
		WorkspaceID:    workspaceID,
		ProtoClusterID: "cluster:fairness",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Open fairness peer",
		AnchorKind:     "task",
		AnchorRef:      "task-open",
		TaskIDs:        []string{"task-open"},
		BaseScore:      85,
		SurfaceScore:   90,
		CreatedAt:      "2026-03-28T00:00:00Z",
		UpdatedAt:      "2026-03-28T00:05:00Z",
	})

	coalition, err := store.CreateCoalition(ctx, workspaceID, "tension:fairness:crowded", "crowded fairness coalition")
	if err != nil {
		t.Fatalf("create fairness coalition: %v", err)
	}
	for _, agentID := range []string{"agent-busy-a", "agent-busy-b"} {
		if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, agentID, 0.7, 0.5); err != nil {
			t.Fatalf("add fairness coalition member %s: %v", agentID, err)
		}
	}

	frontier, err := store.ListTensionFrontier(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list fairness frontier: %v", err)
	}
	if len(frontier) < 2 {
		t.Fatalf("expected two frontier items, got %+v", frontier)
	}
	if frontier[0].CrowdingRatio == 0 && frontier[1].CrowdingRatio == 0 && !frontier[0].LeaseSensitive && !frontier[1].LeaseSensitive {
		t.Fatal("expected frontier fairness advisory ordering to be landed")
	}
	if frontier[0].TensionID != "tension:fairness:open" || frontier[1].TensionID != "tension:fairness:crowded" {
		t.Fatalf("expected fairness-aware ordering to prefer less crowded peer when scores match, got %+v", frontier[:2])
	}
	if frontier[0].CrowdingRatio >= frontier[1].CrowdingRatio {
		t.Fatalf("expected crowded peer to expose larger crowding_ratio, got open=%+v crowded=%+v", frontier[0], frontier[1])
	}
}

func TestTensionReportsExposeWorkspaceTimeAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "time-authority")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create tensions, got %+v", refresh)
	}
	if refresh.TimeAuthority.WorkspaceID != scenario.workspaceID || refresh.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected refresh time authority, got %+v", refresh.TimeAuthority)
	}
	if refresh.RefreshedAt != refresh.TimeAuthority.ReferenceAt {
		t.Fatalf("expected refresh refreshed_at %q to mirror authority reference_at %q", refresh.RefreshedAt, refresh.TimeAuthority.ReferenceAt)
	}

	report, err := store.BuildTensionReport(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build tension report: %v", err)
	}
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected report time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected report generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}

	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	if len(items) == 0 {
		t.Fatalf("expected tensions for scenario, got %+v", items)
	}

	detail, err := store.GetTension(ctx, scenario.workspaceID, items[0].TensionID)
	if err != nil {
		t.Fatalf("get tension: %v", err)
	}
	if detail.TimeAuthority.WorkspaceID != scenario.workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected detail time authority, got %+v", detail.TimeAuthority)
	}
}

func TestRefreshTensionsSkipsStaleClaimTaskReference(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tension-stale-claim-task"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO knowledge_claims(
		claim_id, workspace_id, claim_type, status, subject, body, summary, confidence,
		source_kind, source_id, task_id, agent_id, evidence_json, tags_json,
		lifecycle_reason, created_at, updated_at, recovery_reason
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"claim-stale-task-reference",
		workspaceID,
		"BLOCKER",
		"ACTIVE",
		"Stale project blocker",
		"This claim references an operator task from a previous reset and must not break tension refresh.",
		"stale task claim",
		0.8,
		"agent",
		"agent-a",
		"task-deleted-from-previous-deployment-run",
		"agent-a",
		"[]",
		"[]",
		"",
		now,
		now,
		"",
	); err != nil {
		t.Fatalf("insert stale task claim fixture: %v", err)
	}

	if _, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 10,
	}); err != nil {
		t.Fatalf("refresh tensions with stale claim task reference: %v", err)
	}
}

func TestRefreshTensionsKeepsStableIDsAndDedupesEvidence(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tension-refresh-stable"
		taskID      = "task-tension-refresh-stable"
		docKey      = "refresh-runbook"
		artifactID  = "artifact-refresh-stable"
		artifactRef = "artifact://refresh-stable"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-tension-refresh-stable")
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, "agent-a")

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Refresh Runbook",
		Content:     "refresh stability doc",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, WorkspaceArtifactInput{
		ArtifactID:  artifactID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Title:       "Refresh Artifact",
		ArtifactRef: artifactRef,
		Kind:        "log",
		ContentType: "text/plain",
		CreatedBy:   "agent-a",
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		UpdateType:  "status",
		Summary:     "refresh-linked update",
		PayloadJSON: `{"task_ids":["` + taskID + `"],"doc_keys":["` + docKey + `"],"artifacts":[{"ref":"` + artifactRef + `"}]}`,
	}); err != nil {
		t.Fatalf("record agent update: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-refresh-stable",
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "start refresh-backed session",
		OwnerScope:  "task/session",
		RelatedDocKeys: []string{
			docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: artifactRef},
		},
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   "sess-refresh-stable",
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "blocked for refresh stability",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "approve rollout"},
		},
		RelatedDocKeys:      []string{docKey},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{{Ref: artifactRef}},
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue from session state: %v", err)
	}

	first, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("first refresh tensions: %v", err)
	}
	if first.TimeAuthority.WorkspaceID != workspaceID || first.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected first refresh time authority, got %+v", first.TimeAuthority)
	}
	if first.RefreshedAt != first.TimeAuthority.ReferenceAt {
		t.Fatalf("expected first refresh refreshed_at %q to mirror authority reference_at %q", first.RefreshedAt, first.TimeAuthority.ReferenceAt)
	}
	if first.Report.TimeAuthority.WorkspaceID != workspaceID || first.Report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected first refresh report time authority, got %+v", first.Report.TimeAuthority)
	}
	if first.Report.GeneratedAt != first.Report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected first refresh report generated_at %q to mirror authority reference_at %q", first.Report.GeneratedAt, first.Report.TimeAuthority.ReferenceAt)
	}
	if first.CreatedCount == 0 || len(first.Report.Frontier) == 0 {
		t.Fatalf("expected first refresh to detect tensions, got %+v", first)
	}

	firstItems, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after first refresh: %v", err)
	}
	if len(firstItems) == 0 {
		t.Fatalf("expected task-backed tensions after first refresh")
	}
	firstByType := tensionRecordIDsByType(firstItems)
	primary := requireStorageTensionByType(t, firstItems, "bottleneck")

	firstDetail, err := store.GetTension(ctx, workspaceID, primary.TensionID)
	if err != nil {
		t.Fatalf("get primary tension after first refresh: %v", err)
	}
	if len(firstDetail.Evidence) == 0 {
		t.Fatalf("expected evidence after first refresh")
	}
	if firstDetail.TimeAuthority.WorkspaceID != workspaceID || firstDetail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected first detail time authority, got %+v", firstDetail.TimeAuthority)
	}
	assertUniqueStorageTensionEvidence(t, firstDetail.Evidence)

	second, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("second refresh tensions: %v", err)
	}
	if second.TimeAuthority.WorkspaceID != workspaceID || second.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected second refresh time authority, got %+v", second.TimeAuthority)
	}
	if second.RefreshedAt != second.TimeAuthority.ReferenceAt {
		t.Fatalf("expected second refresh refreshed_at %q to mirror authority reference_at %q", second.RefreshedAt, second.TimeAuthority.ReferenceAt)
	}
	if second.Report.TimeAuthority.WorkspaceID != workspaceID || second.Report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected second refresh report time authority, got %+v", second.Report.TimeAuthority)
	}
	if second.Report.GeneratedAt != second.Report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected second refresh report generated_at %q to mirror authority reference_at %q", second.Report.GeneratedAt, second.Report.TimeAuthority.ReferenceAt)
	}
	if second.CreatedCount != 0 {
		t.Fatalf("expected second refresh to avoid duplicate tension creation, got %+v", second)
	}

	secondItems, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after second refresh: %v", err)
	}
	if len(secondItems) != len(firstItems) {
		t.Fatalf("expected stable tension count across refresh, got first=%d second=%d", len(firstItems), len(secondItems))
	}
	secondByType := tensionRecordIDsByType(secondItems)
	for tensionType, tensionID := range firstByType {
		if secondByType[tensionType] != tensionID {
			t.Fatalf("expected stable tension_id for %s, got first=%s second=%s", tensionType, tensionID, secondByType[tensionType])
		}
	}

	secondDetail, err := store.GetTension(ctx, workspaceID, primary.TensionID)
	if err != nil {
		t.Fatalf("get primary tension after second refresh: %v", err)
	}
	if secondDetail.TimeAuthority.WorkspaceID != workspaceID || secondDetail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected second detail time authority, got %+v", secondDetail.TimeAuthority)
	}
	assertUniqueStorageTensionEvidence(t, secondDetail.Evidence)
	if len(secondDetail.Evidence) != len(firstDetail.Evidence) {
		t.Fatalf("expected repeated refresh to avoid duplicate evidence growth, got first=%d second=%d", len(firstDetail.Evidence), len(secondDetail.Evidence))
	}

	detectedEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tension.detected",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list detected tension runtime events: %v", err)
	}
	if len(detectedEvents) != 1 {
		t.Fatalf("expected one tension.detected event for stable tension, got %+v", detectedEvents)
	}
}

func TestRefreshTensionsUsesFixedRecentEventWindow(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tension-window"
		taskID      = "task-tension-window"
		sessionID   = "sess-tension-window"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-tension-window")
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, "agent-a")

	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "start window scenario",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record start session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "stale blocker",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "stale approval"},
		},
	}); err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	for idx := 0; idx < 205; idx++ {
		if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
			EventType:   model.SessionEventKeepalive,
			WorkspaceID: workspaceID,
			SessionID:   sessionID,
			AgentID:     "agent-a",
			TaskID:      taskID,
			Summary:     "keepalive window signal",
		}); err != nil {
			t.Fatalf("record keepalive %d: %v", idx, err)
		}
	}

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  workspaceID,
		ActorID:      "tests",
		Limit:        1000,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("refresh tensions with stale blocker: %v", err)
	}
	if refresh.CreatedCount != 0 {
		t.Fatalf("expected fixed recent-event window to suppress stale blocker detection, got %+v", refresh)
	}

	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after window refresh: %v", err)
	}
	for _, item := range items {
		if item.TensionType == "bottleneck" {
			t.Fatalf("expected stale blocker outside backend refresh window to avoid bottleneck tension, got %+v", items)
		}
	}
}

func TestRefreshTensionsAutoRetiresStaleTensionAfterThreshold(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "retire")

	first, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("initial refresh tensions: %v", err)
	}
	if first.CreatedCount == 0 {
		t.Fatalf("expected initial refresh to detect tension, got %+v", first)
	}

	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after initial refresh: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")

	if _, err := store.ResolveOperatorQueueItem(ctx, OperatorQueueResolveInput{
		WorkspaceID: scenario.workspaceID,
		QueueKey:    sessionOperatorQueueKey(scenario.sessionID, "BLOCKER"),
		Status:      "RESOLVED",
		ResolvedBy:  "tests",
		Resolution:  "clear blocker before stale refresh coverage",
	}); err != nil {
		t.Fatalf("resolve blocker queue before stale refresh coverage: %v", err)
	}

	var retiredRefresh TensionRefreshResult
	for idx := 0; idx < tensionRetireRefreshThreshold; idx++ {
		retiredRefresh, err = store.RefreshTensions(ctx, TensionRefreshInput{
			WorkspaceID:  scenario.workspaceID,
			ActorID:      "tests",
			Limit:        100,
			ClusterLimit: 20,
		})
		if err != nil {
			t.Fatalf("refresh tensions without new evidence (%d): %v", idx, err)
		}
	}
	if retiredRefresh.RetiredCount == 0 {
		t.Fatalf("expected stale tension to auto-retire after refresh threshold, got %+v", retiredRefresh)
	}

	retiredDetail, err := store.GetTension(ctx, scenario.workspaceID, primary.TensionID)
	if err != nil {
		t.Fatalf("get retired tension: %v", err)
	}
	if retiredDetail.Tension.LifecycleState != tensionLifecycleArchived || retiredDetail.Tension.ArchivedBy != "system:auto-retire" {
		t.Fatalf("expected auto-retired tension metadata, got %+v", retiredDetail.Tension)
	}

	retiredEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.archived",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list auto-retire runtime events: %v", err)
	}
	if len(retiredEvents) == 0 {
		t.Fatalf("expected auto-retire runtime event for %s", primary.TensionID)
	}
	var retiredPayload map[string]any
	if err := json.Unmarshal([]byte(retiredEvents[0].PayloadJSON), &retiredPayload); err != nil {
		t.Fatalf("decode auto-retire payload: %v", err)
	}
	if retiredPayload["reason"] != "stale_after_refresh" {
		t.Fatalf("expected stale_after_refresh reason in auto-retire payload, got %+v", retiredPayload)
	}
}

func TestArchivedTensionReemergesOnFreshEvidence(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "reemerge")

	first, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("initial refresh tensions: %v", err)
	}
	if first.CreatedCount == 0 {
		t.Fatalf("expected initial refresh to detect tension, got %+v", first)
	}

	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after initial refresh: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")

	if _, err := store.ResolveOperatorQueueItem(ctx, OperatorQueueResolveInput{
		WorkspaceID: scenario.workspaceID,
		QueueKey:    sessionOperatorQueueKey(scenario.sessionID, "BLOCKER"),
		Status:      "RESOLVED",
		ResolvedBy:  "tests",
		Resolution:  "clear blocker before re-emergence coverage",
	}); err != nil {
		t.Fatalf("resolve blocker queue before re-emergence coverage: %v", err)
	}

	var retiredRefresh TensionRefreshResult
	for idx := 0; idx < tensionRetireRefreshThreshold; idx++ {
		retiredRefresh, err = store.RefreshTensions(ctx, TensionRefreshInput{
			WorkspaceID:  scenario.workspaceID,
			ActorID:      "tests",
			Limit:        100,
			ClusterLimit: 20,
		})
		if err != nil {
			t.Fatalf("refresh tensions without new evidence (%d): %v", idx, err)
		}
	}
	if retiredRefresh.RetiredCount == 0 {
		t.Fatalf("expected tension to auto-retire before re-emergence, got %+v", retiredRefresh)
	}

	retiredDetail, err := store.GetTension(ctx, scenario.workspaceID, primary.TensionID)
	if err != nil {
		t.Fatalf("get retired tension before re-emergence: %v", err)
	}
	if retiredDetail.Tension.LifecycleState != tensionLifecycleArchived {
		t.Fatalf("expected archived tension before re-emergence, got %+v", retiredDetail.Tension)
	}

	blockedState, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "new blocker after retirement",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "renew approval"},
		},
		RelatedDocKeys:      []string{scenario.docKey},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{{Ref: scenario.artifactRef}},
	})
	if err != nil {
		t.Fatalf("record fresh blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue after re-emergence: %v", err)
	}

	reemergedRefresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("refresh tensions after new evidence: %v", err)
	}
	if reemergedRefresh.RecoveredCount == 0 {
		t.Fatalf("expected archived tension to recover after fresh evidence, got %+v", reemergedRefresh)
	}

	reemergedDetail, err := store.GetTension(ctx, scenario.workspaceID, primary.TensionID)
	if err != nil {
		t.Fatalf("get re-emerged tension: %v", err)
	}
	if reemergedDetail.Tension.TensionID != primary.TensionID {
		t.Fatalf("expected recovered tension to keep stable tension_id, got %+v", reemergedDetail.Tension)
	}
	if reemergedDetail.Tension.LifecycleState != tensionLifecycleActive || reemergedDetail.Tension.ReviewStatus != tensionReviewPending {
		t.Fatalf("expected recovered tension to return active/pending, got %+v", reemergedDetail.Tension)
	}

	recoveredEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.recovered",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list tension.recovered events: %v", err)
	}
	if len(recoveredEvents) == 0 {
		t.Fatalf("expected tension.recovered runtime event for %s", primary.TensionID)
	}
}

func TestTensionDetectorsUseTightenedThresholds(t *testing.T) {
	t.Parallel()

	t.Run("gap_requires_two_surface_events", func(t *testing.T) {
		store := NewTestStore(t)
		ctx := context.Background()

		const (
			workspaceID = "ws-tension-threshold-gap"
			taskID      = "task-tension-threshold-gap"
			docKey      = "threshold-gap-runbook"
		)

		setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
		createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-threshold-gap")

		if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      docKey,
			Title:       "Threshold Gap Runbook",
			Content:     "single surface event",
			UpdatedBy:   "developer",
		}); err != nil {
			t.Fatalf("upsert workspace doc: %v", err)
		}

		refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
			WorkspaceID:  workspaceID,
			ActorID:      "tests",
			Limit:        100,
			ClusterLimit: 20,
		})
		if err != nil {
			t.Fatalf("refresh tensions for gap threshold: %v", err)
		}
		if refresh.CreatedCount != 0 {
			t.Fatalf("expected single surface event to stay below gap threshold, got %+v", refresh)
		}
	})

	t.Run("bridge_requires_six_messages_and_three_agents", func(t *testing.T) {
		store := NewTestStore(t)
		ctx := context.Background()

		const (
			workspaceID = "ws-tension-threshold-bridge"
			taskID      = "task-tension-threshold-bridge"
			docKey      = "threshold-bridge-runbook"
			artifactRef = "artifact://threshold-bridge"
		)

		setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b", "agent-c")
		createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-threshold-bridge")
		claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, "agent-a")

		if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      docKey,
			Title:       "Bridge Runbook",
			Content:     "bridge threshold doc",
			UpdatedBy:   "developer",
		}); err != nil {
			t.Fatalf("upsert workspace doc: %v", err)
		}
		if err := store.CreateWorkspaceArtifact(ctx, WorkspaceArtifactInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			Title:       "Bridge Artifact",
			ArtifactRef: artifactRef,
			Kind:        "log",
			ContentType: "text/plain",
			CreatedBy:   "agent-a",
		}); err != nil {
			t.Fatalf("create workspace artifact: %v", err)
		}
		for _, agentID := range []string{"agent-a", "agent-b", "agent-c"} {
			sessionTaskID := ""
			if agentID == "agent-a" {
				sessionTaskID = taskID
			}
			if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
				EventType:   model.SessionEventStart,
				WorkspaceID: workspaceID,
				SessionID:   "sess-" + agentID,
				AgentID:     agentID,
				TaskID:      sessionTaskID,
				Summary:     "bridge threshold session",
				OwnerScope:  "task/session",
				RelatedDocKeys: []string{
					docKey,
				},
				RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
					{Ref: artifactRef},
				},
			}); err != nil {
				t.Fatalf("record session start for %s: %v", agentID, err)
			}
		}

		pairs := [][2]string{
			{"agent-a", "agent-b"},
			{"agent-b", "agent-a"},
			{"agent-a", "agent-c"},
			{"agent-c", "agent-a"},
			{"agent-a", "agent-c"},
		}
		for idx, pair := range pairs {
			if _, err := store.SendMessage(ctx, MessageSendInput{
				WorkspaceID: workspaceID,
				FromAgentID: pair[0],
				ToAgentID:   pair[1],
				Channel:     "ops",
				Content:     "bridge threshold message",
			}); err != nil {
				t.Fatalf("send threshold message %d: %v", idx, err)
			}
		}

		firstRefresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
			WorkspaceID:  workspaceID,
			ActorID:      "tests",
			Limit:        100,
			ClusterLimit: 20,
		})
		if err != nil {
			t.Fatalf("refresh tensions before bridge threshold: %v", err)
		}
		firstItems, err := store.ListTensions(ctx, TensionFilter{
			WorkspaceID: workspaceID,
			Limit:       20,
		})
		if err != nil {
			t.Fatalf("list tensions before bridge threshold: %v", err)
		}
		for _, item := range firstItems {
			if item.TensionType == "bridge" {
				t.Fatalf("expected five messages to stay below bridge threshold, got refresh=%+v tensions=%+v", firstRefresh, firstItems)
			}
		}

		if _, err := store.SendMessage(ctx, MessageSendInput{
			WorkspaceID: workspaceID,
			FromAgentID: "agent-c",
			ToAgentID:   "agent-a",
			Channel:     "ops",
			Content:     "bridge threshold message 6",
		}); err != nil {
			t.Fatalf("send threshold message 6: %v", err)
		}

		secondRefresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
			WorkspaceID:  workspaceID,
			ActorID:      "tests",
			Limit:        100,
			ClusterLimit: 20,
		})
		if err != nil {
			t.Fatalf("refresh tensions after bridge threshold: %v", err)
		}
		secondItems, err := store.ListTensions(ctx, TensionFilter{
			WorkspaceID: workspaceID,
			Limit:       20,
		})
		if err != nil {
			t.Fatalf("list tensions after bridge threshold: %v", err)
		}
		requireStorageTensionByType(t, secondItems, "bridge")
		if secondRefresh.CreatedCount == 0 && secondRefresh.UpdatedCount == 0 {
			t.Fatalf("expected threshold-crossing refresh to persist tension state, got %+v", secondRefresh)
		}
	})
}

func TestTensionRuntimeEventsExposeRichPayload(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-tension-runtime-payload"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	ensureTensionOverlayTables(t, ctx, store)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	err := store.EnsureGovernedTension(ctx, EnsureGovernedTensionInput{
		WorkspaceID:    workspaceID,
		TensionType:    "failure",
		ProtoClusterID: "test-cluster",
		AnchorRef:      "test-entity",
		Title:          "Pathological failure",
		Summary:        "Detected by testing",
		EvidenceRefs:   []string{"ev1", "ev2"},
	})
	if err != nil {
		t.Fatalf("ensure governed tension: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil || len(events) == 0 {
		t.Fatalf("expected tension creation runtime events: %v, events=%d", err, len(events))
	}

	var creationEvent RuntimeEventRecord
	for _, e := range events {
		if e.EventType == "tension.emerged" {
			creationEvent = e
			break
		}
	}
	if creationEvent.EventID == "" {
		t.Fatalf("expected tension.emerged event")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(creationEvent.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode emerge event payload: %v", err)
	}
	if payload["tension_id"] == "" || payload["proto_cluster_id"] == "" || payload["tension_type"] == "" {
		t.Fatalf("expected rich identity fields in emerge payload, got %+v", payload)
	}
	if payload["typed_event_type"] != "TENSION_UPDATE" {
		t.Fatalf("expected typed_event_type, got %+v", payload)
	}
	if payload["event_kind"] != "tension.emerged" {
		t.Fatalf("expected event_kind, got %+v", payload)
	}
	evidenceRefs, ok := payload["evidence_refs"].([]any)
	if !ok || len(evidenceRefs) == 0 {
		t.Fatalf("expected evidence_refs in payload, got %+v", payload)
	}
	if _, ok := payload["surface_score"].(float64); !ok {
		t.Fatalf("expected numeric surface_score in payload, got %+v", payload)
	}

	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "failure")

	_, _ = store.DiscardTension(ctx, TensionMutationInput{
		WorkspaceID: workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "discard before archive",
	})

	archived, err := store.ArchiveTension(ctx, TensionMutationInput{
		WorkspaceID: workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "retire for payload coverage",
	})
	if err != nil {
		t.Fatalf("archive tension for payload assertions: %v", err)
	}
	var archivePayload map[string]any
	if err := json.Unmarshal([]byte(archived.Event.PayloadJSON), &archivePayload); err != nil {
		t.Fatalf("decode archive event payload: %v", err)
	}
	if archivePayload["event_kind"] != "tension.archived" {
		t.Fatalf("expected archive payload event_kind, got %+v", archivePayload)
	}
	if archivePayload["reason"] != "retire for payload coverage" {
		t.Fatalf("expected archive payload reason, got %+v", archivePayload)
	}
	if archivePayload["typed_event_type"] != "TENSION_UPDATE" {
		t.Fatalf("expected typed_event_type in archive payload, got %+v", archivePayload)
	}
}

func TestGetTensionLoadsEvidenceAndRelatedRecords(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tension-storage-detail"
		taskID      = "task-tension-storage-detail"
		docKey      = "tension-runbook"
		artifactRef = "artifact://tension-detail"
	)
	queueKey := sessionOperatorQueueKey("sess-tension-detail", "BLOCKER")

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-tension-detail")
	ensureTensionOverlayTables(t, ctx, store)

	if err := store.ClaimTask(ctx, TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Summary:     "claim tension detail task",
	}); err != nil {
		t.Fatalf("claim tension detail task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, AgentSessionCreateInput{
		SessionID:   "sess-tension-detail",
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("start tension detail session: %v", err)
	}

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Tension Detail Runbook",
		Content:     "Tension detail doc",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, WorkspaceArtifactInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Title:       "Tension Detail Artifact",
		ArtifactRef: artifactRef,
		Kind:        "log",
		ContentType: "text/plain",
		CreatedBy:   "agent-a",
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-tension-detail",
		ClaimType:   "FACT",
		Subject:     "Blocked deploy requires attention",
		Body:        "This claim is linked into tension detail coverage.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	state, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   "sess-tension-detail",
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Blocked for detail coverage",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve deploy"}},
		RelatedDocKeys: []string{
			docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: artifactRef},
		},
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, state); err != nil {
		t.Fatalf("sync operator queue from session state: %v", err)
	}
	queue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey)
	if err != nil {
		t.Fatalf("get synced operator queue item: %v", err)
	}
	runtimeEvent, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "tension.updated",
		EntityType:  "tension",
		EntityID:    "tension:task:" + workspaceID + "/" + taskID,
		ActorType:   "user",
		ActorID:     "dashboard",
		PayloadJSON: `{"tension_id":"tension:task:` + workspaceID + `/` + taskID + `","workspace_id":"` + workspaceID + `","proto_cluster_id":"task:` + workspaceID + `/` + taskID + `"}`,
		CreatedAt:   "2026-03-23T00:10:00Z",
	})
	if err != nil {
		t.Fatalf("record runtime event: %v", err)
	}

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       "tension:task:" + workspaceID + "/" + taskID,
		WorkspaceID:     workspaceID,
		ProtoClusterID:  "task:" + workspaceID + "/" + taskID,
		TensionType:     "bottleneck",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewPending,
		Title:           "Blocked task tension",
		Summary:         "Queue, claim, doc, and artifact are all attached",
		AnchorKind:      "task",
		AnchorRef:       taskID,
		TaskIDs:         []string{taskID},
		SessionIDs:      []string{"sess-tension-detail"},
		DocKeys:         []string{docKey},
		ArtifactRefs:    []string{artifactRef},
		AgentIDs:        []string{"agent-a"},
		ConstraintRefs:  []string{"claim:" + claim.ClaimID, "queue:" + queue.QueueKey},
		BaseScore:       55,
		SurfaceScore:    88,
		EvidenceCount:   3,
		LastSeenEventID: runtimeEvent.EventID,
		LastSeenAt:      runtimeEvent.CreatedAt,
		CreatedAt:       "2026-03-23T00:10:00Z",
		UpdatedAt:       "2026-03-23T00:10:00Z",
	})
	insertTensionEvidenceFixture(t, ctx, store, workspaceID, "tension:task:"+workspaceID+"/"+taskID, "runtime_event", runtimeEvent.EventID, runtimeEvent.EventID, 5, "refresh event", runtimeEvent.CreatedAt)
	insertTensionEvidenceFixture(t, ctx, store, workspaceID, "tension:task:"+workspaceID+"/"+taskID, "claim", claim.ClaimID, "", 4, "linked claim", "2026-03-23T00:10:01Z")
	insertTensionEvidenceFixture(t, ctx, store, workspaceID, "tension:task:"+workspaceID+"/"+taskID, "queue", queue.QueueKey, "", 3, "linked queue", "2026-03-23T00:10:02Z")

	detail, err := store.GetTension(ctx, workspaceID, "tension:task:"+workspaceID+"/"+taskID)
	if err != nil {
		t.Fatalf("get tension detail: %v", err)
	}
	if detail.Tension.TensionID != "tension:task:"+workspaceID+"/"+taskID {
		t.Fatalf("unexpected tension detail record %+v", detail.Tension)
	}
	if len(detail.Evidence) != 3 {
		t.Fatalf("expected exact unique evidence rows, got %+v", detail.Evidence)
	}
	if len(detail.Events) != 1 || detail.Events[0].EventID != runtimeEvent.EventID {
		t.Fatalf("expected linked runtime event, got %+v", detail.Events)
	}
	if len(detail.Claims) != 1 || detail.Claims[0].ClaimID != claim.ClaimID {
		t.Fatalf("expected linked claim, got %+v", detail.Claims)
	}
	if len(detail.Queues) == 0 {
		t.Fatalf("expected linked queue, got %+v", detail.Queues)
	}
	if len(detail.Docs) != 1 || detail.Docs[0].DocKey != docKey {
		t.Fatalf("expected linked docs, got %+v", detail.Docs)
	}
	if len(detail.Artifacts) != 1 || detail.Artifacts[0].ArtifactRef != artifactRef {
		t.Fatalf("expected linked artifacts, got %+v", detail.Artifacts)
	}
	if detail.ProtoCluster == nil || detail.ProtoCluster.ProtoClusterID != "task:"+workspaceID+"/"+taskID {
		t.Fatalf("expected linked proto cluster, got %+v", detail.ProtoCluster)
	}
	if detail.TimeAuthority.WorkspaceID != workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected detail time authority, got %+v", detail.TimeAuthority)
	}
}

type tensionRecordFixture struct {
	TensionID       string
	WorkspaceID     string
	ProtoClusterID  string
	TensionType     string
	LifecycleState  string
	ReviewStatus    string
	Title           string
	Summary         string
	AnchorKind      string
	AnchorRef       string
	TaskIDs         []string
	SessionIDs      []string
	DocKeys         []string
	ArtifactRefs    []string
	SegmentRefs     []string
	AgentIDs        []string
	ConstraintRefs  []string
	BaseScore       int
	SurfaceScore    int
	EvidenceCount   int
	LastSeenEventID string
	LastSeenAt      string
	ConfirmedBy     string
	ArchivedBy      string
	DismissedReason string
	CreatedAt       string
	UpdatedAt       string
}

func ensureTensionOverlayTables(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()

	if _, err := store.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS workspace_tensions (
			tension_id TEXT PRIMARY KEY,
			workspace_id TEXT NOT NULL,
			proto_cluster_id TEXT NOT NULL,
			tension_type TEXT NOT NULL,
			lifecycle_state TEXT NOT NULL,
			review_status TEXT NOT NULL,
			title TEXT NOT NULL,
			summary TEXT NOT NULL,
			anchor_kind TEXT NOT NULL,
			anchor_ref TEXT NOT NULL,
			task_ids_json TEXT NOT NULL,
			session_ids_json TEXT NOT NULL,
			doc_keys_json TEXT NOT NULL,
			artifact_refs_json TEXT NOT NULL,
			segment_refs_json TEXT NOT NULL,
			agent_ids_json TEXT NOT NULL,
			constraint_refs_json TEXT NOT NULL,
			base_score INTEGER NOT NULL,
			surface_score INTEGER NOT NULL,
			evidence_count INTEGER NOT NULL,
			last_seen_event_id TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			confirmed_by TEXT NOT NULL,
			archived_by TEXT NOT NULL,
			dismissed_reason TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS workspace_tension_evidence (
			tension_id TEXT NOT NULL,
			workspace_id TEXT NOT NULL,
			evidence_kind TEXT NOT NULL,
			evidence_ref TEXT NOT NULL,
			event_id TEXT NOT NULL,
			weight INTEGER NOT NULL,
			summary TEXT NOT NULL,
			created_at TEXT NOT NULL
		);
	`); err != nil {
		t.Fatalf("create tension overlay tables: %v", err)
	}
}

func insertTensionRecordFixture(t *testing.T, ctx context.Context, store *Store, record tensionRecordFixture) {
	t.Helper()

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status,
			title, summary, anchor_kind, anchor_ref, task_ids_json, session_ids_json, doc_keys_json,
			artifact_refs_json, segment_refs_json, agent_ids_json, constraint_refs_json, base_score,
			surface_score, evidence_count, last_seen_event_id, last_seen_at, confirmed_by, archived_by,
			dismissed_reason, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
		mustJSONString(t, record.TaskIDs),
		mustJSONString(t, record.SessionIDs),
		mustJSONString(t, record.DocKeys),
		mustJSONString(t, record.ArtifactRefs),
		mustJSONString(t, record.SegmentRefs),
		mustJSONString(t, record.AgentIDs),
		mustJSONString(t, record.ConstraintRefs),
		record.BaseScore,
		record.SurfaceScore,
		record.EvidenceCount,
		record.LastSeenEventID,
		record.LastSeenAt,
		record.ConfirmedBy,
		record.ArchivedBy,
		record.DismissedReason,
		record.CreatedAt,
		record.UpdatedAt,
	); err != nil {
		t.Fatalf("insert tension record: %v", err)
	}
}

func insertTensionEvidenceFixture(t *testing.T, ctx context.Context, store *Store, workspaceID, tensionID, kind, ref, eventID string, weight int, summary, createdAt string) {
	t.Helper()

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO workspace_tension_evidence (
			tension_id, workspace_id, evidence_kind, evidence_ref, event_id, weight, summary, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		tensionID,
		workspaceID,
		kind,
		ref,
		eventID,
		weight,
		summary,
		createdAt,
	); err != nil {
		t.Fatalf("insert tension evidence: %v", err)
	}
}

func requireFrontierAdvisoryFields(t *testing.T, items []TensionFrontierItem, tensionID string) TensionFrontierItem {
	t.Helper()
	for _, item := range items {
		if item.TensionID != tensionID {
			continue
		}
		if item.CrowdingRatio == 0 && item.ArchivePropensity == 0 && item.RecoveryRisk == 0 && !item.LeaseSensitive {
			t.Fatal("expected tension frontier advisory fields to be hydrated")
		}
		return item
	}
	t.Fatalf("expected frontier item %s, got %+v", tensionID, items)
	return TensionFrontierItem{}
}

func mustJSONString(t *testing.T, value any) string {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return string(data)
}

func requireStorageTensionByType(t *testing.T, items []TensionRecord, tensionType string) TensionRecord {
	t.Helper()

	for _, item := range items {
		if item.TensionType == tensionType {
			return item
		}
	}
	t.Fatalf("tension with type %s not found in %+v", tensionType, items)
	return TensionRecord{}
}

func tensionRecordIDsByType(items []TensionRecord) map[string]string {
	out := make(map[string]string, len(items))
	for _, item := range items {
		out[item.TensionType] = item.TensionID
	}
	return out
}

func assertUniqueStorageTensionEvidence(t *testing.T, evidence []TensionEvidenceRecord) {
	t.Helper()

	seen := make(map[string]struct{}, len(evidence))
	for _, item := range evidence {
		key := item.EvidenceKind + "|" + item.EvidenceRef + "|" + item.EventID
		if _, ok := seen[key]; ok {
			t.Fatalf("duplicate evidence row detected for %s in %+v", key, evidence)
		}
		seen[key] = struct{}{}
	}
}

type blockedTensionScenario struct {
	workspaceID string
	taskID      string
	docKey      string
	artifactRef string
	sessionID   string
}

func seedBlockedTensionScenario(t *testing.T, ctx context.Context, store *Store, suffix string) blockedTensionScenario {
	t.Helper()

	scenario := blockedTensionScenario{
		workspaceID: "ws-tension-hygiene-" + suffix,
		taskID:      "task-tension-hygiene-" + suffix,
		docKey:      "tension-hygiene-doc-" + suffix,
		artifactRef: "artifact://tension-hygiene-" + suffix,
		sessionID:   "sess-tension-hygiene-" + suffix,
	}

	setupInstrumentationInternalWorkspace(t, ctx, store, scenario.workspaceID, "agent-a", "agent-b")
	createInstrumentationInternalTask(t, ctx, store, scenario.workspaceID, scenario.taskID, "node-tension-hygiene-"+suffix)

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.docKey,
		Title:       "Tension Hygiene Runbook",
		Content:     "tension hygiene doc",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, WorkspaceArtifactInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Title:       "Tension Hygiene Artifact",
		ArtifactRef: scenario.artifactRef,
		Kind:        "log",
		ContentType: "text/plain",
		CreatedBy:   "agent-a",
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, AgentUpdateInput{
		WorkspaceID: scenario.workspaceID,
		AgentID:     "agent-b",
		UpdateType:  "status",
		Summary:     "tension hygiene update",
		PayloadJSON: `{"task_ids":["` + scenario.taskID + `"],"doc_keys":["` + scenario.docKey + `"],"artifacts":[{"ref":"` + scenario.artifactRef + `"}]}`,
	}); err != nil {
		t.Fatalf("record agent update: %v", err)
	}
	claimInternalTaskForSessionStart(t, ctx, store, scenario.workspaceID, scenario.taskID, "agent-a")
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "start tension hygiene session",
		OwnerScope:  "task/session",
		RelatedDocKeys: []string{
			scenario.docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: scenario.artifactRef},
		},
	}); err != nil {
		t.Fatalf("record start session: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "blocked tension hygiene session",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "approve tension hygiene"},
		},
		RelatedDocKeys:      []string{scenario.docKey},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{{Ref: scenario.artifactRef}},
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue from session state: %v", err)
	}

	return scenario
}

func TestRebootStalledSessions(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-tension-reboot"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)

	taskID := "task1"
	sessionID := "sess1"
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node1")

	staleAt := time.Now().UTC().Add(-(localOwnershipReclaimGrace + time.Minute)).Format(time.RFC3339Nano)
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, "agent-a")
	_, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "start session",
		Status:      model.SessionStatusActive,
		UpdatedAt:   staleAt,
	})
	if err != nil {
		t.Fatalf("record start session: %v", err)
	}

	tensionID := "tension:test:reboot"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      tensionID,
		WorkspaceID:    workspaceID,
		ProtoClusterID: "test",
		TensionType:    "bottleneck",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Stalled session tension",
		SessionIDs:     []string{sessionID},
		SurfaceScore:   100,
	})

	rebooted, err := store.RebootStalledSessions(ctx, 100)
	if err != nil {
		t.Fatalf("reboot stalled sessions: %v", err)
	}
	if len(rebooted) != 1 || rebooted[0] != sessionID {
		t.Fatalf("expected rebooted session %s, got %v", sessionID, rebooted)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state: %v", err)
	}
	if state.Status != model.SessionStatusEnded {
		t.Fatalf("expected session status ENDED, got %s", state.Status)
	}

	tensionDetail, err := store.GetTension(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension: %v", err)
	}
	if tensionDetail.Tension.LifecycleState != tensionLifecycleDiscarded {
		t.Fatalf("expected tension to be DISCARDED by system_anti_stall, got %s", tensionDetail.Tension.LifecycleState)
	}
}

func TestRebootStalledSessionsSkipsRecentlyActiveSession(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-tension-reboot-recent-session"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)

	taskID := "task1"
	sessionID := "sess1"
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node1")

	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, "agent-a")
	_, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "recent active session",
		Status:      model.SessionStatusActive,
	})
	if err != nil {
		t.Fatalf("record start session: %v", err)
	}

	tensionID := "tension:test:reboot-recent-session"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      tensionID,
		WorkspaceID:    workspaceID,
		ProtoClusterID: "test",
		TensionType:    "bottleneck",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Fresh session tension",
		SessionIDs:     []string{sessionID},
		SurfaceScore:   100,
	})

	rebooted, err := store.RebootStalledSessions(ctx, 100)
	if err != nil {
		t.Fatalf("reboot stalled sessions: %v", err)
	}
	if len(rebooted) != 0 {
		t.Fatalf("expected fresh session to be skipped, got rebooted=%v", rebooted)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state: %v", err)
	}
	if state.Status != model.SessionStatusActive {
		t.Fatalf("expected recent session status ACTIVE, got %s", state.Status)
	}

	tensionDetail, err := store.GetTension(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension: %v", err)
	}
	if tensionDetail.Tension.LifecycleState != tensionLifecycleActive {
		t.Fatalf("expected skipped tension to remain ACTIVE, got %s", tensionDetail.Tension.LifecycleState)
	}
}

func TestRebootStalledSessionsSkipsFreshHeartbeatSession(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-tension-reboot-fresh-heartbeat"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)

	taskID := "task1"
	sessionID := "sess1"
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node1")

	staleAt := time.Now().UTC().Add(-(localOwnershipReclaimGrace + time.Minute)).Format(time.RFC3339Nano)
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, "agent-a")
	_, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "session activity is old but agent heartbeat is fresh",
		Status:      model.SessionStatusActive,
		UpdatedAt:   staleAt,
	})
	if err != nil {
		t.Fatalf("record start session: %v", err)
	}
	if err := store.TouchAgentActivity(ctx, workspaceID, "agent-a"); err != nil {
		t.Fatalf("touch agent activity: %v", err)
	}

	tensionID := "tension:test:reboot-fresh-heartbeat"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      tensionID,
		WorkspaceID:    workspaceID,
		ProtoClusterID: "test",
		TensionType:    "bottleneck",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Fresh heartbeat tension",
		SessionIDs:     []string{sessionID},
		SurfaceScore:   100,
	})

	rebooted, err := store.RebootStalledSessions(ctx, 100)
	if err != nil {
		t.Fatalf("reboot stalled sessions: %v", err)
	}
	if len(rebooted) != 0 {
		t.Fatalf("expected fresh heartbeat session to be skipped, got rebooted=%v", rebooted)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state: %v", err)
	}
	if state.Status != model.SessionStatusActive {
		t.Fatalf("expected fresh heartbeat session status ACTIVE, got %s", state.Status)
	}

	tensionDetail, err := store.GetTension(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension: %v", err)
	}
	if tensionDetail.Tension.LifecycleState != tensionLifecycleActive {
		t.Fatalf("expected skipped heartbeat tension to remain ACTIVE, got %s", tensionDetail.Tension.LifecycleState)
	}
}
