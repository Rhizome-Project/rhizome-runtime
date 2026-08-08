package server

import (
	"strings"
	"testing"
)

func TestDashboardIncludesCorridorReadinessSurface(t *testing.T) {
	required := []string{
		"Task-Class / Corridor Readiness",
		`id="corridor-readiness-state"`,
		`id="corridor-readiness-summary"`,
		`id="corridor-readiness-snapshot-btn"`,
		"function corridorReadinessParams(",
		"function corridorReadinessColor(",
		"function corridorTaskClassColor(",
		"corridorReadinessReportCache",
		"corridorReadinessDetailCache",
		"corridorReadinessSnapshotEventCache",
		"renderCorridorReadinessState()",
		"syncCorridorReadinessSnapshotFromRuntimeEvents()",
		"async function loadCorridorReadiness()",
		"async function showCorridorReadinessClusterDetail(",
		"async function createCorridorReadinessSnapshot()",
		"workspace.instrumentation.corridor.report",
		"workspace.instrumentation.corridor.cluster",
		"workspace.instrumentation.corridor.snapshot",
		"cluster.corridor_readiness_snapshot",
		"Task-class evidence and corridor-readiness approximation will appear once the corridor read-side report loads.",
		"Read-only approximation over task metadata and proto-cluster evidence. task_class, task_class_source, and corridor_readiness support operator inspection only; they do not assign a corridor or carry policy authority.",
		"Selected Cluster Approximation",
		"Task Class Evidence",
		"Class Source",
		"Task Class Source",
		"Readiness Approximation",
		"Lookup Basis Freshness",
		"Task-First Corridor Authority",
		"Authority Basis Freshness",
		"Corridor Catalog",
		"Lookup Approximation",
		"Stale Lookup Basis",
		"Corridor Readiness Snapshot Summary",
		"Corridor Fit",
		"Read-only corridor-fit approximation over task-class evidence, corridor catalog lookup, proto-cluster metrics, and confirmed tensions. It stays operator-facing and does not apply policy.",
		"Fit Status",
		"Fit Confidence",
		"Fit Score",
		"Catalog Range",
		"Metric Gap Breakdown",
		"Confirmed Corroborating Tensions",
		"Corridor Fit Snapshot Summary",
		"Open Latest Corridor Snapshot",
		"Latest Fit Snapshot",
		"Open Corridor Surface",
		"Latest Runtime Event",
		"Clusters without enough task metadata for corridor catalog lookup",
		"function openCorridorSurface(",
		"No dominant task-class evidence is visible yet.",
		"function corridorTaskClassValue(",
		"function corridorTaskClassSource(",
		"function corridorWorkspaceTaskClassCounts(",
		"function corridorCatalogApproximation(",
		"function corridorLookupApproximation(",
		"function corridorBasisFreshnessApproximation(",
		"function corridorAuthorityApproximation(",
		"function corridorAuthorityBasisFreshnessApproximation(",
		"function corridorSegmentEntries(",
		"Document Segments",
		"Artifact Segments",
		"Related Segments",
		"authority and freshness stay operator-facing approximations only",
		"corridorFitReportCache",
		"corridorFitDetailCache",
		"corridorFitSnapshotEventCache",
		"function corridorFitParams(",
		"function corridorFitStatusColor(",
		"function corridorFitSummaryCounts(",
		"function syncCorridorFitSnapshotFromRuntimeEvents()",
		"async function loadCorridorFit()",
		"async function showCorridorFitClusterDetail(",
		"async function createCorridorFitSnapshot()",
		"workspace.instrumentation.corridor.fit.report",
		"workspace.instrumentation.corridor.fit.cluster",
		"workspace.instrumentation.corridor.fit.snapshot",
		"cluster.corridor_fit_snapshot",
		"showProtoClusterDetail(",
		"openTensionsForProtoCluster(",
		"showRuntimeEventDetail(",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard corridor readiness surface is missing %s", needle)
		}
	}
}

func TestDashboardCorridorReadinessKeepsReadOnlyWording(t *testing.T) {
	for _, forbidden := range []string{
		"corridor assignment",
		"activate corridor",
		"apply corridor",
		"apply corridor fit",
		"automatic corridor writes",
		"workspace.policy.corridor",
	} {
		if strings.Contains(strings.ToLower(dashboardHTML), strings.ToLower(forbidden)) {
			t.Fatalf("dashboard corridor surface leaked active policy wording via %s", forbidden)
		}
	}
	if !strings.Contains(dashboardHTML, "they do not assign a corridor or carry policy authority") {
		t.Fatalf("dashboard corridor surface should explicitly disclaim corridor assignment and policy authority")
	}
	if strings.Contains(dashboardHTML, "Catalog Hint") {
		t.Fatalf("dashboard corridor surface still leaks old catalog-hint wording")
	}
	for _, forbidden := range []string{
		"Task Class Hint",
		"task class hint ",
		"No task-class hint yet",
		"task_class_hint and corridor_readiness",
		"Task-class hints and corridor-readiness approximation",
	} {
		if strings.Contains(dashboardHTML, forbidden) {
			t.Fatalf("dashboard corridor surface still leaks legacy task-class-hint wording via %s", forbidden)
		}
	}
}

func TestCorridorReadinessSchemaContractsStayReadOnly(t *testing.T) {
	report, ok := rpcMethodSchemas["workspace.instrumentation.corridor.report"]
	if !ok {
		t.Fatal("missing workspace.instrumentation.corridor.report schema")
	}
	cluster, ok := rpcMethodSchemas["workspace.instrumentation.corridor.cluster"]
	if !ok {
		t.Fatal("missing workspace.instrumentation.corridor.cluster schema")
	}
	snapshot, ok := rpcMethodSchemas["workspace.instrumentation.corridor.snapshot"]
	if !ok {
		t.Fatal("missing workspace.instrumentation.corridor.snapshot schema")
	}

	if !strings.Contains(strings.ToLower(report.Description), "read-only") || !strings.Contains(strings.ToLower(report.Description), "not policy authority") {
		t.Fatalf("expected corridor report schema to stay read-only and non-authoritative, got %+v", report)
	}
	if !strings.Contains(strings.ToLower(cluster.Description), "read-only") || !strings.Contains(strings.ToLower(cluster.Description), "separate from policy governance") {
		t.Fatalf("expected corridor cluster schema to stay read-only and separate from governance, got %+v", cluster)
	}
	if !strings.Contains(strings.ToLower(snapshot.Description), "persisted event") || !strings.Contains(strings.ToLower(snapshot.Description), "read-only corridor readiness report") {
		t.Fatalf("expected corridor snapshot schema to describe persisted-event parity, got %+v", snapshot)
	}
	if report.Params["actor_id"].Description != "Accepted for param parity; ignored by report-only reads" {
		t.Fatalf("unexpected actor_id report param contract: %+v", report.Params["actor_id"])
	}
	if cluster.Params["limit"].Description != "Accepted for param parity; ignored by cluster detail reads" {
		t.Fatalf("unexpected limit cluster param contract: %+v", cluster.Params["limit"])
	}
	if cluster.Params["actor_id"].Description != "Accepted for param parity; ignored by detail reads" {
		t.Fatalf("unexpected actor_id cluster param contract: %+v", cluster.Params["actor_id"])
	}
}
