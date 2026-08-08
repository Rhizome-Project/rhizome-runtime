package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationControlReportExposesTimeAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected tension refresh to create control advisory seed tensions, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireTensionRecordByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "confirm for control advisory read-side",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	raw, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal control report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationControlReport(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlReport rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	report := payload["report"].(sqlite.ControlReport)
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected control report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected control report generated_at %q to mirror time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if len(report.Clusters) == 0 {
		t.Fatalf("expected control report to expose at least one cluster, got %+v", report)
	}

	clusterAny, rpcErr := h.workspaceInstrumentationControlCluster(ctx, mustJSONRaw(workspaceInstrumentationControlParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: report.Clusters[0].ProtoClusterID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlCluster rpc error: %+v", rpcErr)
	}
	clusterPayload := clusterAny.(map[string]any)
	detail := clusterPayload["detail"].(sqlite.ControlClusterDetail)
	if detail.TimeAuthority.WorkspaceID != scenario.workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected control cluster detail to expose workspace time authority, got %+v", detail.TimeAuthority)
	}
}

func TestWorkspaceInstrumentationCorridorReportExposesTimeAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE tasks SET task_kind = ?, task_template = ?, title = ?, description = ?, tags_json = ? WHERE task_id = ?`,
		model.TaskKindCoordination,
		model.TaskTemplateResearch,
		"Explore instrumentation rollout",
		"Research the corridor basis for this cluster.",
		`["discovery"]`,
		scenario.primaryTaskID,
	); err != nil {
		t.Fatalf("update primary task metadata: %v", err)
	}

	raw, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal corridor report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorReport(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorReport rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	report := payload["report"].(sqlite.CorridorReadinessReport)
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if len(report.Clusters) == 0 {
		t.Fatalf("expected corridor report to expose at least one cluster, got %+v", report)
	}

	clusterAny, rpcErr := h.workspaceInstrumentationCorridorCluster(ctx, mustJSONRaw(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: report.Clusters[0].ProtoClusterID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorCluster rpc error: %+v", rpcErr)
	}
	clusterPayload := clusterAny.(map[string]any)
	detail := clusterPayload["detail"].(sqlite.CorridorClusterDetail)
	if detail.TimeAuthority.WorkspaceID != scenario.workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor cluster detail to expose workspace time authority, got %+v", detail.TimeAuthority)
	}
}

func TestWorkspaceInstrumentationControlStateReportExposesTimeAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	clusterID := seedConfirmedControlStateRPCScenario(t, ctx, store, scenario)

	raw, err := json.Marshal(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("marshal control state report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationControlStateReport(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateReport rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	report := payload["report"].(sqlite.ClusterControlStateReport)
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected control state report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected control state report generated_at %q to mirror time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}

	clusterAny, rpcErr := h.workspaceInstrumentationControlStateCluster(ctx, mustJSONRaw(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateCluster rpc error: %+v", rpcErr)
	}
	clusterPayload := clusterAny.(map[string]any)
	detail := clusterPayload["detail"].(sqlite.ClusterControlStateDetail)
	if detail.TimeAuthority.WorkspaceID != scenario.workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected control state cluster detail to expose workspace time authority, got %+v", detail.TimeAuthority)
	}
}

func TestWorkspaceMemoryCoherenceReportExposesTimeAuthority(t *testing.T) {
	store, h, ctx, workspaceID, agentID, _ := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-time-authority", "agent-handler-memory-time-authority", "coherence-doc")

	reportAny, rpcErr := callWorkspaceMemoryCoherenceReportRaw(t, h, ctx, mustJSONRaw(workspaceMemoryCoherenceReportParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryCoherenceReport rpc error: %+v", rpcErr)
	}
	report := reportAny.(sqlite.MemoryCoherenceReport)
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected memory coherence report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected memory coherence report generated_at %q to mirror time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}

	scopeAny, rpcErr := callWorkspaceMemoryCoherenceScopeRaw(t, h, ctx, mustJSONRaw(workspaceMemoryCoherenceScopeParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryCoherenceScope rpc error: %+v", rpcErr)
	}
	scope := scopeAny.(sqlite.MemoryCoherenceScopeReport)
	if scope.TimeAuthority.WorkspaceID != workspaceID || scope.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected memory coherence scope to expose workspace time authority, got %+v", scope.TimeAuthority)
	}

	// Keep one direct store read to confirm the same surface is visible below the handler.
	scopeReport, err := store.BuildMemoryCoherenceReport(context.Background(), sqlite.MemoryCoherenceReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build memory coherence report: %v", err)
	}
	if scopeReport.TimeAuthority.WorkspaceID != workspaceID || scopeReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected storage memory coherence report to expose workspace time authority, got %+v", scopeReport.TimeAuthority)
	}
	if scopeReport.GeneratedAt != scopeReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected storage memory coherence report generated_at %q to mirror time authority reference_at %q", scopeReport.GeneratedAt, scopeReport.TimeAuthority.ReferenceAt)
	}
	scopeDetail, err := store.GetMemoryCoherenceScope(context.Background(), workspaceID, agentID, "", "")
	if err != nil {
		t.Fatalf("get memory coherence scope: %v", err)
	}
	if scopeDetail.TimeAuthority.WorkspaceID != workspaceID || scopeDetail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected storage memory coherence scope to expose workspace time authority, got %+v", scopeDetail.TimeAuthority)
	}
}

func TestWorkspaceSecondaryCorridorSurfacesExposeTimeAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE tasks SET task_kind = ?, task_template = ?, title = ?, description = ?, tags_json = ? WHERE task_id = ?`,
		model.TaskKindCoordination,
		model.TaskTemplateResearch,
		"Map rollout corridor",
		"Establish corridor ownership and boundary state for this cluster.",
		`["corridor"]`,
		scenario.primaryTaskID,
	); err != nil {
		t.Fatalf("update primary task metadata: %v", err)
	}

	corridorAny, rpcErr := h.workspaceInstrumentationCorridorReport(ctx, mustJSONRaw(workspaceInstrumentationCorridorParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorReport rpc error: %+v", rpcErr)
	}
	corridorPayload := corridorAny.(map[string]any)
	corridorReport := corridorPayload["report"].(sqlite.CorridorReadinessReport)
	if len(corridorReport.Clusters) == 0 {
		t.Fatalf("expected corridor report clusters, got %+v", corridorReport)
	}
	if corridorReport.GeneratedAt != corridorReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor report generated_at %q to mirror time authority reference_at %q", corridorReport.GeneratedAt, corridorReport.TimeAuthority.ReferenceAt)
	}
	clusterID := corridorReport.Clusters[0].ProtoClusterID

	fitAny, rpcErr := h.workspaceInstrumentationCorridorFitReport(ctx, mustJSONRaw(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Limit:          10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorFitReport rpc error: %+v", rpcErr)
	}
	fitPayload := fitAny.(map[string]any)
	fitReport := fitPayload["report"].(sqlite.CorridorFitReport)
	if fitReport.TimeAuthority.WorkspaceID != scenario.workspaceID || fitReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor fit report to expose workspace time authority, got %+v", fitReport.TimeAuthority)
	}
	if fitReport.GeneratedAt != fitReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor fit report generated_at %q to mirror time authority reference_at %q", fitReport.GeneratedAt, fitReport.TimeAuthority.ReferenceAt)
	}
	fitClusterAny, rpcErr := h.workspaceInstrumentationCorridorFitCluster(ctx, mustJSONRaw(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorFitCluster rpc error: %+v", rpcErr)
	}
	fitClusterPayload := fitClusterAny.(map[string]any)
	fitDetail := fitClusterPayload["detail"].(sqlite.CorridorFitClusterDetail)
	if fitDetail.TimeAuthority.WorkspaceID != scenario.workspaceID || fitDetail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor fit cluster detail to expose workspace time authority, got %+v", fitDetail.TimeAuthority)
	}

	ownershipAny, rpcErr := h.workspaceInstrumentationCorridorOwnershipReport(ctx, mustJSONRaw(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Limit:          10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorOwnershipReport rpc error: %+v", rpcErr)
	}
	ownershipPayload := ownershipAny.(map[string]any)
	ownershipReport := ownershipPayload["report"].(sqlite.CorridorOwnershipReport)
	if ownershipReport.TimeAuthority.WorkspaceID != scenario.workspaceID || ownershipReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor ownership report to expose workspace time authority, got %+v", ownershipReport.TimeAuthority)
	}
	if ownershipReport.GeneratedAt != ownershipReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor ownership report generated_at %q to mirror time authority reference_at %q", ownershipReport.GeneratedAt, ownershipReport.TimeAuthority.ReferenceAt)
	}
	ownershipClusterAny, rpcErr := h.workspaceInstrumentationCorridorOwnershipCluster(ctx, mustJSONRaw(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorOwnershipCluster rpc error: %+v", rpcErr)
	}
	ownershipClusterPayload := ownershipClusterAny.(map[string]any)
	ownershipDetail := ownershipClusterPayload["detail"].(sqlite.CorridorOwnershipClusterDetail)
	if ownershipDetail.TimeAuthority.WorkspaceID != scenario.workspaceID || ownershipDetail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor ownership cluster detail to expose workspace time authority, got %+v", ownershipDetail.TimeAuthority)
	}

	boundaryAny, rpcErr := h.workspaceInstrumentationCorridorBoundaryReport(ctx, mustJSONRaw(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Limit:          10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorBoundaryReport rpc error: %+v", rpcErr)
	}
	boundaryPayload := boundaryAny.(map[string]any)
	boundaryReport := boundaryPayload["report"].(sqlite.CorridorBoundaryReport)
	if boundaryReport.TimeAuthority.WorkspaceID != scenario.workspaceID || boundaryReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor boundary report to expose workspace time authority, got %+v", boundaryReport.TimeAuthority)
	}
	if boundaryReport.GeneratedAt != boundaryReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor boundary report generated_at %q to mirror time authority reference_at %q", boundaryReport.GeneratedAt, boundaryReport.TimeAuthority.ReferenceAt)
	}
	boundaryClusterAny, rpcErr := h.workspaceInstrumentationCorridorBoundaryCluster(ctx, mustJSONRaw(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorBoundaryCluster rpc error: %+v", rpcErr)
	}
	boundaryClusterPayload := boundaryClusterAny.(map[string]any)
	boundaryDetail := boundaryClusterPayload["detail"].(sqlite.CorridorBoundaryClusterDetail)
	if boundaryDetail.TimeAuthority.WorkspaceID != scenario.workspaceID || boundaryDetail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor boundary cluster detail to expose workspace time authority, got %+v", boundaryDetail.TimeAuthority)
	}

	authorityAny, rpcErr := h.workspaceInstrumentationCorridorAuthorityReport(ctx, mustJSONRaw(workspaceInstrumentationCorridorAuthorityParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorAuthorityReport rpc error: %+v", rpcErr)
	}
	authorityPayload := authorityAny.(map[string]any)
	authorityReport := authorityPayload["report"].(sqlite.CorridorAuthorityReport)
	if authorityReport.TimeAuthority.WorkspaceID != scenario.workspaceID || authorityReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor authority report to expose workspace time authority, got %+v", authorityReport.TimeAuthority)
	}
	if authorityReport.GeneratedAt != authorityReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor authority report generated_at %q to mirror time authority reference_at %q", authorityReport.GeneratedAt, authorityReport.TimeAuthority.ReferenceAt)
	}
	authorityTaskAny, rpcErr := h.workspaceInstrumentationCorridorAuthorityTask(ctx, mustJSONRaw(workspaceInstrumentationCorridorAuthorityParams{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorAuthorityTask rpc error: %+v", rpcErr)
	}
	authorityTaskPayload := authorityTaskAny.(map[string]any)
	authorityDetail := authorityTaskPayload["detail"].(sqlite.CorridorAuthorityTaskDetail)
	if authorityDetail.TimeAuthority.WorkspaceID != scenario.workspaceID || authorityDetail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor authority task detail to expose workspace time authority, got %+v", authorityDetail.TimeAuthority)
	}
}

func TestWorkspaceInstrumentationCorridorOwnershipReportExposesTimeAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario, _ := prepareIncidentCorridorTimeAuthorityScenario(t, ctx, store)

	raw, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal corridor ownership report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorOwnershipReport(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorOwnershipReport rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	report := payload["report"].(sqlite.CorridorOwnershipReport)
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor ownership report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor ownership report generated_at %q to mirror time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
}

func TestWorkspaceInstrumentationCorridorFitReportExposesTimeAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario, _ := prepareIncidentCorridorTimeAuthorityScenario(t, ctx, store)

	raw, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal corridor fit report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorFitReport(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorFitReport rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	report := payload["report"].(sqlite.CorridorFitReport)
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor fit report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor fit report generated_at %q to mirror time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
}

func TestWorkspaceInstrumentationCorridorBoundaryReportExposesTimeAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario, _ := prepareIncidentCorridorTimeAuthorityScenario(t, ctx, store)

	raw, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal corridor boundary report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorBoundaryReport(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorBoundaryReport rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	report := payload["report"].(sqlite.CorridorBoundaryReport)
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor boundary report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor boundary report generated_at %q to mirror time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
}

func TestWorkspaceInstrumentationCorridorAuthorityReportExposesTimeAuthority(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-corridor-authority-time-authority"
		taskID      = "task-corridor-authority-time-authority"
	)

	createServerTaskClassWorkspace(t, ctx, store, workspaceID)
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          taskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Repair rollout",
		Description:     "Explicit task-authored class evidence should stay visible before runtime activity.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateResearch,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
	})

	raw, err := json.Marshal(workspaceInstrumentationCorridorAuthorityParams{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal corridor authority report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorAuthorityReport(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorAuthorityReport rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	report := payload["report"].(sqlite.CorridorAuthorityReport)
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected corridor authority report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor authority report generated_at %q to mirror time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
}

func prepareIncidentCorridorTimeAuthorityScenario(t *testing.T, ctx context.Context, store *sqlite.Store) (instrumentationRPCScenario, string) {
	t.Helper()

	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE tasks
		 SET task_kind = ?, task_template = ?, task_class = ?, task_class_source = ?, task_class_updated_at = ?, title = ?, description = ?, tags_json = ?
		 WHERE task_id = ?`,
		model.TaskKindExecution,
		model.TaskTemplateBugfix,
		model.TaskClassIncident,
		model.TaskClassSourceExplicit,
		now,
		"Repair failing rollout",
		"Fix the deploy regression and restore the operator path.",
		`["incident","ops"]`,
		scenario.primaryTaskID,
	); err != nil {
		t.Fatalf("update task metadata: %v", err)
	}

	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected corridor time-authority scenario to create tensions, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireTensionRecordByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for corridor time-authority rpc",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	return scenario, "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID
}

func assertJSONDoesNotContainField(t *testing.T, value any, field string) {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal value: %v", err)
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal value: %v", err)
	}
	if jsonContainsField(decoded, field) {
		t.Fatalf("unexpected %s in payload: %s", field, string(raw))
	}
}

func jsonContainsField(value any, field string) bool {
	switch typed := value.(type) {
	case map[string]any:
		if _, ok := typed[field]; ok {
			return true
		}
		for _, child := range typed {
			if jsonContainsField(child, field) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if jsonContainsField(child, field) {
				return true
			}
		}
	}
	return false
}
