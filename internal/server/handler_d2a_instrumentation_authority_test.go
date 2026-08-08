package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationSnapshotRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, "workspace"); err != nil {
		t.Fatalf("remove workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)

	raw, err := json.Marshal(workspaceInstrumentationParams{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "dashboard",
		Limit:        50,
		ClusterLimit: 5,
	})
	if err != nil {
		t.Fatalf("marshal instrumentation snapshot params: %v", err)
	}

	result, rpcErr := h.workspaceInstrumentationSnapshot(testAuthContext(scenario.workspaceID, "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on missing authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.instrumentation.snapshot")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "cluster.metric_snapshot")
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority instrumentation reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceInstrumentationControlSnapshotRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
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
		t.Fatalf("expected tension refresh to seed control snapshot scenario, got %+v", refresh)
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
		Reason:      "confirm for stale authority snapshot reject",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	current := claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-2101")

	raw, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "dashboard",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal control snapshot params: %v", err)
	}

	result, rpcErr := h.workspaceInstrumentationControlSnapshot(testAuthContext(scenario.workspaceID, "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.instrumentation.control.snapshot")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "cluster.control_advisory_snapshot")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, scenario.workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected stale control snapshot reject to advance workspace updated_at due to reject journaling, still %q", afterUpdatedAt)
	}
}

func TestWorkspaceInstrumentationControlStateTickRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	clusterID := seedConfirmedControlStateRPCScenario(t, ctx, store, scenario)

	current := claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-2102")

	raw, err := json.Marshal(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		ActorID:        "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal control state tick params: %v", err)
	}

	result, rpcErr := h.workspaceInstrumentationControlStateTick(testAuthContext(scenario.workspaceID, "human", "dashboard"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.instrumentation.control.state.tick")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "cluster.control_state_ticked")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "cluster.control_state_stabilized")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, scenario.workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected stale control-state tick reject to advance workspace updated_at due to reject journaling, still %q", afterUpdatedAt)
	}
}

func TestWorkspaceInstrumentationUnifiedControlSnapshotRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	clusterID := seedConfirmedControlStateRPCScenario(t, ctx, store, scenario)

	current := claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for stale authority snapshot reject",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}
	if _, err := store.ReportMemoryMetrics(ctx, sqlite.MemoryMetricsReportInput{
		WorkspaceID:        scenario.workspaceID,
		AgentID:            "agent-a",
		SessionID:          scenario.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-handler-unified-control-stale",
		LookupCount:        4,
		L1HitCount:         1,
		L2HitCount:         1,
		StaleHitCount:      2,
		PromotionCount:     1,
		FlushCount:         1,
		FlushPositiveCount: 1,
	}); err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-handler-unified-governed-lineage-stale",
		WorkspaceID:       scenario.workspaceID,
		EventType:         "tests.handler.unified.governed_hint_lineage",
		EntityType:        "test_scope",
		EntityID:          clusterID,
		ActorType:         "tester",
		ActorID:           "tester",
		RootCauseID:       "RC-handler-unified-governed-stale",
		ProvenanceGroupID: "PG-handler-unified-governed-stale",
		PayloadJSON:       `{}`,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record governed hint lineage event: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-2103")

	raw, err := json.Marshal(workspaceInstrumentationUnifiedControlSnapshotParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		AgentID:        "agent-a",
		TaskID:         scenario.primaryTaskID,
		SessionID:      scenario.sessionID,
		DocKeys:        []string{scenario.runbookDocKey},
		ArtifactRefs:   []string{scenario.artifactRef},
		FrontierLimit:  2,
		ActorID:        "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal unified control snapshot params: %v", err)
	}

	result, rpcErr := h.workspaceInstrumentationUnifiedControlSnapshot(testAuthContext(scenario.workspaceID, "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.instrumentation.unified_control.snapshot")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "cluster.unified_control_advisory_snapshot")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "cluster.unified_control_effective_snapshot")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, scenario.workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected stale unified control snapshot reject to advance workspace updated_at due to reject journaling, still %q", afterUpdatedAt)
	}
}

func assertNoServerRuntimeEventsOfType(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events: %v", eventType, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events, got %+v", eventType, events)
	}
}
