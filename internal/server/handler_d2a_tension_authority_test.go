package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceTensionConfirmRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	primary := seedServerTensionAuthoritySurface(t, ctx, store, scenario)

	beforeDetail := mustServerTensionDetail(t, ctx, store, scenario.workspaceID, primary.TensionID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
	beforeRejects := countServerAuthorityRejectEventsForTension(t, ctx, store, scenario.workspaceID)
	beforeConfirmed := countServerTensionRuntimeEvents(t, ctx, store, scenario.workspaceID, "tension.confirmed", primary.TensionID)

	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, "workspace"); err != nil {
		t.Fatalf("remove workspace authority: %v", err)
	}

	result, rpcErr := h.workspaceTensionConfirm(ctx, mustJSONRaw(workspaceTensionLifecycleParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "missing authority should fail closed",
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.tension.confirm")

	afterDetail := mustServerTensionDetail(t, ctx, store, scenario.workspaceID, primary.TensionID)
	if afterDetail.Tension.LifecycleState != beforeDetail.Tension.LifecycleState ||
		afterDetail.Tension.ReviewStatus != beforeDetail.Tension.ReviewStatus ||
		afterDetail.Tension.UpdatedAt != beforeDetail.Tension.UpdatedAt {
		t.Fatalf("expected missing-authority reject not to mutate tension, before=%+v after=%+v", beforeDetail.Tension, afterDetail.Tension)
	}
	if got := countServerTensionRuntimeEvents(t, ctx, store, scenario.workspaceID, "tension.confirmed", primary.TensionID); got != beforeConfirmed {
		t.Fatalf("expected no new tension.confirmed runtime event after authority reject, before=%d after=%d", beforeConfirmed, got)
	}
	if got := countServerAuthorityRejectEventsForTension(t, ctx, store, scenario.workspaceID); got != beforeRejects {
		t.Fatalf("expected missing-authority reject not to fabricate authority.rejected evidence, before=%d after=%d", beforeRejects, got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceTensionRefreshRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	current := claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
	beforeRejects := countServerAuthorityRejectEventsForTension(t, ctx, store, scenario.workspaceID)
	beforeDetected := countServerTensionRuntimeEventsByType(t, ctx, store, scenario.workspaceID, "tension.detected")
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-3402")

	result, rpcErr := h.workspaceTensionRefresh(ctx, mustJSONRaw(workspaceTensionRefreshParams{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "dashboard",
		Limit:        100,
		ClusterLimit: 10,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale-authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.tension.refresh")

	if got := countServerTensionRuntimeEventsByType(t, ctx, store, scenario.workspaceID, "tension.detected"); got != beforeDetected {
		t.Fatalf("expected stale-authority reject not to append tension.detected events, before=%d after=%d", beforeDetected, got)
	}
	assertServerTaskAuthorityRejectEvent(t, ctx, store, scenario.workspaceID, string(sqlite.AuthorityRejectStale))
	if got := countServerAuthorityRejectEventsForTension(t, ctx, store, scenario.workspaceID); got != beforeRejects+1 {
		t.Fatalf("expected stale refresh reject to journal one authority.rejected event, before=%d after=%d", beforeRejects, got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to stale refresh reject journaling, still %q", afterUpdatedAt)
	}
}

func TestWorkspaceTensionLifecycleUpdateRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	primary := seedServerTensionAuthoritySurface(t, ctx, store, scenario)
	current := claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	beforeDetail := mustServerTensionDetail(t, ctx, store, scenario.workspaceID, primary.TensionID)
	beforeResolved := countServerTensionRuntimeEvents(t, ctx, store, scenario.workspaceID, "tension.resolved", primary.TensionID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-3403")

	result, rpcErr := h.workspaceTensionLifecycleUpdate(ctx, mustJSONRaw(workspaceTensionLifecycleUpdateParams{
		WorkspaceID:    scenario.workspaceID,
		TensionID:      primary.TensionID,
		LifecycleState: "RESOLVED",
		UpdatedBy:      "dashboard",
		Reason:         "stale lifecycle update should fail closed",
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale-authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.tension.lifecycle.update")

	afterDetail := mustServerTensionDetail(t, ctx, store, scenario.workspaceID, primary.TensionID)
	if afterDetail.Tension.LifecycleState != beforeDetail.Tension.LifecycleState ||
		afterDetail.Tension.ReviewStatus != beforeDetail.Tension.ReviewStatus ||
		afterDetail.Tension.UpdatedAt != beforeDetail.Tension.UpdatedAt {
		t.Fatalf("expected stale lifecycle update reject not to mutate tension, before=%+v after=%+v", beforeDetail.Tension, afterDetail.Tension)
	}
	if got := countServerTensionRuntimeEvents(t, ctx, store, scenario.workspaceID, "tension.resolved", primary.TensionID); got != beforeResolved {
		t.Fatalf("expected no new tension.resolved runtime event after stale-authority reject, before=%d after=%d", beforeResolved, got)
	}
}

func seedServerTensionAuthoritySurface(t *testing.T, ctx context.Context, store *sqlite.Store, scenario instrumentationRPCScenario) sqlite.TensionRecord {
	t.Helper()

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.runbookDocKey,
		Title:       "Runbook",
		Content:     "Instrumentation RPC tension authority runbook v2",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert workspace doc for tension authority surface: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:  "artifact-" + scenario.workspaceID + "-gap",
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
		Title:       "Gap Evidence",
		ArtifactRef: "artifact://" + scenario.workspaceID + "/gap",
		Kind:        "note",
		ContentType: "text/plain",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace artifact for tension authority surface: %v", err)
	}
	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "dashboard",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if len(refresh.Events) == 0 {
		t.Fatalf("expected refresh to emit tension events, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	return requireTensionRecordByType(t, items, "bottleneck")
}

func mustServerTensionDetail(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID string) sqlite.TensionDetail {
	t.Helper()

	detail, err := store.GetTension(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension detail %s/%s: %v", workspaceID, tensionID, err)
	}
	return detail
}

func countServerTensionRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType, tensionID string) int {
	t.Helper()

	return countServerTensionRuntimeEventsByFilter(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "tension",
		EntityID:    tensionID,
		Limit:       20,
	})
}

func countServerTensionRuntimeEventsByType(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType string) int {
	t.Helper()

	return countServerTensionRuntimeEventsByFilter(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "tension",
		Limit:       50,
	})
}

func countServerAuthorityRejectEventsForTension(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) int {
	t.Helper()

	return countServerTensionRuntimeEventsByFilter(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   sqlite.AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
}

func countServerTensionRuntimeEventsByFilter(t *testing.T, ctx context.Context, store *sqlite.Store, filter sqlite.RuntimeEventFilter) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events %+v: %v", filter, err)
	}
	return len(events)
}
