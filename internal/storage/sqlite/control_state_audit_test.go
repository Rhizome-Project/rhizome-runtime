package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildClusterControlStateReportPreviewKeepsDerivedHintsUntilTick(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-state-preview")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for control-state preview audit",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	report, err := store.BuildClusterControlStateReport(ctx, ClusterControlStateFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build cluster control state report: %v", err)
	}
	authority, err := store.GetWorkspaceTimeAuthority(ctx, scenario.workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFields(t, report.TimeAuthority, authority)
	cluster := requireControlStateClusterByID(t, report.Clusters, "task:"+scenario.workspaceID+"/"+scenario.taskID)
	if cluster.State.Epoch != 0 {
		t.Fatalf("expected preview state before first tick, got %+v", cluster.State)
	}
	if cluster.State.CandidateMode == cluster.State.CurrentMode {
		t.Fatalf("expected preview candidate mode to differ for blocked cluster, got %+v", cluster.State)
	}
	if cluster.State.CandidateStreak != 0 {
		t.Fatalf("expected preview candidate streak 0 before first tick, got %+v", cluster.State)
	}
	if cluster.State.LastTickAt != "" || cluster.State.LastTickEventID != "" {
		t.Fatalf("expected preview state to have no tick metadata, got %+v", cluster.State)
	}
	if !clusterControlOperatorHintsEqual(cluster.State.OperatorHints, cluster.SuggestedControls) {
		t.Fatalf("expected preview state to expose derived operator hints before the first tick, got state=%+v suggested=%+v", cluster.State.OperatorHints, cluster.SuggestedControls)
	}
}

func TestTickClusterControlStateSetsLastTickMetadataAndEventPayload(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-state-tick-meta")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for control-state tick audit",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	result, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "dashboard",
	})
	if err != nil {
		t.Fatalf("tick cluster control state: %v", err)
	}
	if len(result.Events) == 0 {
		t.Fatalf("expected control-state tick to append runtime events, got %+v", result)
	}
	cluster := requireControlStateClusterByID(t, result.Report.Clusters, "task:"+scenario.workspaceID+"/"+scenario.taskID)
	authority, err := store.GetWorkspaceTimeAuthority(ctx, scenario.workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFields(t, result.Report.TimeAuthority, authority)
	if strings.TrimSpace(cluster.State.LastTickAt) == "" {
		t.Fatalf("expected persisted last_tick_at after tick, got %+v", cluster.State)
	}
	if cluster.State.LastTickAt != result.TickedAt {
		t.Fatalf("expected last_tick_at %s to match ticked_at, got %+v", result.TickedAt, cluster.State)
	}
	if cluster.State.LastTickEventID != result.Events[0].EventID {
		t.Fatalf("expected last_tick_event_id %s, got %+v", result.Events[0].EventID, cluster.State)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(result.Events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode control-state tick payload: %v", err)
	}
	if payload["last_tick_at"] != result.TickedAt {
		t.Fatalf("expected payload last_tick_at %s, got %+v", result.TickedAt, payload)
	}
	if payload["last_tick_event_id"] != result.Events[0].EventID {
		t.Fatalf("expected payload last_tick_event_id %s, got %+v", result.Events[0].EventID, payload)
	}
	if payload["typed_event_type"] != "CONTROL_STATE_INTERPRETATION" || payload["event_kind"] != "cluster.control_state_ticked" {
		t.Fatalf("expected control-state interpretation tick payload, got %+v", payload)
	}
	assertNoLegacyControlStateKeys(t, payload)
	assertHasControlStateRenamedKeys(t, payload)
}

func TestBuildClusterControlStateReportRejectsInvalidModeFilter(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	setupInstrumentationInternalWorkspace(t, ctx, store, "ws-control-state-invalid-mode", "agent-a")

	_, err := store.BuildClusterControlStateReport(ctx, ClusterControlStateFilter{
		WorkspaceID: "ws-control-state-invalid-mode",
		Mode:        "definitely-not-a-mode",
		Limit:       10,
	})
	if err == nil || !strings.Contains(err.Error(), "mode is invalid") {
		t.Fatalf("expected invalid mode error, got %v", err)
	}
}

func requireControlStateClusterByID(t *testing.T, items []ClusterControlStateCluster, protoClusterID string) ClusterControlStateCluster {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == protoClusterID {
			return item
		}
	}
	t.Fatalf("control-state cluster %s not found in %+v", protoClusterID, items)
	return ClusterControlStateCluster{}
}
