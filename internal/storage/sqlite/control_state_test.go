package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

var controlStateStabilizationEventAliases = []string{
	"cluster.control_state_stabilized",
	"cluster.control_state_stabilization",
}

var controlStateCurrentKeyAliases = []string{
	"stabilized_mode_hint",
}

var controlStateCandidateKeyAliases = []string{
	"candidate_mode_hint",
}

var controlStateProfileKeyAliases = []string{
	"heuristic_profile",
}

var controlStateHintsKeyAliases = []string{
	"operator_hints",
}

type controlStateTestScenario struct {
	blockedTensionScenario
	clusterID string
}

func TestTickClusterControlStateRequiresCandidateStreakBeforeTransitionAndResetsOnCandidateChange(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedControlledTensionOnlyControlStateScenario(t, ctx, store, "control-state-hysteresis")

	first, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	})
	if err != nil {
		t.Fatalf("first tick control state: %v", err)
	}
	if first.EvaluatedClusters != 1 || first.UpdatedCount != 1 || first.TransitionedCount != 0 {
		t.Fatalf("expected first tick to seed one non-transitioned cluster, got %+v", first)
	}
	if len(first.Events) != 1 || first.Events[0].EventType != "cluster.control_state_ticked" {
		t.Fatalf("expected first tick to emit cluster.control_state_ticked, got %+v", first.Events)
	}
	assertClusterControlRuntimeEventPayload(t, first.Events[0], []string{"cluster.control_state_ticked"}, scenario.clusterID)
	firstCluster := requireClusterControlStateCluster(t, first.Report.Clusters, scenario.clusterID)
	if firstCluster.State.CurrentMode != clusterControlModeSteady {
		t.Fatalf("expected current mode to stay steady before hysteresis threshold, got %+v", firstCluster.State)
	}
	if firstCluster.State.CandidateMode == clusterControlModeSteady {
		t.Fatalf("expected a non-steady candidate mode after confirmed bottleneck, got %+v", firstCluster.State)
	}
	if firstCluster.State.CandidateStreak != 1 {
		t.Fatalf("expected first tick candidate streak to be 1, got %+v", firstCluster.State)
	}

	insertConfirmedContradictionControlStateFixture(t, ctx, store, scenario)

	second, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	})
	if err != nil {
		t.Fatalf("second tick control state: %v", err)
	}
	if second.EvaluatedClusters != 1 || second.TransitionedCount != 0 {
		t.Fatalf("expected candidate change tick without transition, got %+v", second)
	}
	if len(second.Events) != 1 || second.Events[0].EventType != "cluster.control_state_ticked" {
		t.Fatalf("expected second tick to emit cluster.control_state_ticked, got %+v", second.Events)
	}
	assertClusterControlRuntimeEventPayload(t, second.Events[0], []string{"cluster.control_state_ticked"}, scenario.clusterID)
	secondCluster := requireClusterControlStateCluster(t, second.Report.Clusters, scenario.clusterID)
	if secondCluster.State.CurrentMode != firstCluster.State.CurrentMode {
		t.Fatalf("expected candidate change not to transition current mode, got before=%+v after=%+v", firstCluster.State, secondCluster.State)
	}
	if secondCluster.State.CandidateMode == firstCluster.State.CandidateMode {
		t.Fatalf("expected confirmed contradiction to change candidate mode, got before=%+v after=%+v", firstCluster.State, secondCluster.State)
	}
	if secondCluster.State.CandidateStreak != 1 {
		t.Fatalf("expected candidate streak reset to 1 on candidate change, got %+v", secondCluster.State)
	}

	third, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	})
	if err != nil {
		t.Fatalf("third tick control state: %v", err)
	}
	if third.EvaluatedClusters != 1 || third.TransitionedCount != 1 {
		t.Fatalf("expected third tick to cross hysteresis threshold and transition, got %+v", third)
	}
	if len(third.Events) != 1 || !isControlStateStabilizationEvent(third.Events[0].EventType) {
		t.Fatalf("expected third tick to emit stabilization event, got %+v", third.Events)
	}
	assertClusterControlRuntimeEventPayload(t, third.Events[0], controlStateStabilizationEventAliases, scenario.clusterID)
	thirdRow := requireStoredClusterControlStateRow(t, ctx, store, scenario.workspaceID, scenario.clusterID)
	if thirdRow.CurrentMode != secondCluster.State.CandidateMode {
		t.Fatalf("expected current mode to transition into repeated candidate, got before=%+v after=%+v", secondCluster.State, thirdRow)
	}
	if thirdRow.CandidateStreak != 0 || thirdRow.LastTransitionAt == "" {
		t.Fatalf("expected transitioned cluster to reset candidate streak and stamp last_transition_at, got %+v", thirdRow)
	}
}

func TestTickClusterControlStateKeepsPendingContextAndPreservesConfirmedDrivenHints(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedConfirmedMetricsBackedControlStateScenario(t, ctx, store, "control-state-pending")

	if _, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	}); err != nil {
		t.Fatalf("first tick control state: %v", err)
	}
	second, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	})
	if err != nil {
		t.Fatalf("second tick control state: %v", err)
	}
	if second.TransitionedCount != 1 {
		t.Fatalf("expected second tick to transition into stable confirmed mode, got %+v", second)
	}
	secondCluster := requireClusterControlStateCluster(t, second.Report.Clusters, scenario.clusterID)
	secondRow := requireStoredClusterControlStateRow(t, ctx, store, scenario.workspaceID, scenario.clusterID)
	assertOperatorHintsEqual(t, secondRow.OperatorHints, secondCluster.SuggestedControls, "persisted operator hints should mirror confirmed-driven scaffold suggestions")
	assertOperatorHintsEqual(t, secondCluster.State.OperatorHints, secondCluster.SuggestedControls, "report state should expose the same operator hints as suggested controls")
	if secondRow.CurrentMode == clusterControlModeSteady {
		t.Fatalf("expected second tick to stabilize into a non-steady interpretation, got %+v", secondRow)
	}

	insertPendingAmbiguityControlStateFixture(t, ctx, store, scenario)

	third, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	})
	if err != nil {
		t.Fatalf("third tick control state: %v", err)
	}
	if third.TransitionedCount != 0 {
		t.Fatalf("expected pending-only addition not to trigger mode transition, got %+v", third)
	}
	if len(third.Events) != 1 || third.Events[0].EventType != "cluster.control_state_ticked" {
		t.Fatalf("expected pending-context tick to emit cluster.control_state_ticked, got %+v", third.Events)
	}
	assertClusterControlRuntimeEventPayload(t, third.Events[0], []string{"cluster.control_state_ticked"}, scenario.clusterID)
	thirdRow := requireStoredClusterControlStateRow(t, ctx, store, scenario.workspaceID, scenario.clusterID)
	report, err := store.BuildClusterControlStateReport(ctx, ClusterControlStateFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Mode:           secondRow.CurrentMode,
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("build control state report after pending tick: %v", err)
	}
	authority, err := store.GetWorkspaceTimeAuthority(ctx, scenario.workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFields(t, report.TimeAuthority, authority)
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected control state report generated_at %q to mirror time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	thirdCluster := requireClusterControlStateCluster(t, report.Clusters, scenario.clusterID)
	if thirdRow.CurrentMode != secondRow.CurrentMode || thirdRow.CandidateMode != secondRow.CurrentMode {
		t.Fatalf("expected pending tension to remain contextual while confirmed bottleneck keeps mode stable, got before=%+v after=%+v", secondRow, thirdRow)
	}
	if thirdCluster.State.PendingTensionCount != 1 || thirdCluster.PendingCountsByType["ambiguity"] != 1 {
		t.Fatalf("expected pending ambiguity to remain visible in contextual counts, got %+v", thirdCluster)
	}
	if thirdRow.ConfirmedTensionCount != secondRow.ConfirmedTensionCount {
		t.Fatalf("expected pending context not to rewrite confirmed tension basis, got before=%+v after=%+v", secondRow, thirdRow)
	}
	assertOperatorHintsEqual(t, thirdRow.OperatorHints, secondRow.OperatorHints, "pending-only context should not rewrite stabilized operator hints")
	assertOperatorHintsEqual(t, thirdCluster.State.OperatorHints, thirdCluster.SuggestedControls, "cluster detail should keep operator hints aligned with the scaffold")
}

func TestRecordClusterControlStateSnapshotProducesRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedConfirmedMetricsBackedControlStateScenario(t, ctx, store, "control-state-snapshot")

	if _, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	}); err != nil {
		t.Fatalf("first tick control state: %v", err)
	}
	if _, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	}); err != nil {
		t.Fatalf("second tick control state: %v", err)
	}

	row := requireStoredClusterControlStateRow(t, ctx, store, scenario.workspaceID, scenario.clusterID)
	report, err := store.BuildClusterControlStateReport(ctx, ClusterControlStateFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Mode:           row.CurrentMode,
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("build control state report: %v", err)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected control state snapshot source report generated_at %q to mirror time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	event, err := store.RecordClusterControlStateSnapshot(ctx, report, ClusterControlSnapshotInput{
		ActorID: "dashboard",
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("record control state snapshot: %v", err)
	}
	if event.EventType != "cluster.control_state_snapshot" || event.EntityType != "instrumentation_control_state" || event.EntityID != scenario.clusterID {
		t.Fatalf("unexpected control state snapshot event %+v", event)
	}

	payload := decodeClusterControlPayload(t, event.PayloadJSON)
	if payload["workspace_id"] != scenario.workspaceID {
		t.Fatalf("expected workspace payload %s, got %+v", scenario.workspaceID, payload)
	}
	if payload["typed_event_type"] != "CONTROL_STATE_SNAPSHOT" || payload["event_kind"] != "cluster.control_state_snapshot" {
		t.Fatalf("expected control-state snapshot envelope, got %+v", payload)
	}
	assertNoLegacyControlStateKeys(t, payload)
	assertHasControlStateRenamedKeys(t, payload)
	clusters, ok := payload["clusters"].([]any)
	if !ok || len(clusters) != 1 {
		t.Fatalf("expected snapshot payload to trim to one cluster, got %+v", payload["clusters"])
	}
	if payload["captured_cluster_count"] != float64(1) || payload["source_cluster_count"] != float64(1) {
		t.Fatalf("expected control-state snapshot to expose captured/source counts, got %+v", payload)
	}
	if payload["snapshot_limit"] != float64(1) || payload["snapshot_truncated"] != false {
		t.Fatalf("expected control-state snapshot limit/truncation metadata, got %+v", payload)
	}
	if _, ok := payload["summary"].(string); !ok {
		t.Fatalf("expected snapshot payload summary, got %+v", payload)
	}

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.control_state_snapshot",
		EntityType:  "instrumentation_control_state",
		EntityID:    scenario.clusterID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list control state snapshot events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one persisted control state snapshot event, got %+v", events)
	}
}

func TestClusterControlStateWritesAttachPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedControlledTensionOnlyControlStateScenario(t, ctx, store, "control-state-prompt-context")

	tick, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:                scenario.workspaceID,
		ProtoClusterID:             scenario.clusterID,
		ActorID:                    "dashboard",
		PromptContextEnvelope:      BuildControlStatePromptContextEnvelope("workspace.instrumentation.control.state.tick", "server_rpc", scenario.workspaceID, "human", "dashboard"),
		PromptContextSurface:       "workspace.instrumentation.control.state.tick",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "dashboard",
	})
	if err != nil {
		t.Fatalf("tick control state with prompt context: %v", err)
	}
	if len(tick.Events) != 1 {
		t.Fatalf("expected one control-state tick event, got %+v", tick.Events)
	}
	assertControlStatePromptContextEnvelope(t, tick.Events[0].PayloadJSON, "workspace.instrumentation.control.state.tick", scenario.workspaceID, "human", "dashboard", map[string]string{
		"actor_id":           "dashboard",
		"proto_cluster_id":   scenario.clusterID,
		"event_type":         "cluster.control_state_ticked",
		"entity_type":        "instrumentation_control_state",
		"entity_id":          scenario.clusterID,
		"actor_type":         "operator",
		"epoch":              "1",
		"last_tick_event_id": tick.Events[0].EventID,
		"last_tick_at":       tick.TickedAt,
		"event_kind":         "cluster.control_state_ticked",
		"typed_event_type":   "CONTROL_STATE_INTERPRETATION",
	})

	report, err := store.BuildClusterControlStateReport(ctx, ClusterControlStateFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("build control-state report for prompt-context snapshot: %v", err)
	}
	snapshot, err := store.RecordClusterControlStateSnapshot(ctx, report, ClusterControlSnapshotInput{
		ActorID:                    "dashboard",
		Limit:                      1,
		PromptContextEnvelope:      BuildControlStatePromptContextEnvelope("workspace.instrumentation.control.state.snapshot", "server_rpc", scenario.workspaceID, "human", "dashboard"),
		PromptContextSurface:       "workspace.instrumentation.control.state.snapshot",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "dashboard",
	})
	if err != nil {
		t.Fatalf("record control-state snapshot with prompt context: %v", err)
	}
	assertControlStatePromptContextEnvelope(t, snapshot.PayloadJSON, "workspace.instrumentation.control.state.snapshot", scenario.workspaceID, "human", "dashboard", map[string]string{
		"actor_id":                        "dashboard",
		"filter_proto_cluster_id":         scenario.clusterID,
		"event_type":                      "cluster.control_state_snapshot",
		"entity_type":                     "instrumentation_control_state",
		"entity_id":                       scenario.clusterID,
		"actor_type":                      "operator",
		"captured_cluster_count":          "1",
		"source_cluster_count":            "1",
		"snapshot_limit":                  "1",
		"snapshot_truncated":              "false",
		"snapshot_scope":                  "proto_cluster",
		"captured_proto_cluster_ids_hash": clusterControlSnapshotClusterIDsHash([]string{scenario.clusterID}),
		"event_kind":                      "cluster.control_state_snapshot",
		"typed_event_type":                "CONTROL_STATE_SNAPSHOT",
	})
}

func TestClusterControlStatePromptContextRejectsForgedPrincipalAndRollsBack(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedControlledTensionOnlyControlStateScenario(t, ctx, store, "control-state-prompt-forged")

	_, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:                scenario.workspaceID,
		ProtoClusterID:             scenario.clusterID,
		ActorID:                    "dashboard",
		PromptContextEnvelope:      BuildControlStatePromptContextEnvelope("workspace.instrumentation.control.state.tick", "server_rpc", scenario.workspaceID, "human", "mallory"),
		PromptContextSurface:       "workspace.instrumentation.control.state.tick",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "dashboard",
	})
	if err == nil {
		t.Fatal("expected forged control-state prompt context to fail")
	}
	if !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("unexpected forged control-state prompt context error: %v", err)
	}

	rows, listRowsErr := store.listClusterControlStateRows(ctx, scenario.workspaceID, scenario.clusterID)
	if listRowsErr != nil {
		t.Fatalf("list control-state rows after forged reject: %v", listRowsErr)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no control-state read-model row after forged prompt context reject, got %+v", rows)
	}
	if got := countPolicyControlRows(t, ctx, store, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND event_type IN ('cluster.control_state_ticked', 'cluster.control_state_stabilized')`, scenario.workspaceID); got != 0 {
		t.Fatalf("expected no control-state runtime event after forged prompt context reject, got %d", got)
	}
}

func TestClusterControlStateSnapshotPromptContextRejectsForgedActorAndRollsBack(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedControlledTensionOnlyControlStateScenario(t, ctx, store, "control-state-snapshot-prompt-forged")
	report, err := store.BuildClusterControlStateReport(ctx, ClusterControlStateFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("build control-state report for forged snapshot prompt context: %v", err)
	}
	envelope := BuildControlStatePromptContextEnvelope("workspace.instrumentation.control.state.snapshot", "server_rpc", scenario.workspaceID, "human", "dashboard")
	envelope["actor_id"] = "mallory"
	_, err = store.RecordClusterControlStateSnapshot(ctx, report, ClusterControlSnapshotInput{
		ActorID:                    "dashboard",
		Limit:                      1,
		PromptContextEnvelope:      envelope,
		PromptContextSurface:       "workspace.instrumentation.control.state.snapshot",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "dashboard",
	})
	if err == nil {
		t.Fatal("expected forged snapshot prompt context actor binding to fail")
	}
	if !strings.Contains(err.Error(), "actor_id") {
		t.Fatalf("unexpected forged snapshot prompt context error: %v", err)
	}
	if got := countPolicyControlRows(t, ctx, store, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND event_type = 'cluster.control_state_snapshot'`, scenario.workspaceID); got != 0 {
		t.Fatalf("expected no snapshot runtime event after forged prompt context reject, got %d", got)
	}
}

func TestClusterControlStateWorkspaceSnapshotPromptContextBindsCapturedClusterHash(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedControlledTensionOnlyControlStateScenario(t, ctx, store, "control-state-snapshot-workspace-hash")
	secondTaskID := scenario.taskID + "-secondary"
	secondClusterID := "task:" + scenario.workspaceID + "/" + secondTaskID
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       "tension:" + scenario.workspaceID + "/bottleneck/" + secondTaskID,
		WorkspaceID:     scenario.workspaceID,
		ProtoClusterID:  secondClusterID,
		TensionType:     "bottleneck",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewConfirmed,
		Title:           "Second confirmed bottleneck for workspace snapshot hash",
		Summary:         "Workspace-wide snapshot should bind the captured cluster-id set.",
		AnchorKind:      "workspace_doc",
		AnchorRef:       scenario.docKey,
		TaskIDs:         []string{secondTaskID},
		DocKeys:         []string{scenario.docKey},
		ArtifactRefs:    []string{scenario.artifactRef + "-secondary"},
		AgentIDs:        []string{"agent-a", "agent-b"},
		BaseScore:       67,
		SurfaceScore:    83,
		EvidenceCount:   1,
		LastSeenEventID: "rtev-bottleneck-" + secondTaskID,
		LastSeenAt:      "2026-03-23T04:00:00Z",
		ConfirmedBy:     "operator",
		CreatedAt:       "2026-03-23T04:00:00Z",
		UpdatedAt:       "2026-03-23T04:00:00Z",
	})
	report, err := store.BuildClusterControlStateReport(ctx, ClusterControlStateFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build workspace-wide control-state report: %v", err)
	}
	if len(report.Clusters) < 2 {
		t.Fatalf("expected at least two workspace-wide control-state clusters, got %+v", report.Clusters)
	}
	event, err := store.RecordClusterControlStateSnapshot(ctx, report, ClusterControlSnapshotInput{
		ActorID:                    "dashboard",
		Limit:                      1,
		PromptContextEnvelope:      BuildControlStatePromptContextEnvelope("workspace.instrumentation.control.state.snapshot", "server_rpc", scenario.workspaceID, "human", "dashboard"),
		PromptContextSurface:       "workspace.instrumentation.control.state.snapshot",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "dashboard",
	})
	if err != nil {
		t.Fatalf("record workspace-wide control-state snapshot with prompt context: %v", err)
	}
	payload := decodeClusterControlPayload(t, event.PayloadJSON)
	capturedIDs := capturedControlStateClusterIDsFromPayload(t, payload)
	if len(capturedIDs) != 1 {
		t.Fatalf("expected snapshot limit to capture one cluster, got ids=%v payload=%+v", capturedIDs, payload)
	}
	assertControlStatePromptContextEnvelope(t, event.PayloadJSON, "workspace.instrumentation.control.state.snapshot", scenario.workspaceID, "human", "dashboard", map[string]string{
		"actor_id":                        "dashboard",
		"filter_proto_cluster_id":         "",
		"event_type":                      "cluster.control_state_snapshot",
		"entity_type":                     "instrumentation_control_state",
		"entity_id":                       scenario.workspaceID,
		"actor_type":                      "operator",
		"captured_cluster_count":          "1",
		"source_cluster_count":            fmt.Sprintf("%d", len(report.Clusters)),
		"snapshot_limit":                  "1",
		"snapshot_truncated":              "true",
		"snapshot_scope":                  "workspace",
		"captured_proto_cluster_ids_hash": clusterControlSnapshotClusterIDsHash(capturedIDs),
		"event_kind":                      "cluster.control_state_snapshot",
		"typed_event_type":                "CONTROL_STATE_SNAPSHOT",
	})
}

func seedConfirmedMetricsBackedControlStateScenario(t *testing.T, ctx context.Context, store *Store, suffix string) controlStateTestScenario {
	t.Helper()

	base := seedBlockedTensionScenario(t, ctx, store, suffix)
	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: base.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: base.workspaceID,
		TaskID:      base.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: base.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "seed control-state tests",
	}); err != nil {
		t.Fatalf("confirm bottleneck tension: %v", err)
	}

	return controlStateTestScenario{
		blockedTensionScenario: base,
		clusterID:              "task:" + base.workspaceID + "/" + base.taskID,
	}
}

func seedControlledTensionOnlyControlStateScenario(t *testing.T, ctx context.Context, store *Store, suffix string) controlStateTestScenario {
	t.Helper()

	scenario := controlStateTestScenario{
		blockedTensionScenario: blockedTensionScenario{
			workspaceID: "ws-control-state-" + suffix,
			taskID:      "task-control-state-" + suffix,
			docKey:      "control-state-doc-" + suffix,
			artifactRef: "artifact://control-state-" + suffix,
		},
	}
	scenario.clusterID = "task:" + scenario.workspaceID + "/" + scenario.taskID

	setupInstrumentationInternalWorkspace(t, ctx, store, scenario.workspaceID, "agent-a", "agent-b")
	ensureTensionOverlayTables(t, ctx, store)

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       "tension:" + scenario.workspaceID + "/bottleneck/" + scenario.taskID,
		WorkspaceID:     scenario.workspaceID,
		ProtoClusterID:  scenario.clusterID,
		TensionType:     "bottleneck",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewConfirmed,
		Title:           "Confirmed bottleneck for hysteresis seed",
		Summary:         "Throughput pressure should seed the first candidate mode.",
		AnchorKind:      "workspace_doc",
		AnchorRef:       scenario.docKey,
		TaskIDs:         []string{scenario.taskID},
		DocKeys:         []string{scenario.docKey},
		ArtifactRefs:    []string{scenario.artifactRef},
		AgentIDs:        []string{"agent-a", "agent-b"},
		BaseScore:       68,
		SurfaceScore:    84,
		EvidenceCount:   1,
		LastSeenEventID: "rtev-bottleneck-" + scenario.taskID,
		LastSeenAt:      "2026-03-23T01:00:00Z",
		ConfirmedBy:     "operator",
		CreatedAt:       "2026-03-23T01:00:00Z",
		UpdatedAt:       "2026-03-23T01:00:00Z",
	})

	return scenario
}

func insertConfirmedContradictionControlStateFixture(t *testing.T, ctx context.Context, store *Store, scenario controlStateTestScenario) {
	t.Helper()

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       "tension:" + scenario.workspaceID + "/contradiction/" + scenario.taskID,
		WorkspaceID:     scenario.workspaceID,
		ProtoClusterID:  scenario.clusterID,
		TensionType:     "contradiction",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewConfirmed,
		Title:           "Confirmed contradiction for control-state hysteresis",
		Summary:         "Review pressure should replace bottleneck candidate once repeated.",
		AnchorKind:      "workspace_doc",
		AnchorRef:       scenario.docKey,
		TaskIDs:         []string{scenario.taskID},
		SessionIDs:      []string{scenario.sessionID},
		DocKeys:         []string{scenario.docKey},
		ArtifactRefs:    []string{scenario.artifactRef},
		AgentIDs:        []string{"agent-a", "agent-b"},
		BaseScore:       72,
		SurfaceScore:    90,
		EvidenceCount:   1,
		LastSeenEventID: "rtev-contradiction-" + scenario.taskID,
		LastSeenAt:      "2026-03-23T02:00:00Z",
		ConfirmedBy:     "operator",
		CreatedAt:       "2026-03-23T02:00:00Z",
		UpdatedAt:       "2026-03-23T02:00:00Z",
	})
}

func insertPendingAmbiguityControlStateFixture(t *testing.T, ctx context.Context, store *Store, scenario controlStateTestScenario) {
	t.Helper()

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       "tension:" + scenario.workspaceID + "/pending-ambiguity/" + scenario.taskID,
		WorkspaceID:     scenario.workspaceID,
		ProtoClusterID:  scenario.clusterID,
		TensionType:     "ambiguity",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewPending,
		Title:           "Pending ambiguity for control-state context",
		Summary:         "Should remain contextual without rewriting control mode.",
		AnchorKind:      "workspace_doc",
		AnchorRef:       scenario.docKey,
		TaskIDs:         []string{scenario.taskID},
		SessionIDs:      []string{scenario.sessionID},
		DocKeys:         []string{scenario.docKey},
		ArtifactRefs:    []string{scenario.artifactRef},
		AgentIDs:        []string{"agent-a", "agent-b"},
		BaseScore:       55,
		SurfaceScore:    63,
		EvidenceCount:   1,
		LastSeenEventID: "rtev-pending-ambiguity-" + scenario.taskID,
		LastSeenAt:      "2026-03-23T03:00:00Z",
		CreatedAt:       "2026-03-23T03:00:00Z",
		UpdatedAt:       "2026-03-23T03:00:00Z",
	})
}

func requireClusterControlStateCluster(t *testing.T, clusters []ClusterControlStateCluster, clusterID string) ClusterControlStateCluster {
	t.Helper()

	for _, cluster := range clusters {
		if cluster.ProtoClusterID == clusterID {
			return cluster
		}
	}
	t.Fatalf("control-state cluster %s not found in %+v", clusterID, clusters)
	return ClusterControlStateCluster{}
}

func requireStoredClusterControlStateRow(t *testing.T, ctx context.Context, store *Store, workspaceID, clusterID string) ClusterControlStateRecord {
	t.Helper()

	rows, err := store.listClusterControlStateRows(ctx, workspaceID, clusterID)
	if err != nil {
		t.Fatalf("list stored cluster control state rows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one stored control-state row for %s, got %+v", clusterID, rows)
	}
	return rows[0]
}

func assertClusterControlRuntimeEventPayload(t *testing.T, event RuntimeEventRecord, wantEventTypes []string, wantClusterID string) {
	t.Helper()

	if !containsControlStateAlias(wantEventTypes, event.EventType) || event.EntityType != "instrumentation_control_state" || event.EntityID != wantClusterID {
		t.Fatalf("unexpected control state runtime event %+v", event)
	}
	payload := decodeClusterControlPayload(t, event.PayloadJSON)
	if payload["typed_event_type"] != "CONTROL_STATE_INTERPRETATION" {
		t.Fatalf("expected control-state interpretation envelope, got %+v", payload)
	}
	eventKind, ok := payload["event_kind"].(string)
	if !ok || !containsControlStateAlias(wantEventTypes, eventKind) {
		t.Fatalf("expected control-state event kind in %v, got %+v", wantEventTypes, payload)
	}
	if payload["proto_cluster_id"] != wantClusterID {
		t.Fatalf("expected payload proto_cluster_id %s, got %+v", wantClusterID, payload)
	}
	if _, ok := payload["stability_streak"].(float64); !ok {
		t.Fatalf("expected payload stability_streak, got %+v", payload)
	}
	if _, ok := payload["pending_tension_count"].(float64); !ok {
		t.Fatalf("expected payload pending_tension_count to preserve context, got %+v", payload)
	}
	assertNoLegacyControlStateKeys(t, payload)
	assertHasControlStateRenamedKeys(t, payload)
}

func assertControlStatePromptContextEnvelope(t *testing.T, payloadJSON, surface, workspaceID, principalType, principalID string, extra map[string]string) {
	t.Helper()

	payload := decodeClusterControlPayload(t, payloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected control-state prompt_context_envelope, got %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_control_state_write",
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	for key, value := range extra {
		expected[key] = value
	}
	for key, want := range expected {
		if got, _ := envelope[key].(string); got != want {
			t.Fatalf("expected control-state prompt_context_envelope[%s]=%q, got %q in %+v", key, want, got, envelope)
		}
	}
}

func capturedControlStateClusterIDsFromPayload(t *testing.T, payload map[string]any) []string {
	t.Helper()
	clusters, ok := payload["clusters"].([]any)
	if !ok {
		t.Fatalf("expected snapshot clusters array, got %+v", payload["clusters"])
	}
	out := make([]string, 0, len(clusters))
	for _, item := range clusters {
		cluster, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("expected snapshot cluster object, got %T %+v", item, item)
		}
		clusterID, _ := cluster["proto_cluster_id"].(string)
		if strings.TrimSpace(clusterID) != "" {
			out = append(out, strings.TrimSpace(clusterID))
		}
	}
	return out
}

func decodeClusterControlPayload(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode control-state payload: %v", err)
	}
	return payload
}

func assertNoLegacyControlStateKeys(t *testing.T, payload map[string]any) {
	t.Helper()

	keys := flattenControlStatePayloadKeys(payload)
	for _, legacy := range []string{"current_mode", "candidate_mode", "corridor_profile", "control_vector"} {
		if keys[legacy] {
			t.Fatalf("expected cut-back control-state payload to drop legacy key %s, got keys=%v payload=%+v", legacy, sortedControlStateKeys(keys), payload)
		}
	}
}

func assertHasControlStateRenamedKeys(t *testing.T, payload map[string]any) {
	t.Helper()

	keys := flattenControlStatePayloadKeys(payload)
	assertHasAnyControlStateKeyAlias(t, keys, "current interpretation", controlStateCurrentKeyAliases)
	assertHasAnyControlStateKeyAlias(t, keys, "candidate interpretation", controlStateCandidateKeyAliases)
	assertHasAnyControlStateKeyAlias(t, keys, "scaffold profile", controlStateProfileKeyAliases)
	assertHasAnyControlStateKeyAlias(t, keys, "operator hints", controlStateHintsKeyAliases)
}

func assertHasAnyControlStateKeyAlias(t *testing.T, keys map[string]bool, label string, aliases []string) {
	t.Helper()
	for _, alias := range aliases {
		if keys[alias] {
			return
		}
	}
	t.Fatalf("expected %s key alias in payload, wanted one of %v, got keys=%v", label, aliases, sortedControlStateKeys(keys))
}

func flattenControlStatePayloadKeys(value any) map[string]bool {
	out := map[string]bool{}
	var visit func(any)
	visit = func(node any) {
		switch typed := node.(type) {
		case map[string]any:
			for key, child := range typed {
				out[key] = true
				visit(child)
			}
		case []any:
			for _, child := range typed {
				visit(child)
			}
		}
	}
	visit(value)
	return out
}

func sortedControlStateKeys(keys map[string]bool) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if strings.Compare(out[j], out[i]) < 0 {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func isControlStateStabilizationEvent(eventType string) bool {
	return containsControlStateAlias(controlStateStabilizationEventAliases, eventType)
}

func containsControlStateAlias(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func assertOperatorHintsEqual(t *testing.T, got, want ControlSuggestedControls, message string) {
	t.Helper()
	if !clusterControlOperatorHintsEqual(got, want) {
		t.Fatalf("%s, got=%+v want=%+v", message, got, want)
	}
}
