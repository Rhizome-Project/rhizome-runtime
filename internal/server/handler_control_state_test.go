package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
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

func TestWorkspaceInstrumentationControlStateRPCSurfaceAndSSE(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	clusterID := seedConfirmedControlStateRPCScenario(t, ctx, store, scenario)
	rpcCtx := testAuthContext(scenario.workspaceID, "human", "developer")

	rawReport, err := json.Marshal(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("marshal control state report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationControlStateReport(rpcCtx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateReport rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected control state report result type %T", result)
	}
	report, ok := reportPayload["report"].(sqlite.ClusterControlStateReport)
	if !ok {
		t.Fatalf("unexpected control state report payload type %T", reportPayload["report"])
	}
	preview := requireServerControlStateCluster(t, report.Clusters, clusterID)
	if preview.State.CurrentMode != "STEADY" || preview.State.CandidateMode == "STEADY" || preview.State.CandidateStreak != 0 {
		t.Fatalf("expected preview report to stay steady with a non-steady candidate, got %+v", preview.State)
	}

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	rawTick, err := json.Marshal(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		ActorID:        "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal control state tick params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlStateTick(rpcCtx, rawTick)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateTick first rpc error: %+v", rpcErr)
	}
	tickPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first tick result type %T", result)
	}
	firstTick, ok := tickPayload["result"].(sqlite.ClusterControlTickResult)
	if !ok {
		t.Fatalf("unexpected first tick payload type %T", tickPayload["result"])
	}
	if firstTick.TransitionedCount != 0 || len(firstTick.Events) != 1 || firstTick.Events[0].EventType != "cluster.control_state_ticked" {
		t.Fatalf("expected first tick to emit non-transitioned control event, got %+v", firstTick)
	}
	assertServerControlStatePayloadContract(t, firstTick.Events[0].PayloadJSON, []string{"cluster.control_state_ticked"})
	assertServerControlStatePromptContext(t, firstTick.Events[0], controlStateTickSurface, scenario.workspaceID, "human", "developer", map[string]string{
		"actor_id":           "dashboard",
		"proto_cluster_id":   clusterID,
		"event_type":         "cluster.control_state_ticked",
		"entity_type":        "instrumentation_control_state",
		"entity_id":          clusterID,
		"actor_type":         "operator",
		"last_tick_event_id": firstTick.Events[0].EventID,
		"last_tick_at":       firstTick.TickedAt,
		"event_kind":         "cluster.control_state_ticked",
		"typed_event_type":   "CONTROL_STATE_INTERPRETATION",
	})
	liveTick := nextEvent(t, ch)
	if liveTick.Type != "cluster.control_state_ticked" {
		t.Fatalf("expected cluster.control_state_ticked live event, got %+v", liveTick)
	}
	assertValidEventTimestamp(t, liveTick.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, liveTick, firstTick.Events[0], "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveTick.PayloadJSON), firstTick.Events[0].PayloadJSON)

	rawCluster, err := json.Marshal(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
	})
	if err != nil {
		t.Fatalf("marshal control state cluster params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlStateCluster(rpcCtx, rawCluster)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateCluster rpc error: %+v", rpcErr)
	}
	detailPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected control state cluster result type %T", result)
	}
	detail, ok := detailPayload["detail"].(sqlite.ClusterControlStateDetail)
	if !ok {
		t.Fatalf("unexpected control state cluster detail payload type %T", detailPayload["detail"])
	}
	if detail.State.State.CurrentMode != "STEADY" || detail.State.State.CandidateStreak != 1 {
		t.Fatalf("expected stored detail to reflect first hysteresis epoch, got %+v", detail.State.State)
	}
	if len(detail.Tensions) == 0 || len(detail.Events) == 0 {
		t.Fatalf("expected control state detail to include tensions and runtime events, got %+v", detail)
	}

	rawSnapshot, err := json.Marshal(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		ActorID:        "dashboard",
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("marshal control state snapshot params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlStateSnapshot(rpcCtx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected control state snapshot result type %T", result)
	}
	snapshotReport, ok := snapshotPayload["report"].(sqlite.ClusterControlStateReport)
	if !ok {
		t.Fatalf("unexpected control state snapshot report payload type %T", snapshotPayload["report"])
	}
	if snapshotReport.Filter.ProtoClusterID != clusterID || len(snapshotReport.Clusters) != 1 {
		t.Fatalf("expected scoped control state snapshot report for %s, got %+v", clusterID, snapshotReport)
	}
	snapshotEvent, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected control state snapshot event payload type %T", snapshotPayload["event"])
	}
	if snapshotEvent.EventType != "cluster.control_state_snapshot" || snapshotEvent.EntityID != clusterID {
		t.Fatalf("unexpected control state snapshot event %+v", snapshotEvent)
	}
	assertServerControlStatePromptContext(t, snapshotEvent, controlStateSnapshotSurface, scenario.workspaceID, "human", "developer", map[string]string{
		"actor_id":                        "dashboard",
		"filter_proto_cluster_id":         clusterID,
		"event_type":                      "cluster.control_state_snapshot",
		"entity_type":                     "instrumentation_control_state",
		"entity_id":                       clusterID,
		"actor_type":                      "operator",
		"captured_cluster_count":          "1",
		"source_cluster_count":            "1",
		"snapshot_limit":                  "1",
		"snapshot_truncated":              "false",
		"snapshot_scope":                  "proto_cluster",
		"captured_proto_cluster_ids_hash": controlStateClusterIDsHashForServerTest(clusterID),
		"event_kind":                      "cluster.control_state_snapshot",
		"typed_event_type":                "CONTROL_STATE_SNAPSHOT",
	})
	liveSnapshot := nextEvent(t, ch)
	if liveSnapshot.Type != "cluster.control_state_snapshot" {
		t.Fatalf("expected cluster.control_state_snapshot live event, got %+v", liveSnapshot)
	}
	assertValidEventTimestamp(t, liveSnapshot.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, liveSnapshot, snapshotEvent, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveSnapshot.PayloadJSON), snapshotEvent.PayloadJSON)

	result, rpcErr = h.workspaceInstrumentationControlStateTick(rpcCtx, rawTick)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateTick second rpc error: %+v", rpcErr)
	}
	tickPayload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second tick result type %T", result)
	}
	secondTick, ok := tickPayload["result"].(sqlite.ClusterControlTickResult)
	if !ok {
		t.Fatalf("unexpected second tick payload type %T", tickPayload["result"])
	}
	if secondTick.TransitionedCount != 1 || len(secondTick.Events) != 1 || !isServerControlStateStabilizationEvent(secondTick.Events[0].EventType) {
		t.Fatalf("expected second tick to emit stabilization event, got %+v", secondTick)
	}
	liveTransition := nextEvent(t, ch)
	if !isServerControlStateStabilizationEvent(liveTransition.Type) {
		t.Fatalf("expected control-state stabilization live event, got %+v", liveTransition)
	}
	assertValidEventTimestamp(t, liveTransition.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, liveTransition, secondTick.Events[0], "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveTransition.PayloadJSON), secondTick.Events[0].PayloadJSON)
	assertServerControlStatePayloadContract(t, secondTick.Events[0].PayloadJSON, controlStateStabilizationEventAliases)
	assertServerControlStatePromptContext(t, secondTick.Events[0], controlStateTickSurface, scenario.workspaceID, "human", "developer", map[string]string{
		"actor_id":           "dashboard",
		"proto_cluster_id":   clusterID,
		"event_type":         secondTick.Events[0].EventType,
		"entity_type":        "instrumentation_control_state",
		"entity_id":          clusterID,
		"actor_type":         "operator",
		"last_tick_event_id": secondTick.Events[0].EventID,
		"last_tick_at":       secondTick.TickedAt,
		"event_kind":         secondTick.Events[0].EventType,
		"typed_event_type":   "CONTROL_STATE_INTERPRETATION",
	})

	transitionPayload := decodeEventPayloadMap(t, secondTick.Events[0].PayloadJSON)
	currentMode, ok := transitionPayload["stabilized_mode_hint"].(string)
	if !ok || currentMode == "" {
		t.Fatalf("expected transitioned stabilized_mode_hint in runtime payload, got %+v", transitionPayload)
	}
	rawReport, err = json.Marshal(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Mode:           currentMode,
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("marshal transitioned control state report params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlStateReport(rpcCtx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateReport transitioned rpc error: %+v", rpcErr)
	}
	reportPayload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected transitioned control state report result type %T", result)
	}
	transitionedReport, ok := reportPayload["report"].(sqlite.ClusterControlStateReport)
	if !ok {
		t.Fatalf("unexpected transitioned control state report payload type %T", reportPayload["report"])
	}
	transitionedCluster := requireServerControlStateCluster(t, transitionedReport.Clusters, clusterID)
	if transitionedCluster.State.CurrentMode != currentMode || transitionedCluster.State.CandidateStreak != 0 {
		t.Fatalf("expected transitioned report to reflect persisted control mode, got %+v", transitionedCluster.State)
	}
}

func TestWorkspaceInstrumentationControlStateReplayAndContracts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	clusterID := seedConfirmedControlStateRPCScenario(t, ctx, store, scenario)

	invalidCalls := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params workspaceInstrumentationControlStateParams
	}{
		{name: "report missing workspace", call: h.workspaceInstrumentationControlStateReport, params: workspaceInstrumentationControlStateParams{Limit: 5}},
		{name: "report invalid mode", call: h.workspaceInstrumentationControlStateReport, params: workspaceInstrumentationControlStateParams{WorkspaceID: scenario.workspaceID, Mode: "NOT_A_MODE"}},
		{name: "cluster missing proto_cluster_id", call: h.workspaceInstrumentationControlStateCluster, params: workspaceInstrumentationControlStateParams{WorkspaceID: scenario.workspaceID}},
		{name: "tick missing actor", call: h.workspaceInstrumentationControlStateTick, params: workspaceInstrumentationControlStateParams{WorkspaceID: scenario.workspaceID, ProtoClusterID: clusterID}},
		{name: "snapshot missing workspace", call: h.workspaceInstrumentationControlStateSnapshot, params: workspaceInstrumentationControlStateParams{ActorID: "dashboard", Limit: 1}},
	}
	for _, tc := range invalidCalls {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			if _, rpcErr := tc.call(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params error, got %+v", rpcErr)
			}
		})
	}

	rawTick, err := json.Marshal(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		ActorID:        "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal control state tick params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationControlStateTick(ctx, rawTick)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateTick first rpc error: %+v", rpcErr)
	}
	firstTickPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first tick result type %T", result)
	}
	firstTick, ok := firstTickPayload["result"].(sqlite.ClusterControlTickResult)
	if !ok {
		t.Fatalf("unexpected first tick payload type %T", firstTickPayload["result"])
	}
	if len(firstTick.Events) != 1 || firstTick.Events[0].EventType != "cluster.control_state_ticked" {
		t.Fatalf("expected first tick to emit cluster.control_state_ticked, got %+v", firstTick)
	}
	assertServerControlStatePayloadContract(t, firstTick.Events[0].PayloadJSON, []string{"cluster.control_state_ticked"})

	rawSnapshot, err := json.Marshal(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		ActorID:        "dashboard",
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("marshal control state snapshot params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlStateSnapshot(ctx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected snapshot result type %T", result)
	}
	snapshotEvent, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected snapshot event payload type %T", snapshotPayload["event"])
	}

	result, rpcErr = h.workspaceInstrumentationControlStateTick(ctx, rawTick)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateTick second rpc error: %+v", rpcErr)
	}
	secondTickPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second tick result type %T", result)
	}
	secondTick, ok := secondTickPayload["result"].(sqlite.ClusterControlTickResult)
	if !ok {
		t.Fatalf("unexpected second tick payload type %T", secondTickPayload["result"])
	}
	if len(secondTick.Events) != 1 || !isServerControlStateStabilizationEvent(secondTick.Events[0].EventType) {
		t.Fatalf("expected second tick to emit stabilization event, got %+v", secondTick)
	}
	assertServerControlStatePayloadContract(t, secondTick.Events[0].PayloadJSON, controlStateStabilizationEventAliases)

	rawEvents, err := json.Marshal(workspaceEventsListParams{
		WorkspaceID: scenario.workspaceID,
		EntityType:  "instrumentation_control_state",
		EntityID:    clusterID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal control state events list params: %v", err)
	}
	result, rpcErr = h.workspaceEventsList(ctx, rawEvents)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsList control state rpc error: %+v", rpcErr)
	}
	eventListPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected control state events list result type %T", result)
	}
	items, ok := eventListPayload["items"].([]sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected control state events list payload type %T", eventListPayload["items"])
	}
	counts := map[string]int{}
	snapshotFound := false
	for _, item := range items {
		counts[item.EventType]++
		if item.EventID == snapshotEvent.EventID {
			snapshotFound = true
		}
	}
	if counts["cluster.control_state_ticked"] == 0 || !hasAnyServerControlStateCount(counts, controlStateStabilizationEventAliases) || !snapshotFound {
		t.Fatalf("expected events list to include tick, transition, and snapshot events, got counts=%+v items=%+v", counts, items)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   scenario.workspaceID,
		IncludeEvents: true,
		Limit:         100,
	})
	if err != nil {
		t.Fatalf("marshal control state replay params: %v", err)
	}
	result, rpcErr = h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay control state rpc error: %+v", rpcErr)
	}
	replayPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	replay, ok := replayPayload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay payload type %T", replayPayload["report"])
	}
	replayCounts := map[string]int{}
	for _, item := range replay.Events {
		replayCounts[item.EventType]++
	}
	if replayCounts["cluster.control_state_ticked"] == 0 || !hasAnyServerControlStateCount(replayCounts, controlStateStabilizationEventAliases) || replayCounts["cluster.control_state_snapshot"] == 0 {
		t.Fatalf("expected replay to surface control-state synthetic events, got %+v", replayCounts)
	}

	currentMode, ok := decodeEventPayloadMap(t, secondTick.Events[0].PayloadJSON)["stabilized_mode_hint"].(string)
	if !ok || currentMode == "" {
		t.Fatalf("expected transitioned stabilized_mode_hint in runtime payload, got %+v", secondTick.Events[0])
	}
	rawReport, err := json.Marshal(workspaceInstrumentationControlStateParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Mode:           currentMode,
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("marshal transitioned control state report params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlStateReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlStateReport post-replay rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected post-replay report result type %T", result)
	}
	report, ok := reportPayload["report"].(sqlite.ClusterControlStateReport)
	if !ok {
		t.Fatalf("unexpected post-replay report payload type %T", reportPayload["report"])
	}
	cluster := requireServerControlStateCluster(t, report.Clusters, clusterID)
	if cluster.State.CurrentMode == "STEADY" || cluster.State.CandidateStreak != 0 {
		t.Fatalf("expected replay to preserve transitioned control-state read model, got %+v", cluster.State)
	}
}

func seedConfirmedControlStateRPCScenario(t *testing.T, ctx context.Context, store *sqlite.Store, scenario instrumentationRPCScenario) string {
	t.Helper()

	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected tension refresh to create control-state seed tensions, got %+v", refresh)
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
		Reason:      "confirm for control-state RPC tests",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}
	return "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID
}

func requireServerControlStateCluster(t *testing.T, clusters []sqlite.ClusterControlStateCluster, clusterID string) sqlite.ClusterControlStateCluster {
	t.Helper()
	for _, cluster := range clusters {
		if cluster.ProtoClusterID == clusterID {
			return cluster
		}
	}
	t.Fatalf("control-state cluster %s not found in %+v", clusterID, clusters)
	return sqlite.ClusterControlStateCluster{}
}

func assertServerControlStatePayloadContract(t *testing.T, payloadJSON string, eventKinds []string) {
	t.Helper()

	payload := decodeEventPayloadMap(t, payloadJSON)
	if payload["typed_event_type"] != "CONTROL_STATE_INTERPRETATION" {
		t.Fatalf("expected control-state interpretation envelope, got %+v", payload)
	}
	eventKind, ok := payload["event_kind"].(string)
	if !ok || !hasServerControlStateAlias(eventKinds, eventKind) {
		t.Fatalf("expected control-state event_kind in %v, got %+v", eventKinds, payload)
	}
	if _, ok := payload["pending_tension_count"].(float64); !ok {
		t.Fatalf("expected pending_tension_count to stay visible as context, got %+v", payload)
	}
	if _, ok := payload["stability_streak"].(float64); !ok {
		t.Fatalf("expected stability_streak in control-state payload, got %+v", payload)
	}
	assertServerControlStateNoLegacyKeys(t, payload)
	assertServerControlStateRenamedKeys(t, payload)
}

func assertServerControlStatePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, surface, workspaceID, principalType, principalID string, extra map[string]string) {
	t.Helper()

	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected control-state prompt_context_envelope in runtime event payload, got %+v", payload)
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

func assertServerControlStateNoLegacyKeys(t *testing.T, payload map[string]any) {
	t.Helper()

	keys := flattenServerControlStateKeys(payload)
	for _, legacy := range []string{"current_mode", "candidate_mode", "corridor_profile", "control_vector"} {
		if keys[legacy] {
			t.Fatalf("expected control-state payload to drop legacy key %s, got keys=%v payload=%+v", legacy, sortedServerControlStateKeys(keys), payload)
		}
	}
}

func assertServerControlStateRenamedKeys(t *testing.T, payload map[string]any) {
	t.Helper()

	keys := flattenServerControlStateKeys(payload)
	assertServerControlStateHasAlias(t, keys, "current interpretation", controlStateCurrentKeyAliases)
	assertServerControlStateHasAlias(t, keys, "candidate interpretation", controlStateCandidateKeyAliases)
	assertServerControlStateHasAlias(t, keys, "scaffold profile", controlStateProfileKeyAliases)
	assertServerControlStateHasAlias(t, keys, "operator hints", controlStateHintsKeyAliases)
}

func assertServerControlStateHasAlias(t *testing.T, keys map[string]bool, label string, aliases []string) {
	t.Helper()
	for _, alias := range aliases {
		if keys[alias] {
			return
		}
	}
	t.Fatalf("expected %s key alias in payload, wanted one of %v, got keys=%v", label, aliases, sortedServerControlStateKeys(keys))
}

func flattenServerControlStateKeys(value any) map[string]bool {
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

func sortedServerControlStateKeys(keys map[string]bool) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func isServerControlStateStabilizationEvent(eventType string) bool {
	return hasServerControlStateAlias(controlStateStabilizationEventAliases, eventType)
}

func hasAnyServerControlStateCount(counts map[string]int, aliases []string) bool {
	for _, alias := range aliases {
		if counts[alias] > 0 {
			return true
		}
	}
	return false
}

func hasServerControlStateAlias(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func controlStateClusterIDsHashForServerTest(clusterIDs ...string) string {
	cleaned := make([]string, 0, len(clusterIDs))
	for _, clusterID := range clusterIDs {
		if trimmed := strings.TrimSpace(clusterID); trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	for i := 0; i < len(cleaned); i++ {
		for j := i + 1; j < len(cleaned); j++ {
			if cleaned[j] < cleaned[i] {
				cleaned[i], cleaned[j] = cleaned[j], cleaned[i]
			}
		}
	}
	sum := sha256.Sum256([]byte(strings.Join(cleaned, "\n")))
	return fmt.Sprintf("sha256:%x", sum[:])
}
