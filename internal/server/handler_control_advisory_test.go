package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationControlRPCSurfaceAndSSE(t *testing.T) {
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
	confirmed, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "confirm for advisory control read-side",
	})
	if err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	rawReport, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal control report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationControlReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlReport rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected report result type %T", result)
	}
	report, ok := reportPayload["report"].(sqlite.ControlReport)
	if !ok {
		t.Fatalf("unexpected report payload type %T", reportPayload["report"])
	}
	if report.WorkspaceID != scenario.workspaceID || len(report.Clusters) == 0 {
		t.Fatalf("expected populated control report for %s, got %+v", scenario.workspaceID, report)
	}
	cluster := requireServerControlCluster(t, report.Clusters, "task:"+scenario.workspaceID+"/"+scenario.primaryTaskID)
	if cluster.ConfirmedCountsByType["bottleneck"] == 0 || cluster.ConfirmedTensionCount == 0 {
		t.Fatalf("expected confirmed bottleneck tension in control report, got %+v", cluster)
	}
	if cluster.Signals.AttentionBand != "HOT" {
		t.Fatalf("expected hot control cluster, got %+v", cluster.Signals)
	}
	if cluster.SuggestedControls.PriorityFocus != "throughput" {
		t.Fatalf("expected throughput focus for blocked task cluster, got %+v", cluster.SuggestedControls)
	}

	rawCluster, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: cluster.ProtoClusterID,
	})
	if err != nil {
		t.Fatalf("marshal control cluster params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlCluster(ctx, rawCluster)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlCluster rpc error: %+v", rpcErr)
	}
	detailPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected control cluster result type %T", result)
	}
	detail, ok := detailPayload["detail"].(sqlite.ControlClusterDetail)
	if !ok {
		t.Fatalf("unexpected control cluster detail payload type %T", detailPayload["detail"])
	}
	if detail.Cluster.ProtoClusterID != cluster.ProtoClusterID {
		t.Fatalf("expected control cluster detail %s, got %+v", cluster.ProtoClusterID, detail.Cluster)
	}
	if !containsServerTensionID(detail.Tensions, confirmed.Tension.TensionID) {
		t.Fatalf("expected control cluster detail to include confirmed tension %s, got %+v", confirmed.Tension.TensionID, detail.Tensions)
	}

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	rawSnapshot, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "dashboard",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal control snapshot params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlSnapshot(ctx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected snapshot result type %T", result)
	}
	snapshotReport, ok := snapshotPayload["report"].(sqlite.ControlReport)
	if !ok {
		t.Fatalf("unexpected snapshot report payload type %T", snapshotPayload["report"])
	}
	if snapshotReport.WorkspaceID != scenario.workspaceID || snapshotReport.Workspace.HotClusterCount == 0 {
		t.Fatalf("expected snapshot report workspace summary for %s, got %+v", scenario.workspaceID, snapshotReport)
	}
	event, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected snapshot event payload type %T", snapshotPayload["event"])
	}
	if event.EventType != "cluster.control_advisory_snapshot" || event.EntityType != "instrumentation_control" || event.EntityID != scenario.workspaceID {
		t.Fatalf("unexpected control advisory snapshot event %+v", event)
	}

	liveEvent := nextEvent(t, ch)
	if liveEvent.Type != "cluster.control_advisory_snapshot" {
		t.Fatalf("expected cluster.control_advisory_snapshot live event, got %+v", liveEvent)
	}
	assertValidEventTimestamp(t, liveEvent.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, liveEvent, event, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), event.PayloadJSON)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.control_advisory_snapshot",
		EntityType:  "instrumentation_control",
		EntityID:    scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list control advisory snapshot runtime events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one persisted cluster.control_advisory_snapshot event, got %+v", events)
	}
}

func TestWorkspaceInstrumentationControlRPCContractsRejectMissingRequiredParams(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	tests := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params workspaceInstrumentationControlParams
	}{
		{name: "report", call: h.workspaceInstrumentationControlReport, params: workspaceInstrumentationControlParams{Limit: 10}},
		{name: "cluster missing workspace", call: h.workspaceInstrumentationControlCluster, params: workspaceInstrumentationControlParams{ProtoClusterID: "task:ws/task"}},
		{name: "cluster missing proto cluster", call: h.workspaceInstrumentationControlCluster, params: workspaceInstrumentationControlParams{WorkspaceID: "ws-test"}},
		{name: "snapshot", call: h.workspaceInstrumentationControlSnapshot, params: workspaceInstrumentationControlParams{ActorID: "dashboard", Limit: 10}},
	}

	for _, tc := range tests {
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
}

func TestWorkspaceInstrumentationControlClusterRejectsUnknownProtoCluster(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	raw, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: "task:" + scenario.workspaceID + "/missing",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	if _, rpcErr := h.workspaceInstrumentationControlCluster(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for unknown proto cluster, got %+v", rpcErr)
	}
}

func TestWorkspaceInstrumentationControlRPCSupportsScopedClusterReadsAndSnapshots(t *testing.T) {
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
		Reason:      "confirm for scoped advisory control RPC",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID
	rawReport, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("marshal scoped control report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationControlReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlReport scoped rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected scoped report result type %T", result)
	}
	report, ok := reportPayload["report"].(sqlite.ControlReport)
	if !ok {
		t.Fatalf("unexpected scoped report payload type %T", reportPayload["report"])
	}
	if report.Filter.ProtoClusterID != clusterID {
		t.Fatalf("expected report filter proto_cluster_id %s, got %+v", clusterID, report.Filter)
	}
	if len(report.Clusters) != 1 || report.Clusters[0].ProtoClusterID != clusterID {
		t.Fatalf("expected one scoped control cluster %s, got %+v", clusterID, report.Clusters)
	}

	rawCluster, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
	})
	if err != nil {
		t.Fatalf("marshal scoped control cluster params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlCluster(ctx, rawCluster)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlCluster scoped rpc error: %+v", rpcErr)
	}
	detailPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected scoped detail result type %T", result)
	}
	detail, ok := detailPayload["detail"].(sqlite.ControlClusterDetail)
	if !ok {
		t.Fatalf("unexpected scoped detail payload type %T", detailPayload["detail"])
	}
	if detail.Cluster.ProtoClusterID != clusterID || len(detail.Tensions) == 0 {
		t.Fatalf("expected scoped control detail for %s, got %+v", clusterID, detail)
	}

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	rawSnapshot, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		ActorID:        "dashboard",
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("marshal scoped control snapshot params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlSnapshot(ctx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlSnapshot scoped rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected scoped snapshot result type %T", result)
	}
	snapshotReport, ok := snapshotPayload["report"].(sqlite.ControlReport)
	if !ok {
		t.Fatalf("unexpected scoped snapshot report payload type %T", snapshotPayload["report"])
	}
	if snapshotReport.Filter.ProtoClusterID != clusterID || len(snapshotReport.Clusters) != 1 {
		t.Fatalf("expected scoped snapshot report for %s, got %+v", clusterID, snapshotReport)
	}
	event, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected scoped snapshot event payload type %T", snapshotPayload["event"])
	}
	if event.EntityID != clusterID || event.EventType != "cluster.control_advisory_snapshot" {
		t.Fatalf("unexpected scoped control snapshot event %+v", event)
	}

	liveEvent := nextEvent(t, ch)
	if liveEvent.Type != "cluster.control_advisory_snapshot" {
		t.Fatalf("expected cluster.control_advisory_snapshot live event, got %+v", liveEvent)
	}
	assertValidEventTimestamp(t, liveEvent.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, liveEvent, event, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), event.PayloadJSON)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.control_advisory_snapshot",
		EntityType:  "instrumentation_control",
		EntityID:    clusterID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list scoped control advisory snapshot runtime events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one persisted scoped cluster.control_advisory_snapshot event, got %+v", events)
	}
}

func TestWorkspaceInstrumentationControlSnapshotAppearsInReplayWithoutChangingReport(t *testing.T) {
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
		Reason:      "confirm for replay-ish advisory control RPC",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID
	rawReport, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal baseline control report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationControlReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlReport baseline rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected baseline report result type %T", result)
	}
	baselineReport, ok := reportPayload["report"].(sqlite.ControlReport)
	if !ok {
		t.Fatalf("unexpected baseline report payload type %T", reportPayload["report"])
	}
	baselineCluster := requireServerControlCluster(t, baselineReport.Clusters, clusterID)

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	rawSnapshot, err := json.Marshal(workspaceInstrumentationControlParams{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "dashboard",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal snapshot params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationControlSnapshot(ctx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected snapshot result type %T", result)
	}
	event, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected snapshot event payload type %T", snapshotPayload["event"])
	}
	if event.EventType != "cluster.control_advisory_snapshot" {
		t.Fatalf("expected cluster.control_advisory_snapshot event, got %+v", event)
	}

	liveEvent := nextEvent(t, ch)
	if liveEvent.Type != "cluster.control_advisory_snapshot" {
		t.Fatalf("expected cluster.control_advisory_snapshot live event, got %+v", liveEvent)
	}
	assertValidEventTimestamp(t, liveEvent.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, liveEvent, event, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), event.PayloadJSON)

	rawEvents, err := json.Marshal(workspaceEventsListParams{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.control_advisory_snapshot",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal control snapshot events params: %v", err)
	}
	result, rpcErr = h.workspaceEventsList(ctx, rawEvents)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsList control snapshot rpc error: %+v", rpcErr)
	}
	eventListPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected control snapshot events list type %T", result)
	}
	snapshotEvents, ok := eventListPayload["items"].([]sqlite.RuntimeEventRecord)
	if !ok || len(snapshotEvents) != 1 || snapshotEvents[0].EventID != event.EventID {
		t.Fatalf("expected one persisted control snapshot event, got %+v", eventListPayload)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   scenario.workspaceID,
		IncludeEvents: true,
		Limit:         50,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr = h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	replay, ok := replayPayload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay payload type %T", replayPayload["report"])
	}
	snapshotCount := 0
	for _, item := range replay.Events {
		if item.EventType == "cluster.control_advisory_snapshot" {
			snapshotCount++
		}
	}
	if snapshotCount != 1 {
		t.Fatalf("expected replay to include one control advisory snapshot event, got %d from %+v", snapshotCount, replay.Events)
	}

	result, rpcErr = h.workspaceInstrumentationControlReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationControlReport after replay rpc error: %+v", rpcErr)
	}
	reportPayload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected post-snapshot report result type %T", result)
	}
	afterReport, ok := reportPayload["report"].(sqlite.ControlReport)
	if !ok {
		t.Fatalf("unexpected post-snapshot report payload type %T", reportPayload["report"])
	}
	afterCluster := requireServerControlCluster(t, afterReport.Clusters, clusterID)
	if afterCluster.Signals != baselineCluster.Signals {
		t.Fatalf("expected synthetic snapshot replay not to drift control signals, got baseline=%+v after=%+v", baselineCluster.Signals, afterCluster.Signals)
	}
	if afterCluster.SuggestedControls != baselineCluster.SuggestedControls {
		t.Fatalf("expected synthetic snapshot replay not to drift suggested controls, got baseline=%+v after=%+v", baselineCluster.SuggestedControls, afterCluster.SuggestedControls)
	}
	if afterCluster.BasisStale != baselineCluster.BasisStale || afterCluster.LastTensionBasisAt != baselineCluster.LastTensionBasisAt {
		t.Fatalf("expected synthetic snapshot replay not to drift basis freshness, got baseline=%+v after=%+v", baselineCluster, afterCluster)
	}
	if afterCluster.ConfirmedTensionCount != baselineCluster.ConfirmedTensionCount || afterCluster.PendingTensionCount != baselineCluster.PendingTensionCount {
		t.Fatalf("expected synthetic snapshot replay not to change control counts, got baseline=%+v after=%+v", baselineCluster, afterCluster)
	}
}

func requireServerControlCluster(t *testing.T, clusters []sqlite.ControlClusterReport, clusterID string) sqlite.ControlClusterReport {
	t.Helper()
	for _, cluster := range clusters {
		if cluster.ProtoClusterID == clusterID {
			return cluster
		}
	}
	t.Fatalf("control cluster %s not found in %+v", clusterID, clusters)
	return sqlite.ControlClusterReport{}
}

func containsServerTensionID(items []sqlite.TensionRecord, tensionID string) bool {
	for _, item := range items {
		if item.TensionID == tensionID {
			return true
		}
	}
	return false
}
