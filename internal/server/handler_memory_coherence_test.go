package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryCoherenceReportScopeAndSnapshot(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-coherence", "agent-handler-memory-coherence", "coherence-doc")

	if _, err := store.ReportMemoryMetrics(ctx, sqlite.MemoryMetricsReportInput{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		ReportID:      "memmet-handler-coherence",
		LookupCount:   8,
		L1HitCount:    3,
		L2HitCount:    1,
		StaleHitCount: 2,
	}); err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}

	reportAny, rpcErr := callWorkspaceMemoryCoherenceReportRaw(t, h, ctx, mustJSONRaw(workspaceMemoryCoherenceReportParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryCoherenceReport rpc error: %+v", rpcErr)
	}
	report := reportAny.(sqlite.MemoryCoherenceReport)
	if report.ScopeCount != 1 || len(report.Items) != 1 {
		t.Fatalf("unexpected memory coherence report %+v", report)
	}
	if report.Items[0].ReadyInvalidationCount != 1 || report.Items[0].AgentID != item.AgentID {
		t.Fatalf("unexpected memory coherence scope %+v", report.Items[0])
	}

	scopeAny, rpcErr := callWorkspaceMemoryCoherenceScopeRaw(t, h, ctx, mustJSONRaw(workspaceMemoryCoherenceScopeParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryCoherenceScope rpc error: %+v", rpcErr)
	}
	scope := scopeAny.(sqlite.MemoryCoherenceScopeReport)
	if scope.AgentID != agentID || scope.ReportScope != "AGENT" {
		t.Fatalf("unexpected memory coherence scope payload %+v", scope)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	snapshotAny, rpcErr := callWorkspaceMemoryCoherenceSnapshotRaw(t, h, ctx, mustJSONRaw(workspaceMemoryCoherenceReportParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryCoherenceSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload := snapshotAny.(map[string]any)
	event := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if event.EventType != "memory.coherence_snapshot" || event.EntityType != "memory_coherence" {
		t.Fatalf("unexpected memory coherence snapshot event %+v", event)
	}
	liveEvent := expectMemoryInvalidationEvent(t, ch, "memory.coherence_snapshot")
	if liveEvent.EventID != event.EventID || liveEvent.IngestSeq != event.IngestSeq {
		t.Fatalf("expected live memory coherence snapshot to mirror persisted runtime envelope, live=%+v event=%+v", liveEvent, event)
	}
	if liveEvent.EntityType != event.EntityType || liveEvent.EntityID != event.EntityID {
		t.Fatalf("expected live memory coherence snapshot entity envelope to match persisted event, live=%+v event=%+v", liveEvent, event)
	}
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), event.PayloadJSON)
}

func TestWorkspaceMemoryCoherenceScopeRequiresAgent(t *testing.T) {
	h := NewHandler(newServerTestStore(t))
	if _, rpcErr := h.workspaceMemoryCoherenceScope(context.Background(), mustJSONRaw(workspaceMemoryCoherenceScopeParams{
		WorkspaceID: "ws-memory-coherence-missing-agent",
	})); rpcErr == nil {
		t.Fatal("expected missing agent_id error")
	}
}
