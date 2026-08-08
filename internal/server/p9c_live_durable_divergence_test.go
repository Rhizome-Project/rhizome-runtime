package server

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceEventsDurableLogStaysAuthoritativeWhenLiveSubscriberDropsOverflow(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-p9c-live-durable-divergence"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P9C Live/Durable Divergence",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "tests",
		DisplayName: "agent-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	const eventCount = 40
	expectedLiveCount := cap(ch)
	if expectedLiveCount != 32 {
		t.Fatalf("expected workspace event subscriber buffer size to stay at 32 for overflow coverage, got %d", expectedLiveCount)
	}

	publishedIDs := make([]string, 0, eventCount)
	durableIDs := make(map[string]struct{}, eventCount)
	for i := 0; i < eventCount; i++ {
		eventID := fmt.Sprintf("rtev-p9c-live-drop-%02d", i)
		record, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:     eventID,
			WorkspaceID: workspaceID,
			EventType:   "agent.update",
			EntityType:  "agent_update",
			EntityID:    fmt.Sprintf("update-%02d", i),
			ActorType:   "agent",
			ActorID:     "agent-a",
			AgentID:     "agent-a",
			PayloadJSON: fmt.Sprintf(`{"seq":%d}`, i),
			CreatedAt:   time.Date(2026, time.April, 9, 12, 0, i, 0, time.UTC).Format(time.RFC3339Nano),
		})
		if err != nil {
			t.Fatalf("record runtime event %s: %v", eventID, err)
		}
		h.publishRuntimeEventRecord(record, fmt.Sprintf("event %02d", i))
		publishedIDs = append(publishedIDs, eventID)
		durableIDs[eventID] = struct{}{}
	}

	liveEvents := drainLiveEvents(ch)
	if len(liveEvents) != expectedLiveCount {
		t.Fatalf("expected slow live subscriber to retain only %d buffered events, got %d", expectedLiveCount, len(liveEvents))
	}
	for idx, live := range liveEvents {
		expectedEventID := publishedIDs[idx]
		if live.EventID != expectedEventID {
			t.Fatalf("expected live overflow buffer to preserve first buffered events in publish order, idx=%d expected=%s got=%+v", idx, expectedEventID, live)
		}
		if _, ok := durableIDs[live.EventID]; !ok {
			t.Fatalf("expected live event %+v to remain present in durable runtime journal", live)
		}
	}
	for _, droppedID := range publishedIDs[expectedLiveCount:] {
		for _, live := range liveEvents {
			if live.EventID == droppedID {
				t.Fatalf("expected overflowed live subscriber to drop trailing event %s, but it stayed in %+v", droppedID, liveEvents)
			}
		}
	}

	rawList, err := json.Marshal(workspaceEventsListParams{
		WorkspaceID: workspaceID,
		Limit:       eventCount + 5,
	})
	if err != nil {
		t.Fatalf("marshal events list params: %v", err)
	}
	listResult, rpcErr := h.workspaceEventsList(testAuthContext(workspaceID, "human", "developer"), rawList)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsList rpc error: %+v", rpcErr)
	}
	listPayload, ok := listResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected events list result type %T", listResult)
	}
	items, ok := listPayload["items"].([]sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected events list payload type %T", listPayload["items"])
	}
	if len(items) != eventCount {
		t.Fatalf("expected durable events list to retain all %d events despite live overflow, got %d", eventCount, len(items))
	}
	for _, event := range items {
		if _, ok := durableIDs[event.EventID]; !ok {
			t.Fatalf("durable events list returned unexpected event %+v", event)
		}
		delete(durableIDs, event.EventID)
	}
	if len(durableIDs) != 0 {
		t.Fatalf("expected durable events list to cover every published event, missing=%v", durableIDs)
	}
	for idx, expectedEventID := range reverseStrings(publishedIDs[expectedLiveCount:]) {
		if items[idx].EventID != expectedEventID {
			t.Fatalf("expected durable events list to recover dropped tail in descending ingest order, idx=%d expected=%s got=%+v", idx, expectedEventID, items[idx])
		}
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         eventCount + 5,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	replayResult, rpcErr := h.workspaceEventsReplay(testAuthContext(workspaceID, "human", "developer"), rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayPayload, ok := replayResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", replayResult)
	}
	report, ok := replayPayload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", replayPayload["report"])
	}
	if len(report.Events) != eventCount {
		t.Fatalf("expected replay report to retain all %d durable events despite live overflow, got %d", eventCount, len(report.Events))
	}
	if report.Truncated || report.WindowIncomplete {
		t.Fatalf("expected full replay recovery after live overflow to stay complete, got %+v", report)
	}
	if report.Evaluation.Verdict != "pass" || report.Evaluation.FindingSummary.ScopePartialCount != 0 {
		t.Fatalf("expected full replay recovery after live overflow not to look partial, got evaluation=%+v", report.Evaluation)
	}
	for idx, expectedEventID := range reverseStrings(publishedIDs[expectedLiveCount:]) {
		if report.Events[idx].EventID != expectedEventID {
			t.Fatalf("expected replay report to recover dropped tail in descending ingest order, idx=%d expected=%s got=%+v", idx, expectedEventID, report.Events[idx])
		}
	}
}

func drainLiveEvents(ch <-chan EventMessage) []EventMessage {
	events := make([]EventMessage, 0)
	for {
		select {
		case evt := <-ch:
			events = append(events, evt)
		default:
			return events
		}
	}
}

func reverseStrings(items []string) []string {
	out := make([]string, len(items))
	for i := range items {
		out[len(items)-1-i] = items[i]
	}
	return out
}
