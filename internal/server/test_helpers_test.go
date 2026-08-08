package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func withTestPrincipal(ctx context.Context, workspaceID, principalType, principalID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   workspaceID,
		PrincipalType: principalType,
		PrincipalID:   principalID,
	})
}

func testAuthContext(workspaceID, principalType, principalID string) context.Context {
	return withTestPrincipal(context.Background(), workspaceID, principalType, principalID)
}

func seedRuntimeEventParents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, eventIDs ...string) {
	t.Helper()

	for idx, eventID := range eventIDs {
		if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:     eventID,
			WorkspaceID: workspaceID,
			EventType:   "runtime.parent",
			EntityType:  "runtime_event_parent",
			EntityID:    eventID,
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"seed":"parent"}`,
			CreatedAt:   time.Date(2026, time.March, 22, 0, 0, idx, 0, time.UTC).Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("seed runtime parent %s: %v", eventID, err)
		}
	}
}

func assertLiveEventMirrorsRuntimeEvent(t *testing.T, live EventMessage, runtimeEvent sqlite.RuntimeEventRecord, expectedType string) {
	t.Helper()

	assertLiveEventMirrorsRuntimeEventWithAgentID(t, live, runtimeEvent, expectedType, runtimeEvent.AgentID)
}

func assertLiveEventMirrorsRuntimeEventWithAgentID(t *testing.T, live EventMessage, runtimeEvent sqlite.RuntimeEventRecord, expectedType, expectedAgentID string) {
	t.Helper()

	if expectedType == "" {
		expectedType = runtimeEvent.EventType
	}
	if live.Type != expectedType {
		t.Fatalf("expected live type %q, got %+v", expectedType, live)
	}
	if live.CanonicalEventType != runtimeEvent.EventType ||
		live.WorkspaceID != runtimeEvent.WorkspaceID ||
		live.AgentID != expectedAgentID ||
		live.EventID != runtimeEvent.EventID ||
		live.IngestSeq != runtimeEvent.IngestSeq ||
		live.DedupKey != runtimeEvent.DedupKey ||
		live.EntityType != runtimeEvent.EntityType ||
		live.EntityID != runtimeEvent.EntityID ||
		live.RootCauseID != runtimeEvent.RootCauseID ||
		live.ProvenanceGroupID != runtimeEvent.ProvenanceGroupID ||
		live.ParentRefsJSON != runtimeEvent.ParentRefsJSON ||
		live.Timestamp != runtimeEvent.CreatedAt {
		t.Fatalf("expected live event %+v to mirror runtime event %+v", live, runtimeEvent)
	}
}

type runtimeEventExpectation struct {
	Event sqlite.RuntimeEventRecord
	Type  string
}

func nextLiveEventsMirroringRuntimeEventsInOrder(t *testing.T, ch <-chan EventMessage, expectations ...runtimeEventExpectation) ([]runtimeEventExpectation, []EventMessage) {
	t.Helper()

	ordered := append([]runtimeEventExpectation(nil), expectations...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return runtimeEventChronologicalLess(ordered[i].Event, ordered[j].Event)
	})

	liveEvents := make([]EventMessage, 0, len(ordered))
	for _, expectation := range ordered {
		live := nextEvent(t, ch)
		assertLiveEventMirrorsRuntimeEvent(t, live, expectation.Event, expectation.Type)
		liveEvents = append(liveEvents, live)
	}
	return ordered, liveEvents
}

func assertNoEventWithin(t *testing.T, ch <-chan EventMessage, wait time.Duration) {
	t.Helper()

	select {
	case evt := <-ch:
		t.Fatalf("expected no live event within %s, got %+v", wait, evt)
	case <-time.After(wait):
	}
}

func assertSSEAliasTypeAndOptionalCanonicalEventType(t *testing.T, live EventMessage, wantCanonicalType string) {
	t.Helper()

	frame := FormatSSE(live)
	prefix := fmt.Sprintf("event: %s\n", live.Type)
	if !strings.HasPrefix(frame, prefix) {
		t.Fatalf("expected SSE frame to preserve alias event line %q, got %q", prefix, frame)
	}

	dataPrefix := "data: "
	idx := strings.Index(frame, dataPrefix)
	if idx < 0 {
		t.Fatalf("expected SSE frame to include data line, got %q", frame)
	}
	payloadLine := frame[idx+len(dataPrefix):]
	if end := strings.Index(payloadLine, "\n"); end >= 0 {
		payloadLine = payloadLine[:end]
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadLine), &payload); err != nil {
		t.Fatalf("decode SSE payload %q: %v", payloadLine, err)
	}
	if gotAlias, _ := payload["type"].(string); gotAlias != live.Type {
		t.Fatalf("expected SSE payload type %q, got %+v", live.Type, payload)
	}

	gotCanonical, _ := payload["canonical_event_type"].(string)
	if strings.TrimSpace(gotCanonical) == "" {
		t.Fatal("expected EventMessage to expose canonical_event_type")
	}
	if gotCanonical != wantCanonicalType {
		t.Fatalf("expected canonical_event_type %q, got %+v", wantCanonicalType, payload)
	}
}

func snapshotRuntimeEventIDs(t *testing.T, ctx context.Context, store *sqlite.Store, filter sqlite.RuntimeEventFilter) map[string]struct{} {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events %+v: %v", filter, err)
	}
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		if strings.TrimSpace(event.EventID) == "" {
			continue
		}
		seen[event.EventID] = struct{}{}
	}
	return seen
}

func mustNewRuntimeEvent(t *testing.T, ctx context.Context, store *sqlite.Store, filter sqlite.RuntimeEventFilter, seen map[string]struct{}) sqlite.RuntimeEventRecord {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events %+v: %v", filter, err)
	}
	for _, event := range events {
		if _, ok := seen[event.EventID]; ok {
			continue
		}
		return event
	}
	t.Fatalf("expected new runtime event for filter %+v beyond seen ids %v; got %+v", filter, seen, events)
	return sqlite.RuntimeEventRecord{}
}

func assertRuntimeEventSnapshotUnchanged(t *testing.T, ctx context.Context, store *sqlite.Store, filter sqlite.RuntimeEventFilter, seen map[string]struct{}) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events %+v: %v", filter, err)
	}
	if len(events) != len(seen) {
		t.Fatalf("expected runtime event snapshot %+v to stay unchanged, saw %d events vs %d ids", filter, len(events), len(seen))
	}
	for _, event := range events {
		if _, ok := seen[event.EventID]; !ok {
			t.Fatalf("expected runtime event snapshot %+v to stay unchanged, found unexpected event %+v", filter, event)
		}
	}
}

func mustExecutionStepFromDetail(t *testing.T, detail sqlite.ExecutionRunDetail, stepID string) sqlite.ExecutionStepRecord {
	t.Helper()

	for _, step := range detail.Steps {
		if strings.TrimSpace(step.StepID) == strings.TrimSpace(stepID) {
			return step
		}
	}
	t.Fatalf("expected execution step %q in detail %+v", stepID, detail.Steps)
	return sqlite.ExecutionStepRecord{}
}

func assertRuntimeEventParentRefsContain(t *testing.T, event sqlite.RuntimeEventRecord, wantRefs ...string) {
	t.Helper()

	var got []string
	if err := json.Unmarshal([]byte(firstNonEmpty(strings.TrimSpace(event.ParentRefsJSON), "[]")), &got); err != nil {
		t.Fatalf("decode parent_refs_json for %s: %v", event.EventID, err)
	}
	seen := make(map[string]struct{}, len(got))
	for _, ref := range got {
		seen[strings.TrimSpace(ref)] = struct{}{}
	}
	for _, want := range wantRefs {
		trimmed := strings.TrimSpace(want)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; !ok {
			t.Fatalf("expected parent_refs_json for %s to contain %q, got %q", event.EventID, trimmed, event.ParentRefsJSON)
		}
	}
}

func assertSecondRetryPromotionFailsClosed(t *testing.T, ctx context.Context, store *sqlite.Store, h *Handler, workspaceID, failedActionID, sourceQueueID string) string {
	t.Helper()

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("first retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, ok := retryCreateResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected first retry actionCreate response %+v", retryCreateResp)
	}
	if got, _ := retryCreateResp["source_queue_id"].(string); got != sourceQueueID {
		t.Fatalf("first retry actionCreate source_queue_id = %q, want %q", got, sourceQueueID)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)

	if _, rpcErr := h.actionCreate(ctx, retryCreateRaw); rpcErr == nil {
		t.Fatal("expected second retry actionCreate to fail once the reopened source queue is relinked")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "already linked to action") {
		t.Fatalf("expected duplicate retry promotion to fail closed, got %+v", rpcErr)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("ListHumanActions after rejected second retry create: %v", err)
	}
	if len(actions) != 2 {
		t.Fatalf("rejected second retry create should not materialize a third action, got %+v", actions)
	}

	retryLinkedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after rejected second retry create: %v", sourceQueueID, err)
	}
	retryLinkedPayload, err := actionCreateDecodeQueuePayload(retryLinkedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode retry-linked source queue payload: %v", err)
	}
	if retryLinkedPayload.ActionID != retryActionID {
		t.Fatalf("active retry action_id = %q, want %q", retryLinkedPayload.ActionID, retryActionID)
	}
	if retryLinkedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("retry payload should preserve failed attempt lineage, got %+v", retryLinkedPayload)
	}
	return retryActionID
}

func mustReconcileMemoryProjectionWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()

	if _, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 64); err != nil {
		t.Fatalf("reconcile memory projection workspace %s: %v", workspaceID, err)
	}
}
