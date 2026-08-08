package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryInvalidationPollRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-invalidation-poll-missing-authority", "agent-handler-d2a-memory-invalidation-poll-missing-authority", "poll-missing-authority-doc")
	authCtx := withTestAgentPrincipal(ctx, workspaceID, agentID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("marshal invalidation poll params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryInvalidationPoll(authCtx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.memory.invalidation.poll")

	reloaded, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation after authority reject: %v", err)
	}
	if reloaded.DeliveredAt != "" || reloaded.LeaseExpiresAt != "" {
		t.Fatalf("expected missing-authority reject not to deliver invalidation, got %+v", reloaded)
	}
	if got := countHandlerMemoryInvalidationRuntimeEvents(t, ctx, store, workspaceID, item.InvalidationID, "memory.invalidation_delivered"); got != 0 {
		t.Fatalf("expected no memory.invalidation_delivered events after authority reject, got %d", got)
	}
}

func TestWorkspaceMemoryInvalidationPollRejectsAgentPrincipalMismatch(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-invalidation-poll-agent-mismatch", "agent-handler-d2a-memory-invalidation-poll-agent-mismatch", "poll-agent-mismatch-doc")

	result, rpcErr := h.workspaceMemoryInvalidationPoll(withTestAgentPrincipal(context.Background(), workspaceID, "agent-other"), mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr == nil {
		t.Fatal("expected permission denied for agent principal mismatch")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}

	reloaded, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation after agent mismatch reject: %v", err)
	}
	if reloaded.DeliveredAt != "" || reloaded.LeaseExpiresAt != "" {
		t.Fatalf("expected agent mismatch reject not to deliver invalidation, got %+v", reloaded)
	}
	if got := countHandlerMemoryInvalidationRuntimeEvents(t, ctx, store, workspaceID, item.InvalidationID, "memory.invalidation_delivered"); got != 0 {
		t.Fatalf("expected no delivered events after agent mismatch reject, got %d", got)
	}
}

func TestWorkspaceMemoryInvalidationAckRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-invalidation-ack-stale-authority", "agent-handler-d2a-memory-invalidation-ack-stale-authority", "ack-stale-authority-doc")
	authCtx := withTestAgentPrincipal(ctx, workspaceID, agentID)
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, rpcErr := h.workspaceMemoryInvalidationPoll(authCtx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})); rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error before stale ack: %+v", rpcErr)
	}
	before, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation before stale ack: %v", err)
	}
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-3301")

	result, rpcErr := h.workspaceMemoryInvalidationAck(authCtx, mustJSONRaw(workspaceMemoryInvalidationAckParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{item.InvalidationID},
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.memory.invalidation.ack")

	after, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation after stale ack reject: %v", err)
	}
	if after.State != before.State || after.AcknowledgedAt != before.AcknowledgedAt || after.UpdatedAt != before.UpdatedAt {
		t.Fatalf("expected stale authority reject not to ack invalidation, before=%+v after=%+v", before, after)
	}
	if got := countHandlerMemoryInvalidationRuntimeEvents(t, ctx, store, workspaceID, item.InvalidationID, "memory.invalidation_acked"); got != 0 {
		t.Fatalf("expected no memory.invalidation_acked events after authority reject, got %d", got)
	}
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
}

func TestWorkspaceMemoryInvalidationListRejectsWorkspacePrincipalMismatch(t *testing.T) {
	_, h, _, workspaceID, agentID, _ := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-invalidation-list-workspace-mismatch", "agent-handler-d2a-memory-invalidation-list-workspace-mismatch", "list-workspace-mismatch-doc")

	result, rpcErr := h.workspaceMemoryInvalidationList(withTestAgentPrincipal(context.Background(), "ws-other-memory-invalidation", agentID), mustJSONRaw(workspaceMemoryInvalidationListParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceMemoryInvalidationFailRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-invalidation-fail-stale-authority", "agent-handler-d2a-memory-invalidation-fail-stale-authority", "fail-stale-authority-doc")
	authCtx := withTestAgentPrincipal(ctx, workspaceID, agentID)
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, rpcErr := h.workspaceMemoryInvalidationPoll(authCtx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})); rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error before stale fail: %+v", rpcErr)
	}
	before, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation before stale fail: %v", err)
	}
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-3302")

	result, rpcErr := h.workspaceMemoryInvalidationFail(authCtx, mustJSONRaw(workspaceMemoryInvalidationFailParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{item.InvalidationID},
		FailureReason:   "AGENT_ERROR",
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.memory.invalidation.fail")

	after, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation after stale fail reject: %v", err)
	}
	if after.State != before.State || after.FailureCount != before.FailureCount || after.LastFailureAt != before.LastFailureAt || after.UpdatedAt != before.UpdatedAt {
		t.Fatalf("expected stale authority reject not to fail invalidation, before=%+v after=%+v", before, after)
	}
	if got := countHandlerMemoryInvalidationRuntimeEvents(t, ctx, store, workspaceID, item.InvalidationID, "memory.invalidation_failed"); got != 0 {
		t.Fatalf("expected no memory.invalidation_failed events after authority reject, got %d", got)
	}
	if got := countHandlerMemoryInvalidationRuntimeEvents(t, ctx, store, workspaceID, item.InvalidationID, "memory.invalidation_dead_lettered"); got != 0 {
		t.Fatalf("expected no memory.invalidation_dead_lettered events after authority reject, got %d", got)
	}
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
}

func TestWorkspaceMemoryInvalidationRequeueRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-invalidation-requeue-stale-authority", "agent-handler-d2a-memory-invalidation-requeue-stale-authority", "requeue-stale-authority-doc")
	authCtx := withTestAgentPrincipal(ctx, workspaceID, agentID)
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, rpcErr := h.workspaceMemoryInvalidationPoll(authCtx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})); rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error before dead-letter loop: %+v", rpcErr)
	}
	for attempt := 0; attempt < 3; attempt++ {
		if _, rpcErr := h.workspaceMemoryInvalidationFail(authCtx, mustJSONRaw(workspaceMemoryInvalidationFailParams{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{item.InvalidationID},
			FailureReason:   "AGENT_ERROR",
		})); rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationFail rpc error on attempt %d: %+v", attempt+1, rpcErr)
		}
		if attempt < 2 {
			redeliverMemoryInvalidationForServerTest(t, store, ctx, workspaceID, agentID, item.InvalidationID)
		}
	}
	before, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation before stale requeue: %v", err)
	}
	if before.State != "DEAD_LETTER" {
		t.Fatalf("expected dead-letter invalidation before stale requeue, got %+v", before)
	}
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-3303")

	result, rpcErr := h.workspaceMemoryInvalidationRequeue(authCtx, mustJSONRaw(workspaceMemoryInvalidationRequeueParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{item.InvalidationID},
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.memory.invalidation.requeue")

	after, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation after stale requeue reject: %v", err)
	}
	if after.State != before.State || after.UpdatedAt != before.UpdatedAt || after.RecoveredFromInvalidationID != before.RecoveredFromInvalidationID {
		t.Fatalf("expected stale authority reject not to requeue invalidation, before=%+v after=%+v", before, after)
	}
	if got := countHandlerMemoryInvalidationRuntimeEvents(t, ctx, store, workspaceID, item.InvalidationID, "memory.invalidation_requeued"); got != 0 {
		t.Fatalf("expected no memory.invalidation_requeued events after authority reject, got %d", got)
	}
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
}

func countHandlerMemoryInvalidationRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, invalidationID, eventType string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "memory_invalidation",
		EntityID:    invalidationID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s: %v", eventType, invalidationID, err)
	}
	return len(events)
}

func redeliverMemoryInvalidationForServerTest(t *testing.T, store *sqlite.Store, ctx context.Context, workspaceID, agentID, invalidationID string) sqlite.MemoryInvalidationRecord {
	t.Helper()

	setHandlerMemoryInvalidationQueueStringColumn(t, store, invalidationID, "next_delivery_at", "2000-01-01T00:00:00Z")
	items, _, err := store.PollMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("redeliver invalidation %s: %v", invalidationID, err)
	}
	for _, item := range items {
		if item.InvalidationID == invalidationID {
			return item
		}
	}
	t.Fatalf("expected invalidation %s to be redelivered, got %+v", invalidationID, items)
	return sqlite.MemoryInvalidationRecord{}
}
