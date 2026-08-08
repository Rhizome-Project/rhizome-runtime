package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentMessageSendCarriesPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-message-prompt-context"
		fromAgentID = "agent-message-context-a"
		toAgentID   = "agent-message-context-b"
	)
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	messageID, event, err := store.SendMessageWithAuthorityEvent(ctx, sqlite.MessageSendInput{
		WorkspaceID:           workspaceID,
		FromAgentID:           fromAgentID,
		ToAgentID:             toAgentID,
		Channel:               "ops",
		ContentType:           "text/plain",
		Content:               "coordinate this",
		PromptContextEnvelope: boundAgentMessagePromptContextEnvelope("agent.message.send", workspaceID, fromAgentID, toAgentID, "ops"),
	})
	if err != nil {
		t.Fatalf("send message with prompt context: %v", err)
	}
	if messageID == "" {
		t.Fatal("expected message id")
	}
	assertAgentMessagePromptContextEnvelope(t, event.PayloadJSON, "agent.message.send", "server_rpc", workspaceID, "agent", fromAgentID)
}

func TestAgentMessagePromptContextEnvelopeRejectsWrongSurface(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-message-prompt-context-bad"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	_, _, err := store.SendMessageWithAuthorityEvent(ctx, sqlite.MessageSendInput{
		WorkspaceID:           workspaceID,
		FromAgentID:           "agent-message-context-bad-a",
		ToAgentID:             "agent-message-context-bad-b",
		Channel:               "default",
		Content:               "bad context",
		PromptContextEnvelope: boundAgentMessagePromptContextEnvelope("agent.session.start", workspaceID, "agent-message-context-bad-a", "agent-message-context-bad-b", "default"),
	})
	if err == nil {
		t.Fatal("expected wrong agent-message surface to be rejected")
	}
	if !strings.Contains(err.Error(), "not valid for agent_message") {
		t.Fatalf("unexpected error: %v", err)
	}

	var messageCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(1) FROM agent_messages WHERE workspace_id = ?`, workspaceID).Scan(&messageCount); err != nil {
		t.Fatalf("count messages after reject: %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("expected rejected prompt context to roll back agent message insert, got %d messages", messageCount)
	}
}

func TestAgentMessagePromptContextEnvelopeRejectsActorBindingMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-message-prompt-context-actor-bad"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	forged := boundAgentMessagePromptContextEnvelope("agent.message.send", workspaceID, "agent-message-context-forged", "agent-message-context-b", "ops")
	_, _, err := store.SendMessageWithAuthorityEvent(ctx, sqlite.MessageSendInput{
		WorkspaceID:           workspaceID,
		FromAgentID:           "agent-message-context-a",
		ToAgentID:             "agent-message-context-b",
		Channel:               "ops",
		Content:               "forged actor binding",
		PromptContextEnvelope: forged,
	})
	if err == nil {
		t.Fatal("expected forged agent-message prompt context actor binding to fail closed")
	}
	if !strings.Contains(err.Error(), "does not match message payload") {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNoAgentMessageRowsOrEvents(t, ctx, store, workspaceID)
}

func TestAgentMessageLegacyStoreCallDoesNotOverclaimPromptContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-message-legacy-no-context"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	_, event, err := store.SendMessageWithAuthorityEvent(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-message-legacy-a",
		ToAgentID:   "agent-message-legacy-b",
		Content:     "legacy direct call",
	})
	if err != nil {
		t.Fatalf("send legacy message: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode legacy message payload: %v", err)
	}
	if _, ok := payload["prompt_context_envelope"]; ok {
		t.Fatalf("legacy direct send without envelope must not overclaim prompt context: %+v", payload)
	}
}

func TestAgentRequestPromptContextEnvelopeCarriesRequestAndResponse(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-request-prompt-context"
		fromAgentID = "agent-request-context-a"
		toAgentID   = "agent-request-context-b"
		method      = "coordinate.context"
	)
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, requestEvent, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID:           workspaceID,
		FromAgentID:           fromAgentID,
		ToAgentID:             toAgentID,
		Method:                method,
		Payload:               `{"need":"decision"}`,
		PromptContextEnvelope: boundAgentRequestPromptContextEnvelope("agent.request", workspaceID, "", fromAgentID, fromAgentID, toAgentID, method, "PENDING"),
	})
	if err != nil {
		t.Fatalf("create request with prompt context: %v", err)
	}
	if requestID == "" {
		t.Fatal("expected request id")
	}
	assertAgentRequestPromptContextEnvelope(t, requestEvent.PayloadJSON, "agent.request", "server_rpc", workspaceID, fromAgentID, requestID, fromAgentID, toAgentID, method, "PENDING")

	responseEvent, err := store.RespondAgentRequestWithPromptContextAuthorityEvent(
		ctx,
		requestID,
		`{"ok":true}`,
		boundAgentRequestPromptContextEnvelope("agent.respond", workspaceID, requestID, toAgentID, fromAgentID, toAgentID, method, "COMPLETED"),
	)
	if err != nil {
		t.Fatalf("respond request with prompt context: %v", err)
	}
	assertAgentRequestPromptContextEnvelope(t, responseEvent.PayloadJSON, "agent.respond", "server_rpc", workspaceID, toAgentID, requestID, fromAgentID, toAgentID, method, "COMPLETED")
}

func TestAgentRequestPromptContextEnvelopeRejectsWrongSurface(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-request-prompt-context-bad"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	_, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID:           workspaceID,
		FromAgentID:           "agent-request-context-bad-a",
		ToAgentID:             "agent-request-context-bad-b",
		Method:                "bad.context",
		Payload:               `{"bad":true}`,
		PromptContextEnvelope: boundAgentRequestPromptContextEnvelope("agent.message.send", workspaceID, "", "agent-request-context-bad-a", "agent-request-context-bad-a", "agent-request-context-bad-b", "bad.context", "PENDING"),
	})
	if err == nil {
		t.Fatal("expected wrong agent-request surface to be rejected")
	}
	if !strings.Contains(err.Error(), "not valid for agent_request") {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNoAgentRequestRowsOrEvents(t, ctx, store, workspaceID)
}

func TestAgentRequestPromptContextEnvelopeRejectsActorBindingMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-request-prompt-context-actor-bad"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	forged := boundAgentRequestPromptContextEnvelope("agent.request", workspaceID, "", "agent-request-context-forged", "agent-request-context-a", "agent-request-context-b", "bad.actor", "PENDING")
	_, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID:           workspaceID,
		FromAgentID:           "agent-request-context-a",
		ToAgentID:             "agent-request-context-b",
		Method:                "bad.actor",
		Payload:               `{"bad":true}`,
		PromptContextEnvelope: forged,
	})
	if err == nil {
		t.Fatal("expected forged agent-request prompt context actor binding to fail closed")
	}
	if !strings.Contains(err.Error(), "does not match request payload") {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNoAgentRequestRowsOrEvents(t, ctx, store, workspaceID)
}

func TestAgentRequestPromptContextEnvelopeRejectsRespondSurface(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-request-prompt-context-respond-surface"
		fromAgentID = "agent-request-context-a"
		toAgentID   = "agent-request-context-b"
		method      = "wrong.surface"
	)
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	envelope := boundAgentRequestPromptContextEnvelope("agent.respond", workspaceID, "", toAgentID, fromAgentID, toAgentID, method, "PENDING")
	_, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID:           workspaceID,
		FromAgentID:           fromAgentID,
		ToAgentID:             toAgentID,
		Method:                method,
		Payload:               `{"bad":true}`,
		PromptContextEnvelope: envelope,
	})
	if err == nil {
		t.Fatal("expected request creation to reject agent.respond surface")
	}
	if !strings.Contains(err.Error(), "does not match operation surface") {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNoAgentRequestRowsOrEvents(t, ctx, store, workspaceID)
}

func TestAgentRespondPromptContextEnvelopeRejectsActorBindingMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-respond-prompt-context-actor-bad"
		fromAgentID = "agent-respond-context-a"
		toAgentID   = "agent-respond-context-b"
		method      = "respond.bad.actor"
	)
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Method:      method,
		Payload:     `{"need":"response"}`,
	})
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}

	forged := boundAgentRequestPromptContextEnvelope("agent.respond", workspaceID, requestID, "agent-respond-context-forged", fromAgentID, toAgentID, method, "COMPLETED")
	_, err = store.RespondAgentRequestWithPromptContextAuthorityEvent(ctx, requestID, `{"ok":true}`, forged)
	if err == nil {
		t.Fatal("expected forged agent-respond prompt context actor binding to fail closed")
	}
	if !strings.Contains(err.Error(), "does not match request payload") {
		t.Fatalf("unexpected error: %v", err)
	}
	assertAgentRequestStillPending(t, ctx, store, requestID)
	assertNoAgentResponseEvents(t, ctx, store, workspaceID, requestID)
}

func TestAgentRespondPromptContextEnvelopeRejectsRequestSurface(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-respond-prompt-context-request-surface"
		fromAgentID = "agent-respond-context-a"
		toAgentID   = "agent-respond-context-b"
		method      = "respond.wrong.surface"
	)
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Method:      method,
		Payload:     `{"need":"response"}`,
	})
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}

	envelope := boundAgentRequestPromptContextEnvelope("agent.request", workspaceID, requestID, fromAgentID, fromAgentID, toAgentID, method, "COMPLETED")
	_, err = store.RespondAgentRequestWithPromptContextAuthorityEvent(ctx, requestID, `{"ok":true}`, envelope)
	if err == nil {
		t.Fatal("expected response recording to reject agent.request surface")
	}
	if !strings.Contains(err.Error(), "does not match operation surface") {
		t.Fatalf("unexpected error: %v", err)
	}
	assertAgentRequestStillPending(t, ctx, store, requestID)
	assertNoAgentResponseEvents(t, ctx, store, workspaceID, requestID)
}

func TestAgentRequestLegacyStoreCallsDoNotOverclaimPromptContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-request-legacy-no-context"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, requestEvent, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-request-legacy-a",
		ToAgentID:   "agent-request-legacy-b",
		Payload:     `{"legacy":true}`,
	})
	if err != nil {
		t.Fatalf("create legacy request: %v", err)
	}
	assertNoPromptContextEnvelope(t, requestEvent.PayloadJSON)

	responseEvent, err := store.RespondAgentRequestWithAuthorityEvent(ctx, requestID, `{"done":true}`)
	if err != nil {
		t.Fatalf("respond legacy request: %v", err)
	}
	assertNoPromptContextEnvelope(t, responseEvent.PayloadJSON)
}

func TestAgentRequestListPromptContextEnvelopeClaimsRequests(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-request-list-prompt-context"
		fromAgentID = "agent-request-list-context-a"
		toAgentID   = "agent-request-list-context-b"
	)
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	for _, method := range []string{"claim.first", "claim.second"} {
		if _, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
			WorkspaceID: workspaceID,
			FromAgentID: fromAgentID,
			ToAgentID:   toAgentID,
			Method:      method,
			Payload:     `{"claim":true}`,
		}); err != nil {
			t.Fatalf("seed request %s: %v", method, err)
		}
	}

	envelope := boundAgentRequestPromptContextEnvelope("agent.request.list", workspaceID, "", toAgentID, "", toAgentID, "", "PROCESSING")
	envelope["previous_status"] = "PENDING"
	requests, events, err := store.ListPendingAgentRequestsWithPromptContextEvents(ctx, workspaceID, toAgentID, envelope)
	if err != nil {
		t.Fatalf("list pending requests with prompt context events: %v", err)
	}
	if len(requests) != 2 || len(events) != 2 {
		t.Fatalf("expected 2 claimed requests and events, got requests=%+v events=%+v", requests, events)
	}
	for idx, request := range requests {
		if request.Status != "PROCESSING" {
			t.Fatalf("expected PROCESSING request, got %+v", request)
		}
		assertAgentRequestPromptContextEnvelope(t, events[idx].PayloadJSON, "agent.request.list", "server_rpc", workspaceID, toAgentID, request.RequestID, fromAgentID, toAgentID, request.Method, "PROCESSING")
		assertAgentRequestPromptContextExtraField(t, events[idx].PayloadJSON, "previous_status", "PENDING")
	}

	secondRequests, secondEvents, err := store.ListPendingAgentRequestsWithPromptContextEvents(ctx, workspaceID, toAgentID, envelope)
	if err != nil {
		t.Fatalf("second list pending requests with prompt context events: %v", err)
	}
	if len(secondRequests) != 0 || len(secondEvents) != 0 {
		t.Fatalf("expected no second claims, got requests=%+v events=%+v", secondRequests, secondEvents)
	}
}

func TestAgentRequestListPromptContextEnvelopeClaimsTimedOutRequests(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-request-list-timeout-context"
		fromAgentID = "agent-request-timeout-context-a"
		toAgentID   = "agent-request-timeout-context-b"
	)
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Method:      "claim.timeout",
		Payload:     `{"claim":true}`,
		TimeoutSec:  1,
	})
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}

	firstEnvelope := boundAgentRequestPromptContextEnvelope("agent.request.list", workspaceID, "", toAgentID, "", toAgentID, "", "PROCESSING")
	firstEnvelope["previous_status"] = "PENDING"
	firstRequests, firstEvents, err := store.ListPendingAgentRequestsWithPromptContextEvents(ctx, workspaceID, toAgentID, firstEnvelope)
	if err != nil {
		t.Fatalf("claim request first time: %v", err)
	}
	if len(firstRequests) != 1 || len(firstEvents) != 1 || firstRequests[0].RequestID != requestID {
		t.Fatalf("expected first claim for %s, got requests=%+v events=%+v", requestID, firstRequests, firstEvents)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_requests SET responded_at = ? WHERE request_id = ?`,
		time.Now().UTC().Add(-3*time.Second).Format(time.RFC3339Nano), requestID,
	); err != nil {
		t.Fatalf("age claimed request: %v", err)
	}
	expired, err := store.ExpireRequests(ctx)
	if err != nil {
		t.Fatalf("expire request: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected expired=1, got %d", expired)
	}

	timeoutEnvelope := boundAgentRequestPromptContextEnvelope("agent.request.list", workspaceID, "", toAgentID, "", toAgentID, "", "PROCESSING")
	timeoutEnvelope["previous_status"] = "PENDING"
	requests, events, err := store.ListPendingAgentRequestsWithPromptContextEvents(ctx, workspaceID, toAgentID, timeoutEnvelope)
	if err != nil {
		t.Fatalf("claim timed-out request with prompt context events: %v", err)
	}
	if len(requests) != 1 || len(events) != 1 || requests[0].RequestID != requestID {
		t.Fatalf("expected timed-out request to be reclaimed, got requests=%+v events=%+v", requests, events)
	}
	assertAgentRequestPromptContextEnvelope(t, events[0].PayloadJSON, "agent.request.list", "server_rpc", workspaceID, toAgentID, requestID, fromAgentID, toAgentID, "claim.timeout", "PROCESSING")
	assertAgentRequestPromptContextExtraField(t, events[0].PayloadJSON, "previous_status", "TIMEOUT")
}

func TestAgentRequestListPromptContextEnvelopeRejectsActorBindingMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-request-list-prompt-context-actor-bad"
		fromAgentID = "agent-request-list-context-a"
		toAgentID   = "agent-request-list-context-b"
		method      = "claim.bad.actor"
	)
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Method:      method,
		Payload:     `{"claim":true}`,
	})
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}

	forged := boundAgentRequestPromptContextEnvelope("agent.request.list", workspaceID, "", "agent-request-list-context-forged", "", toAgentID, "", "PROCESSING")
	forged["previous_status"] = "PENDING"
	_, _, err = store.ListPendingAgentRequestsWithPromptContextEvents(ctx, workspaceID, toAgentID, forged)
	if err == nil {
		t.Fatal("expected forged claim prompt context actor binding to fail closed")
	}
	if !strings.Contains(err.Error(), "does not match request payload") {
		t.Fatalf("unexpected error: %v", err)
	}
	assertAgentRequestStillPending(t, ctx, store, requestID)
	assertNoAgentClaimEvents(t, ctx, store, workspaceID, requestID)
}

func TestAgentRequestListPromptContextEnvelopeRejectsRequestSurface(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-request-list-prompt-context-request-surface"
		fromAgentID = "agent-request-list-context-a"
		toAgentID   = "agent-request-list-context-b"
		method      = "claim.wrong.surface"
	)
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Method:      method,
		Payload:     `{"claim":true}`,
	})
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}

	envelope := boundAgentRequestPromptContextEnvelope("agent.request", workspaceID, "", fromAgentID, "", toAgentID, "", "PROCESSING")
	envelope["previous_status"] = "PENDING"
	_, _, err = store.ListPendingAgentRequestsWithPromptContextEvents(ctx, workspaceID, toAgentID, envelope)
	if err == nil {
		t.Fatal("expected request claim to reject agent.request surface")
	}
	if !strings.Contains(err.Error(), "does not match operation surface") {
		t.Fatalf("unexpected error: %v", err)
	}
	assertAgentRequestStillPending(t, ctx, store, requestID)
	assertNoAgentClaimEvents(t, ctx, store, workspaceID, requestID)
}

func TestAgentRequestListPromptContextEnvelopeRequiresEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-request-list-prompt-context-required"
		fromAgentID = "agent-request-list-context-a"
		toAgentID   = "agent-request-list-context-b"
	)
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Method:      "claim.requires.envelope",
		Payload:     `{"claim":true}`,
	})
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}

	_, _, err = store.ListPendingAgentRequestsWithPromptContextEvents(ctx, workspaceID, toAgentID, nil)
	if err == nil {
		t.Fatal("expected claim event path to require prompt context envelope")
	}
	if !strings.Contains(err.Error(), "prompt_context_envelope is required") {
		t.Fatalf("unexpected error: %v", err)
	}
	assertAgentRequestStillPending(t, ctx, store, requestID)
	assertNoAgentClaimEvents(t, ctx, store, workspaceID, requestID)
}

func boundAgentMessagePromptContextEnvelope(surface, workspaceID, fromAgentID, toAgentID, channel string) map[string]any {
	envelope := sqlite.BuildAgentMessagePromptContextEnvelope(surface, "server_rpc", workspaceID, "agent", fromAgentID)
	envelope["actor_agent_id"] = fromAgentID
	envelope["from_agent_id"] = fromAgentID
	envelope["to_agent_id"] = toAgentID
	envelope["channel"] = channel
	return envelope
}

func boundAgentRequestPromptContextEnvelope(surface, workspaceID, requestID, actorAgentID, fromAgentID, toAgentID, method, status string) map[string]any {
	envelope := sqlite.BuildAgentRequestPromptContextEnvelope(surface, "server_rpc", workspaceID, "agent", actorAgentID)
	envelope["actor_agent_id"] = actorAgentID
	if requestID != "" {
		envelope["request_id"] = requestID
	}
	envelope["from_agent_id"] = fromAgentID
	envelope["to_agent_id"] = toAgentID
	envelope["method"] = method
	envelope["status"] = status
	return envelope
}

func assertNoAgentMessageRowsOrEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()

	var messageCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(1) FROM agent_messages WHERE workspace_id = ?`, workspaceID).Scan(&messageCount); err != nil {
		t.Fatalf("count messages after reject: %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("expected rejected prompt context to roll back agent message insert, got %d messages", messageCount)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events after reject: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected rejected prompt context to write no runtime events, got %+v", events)
	}
}

func assertNoAgentRequestRowsOrEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()

	var requestCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(1) FROM agent_requests WHERE workspace_id = ?`, workspaceID).Scan(&requestCount); err != nil {
		t.Fatalf("count requests after reject: %v", err)
	}
	if requestCount != 0 {
		t.Fatalf("expected rejected prompt context to roll back agent request insert, got %d requests", requestCount)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_request.sent",
		EntityType:  "agent_request",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list request runtime events after reject: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected rejected prompt context to write no request runtime events, got %+v", events)
	}
}

func assertNoAgentResponseEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, requestID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_response.recorded",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list response runtime events after reject: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected rejected prompt context to write no response runtime events, got %+v", events)
	}
}

func assertNoAgentClaimEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, requestID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_request.claimed",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list claim runtime events after reject: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected rejected prompt context to write no claim runtime events, got %+v", events)
	}
}

func assertAgentRequestStillPending(t *testing.T, ctx context.Context, store *sqlite.Store, requestID string) {
	t.Helper()

	record, err := store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("get request after reject: %v", err)
	}
	if record.Status != "PENDING" || record.Response != "" || record.RespondedAt != "" {
		t.Fatalf("expected request to remain pending after prompt context reject, got %+v", record)
	}
}

func assertAgentMessagePromptContextEnvelope(t *testing.T, payloadJSON, wantSurface, wantOrigin, wantWorkspaceID, wantPrincipalType, wantPrincipalID string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode agent-message prompt context payload: %v; payload=%q", err, payloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected agent-message prompt_context_envelope in payload, got %+v", payload)
	}
	assertAgentMessagePromptContextField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertAgentMessagePromptContextField(t, envelope, "context_kind", "authority_bearing_agent_message")
	assertAgentMessagePromptContextField(t, envelope, "surface", wantSurface)
	assertAgentMessagePromptContextField(t, envelope, "origin", wantOrigin)
	assertAgentMessagePromptContextField(t, envelope, "workspace_id", wantWorkspaceID)
	assertAgentMessagePromptContextField(t, envelope, "principal_type", wantPrincipalType)
	assertAgentMessagePromptContextField(t, envelope, "principal_id", wantPrincipalID)
	assertAgentMessagePromptContextField(t, envelope, "authority_model", "workspace_authority")
	assertAgentMessagePromptContextField(t, envelope, "compiler_status", "non_daemon_context_envelope")
	assertAgentMessagePromptContextField(t, envelope, "daemon_prompt_compiler_convergence", "not_claimed")
	assertAgentMessagePromptContextField(t, envelope, "prompt_capability_evidence", "not_present")
}

func assertAgentRequestPromptContextEnvelope(t *testing.T, payloadJSON, wantSurface, wantOrigin, wantWorkspaceID, wantPrincipalID, wantRequestID, wantFromAgentID, wantToAgentID, wantMethod, wantStatus string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode agent-request prompt context payload: %v; payload=%q", err, payloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected agent-request prompt_context_envelope in payload, got %+v", payload)
	}
	assertAgentMessagePromptContextField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertAgentMessagePromptContextField(t, envelope, "context_kind", "authority_bearing_agent_request")
	assertAgentMessagePromptContextField(t, envelope, "surface", wantSurface)
	assertAgentMessagePromptContextField(t, envelope, "origin", wantOrigin)
	assertAgentMessagePromptContextField(t, envelope, "workspace_id", wantWorkspaceID)
	assertAgentMessagePromptContextField(t, envelope, "principal_type", "agent")
	assertAgentMessagePromptContextField(t, envelope, "principal_id", wantPrincipalID)
	assertAgentMessagePromptContextField(t, envelope, "actor_agent_id", wantPrincipalID)
	assertAgentMessagePromptContextField(t, envelope, "request_id", wantRequestID)
	assertAgentMessagePromptContextField(t, envelope, "from_agent_id", wantFromAgentID)
	assertAgentMessagePromptContextField(t, envelope, "to_agent_id", wantToAgentID)
	assertAgentMessagePromptContextField(t, envelope, "method", wantMethod)
	assertAgentMessagePromptContextField(t, envelope, "status", wantStatus)
	assertAgentMessagePromptContextField(t, envelope, "authority_model", "workspace_authority")
	assertAgentMessagePromptContextField(t, envelope, "compiler_status", "non_daemon_context_envelope")
	assertAgentMessagePromptContextField(t, envelope, "daemon_prompt_compiler_convergence", "not_claimed")
	assertAgentMessagePromptContextField(t, envelope, "prompt_capability_evidence", "not_present")
}

func assertAgentRequestPromptContextExtraField(t *testing.T, payloadJSON, key, want string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode agent-request prompt context payload: %v; payload=%q", err, payloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected agent-request prompt_context_envelope in payload, got %+v", payload)
	}
	assertAgentMessagePromptContextField(t, envelope, key, want)
}

func assertNoPromptContextEnvelope(t *testing.T, payloadJSON string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode payload without prompt context: %v; payload=%q", err, payloadJSON)
	}
	if _, ok := payload["prompt_context_envelope"]; ok {
		t.Fatalf("legacy direct call without envelope must not overclaim prompt context: %+v", payload)
	}
}

func assertAgentMessagePromptContextField(t *testing.T, envelope map[string]any, key, want string) {
	t.Helper()

	got, ok := envelope[key].(string)
	if !ok {
		t.Fatalf("agent-message prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
	}
	if got != want {
		t.Fatalf("agent-message prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
	}
}
