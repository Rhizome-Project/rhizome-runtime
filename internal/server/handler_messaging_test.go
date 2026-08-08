package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func ensureMessagingTestWorkspaceAndAgents(t *testing.T, store *sqlite.Store, workspaceID string, agentIDs ...string) {
	t.Helper()

	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return
	}
	ctx := context.Background()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "messaging-test",
	}); err != nil && !strings.Contains(err.Error(), "UNIQUE constraint failed") {
		t.Fatalf("ensure workspace %s: %v", workspaceID, err)
	}

	seen := make(map[string]struct{}, len(agentIDs))
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, ok := seen[agentID]; ok {
			continue
		}
		seen[agentID] = struct{}{}
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "messaging-test",
			DisplayName: agentID,
		}); err != nil && !strings.Contains(err.Error(), "UNIQUE constraint failed") {
			t.Fatalf("ensure agent %s in %s: %v", agentID, workspaceID, err)
		}
	}

	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
}

func TestAgentMessagePollRechecksAfterSubscribeToCloseRace(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-poll-race"
	agentID := "agent-b"
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", agentID)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	timeoutSec := 1

	var hookErr error
	agentMessagePollBeforeSubscribeHook = func() {
		agentMessagePollBeforeSubscribeHook = nil
		_, hookErr = store.SendMessage(ctx, sqlite.MessageSendInput{
			WorkspaceID: workspaceID,
			FromAgentID: "agent-a",
			ToAgentID:   agentID,
			Content:     "landed in the race window",
		})
	}
	defer func() {
		agentMessagePollBeforeSubscribeHook = nil
		agentMessagePollAfterSubscribeHook = nil
	}()

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         20,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if hookErr != nil {
		t.Fatalf("hook send message: %v", hookErr)
	}
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}

	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if messages[0].Content != "landed in the race window" {
		t.Fatalf("expected race-window message, got %+v", messages[0])
	}
	if count, ok := payload["count"].(int); !ok || count != 1 {
		t.Fatalf("expected count=1, got %#v", payload["count"])
	}
}

func TestAgentMessagePollKeepsWaitingAfterSpuriousWakeUntilVisibleMessageArrives(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-poll-spurious"
	agentID := "agent-b"
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", agentID)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	timeoutSec := 1

	var hookErr error
	agentMessagePollAfterSubscribeHook = func() {
		agentMessagePollAfterSubscribeHook = nil
		h.GetEventBus().Publish(EventMessage{
			Type:        "agent.message",
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
		})
		go func() {
			time.Sleep(20 * time.Millisecond)
			_, hookErr = store.SendMessage(ctx, sqlite.MessageSendInput{
				WorkspaceID: workspaceID,
				FromAgentID: "agent-a",
				ToAgentID:   agentID,
				Content:     "real message after spurious wake",
			})
			if hookErr == nil {
				h.GetEventBus().Publish(EventMessage{
					Type:        "agent.message",
					WorkspaceID: workspaceID,
					AgentID:     agentID,
					Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
				})
			}
		}()
	}
	defer func() {
		agentMessagePollBeforeSubscribeHook = nil
		agentMessagePollAfterSubscribeHook = nil
	}()

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         20,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if hookErr != nil {
		t.Fatalf("hook send message: %v", hookErr)
	}
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message after spurious wake, got %+v", messages)
	}
	if messages[0].Content != "real message after spurious wake" {
		t.Fatalf("unexpected message after spurious wake: %+v", messages[0])
	}
}

func TestAgentMessagePollLongPollWakesForSelfSentDirectMessage(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-poll-self-sent-wake"
	agentID := "agent-b"
	ctx := testAuthContext(workspaceID, "agent", agentID)
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, agentID, "agent-c")
	timeoutSec := 1

	var hookErr error
	agentMessagePollAfterSubscribeHook = func() {
		agentMessagePollAfterSubscribeHook = nil
		go func() {
			time.Sleep(20 * time.Millisecond)
			rawSend, err := json.Marshal(agentMessageSendParams{
				WorkspaceID: workspaceID,
				FromAgentID: agentID,
				ToAgentID:   "agent-c",
				Content:     "self-sent direct wake",
			})
			if err != nil {
				hookErr = err
				return
			}
			_, rpcErr := callAgentMessageSendRaw(t, h, ctx, rawSend)
			if rpcErr != nil {
				hookErr = errors.New(rpcErr.Message)
			}
		}()
	}
	defer func() {
		agentMessagePollBeforeSubscribeHook = nil
		agentMessagePollAfterSubscribeHook = nil
	}()

	rawPoll, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         20,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal poll params: %v", err)
	}

	pollCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	result, rpcErr := h.agentMessagePoll(pollCtx, rawPoll)
	if hookErr != nil {
		t.Fatalf("hook send self-sent message: %v", hookErr)
	}
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 self-sent message after wake, got %+v", messages)
	}
	if messages[0].FromAgentID != agentID || messages[0].ToAgentID != "agent-c" || messages[0].Content != "self-sent direct wake" {
		t.Fatalf("unexpected self-sent wake message: %+v", messages[0])
	}
}

func TestAgentMessagePollIgnoresMalformedSelfSentWakePayloadUntilRealEventArrives(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-poll-self-sent-malformed-payload"
	agentID := "agent-b"
	ctx := testAuthContext(workspaceID, "agent", agentID)
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, agentID, "agent-c")
	timeoutSec := 1

	var hookErr error
	agentMessagePollAfterSubscribeHook = func() {
		agentMessagePollAfterSubscribeHook = nil
		h.GetEventBus().Publish(EventMessage{
			Type:        "agent.message",
			WorkspaceID: workspaceID,
			AgentID:     "agent-c",
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
			PayloadJSON: "{not-json",
		})
		go func() {
			time.Sleep(20 * time.Millisecond)
			rawSend, err := json.Marshal(agentMessageSendParams{
				WorkspaceID: workspaceID,
				FromAgentID: agentID,
				ToAgentID:   "agent-c",
				Content:     "self-sent direct after malformed wake",
			})
			if err != nil {
				hookErr = err
				return
			}
			_, rpcErr := callAgentMessageSendRaw(t, h, ctx, rawSend)
			if rpcErr != nil {
				hookErr = errors.New(rpcErr.Message)
			}
		}()
	}
	defer func() {
		agentMessagePollBeforeSubscribeHook = nil
		agentMessagePollAfterSubscribeHook = nil
	}()

	rawPoll, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         20,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal poll params: %v", err)
	}

	pollCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	result, rpcErr := h.agentMessagePoll(pollCtx, rawPoll)
	if hookErr != nil {
		t.Fatalf("hook send self-sent message after malformed wake: %v", hookErr)
	}
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 self-sent message after malformed wake path, got %+v", messages)
	}
	if messages[0].FromAgentID != agentID || messages[0].ToAgentID != "agent-c" || messages[0].Content != "self-sent direct after malformed wake" {
		t.Fatalf("unexpected self-sent message after malformed wake path: %+v", messages[0])
	}
}

func TestAgentMessagePollIgnoresSelfSentWakePayloadWithoutFromUntilRealEventArrives(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-poll-self-sent-missing-from"
	agentID := "agent-b"
	ctx := testAuthContext(workspaceID, "agent", agentID)
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, agentID, "agent-c")
	timeoutSec := 1

	var hookErr error
	agentMessagePollAfterSubscribeHook = func() {
		agentMessagePollAfterSubscribeHook = nil
		h.GetEventBus().Publish(EventMessage{
			Type:        "agent.message",
			WorkspaceID: workspaceID,
			AgentID:     "agent-c",
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
			PayloadJSON: `{"message_id":"fake-self-sent"}`,
		})
		go func() {
			time.Sleep(20 * time.Millisecond)
			rawSend, err := json.Marshal(agentMessageSendParams{
				WorkspaceID: workspaceID,
				FromAgentID: agentID,
				ToAgentID:   "agent-c",
				Content:     "self-sent direct after missing-from wake",
			})
			if err != nil {
				hookErr = err
				return
			}
			_, rpcErr := callAgentMessageSendRaw(t, h, ctx, rawSend)
			if rpcErr != nil {
				hookErr = errors.New(rpcErr.Message)
			}
		}()
	}
	defer func() {
		agentMessagePollBeforeSubscribeHook = nil
		agentMessagePollAfterSubscribeHook = nil
	}()

	rawPoll, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         20,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal poll params: %v", err)
	}

	pollCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	result, rpcErr := h.agentMessagePoll(pollCtx, rawPoll)
	if hookErr != nil {
		t.Fatalf("hook send self-sent message after missing-from wake: %v", hookErr)
	}
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 self-sent message after missing-from wake path, got %+v", messages)
	}
	if messages[0].FromAgentID != agentID || messages[0].ToAgentID != "agent-c" || messages[0].Content != "self-sent direct after missing-from wake" {
		t.Fatalf("unexpected self-sent message after missing-from wake path: %+v", messages[0])
	}
}

func TestAgentMessagePollIgnoresSpoofedSenderWakeUntilRealSelfSentDirectArrives(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-poll-self-sent-spoofed-from"
	agentID := "agent-a"
	ctx := testAuthContext(workspaceID, "agent", agentID)
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, agentID, "agent-b")
	timeoutSec := 1

	var hookErr error
	agentMessagePollAfterSubscribeHook = func() {
		agentMessagePollAfterSubscribeHook = nil
		h.GetEventBus().Publish(EventMessage{
			Type:        "agent.message",
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
			PayloadJSON: `{"message_id":"spoofed","from":"agent-x"}`,
		})
		go func() {
			time.Sleep(20 * time.Millisecond)
			rawSend, err := json.Marshal(agentMessageSendParams{
				WorkspaceID: workspaceID,
				FromAgentID: agentID,
				ToAgentID:   "agent-b",
				Content:     "real self-sent direct after spoofed wake",
			})
			if err != nil {
				hookErr = err
				return
			}
			_, rpcErr := callAgentMessageSendRaw(t, h, ctx, rawSend)
			if rpcErr != nil {
				hookErr = errors.New(rpcErr.Message)
			}
		}()
	}
	defer func() {
		agentMessagePollBeforeSubscribeHook = nil
		agentMessagePollAfterSubscribeHook = nil
	}()

	rawPoll, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         20,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal poll params: %v", err)
	}

	pollCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	result, rpcErr := h.agentMessagePoll(pollCtx, rawPoll)
	if hookErr != nil {
		t.Fatalf("hook send self-sent message after spoofed wake: %v", hookErr)
	}
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 self-sent message after spoofed wake path, got %+v", messages)
	}
	if messages[0].FromAgentID != agentID || messages[0].ToAgentID != "agent-b" || messages[0].Content != "real self-sent direct after spoofed wake" {
		t.Fatalf("unexpected self-sent message after spoofed wake path: %+v", messages[0])
	}
}

func TestAgentMessagePollLongPollWakesForWhitespaceTrimmedSenderRecipientIDs(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-poll-trimmed-ids-wake"
	agentID := "agent-b"
	ctx := testAuthContext(workspaceID, "agent", agentID)
	senderCtx := testAuthContext(workspaceID, "agent", "agent-a")
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", agentID)
	timeoutSec := 1

	var hookErr error
	agentMessagePollAfterSubscribeHook = func() {
		agentMessagePollAfterSubscribeHook = nil
		go func() {
			time.Sleep(20 * time.Millisecond)
			rawSend, err := json.Marshal(agentMessageSendParams{
				WorkspaceID: workspaceID,
				FromAgentID: "  agent-a  ",
				ToAgentID:   "  agent-b  ",
				Content:     "trimmed recipient wake",
			})
			if err != nil {
				hookErr = err
				return
			}
			_, rpcErr := callAgentMessageSendRaw(t, h, senderCtx, rawSend)
			if rpcErr != nil {
				hookErr = errors.New(rpcErr.Message)
			}
		}()
	}
	defer func() {
		agentMessagePollBeforeSubscribeHook = nil
		agentMessagePollAfterSubscribeHook = nil
	}()

	rawPoll, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         20,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal poll params: %v", err)
	}

	pollCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	result, rpcErr := h.agentMessagePoll(pollCtx, rawPoll)
	if hookErr != nil {
		t.Fatalf("hook send trimmed-id message: %v", hookErr)
	}
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 trimmed-id wake message, got %+v", messages)
	}
	if messages[0].FromAgentID != "agent-a" || messages[0].ToAgentID != agentID || messages[0].Content != "trimmed recipient wake" {
		t.Fatalf("unexpected trimmed-id wake message: %+v", messages[0])
	}
}

func TestAgentMessageSendTrimsSenderIDBeforeUpdatingLastSeen(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-send-trimmed-sender-last-seen"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Trimmed sender last-seen",
		CreatedBy:   "test-user",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "test-user",
		DisplayName: "Agent A",
		Role:        "worker",
		Status:      "REGISTERED",
	}); err != nil {
		t.Fatalf("register sender agent: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		OwnerUserID: "test-user",
		DisplayName: "Agent B",
		Role:        "worker",
		Status:      "REGISTERED",
	}); err != nil {
		t.Fatalf("register recipient agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "  agent-a  ",
		ToAgentID:   "agent-b",
		Content:     "trimmed sender last-seen",
	})

	var lastSeenAt, status string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COALESCE(last_seen_at, ''), status FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID, "agent-a",
	).Scan(&lastSeenAt, &status); err != nil {
		t.Fatalf("query sender agent row: %v", err)
	}
	if lastSeenAt == "" {
		t.Fatal("expected trimmed sender agent last_seen_at to be updated")
	}
	if !strings.EqualFold(status, "ACTIVE") {
		t.Fatalf("expected sender status ACTIVE after send, got %q", status)
	}
}

func TestAgentMessageSendBroadcastNormalizesTrimmedIDsAcrossResponseAndEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-send-broadcast-trimmed-ids"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Broadcast trimmed ids",
		CreatedBy:   "test-user",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "test-user",
		DisplayName: "Agent A",
		Role:        "worker",
		Status:      "REGISTERED",
	}); err != nil {
		t.Fatalf("register sender agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID: "  " + workspaceID + "  ",
		FromAgentID: "  agent-a  ",
		ToAgentID:   "   ",
		Content:     "broadcast trimmed ids",
	})
	if err != nil {
		t.Fatalf("marshal send params: %v", err)
	}
	result, rpcErr := callAgentMessageSendRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessageSend rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected send result type %T", result)
	}
	messageID, ok := payload["message_id"].(string)
	if !ok || messageID == "" {
		t.Fatalf("unexpected message_id payload %#v", payload["message_id"])
	}
	if gotWorkspaceID, ok := payload["workspace_id"].(string); !ok || gotWorkspaceID != workspaceID {
		t.Fatalf("expected trimmed workspace_id %q, got %#v", workspaceID, payload["workspace_id"])
	}

	evt := nextEvent(t, ch)
	if evt.Type != "agent.message" {
		t.Fatalf("expected agent.message event, got %+v", evt)
	}
	if evt.WorkspaceID != workspaceID {
		t.Fatalf("expected trimmed event workspace_id %q, got %+v", workspaceID, evt)
	}
	if evt.AgentID != "" {
		t.Fatalf("expected broadcast event agent_id empty after trim, got %+v", evt)
	}
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{"message_id": messageID, "from": "agent-a"})

	var storedFromAgentID, storedToAgentID string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT from_agent_id, COALESCE(to_agent_id, '') FROM agent_messages WHERE workspace_id = ? AND message_id = ?`,
		workspaceID, messageID,
	).Scan(&storedFromAgentID, &storedToAgentID); err != nil {
		t.Fatalf("query stored message row: %v", err)
	}
	if storedFromAgentID != "agent-a" || storedToAgentID != "" {
		t.Fatalf("expected stored trimmed ids from=agent-a to='', got from=%q to=%q", storedFromAgentID, storedToAgentID)
	}

	var lastSeenAt, status string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COALESCE(last_seen_at, ''), status FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID, "agent-a",
	).Scan(&lastSeenAt, &status); err != nil {
		t.Fatalf("query sender agent row: %v", err)
	}
	if lastSeenAt == "" {
		t.Fatal("expected broadcast sender last_seen_at to be updated")
	}
	if !strings.EqualFold(status, "ACTIVE") {
		t.Fatalf("expected sender status ACTIVE after broadcast send, got %q", status)
	}
}

func TestAgentMessageSendBroadcastDefaultsWhitespaceChannelAndContentType(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-send-broadcast-default-path"

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Broadcast default path",
		CreatedBy:   "test-user",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "test-user",
		DisplayName: "Agent A",
		Role:        "worker",
		Status:      "REGISTERED",
	}); err != nil {
		t.Fatalf("register sender agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID: "  " + workspaceID + "  ",
		FromAgentID: "  agent-a  ",
		ToAgentID:   "   ",
		Channel:     "   ",
		ContentType: " \t ",
		Content:     "broadcast defaults",
	})
	if err != nil {
		t.Fatalf("marshal send params: %v", err)
	}
	result, rpcErr := callAgentMessageSendRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessageSend rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected send result type %T", result)
	}
	messageID, ok := payload["message_id"].(string)
	if !ok || messageID == "" {
		t.Fatalf("unexpected message_id payload %#v", payload["message_id"])
	}
	if gotWorkspaceID, ok := payload["workspace_id"].(string); !ok || gotWorkspaceID != workspaceID {
		t.Fatalf("expected trimmed workspace_id %q, got %#v", workspaceID, payload["workspace_id"])
	}

	evt := nextEvent(t, ch)
	if evt.Type != "agent.message" {
		t.Fatalf("expected agent.message event, got %+v", evt)
	}
	if evt.WorkspaceID != workspaceID || evt.AgentID != "" {
		t.Fatalf("expected normalized broadcast event, got %+v", evt)
	}
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{"message_id": messageID, "from": "agent-a"})

	var storedFromAgentID, storedToAgentID, channel, contentType string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT from_agent_id, COALESCE(to_agent_id, ''), channel, content_type FROM agent_messages WHERE workspace_id = ? AND message_id = ?`,
		workspaceID, messageID,
	).Scan(&storedFromAgentID, &storedToAgentID, &channel, &contentType); err != nil {
		t.Fatalf("query stored message row: %v", err)
	}
	if storedFromAgentID != "agent-a" || storedToAgentID != "" {
		t.Fatalf("expected stored trimmed ids from=agent-a to='', got from=%q to=%q", storedFromAgentID, storedToAgentID)
	}
	if channel != "default" {
		t.Fatalf("expected default channel, got %q", channel)
	}
	if contentType != "text/plain" {
		t.Fatalf("expected default content_type, got %q", contentType)
	}

	var lastSeenAt string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COALESCE(last_seen_at, '') FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID, "agent-a",
	).Scan(&lastSeenAt); err != nil {
		t.Fatalf("query sender last_seen_at: %v", err)
	}
	if lastSeenAt == "" {
		t.Fatal("expected sender last_seen_at to be updated on defaulted broadcast send")
	}
}

func TestAgentMessageSendBroadcastDefaultsWhitespaceMetadataJSON(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-send-broadcast-default-metadata"

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Broadcast default metadata",
		CreatedBy:   "test-user",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "test-user",
		DisplayName: "Agent A",
		Role:        "worker",
		Status:      "REGISTERED",
	}); err != nil {
		t.Fatalf("register sender agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID:  "  " + workspaceID + "  ",
		FromAgentID:  "  agent-a  ",
		ToAgentID:    "   ",
		MetadataJSON: "  \t  ",
		Content:      "broadcast metadata default",
	})
	if err != nil {
		t.Fatalf("marshal send params: %v", err)
	}
	result, rpcErr := callAgentMessageSendRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessageSend rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected send result type %T", result)
	}
	messageID, ok := payload["message_id"].(string)
	if !ok || messageID == "" {
		t.Fatalf("unexpected message_id payload %#v", payload["message_id"])
	}
	if gotWorkspaceID, ok := payload["workspace_id"].(string); !ok || gotWorkspaceID != workspaceID {
		t.Fatalf("expected trimmed workspace_id %q, got %#v", workspaceID, payload["workspace_id"])
	}

	evt := nextEvent(t, ch)
	if evt.Type != "agent.message" {
		t.Fatalf("expected agent.message event, got %+v", evt)
	}
	if evt.WorkspaceID != workspaceID || evt.AgentID != "" {
		t.Fatalf("expected normalized broadcast event, got %+v", evt)
	}
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{"message_id": messageID, "from": "agent-a"})

	var storedFromAgentID, storedToAgentID, metadataJSON string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT from_agent_id, COALESCE(to_agent_id, ''), metadata_json FROM agent_messages WHERE workspace_id = ? AND message_id = ?`,
		workspaceID, messageID,
	).Scan(&storedFromAgentID, &storedToAgentID, &metadataJSON); err != nil {
		t.Fatalf("query stored message row: %v", err)
	}
	if storedFromAgentID != "agent-a" || storedToAgentID != "" {
		t.Fatalf("expected stored trimmed ids from=agent-a to='', got from=%q to=%q", storedFromAgentID, storedToAgentID)
	}
	if metadataJSON != "{}" {
		t.Fatalf("expected default metadata_json '{}', got %q", metadataJSON)
	}

	var lastSeenAt string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COALESCE(last_seen_at, '') FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID, "agent-a",
	).Scan(&lastSeenAt); err != nil {
		t.Fatalf("query sender last_seen_at: %v", err)
	}
	if lastSeenAt == "" {
		t.Fatal("expected sender last_seen_at to be updated on metadata-defaulted broadcast send")
	}
}

func TestAgentMessageSendDirectPreservesExplicitChannelAndContentTypeOnTrimmedPath(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-send-direct-explicit-path"

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Direct explicit path",
		CreatedBy:   "test-user",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "test-user",
			DisplayName: strings.ToUpper(agentID),
			Role:        "worker",
			Status:      "REGISTERED",
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID: "  " + workspaceID + "  ",
		FromAgentID: "  agent-a  ",
		ToAgentID:   "  agent-b  ",
		Channel:     "priority",
		ContentType: "application/json",
		Content:     `{"kind":"direct-explicit"}`,
	})
	if err != nil {
		t.Fatalf("marshal send params: %v", err)
	}
	result, rpcErr := callAgentMessageSendRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessageSend rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected send result type %T", result)
	}
	messageID, ok := payload["message_id"].(string)
	if !ok || messageID == "" {
		t.Fatalf("unexpected message_id payload %#v", payload["message_id"])
	}
	if gotWorkspaceID, ok := payload["workspace_id"].(string); !ok || gotWorkspaceID != workspaceID {
		t.Fatalf("expected trimmed workspace_id %q, got %#v", workspaceID, payload["workspace_id"])
	}

	evt := nextEvent(t, ch)
	if evt.Type != "agent.message" {
		t.Fatalf("expected agent.message event, got %+v", evt)
	}
	if evt.WorkspaceID != workspaceID || evt.AgentID != "agent-b" {
		t.Fatalf("expected normalized direct event for agent-b, got %+v", evt)
	}
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{"message_id": messageID, "from": "agent-a"})

	var storedFromAgentID, storedToAgentID, channel, contentType, content string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT from_agent_id, COALESCE(to_agent_id, ''), channel, content_type, content FROM agent_messages WHERE workspace_id = ? AND message_id = ?`,
		workspaceID, messageID,
	).Scan(&storedFromAgentID, &storedToAgentID, &channel, &contentType, &content); err != nil {
		t.Fatalf("query stored direct message row: %v", err)
	}
	if storedFromAgentID != "agent-a" || storedToAgentID != "agent-b" {
		t.Fatalf("expected stored trimmed ids from=agent-a to=agent-b, got from=%q to=%q", storedFromAgentID, storedToAgentID)
	}
	if channel != "priority" {
		t.Fatalf("expected explicit channel priority, got %q", channel)
	}
	if contentType != "application/json" {
		t.Fatalf("expected explicit content_type application/json, got %q", contentType)
	}
	if content != `{"kind":"direct-explicit"}` {
		t.Fatalf("unexpected stored content %q", content)
	}

	var lastSeenAt, status string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COALESCE(last_seen_at, ''), status FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID, "agent-a",
	).Scan(&lastSeenAt, &status); err != nil {
		t.Fatalf("query sender agent row: %v", err)
	}
	if lastSeenAt == "" {
		t.Fatal("expected sender last_seen_at to be updated on explicit direct send")
	}
	if !strings.EqualFold(status, "ACTIVE") {
		t.Fatalf("expected sender status ACTIVE after direct send, got %q", status)
	}
}

func TestAgentMessageSendDirectDefaultsWhitespaceMetadataJSONOnTrimmedPath(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-send-direct-default-metadata"

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Direct default metadata",
		CreatedBy:   "test-user",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "test-user",
			DisplayName: strings.ToUpper(agentID),
			Role:        "worker",
			Status:      "REGISTERED",
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID:  "  " + workspaceID + "  ",
		FromAgentID:  "  agent-a  ",
		ToAgentID:    "  agent-b  ",
		Channel:      "priority",
		ContentType:  "application/json",
		MetadataJSON: " \t ",
		Content:      `{"kind":"direct-default-metadata"}`,
	})
	if err != nil {
		t.Fatalf("marshal send params: %v", err)
	}
	result, rpcErr := callAgentMessageSendRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessageSend rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected send result type %T", result)
	}
	messageID, ok := payload["message_id"].(string)
	if !ok || messageID == "" {
		t.Fatalf("unexpected message_id payload %#v", payload["message_id"])
	}
	if gotWorkspaceID, ok := payload["workspace_id"].(string); !ok || gotWorkspaceID != workspaceID {
		t.Fatalf("expected trimmed workspace_id %q, got %#v", workspaceID, payload["workspace_id"])
	}

	evt := nextEvent(t, ch)
	if evt.Type != "agent.message" {
		t.Fatalf("expected agent.message event, got %+v", evt)
	}
	if evt.WorkspaceID != workspaceID || evt.AgentID != "agent-b" {
		t.Fatalf("expected normalized direct event for agent-b, got %+v", evt)
	}
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{"message_id": messageID, "from": "agent-a"})

	var storedFromAgentID, storedToAgentID, channel, contentType, metadataJSON, content string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT from_agent_id, COALESCE(to_agent_id, ''), channel, content_type, metadata_json, content FROM agent_messages WHERE workspace_id = ? AND message_id = ?`,
		workspaceID, messageID,
	).Scan(&storedFromAgentID, &storedToAgentID, &channel, &contentType, &metadataJSON, &content); err != nil {
		t.Fatalf("query stored direct message row: %v", err)
	}
	if storedFromAgentID != "agent-a" || storedToAgentID != "agent-b" {
		t.Fatalf("expected stored trimmed ids from=agent-a to=agent-b, got from=%q to=%q", storedFromAgentID, storedToAgentID)
	}
	if channel != "priority" {
		t.Fatalf("expected explicit channel priority, got %q", channel)
	}
	if contentType != "application/json" {
		t.Fatalf("expected explicit content_type application/json, got %q", contentType)
	}
	if metadataJSON != "{}" {
		t.Fatalf("expected default metadata_json '{}', got %q", metadataJSON)
	}
	if content != `{"kind":"direct-default-metadata"}` {
		t.Fatalf("unexpected stored content %q", content)
	}

	var lastSeenAt, status string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COALESCE(last_seen_at, ''), status FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID, "agent-a",
	).Scan(&lastSeenAt, &status); err != nil {
		t.Fatalf("query sender agent row: %v", err)
	}
	if lastSeenAt == "" {
		t.Fatal("expected sender last_seen_at to be updated on direct metadata-defaulted send")
	}
	if !strings.EqualFold(status, "ACTIVE") {
		t.Fatalf("expected sender status ACTIVE after direct send, got %q", status)
	}
}

func TestAgentMessageAckIsScopedPerWorkspaceAgent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-ack-scope"

	broadcastID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	directID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct",
	})

	beforeAckB := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(beforeAckB) != 2 {
		t.Fatalf("agent-b expected 2 messages before ack, got %d", len(beforeAckB))
	}

	beforeAckA := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-a",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(beforeAckA) != 2 {
		t.Fatalf("agent-a expected 2 self-visible messages before ack, got %d", len(beforeAckA))
	}

	rawAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{broadcastID, directID},
	})
	if err != nil {
		t.Fatalf("marshal ack params: %v", err)
	}
	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawAck)
	if rpcErr != nil {
		t.Fatalf("agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 2 {
		t.Fatalf("expected acknowledged=2, got %#v", payload["acknowledged"])
	}

	afterAckB := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterAckB) != 0 {
		t.Fatalf("agent-b expected 0 messages after ack, got %d", len(afterAckB))
	}

	afterAckA := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-a",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterAckA) != 2 {
		t.Fatalf("agent-a expected 2 messages after agent-b ack, got %d", len(afterAckA))
	}
	if afterAckA[0].MessageID != broadcastID || afterAckA[1].MessageID != directID {
		t.Fatalf("agent-a messages changed after agent-b ack: %+v", afterAckA)
	}
}

func TestAgentMessageAckReturnsActualAcknowledgedCount(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-ack-count"

	visibleID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "visible",
	})
	hiddenID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-c",
		Content:     "hidden",
	})

	rawAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{visibleID, visibleID, hiddenID, "", "missing-id"},
	})
	if err != nil {
		t.Fatalf("marshal ack params: %v", err)
	}

	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawAck)
	if rpcErr != nil {
		t.Fatalf("agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected acknowledged=1, got %#v", payload["acknowledged"])
	}

	afterAckB := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterAckB) != 0 {
		t.Fatalf("agent-b expected 0 visible messages after ack, got %+v", afterAckB)
	}

	afterAckC := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-c",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterAckC) != 1 || afterAckC[0].MessageID != hiddenID {
		t.Fatalf("agent-c hidden message should remain visible, got %+v", afterAckC)
	}
}

func TestAgentMessageAckBySenderDoesNotHideMessagesForRecipient(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-ack-sender-scope"

	broadcastID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	directID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct",
	})

	beforeAckA := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-a",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(beforeAckA) != 2 {
		t.Fatalf("agent-a expected 2 self-visible messages before ack, got %+v", beforeAckA)
	}

	rawAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		MessageIDs:  []string{broadcastID, directID},
	})
	if err != nil {
		t.Fatalf("marshal ack params: %v", err)
	}
	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawAck)
	if rpcErr != nil {
		t.Fatalf("agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 2 {
		t.Fatalf("expected acknowledged=2, got %#v", payload["acknowledged"])
	}

	afterAckA := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-a",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterAckA) != 0 {
		t.Fatalf("agent-a expected 0 messages after self-ack, got %+v", afterAckA)
	}

	afterAckB := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterAckB) != 2 {
		t.Fatalf("agent-b expected 2 messages after sender ack, got %+v", afterAckB)
	}
	if afterAckB[0].MessageID != broadcastID || afterAckB[1].MessageID != directID {
		t.Fatalf("agent-b messages changed after sender ack: %+v", afterAckB)
	}
}

func TestAgentMessageAckTracksSenderAndRecipientAcksSeparately(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-ack-two-agent-sequence"

	broadcastID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	directID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct",
	})

	rawSenderAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		MessageIDs:  []string{broadcastID, directID},
	})
	if err != nil {
		t.Fatalf("marshal sender ack params: %v", err)
	}
	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawSenderAck)
	if rpcErr != nil {
		t.Fatalf("sender agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected sender ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 2 {
		t.Fatalf("expected sender acknowledged=2, got %#v", payload["acknowledged"])
	}

	afterSenderAckB := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterSenderAckB) != 2 {
		t.Fatalf("agent-b expected 2 messages after sender ack, got %+v", afterSenderAckB)
	}

	rawRecipientAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{broadcastID, directID},
	})
	if err != nil {
		t.Fatalf("marshal recipient ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawRecipientAck)
	if rpcErr != nil {
		t.Fatalf("recipient agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected recipient ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 2 {
		t.Fatalf("expected recipient acknowledged=2 after sender ack, got %#v", payload["acknowledged"])
	}

	afterRecipientAckB := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterRecipientAckB) != 0 {
		t.Fatalf("agent-b expected 0 messages after recipient ack, got %+v", afterRecipientAckB)
	}

	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawSenderAck)
	if rpcErr != nil {
		t.Fatalf("sender re-ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected sender re-ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 0 {
		t.Fatalf("expected sender re-ack acknowledged=0, got %#v", payload["acknowledged"])
	}
}

func TestAgentMessageAckKeepsBroadcastAckScopedAcrossThreeAgents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-ack-three-agent-broadcast"

	broadcastID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})

	rawSenderAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		MessageIDs:  []string{broadcastID},
	})
	if err != nil {
		t.Fatalf("marshal sender ack params: %v", err)
	}
	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawSenderAck)
	if rpcErr != nil {
		t.Fatalf("sender agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected sender ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected sender acknowledged=1, got %#v", payload["acknowledged"])
	}

	afterSenderAckB := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterSenderAckB) != 1 || afterSenderAckB[0].MessageID != broadcastID {
		t.Fatalf("agent-b expected broadcast after sender ack, got %+v", afterSenderAckB)
	}

	rawAgentBAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{broadcastID},
	})
	if err != nil {
		t.Fatalf("marshal agent-b ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentBAck)
	if rpcErr != nil {
		t.Fatalf("agent-b agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-b ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected agent-b acknowledged=1, got %#v", payload["acknowledged"])
	}

	afterAgentBC := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-c",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterAgentBC) != 1 || afterAgentBC[0].MessageID != broadcastID {
		t.Fatalf("agent-c expected broadcast after sender+agent-b ack, got %+v", afterAgentBC)
	}

	rawAgentCAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-c",
		MessageIDs:  []string{broadcastID},
	})
	if err != nil {
		t.Fatalf("marshal agent-c ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentCAck)
	if rpcErr != nil {
		t.Fatalf("agent-c agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-c ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected agent-c acknowledged=1, got %#v", payload["acknowledged"])
	}

	afterAgentCAck := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-c",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(afterAgentCAck) != 0 {
		t.Fatalf("agent-c expected 0 messages after ack, got %+v", afterAgentCAck)
	}
}

func TestAgentMessageAckSeparatesOldBroadcastFromFreshDirect(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-ack-broadcast-then-direct"

	broadcastID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast-old",
	})

	rawSenderAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		MessageIDs:  []string{broadcastID},
	})
	if err != nil {
		t.Fatalf("marshal sender ack params: %v", err)
	}
	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawSenderAck)
	if rpcErr != nil {
		t.Fatalf("sender agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected sender ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected sender acknowledged=1, got %#v", payload["acknowledged"])
	}

	rawAgentBAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{broadcastID},
	})
	if err != nil {
		t.Fatalf("marshal agent-b ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentBAck)
	if rpcErr != nil {
		t.Fatalf("agent-b old-broadcast ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-b old-broadcast ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected agent-b old broadcast acknowledged=1, got %#v", payload["acknowledged"])
	}

	directID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct-fresh",
	})

	agentBVisible := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(agentBVisible) != 1 || agentBVisible[0].MessageID != directID {
		t.Fatalf("agent-b expected only fresh direct, got %+v", agentBVisible)
	}

	agentCVisible := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-c",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(agentCVisible) != 1 || agentCVisible[0].MessageID != broadcastID {
		t.Fatalf("agent-c expected only old broadcast, got %+v", agentCVisible)
	}

	rawAgentBDirectAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{directID},
	})
	if err != nil {
		t.Fatalf("marshal agent-b direct ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentBDirectAck)
	if rpcErr != nil {
		t.Fatalf("agent-b fresh-direct ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-b fresh-direct ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected agent-b fresh direct acknowledged=1, got %#v", payload["acknowledged"])
	}

	rawAgentCBroadcastAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-c",
		MessageIDs:  []string{broadcastID},
	})
	if err != nil {
		t.Fatalf("marshal agent-c broadcast ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentCBroadcastAck)
	if rpcErr != nil {
		t.Fatalf("agent-c old-broadcast ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-c old-broadcast ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected agent-c old broadcast acknowledged=1, got %#v", payload["acknowledged"])
	}
}

func TestAgentMessageAckDuplicateMixedVisibilityDoesNotInflateCounts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-ack-duplicate-mixed-visibility"

	broadcastID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	directID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct",
	})

	rawAgentBAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{broadcastID, directID},
	})
	if err != nil {
		t.Fatalf("marshal agent-b ack params: %v", err)
	}
	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawAgentBAck)
	if rpcErr != nil {
		t.Fatalf("agent-b mixed ack rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-b mixed ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 2 {
		t.Fatalf("expected agent-b acknowledged=2, got %#v", payload["acknowledged"])
	}

	agentCVisible := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-c",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(agentCVisible) != 1 || agentCVisible[0].MessageID != broadcastID {
		t.Fatalf("agent-c expected only broadcast after agent-b ack, got %+v", agentCVisible)
	}

	rawAgentCAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-c",
		MessageIDs:  []string{broadcastID, broadcastID, directID, "missing-id", "", "  "},
	})
	if err != nil {
		t.Fatalf("marshal agent-c duplicate ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentCAck)
	if rpcErr != nil {
		t.Fatalf("agent-c duplicate ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-c duplicate ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected agent-c acknowledged=1, got %#v", payload["acknowledged"])
	}

	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentCAck)
	if rpcErr != nil {
		t.Fatalf("agent-c duplicate re-ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-c duplicate re-ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 0 {
		t.Fatalf("expected agent-c re-ack acknowledged=0, got %#v", payload["acknowledged"])
	}

	rawAgentBReAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{broadcastID, directID, directID, "missing-id"},
	})
	if err != nil {
		t.Fatalf("marshal agent-b re-ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentBReAck)
	if rpcErr != nil {
		t.Fatalf("agent-b re-ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-b re-ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 0 {
		t.Fatalf("expected agent-b re-ack acknowledged=0, got %#v", payload["acknowledged"])
	}
}

func TestAgentMessageAckSelfSentRowsRemainSeparatelyAckableForRecipient(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-ack-self-sent-separate-scope"

	broadcastID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	selfSentID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-b",
		ToAgentID:   "agent-c",
		Content:     "self-sent-to-c",
	})

	rawAgentBAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{broadcastID, selfSentID},
	})
	if err != nil {
		t.Fatalf("marshal agent-b ack params: %v", err)
	}
	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawAgentBAck)
	if rpcErr != nil {
		t.Fatalf("agent-b self-visible ack rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-b self-visible ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 2 {
		t.Fatalf("expected agent-b acknowledged=2, got %#v", payload["acknowledged"])
	}

	agentCVisible := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-c",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(agentCVisible) != 2 {
		t.Fatalf("agent-c expected broadcast+self-sent after agent-b self-ack, got %+v", agentCVisible)
	}
	if agentCVisible[0].MessageID != broadcastID || agentCVisible[1].MessageID != selfSentID {
		t.Fatalf("unexpected agent-c visible set after agent-b self-ack: %+v", agentCVisible)
	}

	rawAgentCAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-c",
		MessageIDs:  []string{broadcastID, selfSentID, selfSentID, "missing-id", "", "  "},
	})
	if err != nil {
		t.Fatalf("marshal agent-c ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentCAck)
	if rpcErr != nil {
		t.Fatalf("agent-c recipient-visible ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-c recipient-visible ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 2 {
		t.Fatalf("expected agent-c acknowledged=2, got %#v", payload["acknowledged"])
	}

	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentCAck)
	if rpcErr != nil {
		t.Fatalf("agent-c recipient-visible re-ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-c recipient-visible re-ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 0 {
		t.Fatalf("expected agent-c re-ack acknowledged=0, got %#v", payload["acknowledged"])
	}

	rawAgentBReAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{broadcastID, selfSentID, selfSentID, "missing-id"},
	})
	if err != nil {
		t.Fatalf("marshal agent-b re-ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentBReAck)
	if rpcErr != nil {
		t.Fatalf("agent-b self-visible re-ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-b self-visible re-ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 0 {
		t.Fatalf("expected agent-b re-ack acknowledged=0, got %#v", payload["acknowledged"])
	}
}

func TestAgentMessageAckSkipsLegacyHiddenBroadcastButKeepsVisibleDirectAckable(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-ack-legacy-hidden-mixed-visibility"

	broadcastID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "legacy-hidden-broadcast",
	})
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET read_at = ? WHERE workspace_id = ? AND message_id = ?`,
		"2026-03-20T09:25:00Z", workspaceID, broadcastID,
	); err != nil {
		t.Fatalf("set legacy read_at on broadcast: %v", err)
	}

	directID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "fresh-direct",
	})

	agentBVisible := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(agentBVisible) != 1 || agentBVisible[0].MessageID != directID {
		t.Fatalf("agent-b expected only fresh direct, got %+v", agentBVisible)
	}

	rawAgentBAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{broadcastID, directID, directID, "missing-id"},
	})
	if err != nil {
		t.Fatalf("marshal agent-b mixed legacy/direct ack params: %v", err)
	}
	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawAgentBAck)
	if rpcErr != nil {
		t.Fatalf("agent-b mixed legacy/direct ack rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-b mixed legacy/direct ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected agent-b acknowledged=1, got %#v", payload["acknowledged"])
	}

	agentCAfterBAck := pollTestMessages(t, h, ctx, agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-c",
		Limit:         20,
		TimeoutSec:    intPtr(0),
		LookbackHours: 24,
	})
	if len(agentCAfterBAck) != 0 {
		t.Fatalf("agent-c expected no legacy-hidden broadcast, got %+v", agentCAfterBAck)
	}

	rawAgentCAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-c",
		MessageIDs:  []string{broadcastID, directID, broadcastID, "missing-id"},
	})
	if err != nil {
		t.Fatalf("marshal agent-c legacy-hidden mixed ack params: %v", err)
	}
	result, rpcErr = callAgentMessageAckRaw(t, h, ctx, rawAgentCAck)
	if rpcErr != nil {
		t.Fatalf("agent-c legacy-hidden mixed ack rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected agent-c legacy-hidden mixed ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 0 {
		t.Fatalf("expected agent-c acknowledged=0, got %#v", payload["acknowledged"])
	}
}

func TestAgentMessagePollCompositeCursorUsesInsertionOrderNotLexicalMessageID(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-poll-next-rowid-cursor"
	timeoutSec := 0

	firstID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	secondID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})
	thirdID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "third",
	})

	baseTime := recentPollBaseTime(-1 * time.Hour)
	sameTimestamp := formatPollTime(baseTime)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?, ?)`,
		sameTimestamp, firstID, secondID, thirdID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages
		 SET message_id = CASE message_id
		   WHEN ? THEN 'msg-shared-2'
		   WHEN ? THEN 'msg-shared-10'
		   WHEN ? THEN 'msg-shared-11'
		 END
		 WHERE message_id IN (?, ?, ?)`,
		firstID, secondID, thirdID,
		firstID, secondID, thirdID,
	); err != nil {
		t.Fatalf("rewrite message ids for lexical trap: %v", err)
	}

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         1,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 || messages[0].MessageID != "msg-shared-2" {
		t.Fatalf("expected first rowid-ordered message, got %+v", messages)
	}
	nextCursor, ok := payload["next_cursor"].(string)
	if !ok || nextCursor != sqlite.EncodeMessageCursor(sameTimestamp, "msg-shared-2") {
		t.Fatalf("unexpected next_cursor %#v", payload["next_cursor"])
	}

	raw, err = json.Marshal(agentMessagePollParams{
		WorkspaceID:    workspaceID,
		AgentID:        "agent-b",
		AfterCreatedAt: nextCursor,
		Limit:          50,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal second params: %v", err)
	}

	result, rpcErr = callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("second agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second result type %T", result)
	}
	messages, ok = payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected second messages type %T", payload["messages"])
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 rowid-ordered followers, got %+v", messages)
	}
	if messages[0].MessageID != "msg-shared-10" || messages[1].MessageID != "msg-shared-11" {
		t.Fatalf("unexpected rowid-ordered followers: %+v", messages)
	}
}

func TestAgentMessagePollCompositeCursorCanAdvancePastLegacyReadSameTimestampMessage(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-poll-legacy-read-composite-cursor"
	timeoutSec := 0

	firstID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	secondID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})

	baseTime := recentPollBaseTime(-1 * time.Hour)
	sameTimestamp := formatPollTime(baseTime)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, firstID, secondID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET read_at = ? WHERE message_id = ?`,
		formatPollTime(baseTime.Add(1*time.Minute)), firstID,
	); err != nil {
		t.Fatalf("legacy-read first message: %v", err)
	}

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:    workspaceID,
		AgentID:        "agent-b",
		AfterCreatedAt: sqlite.EncodeMessageCursor(sameTimestamp, firstID),
		Limit:          50,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 follower after legacy-read composite cursor, got %+v", messages)
	}
	if messages[0].MessageID != secondID || messages[0].Content != "second" {
		t.Fatalf("unexpected follower after legacy-read composite cursor: %+v", messages)
	}
	if nextCursor, ok := payload["next_cursor"].(string); !ok || nextCursor != sqlite.EncodeMessageCursor(sameTimestamp, secondID) {
		t.Fatalf("unexpected next_cursor %#v", payload["next_cursor"])
	}
}

func TestAgentMessagePollCompositeCursorCanAdvancePastAckedSameTimestampMessage(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-poll-acked-composite-cursor"
	timeoutSec := 0

	firstID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	secondID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})

	baseTime := recentPollBaseTime(-1 * time.Hour)
	sameTimestamp := formatPollTime(baseTime)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, firstID, secondID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}

	rawAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{firstID},
	})
	if err != nil {
		t.Fatalf("marshal ack params: %v", err)
	}
	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawAck)
	if rpcErr != nil {
		t.Fatalf("agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected acknowledged=1, got %#v", payload["acknowledged"])
	}

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:    workspaceID,
		AgentID:        "agent-b",
		AfterCreatedAt: sqlite.EncodeMessageCursor(sameTimestamp, firstID),
		Limit:          50,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr = callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 follower after acked composite cursor, got %+v", messages)
	}
	if messages[0].MessageID != secondID || messages[0].Content != "second" {
		t.Fatalf("unexpected follower after acked composite cursor: %+v", messages)
	}
	if nextCursor, ok := payload["next_cursor"].(string); !ok || nextCursor != sqlite.EncodeMessageCursor(sameTimestamp, secondID) {
		t.Fatalf("unexpected next_cursor %#v", payload["next_cursor"])
	}
}

func TestAgentMessagePollReturnsCompositeNextCursor(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-poll-next-cursor"
	timeoutSec := 0

	firstID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	secondID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})

	baseTime := recentPollBaseTime(-1 * time.Hour)
	sameTimestamp := formatPollTime(baseTime)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, firstID, secondID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         1,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 || messages[0].MessageID != firstID {
		t.Fatalf("expected first message page, got %+v", messages)
	}
	nextCursor, ok := payload["next_cursor"].(string)
	if !ok || nextCursor != sqlite.EncodeMessageCursor(sameTimestamp, firstID) {
		t.Fatalf("unexpected next_cursor %#v", payload["next_cursor"])
	}

	raw, err = json.Marshal(agentMessagePollParams{
		WorkspaceID:    workspaceID,
		AgentID:        "agent-b",
		AfterCreatedAt: nextCursor,
		Limit:          1,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal second params: %v", err)
	}

	result, rpcErr = callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("second agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second result type %T", result)
	}
	messages, ok = payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected second messages type %T", payload["messages"])
	}
	if len(messages) != 1 || messages[0].MessageID != secondID {
		t.Fatalf("expected second message page, got %+v", messages)
	}
}

func TestAgentMessagePollTimestampCursorFillsRemainingLimitWithNewerRows(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-poll-timestamp-fill"
	timeoutSec := 0

	firstID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	secondID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})
	thirdID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "third",
	})

	baseTime := recentPollBaseTime(-1 * time.Hour)
	sameTimestamp := formatPollTime(baseTime)
	newerTimestamp := formatPollTime(baseTime.Add(1 * time.Second))
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = CASE message_id
			WHEN ? THEN ?
			WHEN ? THEN ?
			WHEN ? THEN ?
		END WHERE message_id IN (?, ?, ?)`,
		firstID, sameTimestamp,
		secondID, sameTimestamp,
		thirdID, newerTimestamp,
		firstID, secondID, thirdID,
	); err != nil {
		t.Fatalf("force created_at values: %v", err)
	}

	rawAck, err := json.Marshal(agentMessageAckParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		MessageIDs:  []string{firstID},
	})
	if err != nil {
		t.Fatalf("marshal ack params: %v", err)
	}
	result, rpcErr := callAgentMessageAckRaw(t, h, ctx, rawAck)
	if rpcErr != nil {
		t.Fatalf("agentMessageAck rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected ack result type %T", result)
	}
	if acknowledged, ok := payload["acknowledged"].(int); !ok || acknowledged != 1 {
		t.Fatalf("expected acknowledged=1, got %#v", payload["acknowledged"])
	}

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:    workspaceID,
		AgentID:        "agent-b",
		AfterCreatedAt: sameTimestamp,
		Limit:          2,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr = callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages after fill, got %+v", messages)
	}
	if messages[0].MessageID != secondID || messages[1].MessageID != thirdID {
		t.Fatalf("unexpected timestamp cursor fill order/content: %+v", messages)
	}
	if nextCursor, ok := payload["next_cursor"].(string); !ok || nextCursor != sqlite.EncodeMessageCursor(newerTimestamp, thirdID) {
		t.Fatalf("unexpected next_cursor %#v", payload["next_cursor"])
	}
}

func TestAgentMessagePollTimestampCursorShimReturnsWholeSameTimestampBatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-poll-timestamp-shim-batch"
	timeoutSec := 0

	firstID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	secondID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})

	baseTime := recentPollBaseTime(-1 * time.Hour)
	sameTimestamp := formatPollTime(baseTime)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, firstID, secondID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:    workspaceID,
		AgentID:        "agent-b",
		AfterCreatedAt: sameTimestamp,
		Limit:          1,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 2 {
		t.Fatalf("expected whole same-timestamp batch despite limit=1, got %+v", messages)
	}
	if messages[0].MessageID != firstID || messages[1].MessageID != secondID {
		t.Fatalf("unexpected same-timestamp batch order/content: %+v", messages)
	}
	if nextCursor, ok := payload["next_cursor"].(string); !ok || nextCursor != sqlite.EncodeMessageCursor(sameTimestamp, secondID) {
		t.Fatalf("unexpected next_cursor %#v", payload["next_cursor"])
	}
}

func TestAgentMessagePollRejectsCompositeCursorWithMismatchedTimestamp(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	timeoutSec := 0
	workspaceID := "ws-mismatched-composite-cursor"

	messageID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "hello",
	})
	baseTime := recentPollBaseTime(-1 * time.Hour)
	actualTimestamp := formatPollTime(baseTime)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id = ?`,
		actualTimestamp, messageID,
	); err != nil {
		t.Fatalf("force created_at: %v", err)
	}

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:    workspaceID,
		AgentID:        "agent-b",
		AfterCreatedAt: sqlite.EncodeMessageCursor(formatPollTime(baseTime.Add(1*time.Second)), messageID),
		Limit:          20,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	_, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected rpc error")
	}
	if rpcErr.Code != errCodeInvalidPollCursor || rpcErr.Message != "after_created_at must be a valid poll cursor" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestAgentMessagePollRejectsCompositeCursorForHiddenMessage(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	timeoutSec := 0
	workspaceID := "ws-hidden-composite-cursor"

	hiddenID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-c",
		Content:     "hidden",
	})
	visibleID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "visible",
	})
	baseTime := recentPollBaseTime(-1 * time.Hour)
	sameTimestamp := formatPollTime(baseTime)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, hiddenID, visibleID,
	); err != nil {
		t.Fatalf("force created_at: %v", err)
	}

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:    workspaceID,
		AgentID:        "agent-b",
		AfterCreatedAt: sqlite.EncodeMessageCursor(sameTimestamp, hiddenID),
		Limit:          20,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	_, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected rpc error")
	}
	if rpcErr.Code != errCodeInvalidPollCursor || rpcErr.Message != "after_created_at must be a valid poll cursor" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestAgentMessagePollRejectsUnknownCompositeCursorMessageID(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	timeoutSec := 0
	workspaceID := "ws-invalid-composite-cursor"

	messageID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "hello",
	})
	baseTime := recentPollBaseTime(-1 * time.Hour)
	sameTimestamp := formatPollTime(baseTime)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id = ?`,
		sameTimestamp, messageID,
	); err != nil {
		t.Fatalf("force created_at: %v", err)
	}

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:    workspaceID,
		AgentID:        "agent-b",
		AfterCreatedAt: sqlite.EncodeMessageCursor(sameTimestamp, "msg-missing"),
		Limit:          20,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	_, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected rpc error")
	}
	if rpcErr.Code != errCodeInvalidPollCursor || rpcErr.Message != "after_created_at must be a valid poll cursor" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestAgentMessagePollRejectsInvalidCursorFormat(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	timeoutSec := 0

	for _, cursor := range []string{"not-a-time", "|msg-1", "2026-13-20T13:25:00Z|msg-1", "2026-03-20T13:25:00Z|"} {
		t.Run(cursor, func(t *testing.T) {
			raw, err := json.Marshal(agentMessagePollParams{
				WorkspaceID:    "ws-invalid-cursor",
				AgentID:        "agent-b",
				AfterCreatedAt: cursor,
				Limit:          20,
				TimeoutSec:     &timeoutSec,
				LookbackHours:  24,
			})
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			_, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
			if rpcErr == nil {
				t.Fatal("expected rpc error")
			}
			if rpcErr.Code != errCodeInvalidPollCursor || rpcErr.Message != "after_created_at must be a valid poll cursor" {
				t.Fatalf("unexpected rpc error: %+v", rpcErr)
			}
		})
	}
	_ = store
}

func TestAgentMessagePollReturnsEmptySliceOnImmediateTimeout(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-poll-empty-timeout", "agent", "agent-b")
	timeoutSec := 1
	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   "ws-poll-empty-timeout",
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	pollCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	result, rpcErr := h.agentMessagePoll(pollCtx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if messages == nil {
		t.Fatal("expected empty slice, got nil")
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages, got %+v", messages)
	}
	if count, ok := payload["count"].(int); !ok || count != 0 {
		t.Fatalf("expected count=0, got %#v", payload["count"])
	}
	if nextCursor, ok := payload["next_cursor"].(string); !ok || nextCursor != "" {
		t.Fatalf("expected empty next_cursor, got %#v", payload["next_cursor"])
	}
}

func TestAgentMessagePollPreservesCursorOnEmptyImmediateResult(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	timeoutSec := 0
	cursor := sqlite.EncodeMessageCursor(recentPollTimestamp(-1*time.Hour), "msg-existing")

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:    "ws-poll-empty-immediate-cursor",
		AgentID:        "agent-b",
		AfterCreatedAt: cursor,
		Limit:          20,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected rpc error for unknown cursor anchor")
	}
	if rpcErr.Code != errCodeInvalidPollCursor || rpcErr.Message != "after_created_at must be a valid poll cursor" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}

	messageID := sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: "ws-poll-empty-immediate-cursor",
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "anchor",
	})
	anchorTimestamp := recentPollTimestamp(-1 * time.Hour)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id = ?`,
		anchorTimestamp, messageID,
	); err != nil {
		t.Fatalf("force created_at: %v", err)
	}
	cursor = sqlite.EncodeMessageCursor(anchorTimestamp, messageID)

	raw, err = json.Marshal(agentMessagePollParams{
		WorkspaceID:    "ws-poll-empty-immediate-cursor",
		AgentID:        "agent-b",
		AfterCreatedAt: cursor,
		Limit:          20,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr = callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages, got %+v", messages)
	}
	if nextCursor, ok := payload["next_cursor"].(string); !ok || nextCursor != cursor {
		t.Fatalf("expected preserved next_cursor %q, got %#v", cursor, payload["next_cursor"])
	}
}

func TestAgentMessagePollPreservesCursorOnEmptyLongPollResult(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-poll-empty-long-cursor"
	ctx := testAuthContext(workspaceID, "agent", "agent-b")
	senderCtx := testAuthContext(workspaceID, "agent", "agent-a")
	timeoutSec := 1

	messageID := sendTestMessage(t, h, senderCtx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "anchor",
	})
	anchorTimestamp := recentPollTimestamp(-1 * time.Hour)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id = ?`,
		anchorTimestamp, messageID,
	); err != nil {
		t.Fatalf("force created_at: %v", err)
	}
	cursor := sqlite.EncodeMessageCursor(anchorTimestamp, messageID)

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:    workspaceID,
		AgentID:        "agent-b",
		AfterCreatedAt: cursor,
		Limit:          20,
		TimeoutSec:     &timeoutSec,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	pollCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	result, rpcErr := h.agentMessagePoll(pollCtx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages, got %+v", messages)
	}
	if nextCursor, ok := payload["next_cursor"].(string); !ok || nextCursor != cursor {
		t.Fatalf("expected preserved next_cursor %q, got %#v", cursor, payload["next_cursor"])
	}
}

func TestAgentMessagePollOmitsLegacyReadAtFromRPCPayload(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-poll-payload"
	timeoutSec := 0

	sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "payload-shape",
	})

	raw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         20,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("expected non-empty encoded payload")
	}
	if strings.Contains(string(encoded), "\"read_at\"") {
		t.Fatalf("expected poll payload to omit read_at, got %s", string(encoded))
	}
}

func TestAgentMessageHandlersValidateWorkspaceAndAgentIDsAtRPCBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	timeoutSec := 0

	for _, tc := range []struct {
		name       string
		call       func() (any, *RPCError)
		wantCode   int
		wantErrMsg string
	}{
		{
			name: "send missing workspace",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessageSendParams{FromAgentID: "agent-a", Content: "hello"})
				return callAgentMessageSendRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "workspace_id is required",
		},
		{
			name: "send missing sender",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessageSendParams{WorkspaceID: "ws-test", Content: "hello"})
				return callAgentMessageSendRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "from_agent_id is required",
		},
		{
			name: "send missing content",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessageSendParams{WorkspaceID: "ws-test", FromAgentID: "agent-a"})
				return callAgentMessageSendRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "content is required",
		},
		{
			name: "send whitespace content",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessageSendParams{WorkspaceID: "ws-test", FromAgentID: "agent-a", Content: "   \t\n  "})
				return callAgentMessageSendRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "content is required",
		},
		{
			name: "send mojibake content",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessageSendParams{WorkspaceID: "ws-test", FromAgentID: "agent-a", Content: "  ?????  "})
				return callAgentMessageSendRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "message content appears to contain mojibake (long runs of '?' replacing non-ASCII characters). Your client is likely not sending valid UTF-8. Please write your content in English or fix your HTTP client's encoding to UTF-8.",
		},
		{
			name: "send invalid metadata json",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessageSendParams{WorkspaceID: "ws-test", FromAgentID: "agent-a", Content: "hello", MetadataJSON: "{bad-json"})
				return callAgentMessageSendRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "metadata_json must be valid JSON",
		},
		{
			name: "poll missing workspace",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessagePollParams{AgentID: "agent-b", TimeoutSec: &timeoutSec})
				return callAgentMessagePollRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "workspace_id is required",
		},
		{
			name: "poll missing agent",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessagePollParams{WorkspaceID: "ws-test", TimeoutSec: &timeoutSec})
				return callAgentMessagePollRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "agent_id is required",
		},
		{
			name: "ack empty message ids",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessageAckParams{WorkspaceID: "ws-test", AgentID: "agent-b"})
				return callAgentMessageAckRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "message_ids is required",
		},
		{
			name: "ack whitespace message ids",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessageAckParams{WorkspaceID: "ws-test", AgentID: "agent-b", MessageIDs: []string{"", "  ", "\t"}})
				return callAgentMessageAckRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "message_ids is required",
		},
		{
			name: "ack missing workspace",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessageAckParams{AgentID: "agent-b", MessageIDs: []string{"msg-1"}})
				return callAgentMessageAckRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "workspace_id is required",
		},
		{
			name: "ack missing agent",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentMessageAckParams{WorkspaceID: "ws-test", MessageIDs: []string{"msg-1"}})
				return callAgentMessageAckRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "agent_id is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rpcErr := tc.call()
			if rpcErr == nil {
				t.Fatal("expected rpc error")
			}
			if rpcErr.Code != tc.wantCode || rpcErr.Message != tc.wantErrMsg {
				t.Fatalf("expected (%d, %q), got (%d, %q)", tc.wantCode, tc.wantErrMsg, rpcErr.Code, rpcErr.Message)
			}
		})
	}
	_ = store
}

func TestAgentStateDeleteValidatesRequiredParamsAtRPCBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		call       func() (any, *RPCError)
		wantCode   int
		wantErrMsg string
	}{
		{
			name: "delete missing workspace",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentStateDeleteParams{AgentID: "agent-a", Key: "k"})
				return h.agentStateDelete(ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "workspace_id is required",
		},
		{
			name: "delete missing agent",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentStateDeleteParams{WorkspaceID: "ws-test", Key: "k"})
				return h.agentStateDelete(ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "agent_id is required",
		},
		{
			name: "delete missing key",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentStateDeleteParams{WorkspaceID: "ws-test", AgentID: "agent-a"})
				return h.agentStateDelete(ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "key is required",
		},
		{
			name: "delete whitespace key",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentStateDeleteParams{WorkspaceID: "ws-test", AgentID: "agent-a", Key: "  \t  "})
				return h.agentStateDelete(ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "key is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rpcErr := tc.call()
			if rpcErr == nil {
				t.Fatal("expected rpc error")
			}
			if rpcErr.Code != tc.wantCode || rpcErr.Message != tc.wantErrMsg {
				t.Fatalf("expected (%d, %q), got (%d, %q)", tc.wantCode, tc.wantErrMsg, rpcErr.Code, rpcErr.Message)
			}
		})
	}
	_ = store
}

func TestAgentRequestHandlersValidateRequiredParamsAtRPCBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		call       func() (any, *RPCError)
		wantCode   int
		wantErrMsg string
	}{
		{
			name: "request missing workspace",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRequestParams{FromAgentID: "agent-a", ToAgentID: "agent-b", Payload: `{}`})
				return callAgentRequestRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "workspace_id is required",
		},
		{
			name: "request missing from agent",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRequestParams{WorkspaceID: "ws-test", ToAgentID: "agent-b", Payload: `{}`})
				return callAgentRequestRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "from_agent_id is required",
		},
		{
			name: "request missing to agent",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRequestParams{WorkspaceID: "ws-test", FromAgentID: "agent-a", Payload: `{}`})
				return callAgentRequestRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "to_agent_id is required",
		},
		{
			name: "request missing payload",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRequestParams{WorkspaceID: "ws-test", FromAgentID: "agent-a", ToAgentID: "agent-b"})
				return callAgentRequestRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "payload is required",
		},
		{
			name: "respond missing request id",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRespondParams{Response: "done"})
				return callAgentRespondRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "request_id is required",
		},
		{
			name: "respond missing response",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRespondParams{RequestID: "areq-1"})
				return callAgentRespondRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "response is required",
		},
		{
			name: "respond whitespace response",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRespondParams{RequestID: "areq-1", Response: "  \t  "})
				return callAgentRespondRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "response is required",
		},
		{
			name: "result missing request id",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRequestResultParams{})
				return callAgentRequestResultRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "request_id is required",
		},
		{
			name: "list missing workspace",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRequestListParams{AgentID: "agent-b"})
				return callAgentRequestListRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "workspace_id is required",
		},
		{
			name: "list missing agent",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRequestListParams{WorkspaceID: "ws-test"})
				return callAgentRequestListRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "agent_id is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rpcErr := tc.call()
			if rpcErr == nil {
				t.Fatal("expected rpc error")
			}
			if rpcErr.Code != tc.wantCode || rpcErr.Message != tc.wantErrMsg {
				t.Fatalf("expected (%d, %q), got (%d, %q)", tc.wantCode, tc.wantErrMsg, rpcErr.Code, rpcErr.Message)
			}
		})
	}
	_ = store
}

func TestAgentRequestNormalizesTrimmedIDsAcrossResultAndEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-request-trimmed-ids"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", "agent-b")
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(agentRequestParams{
		WorkspaceID: "  " + workspaceID + "  ",
		FromAgentID: "  agent-a  ",
		ToAgentID:   "  agent-b  ",
		Method:      "  call.trimmed  ",
		Payload:     `{"ok":true}`,
		TimeoutSec:  30,
	})
	if err != nil {
		t.Fatalf("marshal request params: %v", err)
	}

	result, rpcErr := callAgentRequestRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentRequest rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected request result type %T", result)
	}
	requestID, ok := payload["request_id"].(string)
	if !ok || requestID == "" {
		t.Fatalf("unexpected request_id payload %#v", payload["request_id"])
	}
	if gotWorkspaceID, ok := payload["workspace_id"].(string); !ok || gotWorkspaceID != workspaceID {
		t.Fatalf("expected trimmed workspace_id %q, got %#v", workspaceID, payload["workspace_id"])
	}
	if gotToAgentID, ok := payload["to_agent_id"].(string); !ok || gotToAgentID != "agent-b" {
		t.Fatalf("expected trimmed to_agent_id agent-b, got %#v", payload["to_agent_id"])
	}

	evt := nextEvent(t, ch)
	if evt.Type != "agent.request" {
		t.Fatalf("expected agent.request event, got %+v", evt)
	}
	if evt.WorkspaceID != workspaceID || evt.AgentID != "agent-b" {
		t.Fatalf("expected normalized request event, got %+v", evt)
	}
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{
		"request_id": requestID,
		"from":       "agent-a",
		"method":     "call.trimmed",
	})

	stored, err := h.store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("get stored request result: %v", err)
	}
	if stored.WorkspaceID != workspaceID || stored.FromAgentID != "agent-a" || stored.ToAgentID != "agent-b" {
		t.Fatalf("expected stored trimmed ids, got %+v", stored)
	}
	if stored.Method != "call.trimmed" {
		t.Fatalf("expected stored trimmed method call.trimmed, got %q", stored.Method)
	}
}

func TestAgentRequestDefaultsMethodInEventPayloadWhenMissing(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-request-default-method"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", "agent-b")
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Payload:     `{"ok":true}`,
		TimeoutSec:  30,
	})
	if err != nil {
		t.Fatalf("marshal request params: %v", err)
	}
	result, rpcErr := callAgentRequestRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentRequest rpc error: %+v", rpcErr)
	}
	requestPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected request result type %T", result)
	}
	requestID, ok := requestPayload["request_id"].(string)
	if !ok || requestID == "" {
		t.Fatalf("unexpected request_id payload %#v", requestPayload["request_id"])
	}

	evt := nextEvent(t, ch)
	if evt.Type != "agent.request" {
		t.Fatalf("expected agent.request event, got %+v", evt)
	}
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{
		"request_id": requestID,
		"from":       "agent-a",
		"method":     "default",
	})

	stored, err := h.store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("get stored request result: %v", err)
	}
	if stored.Method != "default" {
		t.Fatalf("expected stored default method, got %q", stored.Method)
	}
}

func TestAgentRequestDedupesOpenDelegateTaskForSameRecipient(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-request-dedupe-delegate-task"
	ctx := context.Background()
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "gamma", "delta", "beta")
	payload := `{"request_kind":"delegate_task","task_id":"task-project-claim-repair-1","prompt":"Claim project claim repair task task-project-claim-repair-1."}`

	firstRaw, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: "gamma",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
		TimeoutSec:  60,
	})
	if err != nil {
		t.Fatalf("marshal first request params: %v", err)
	}
	first, rpcErr := callAgentRequestRaw(t, h, ctx, firstRaw)
	if rpcErr != nil {
		t.Fatalf("first agentRequest rpc error: %+v", rpcErr)
	}
	firstPayload, ok := first.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first result type %T", first)
	}
	firstID, _ := firstPayload["request_id"].(string)
	if firstID == "" {
		t.Fatalf("expected first request_id, got %#v", firstPayload)
	}

	secondRaw, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: "delta",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
		TimeoutSec:  60,
	})
	if err != nil {
		t.Fatalf("marshal second request params: %v", err)
	}
	second, rpcErr := callAgentRequestRaw(t, h, ctx, secondRaw)
	if rpcErr != nil {
		t.Fatalf("second agentRequest rpc error: %+v", rpcErr)
	}
	secondPayload, ok := second.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second result type %T", second)
	}
	if got, _ := secondPayload["request_id"].(string); got != firstID {
		t.Fatalf("expected duplicate to reuse first request id %q, got %#v", firstID, secondPayload)
	}
	if deduped, _ := secondPayload["deduped"].(bool); !deduped {
		t.Fatalf("expected deduped=true, got %#v", secondPayload)
	}
	if reusedNoWait, _ := secondPayload["reused_no_wait_authority"].(bool); !reusedNoWait {
		t.Fatalf("expected reused_no_wait_authority=true, got %#v", secondPayload)
	}
	if got := countServerMessagingRows(t, ctx, store, workspaceID, "agent_requests"); got != 1 {
		t.Fatalf("expected one stored agent request after duplicate delegate, got %d", got)
	}
	if got := countServerMessagingRuntimeEvents(t, ctx, store, workspaceID, "agent_request.sent", "agent_request", ""); got != 1 {
		t.Fatalf("expected one request-sent runtime event after duplicate delegate, got %d", got)
	}
	if got := countServerMessagingRuntimeEvents(t, ctx, store, workspaceID, "agent_request.reused", "agent_request", firstID); got != 1 {
		t.Fatalf("expected one request-reused runtime event after duplicate delegate, got %d", got)
	}
}

func TestAgentStateSetRecordsPendingTriggerReceipts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-pending-trigger-receipts"
	agentID := "agent-trigger"
	ctx := testAuthContext(workspaceID, "agent", agentID)
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, agentID)

	queuedRaw, err := json.Marshal(agentStateSetParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Key:         runtimeScratchAgentStateKey,
		Value:       `{"pending_trigger":"runtime_switch_task","pending_trigger_task":"task-switch-1","pending_trigger_session":"session-switch-1","pending_trigger_at":"2026-05-26T00:00:00Z"}`,
	})
	if err != nil {
		t.Fatalf("marshal queued state: %v", err)
	}
	if _, rpcErr := h.agentStateSet(ctx, queuedRaw); rpcErr != nil {
		t.Fatalf("agentStateSet queued rpc error: %+v", rpcErr)
	}
	if got := countServerMessagingRuntimeEvents(t, context.Background(), store, workspaceID, "runtime.pending_trigger.queued", "runtime_pending_trigger", ""); got != 1 {
		t.Fatalf("expected one pending trigger queued receipt, got %d", got)
	}

	consumedRaw, err := json.Marshal(agentStateSetParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Key:         runtimeScratchAgentStateKey,
		Value:       `{"active_task_id":"task-switch-1","active_session_id":"session-switch-1","last_wake_trigger":"runtime_switch_task"}`,
	})
	if err != nil {
		t.Fatalf("marshal consumed state: %v", err)
	}
	if _, rpcErr := h.agentStateSet(ctx, consumedRaw); rpcErr != nil {
		t.Fatalf("agentStateSet consumed rpc error: %+v", rpcErr)
	}
	if got := countServerMessagingRuntimeEvents(t, context.Background(), store, workspaceID, "runtime.pending_trigger.consumed", "runtime_pending_trigger", ""); got != 1 {
		t.Fatalf("expected one pending trigger consumed receipt, got %d", got)
	}
}

func TestAgentStateSetPendingTriggerReceiptFailureRollsBackScratch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-pending-trigger-receipt-rollback"
	agentID := "agent-trigger"
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(context.Background(), sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Pending Trigger Receipt Rollback",
		CreatedBy:   "messaging-test",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(context.Background(), sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "messaging-test",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	raw, err := json.Marshal(agentStateSetParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Key:         runtimeScratchAgentStateKey,
		Value:       `{"pending_trigger":"runtime_switch_task","pending_trigger_task":"task-switch-rollback","pending_trigger_at":"2026-05-26T00:00:00Z"}`,
	})
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	if _, rpcErr := h.agentStateSet(ctx, raw); rpcErr == nil {
		t.Fatal("expected missing workspace authority to reject scratch write")
	}
	if value, err := store.GetAgentState(context.Background(), workspaceID, agentID, runtimeScratchAgentStateKey); err == nil {
		t.Fatalf("scratch state must roll back when receipt cannot be written, got %q", value)
	}
	if got := countServerMessagingRuntimeEvents(t, context.Background(), store, workspaceID, "runtime.pending_trigger.queued", "runtime_pending_trigger", ""); got != 0 {
		t.Fatalf("expected no pending trigger receipt after rollback, got %d", got)
	}
}

func TestAgentRequestListClaimsRequestsAndReturnsProcessingStatus(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-request-list-claim"
	requestCtx := testAuthContext(workspaceID, "agent", "agent-a")
	listCtx := testAuthContext(workspaceID, "agent", "agent-b")
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", "agent-b")

	for _, params := range []agentRequestParams{
		{WorkspaceID: workspaceID, FromAgentID: "agent-a", ToAgentID: "agent-b", Method: "first", Payload: `{"n":1}`},
		{WorkspaceID: workspaceID, FromAgentID: "agent-a", ToAgentID: "agent-b", Method: "second", Payload: `{"n":2}`},
	} {
		raw, err := json.Marshal(params)
		if err != nil {
			t.Fatalf("marshal request params: %v", err)
		}
		if _, rpcErr := callAgentRequestRaw(t, h, requestCtx, raw); rpcErr != nil {
			t.Fatalf("agentRequest rpc error: %+v", rpcErr)
		}
	}

	rawOpenList, err := json.Marshal(agentRequestOpenListParams{WorkspaceID: workspaceID, AgentID: "agent-b", Limit: 10})
	if err != nil {
		t.Fatalf("marshal request open-list params: %v", err)
	}
	openResult, rpcErr := callAgentRequestOpenListRaw(t, h, listCtx, rawOpenList)
	if rpcErr != nil {
		t.Fatalf("agentRequestOpenList rpc error: %+v", rpcErr)
	}
	openPayload, ok := openResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected open-list result type %T", openResult)
	}
	openRequests, ok := openPayload["requests"].([]sqlite.AgentRequestRecord)
	if !ok || len(openRequests) != 2 || openRequests[0].Status != "PENDING" || openRequests[1].Status != "PENDING" {
		t.Fatalf("expected read-only open-list to return pending requests, got %#v", openPayload["requests"])
	}
	timedOutRequestID := openRequests[1].RequestID
	if _, err := store.DB().ExecContext(context.Background(),
		`UPDATE agent_requests SET status = 'TIMEOUT', response = NULL, responded_at = ? WHERE request_id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano), timedOutRequestID,
	); err != nil {
		t.Fatalf("mark second request timeout: %v", err)
	}
	openResult, rpcErr = callAgentRequestOpenListRaw(t, h, listCtx, rawOpenList)
	if rpcErr != nil {
		t.Fatalf("agentRequestOpenList after timeout rpc error: %+v", rpcErr)
	}
	openPayload, ok = openResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second open-list result type %T", openResult)
	}
	openRequests, ok = openPayload["requests"].([]sqlite.AgentRequestRecord)
	if !ok || len(openRequests) != 2 || openRequests[0].Status != "PENDING" || openRequests[1].Status != "TIMEOUT" {
		t.Fatalf("expected read-only open-list to include recoverable timeout request, got %#v", openPayload["requests"])
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawList, err := json.Marshal(agentRequestListParams{WorkspaceID: workspaceID, AgentID: "agent-b"})
	if err != nil {
		t.Fatalf("marshal request list params: %v", err)
	}

	result, rpcErr := callAgentRequestListRaw(t, h, listCtx, rawList)
	if rpcErr != nil {
		t.Fatalf("agentRequestList rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected list result type %T", result)
	}
	requests, ok := payload["requests"].([]sqlite.AgentRequestRecord)
	if !ok {
		t.Fatalf("unexpected requests type %T", payload["requests"])
	}
	if len(requests) != 2 {
		t.Fatalf("expected 2 claimed requests, got %+v", requests)
	}
	if requests[0].Status != "PROCESSING" || requests[1].Status != "PROCESSING" {
		t.Fatalf("expected PROCESSING requests, got %+v", requests)
	}
	if count, ok := payload["count"].(int); !ok || count != 2 {
		t.Fatalf("expected count=2, got %#v", payload["count"])
	}
	for _, request := range requests {
		claimEvent := nextEvent(t, ch)
		if claimEvent.Type != "agent.request.claimed" {
			t.Fatalf("expected agent.request.claimed event, got %+v", claimEvent)
		}
		claimRuntimeEvents, err := store.ListRuntimeEvents(listCtx, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "agent_request.claimed",
			EntityType:  "agent_request",
			EntityID:    request.RequestID,
			Limit:       10,
		})
		if err != nil {
			t.Fatalf("list claim runtime events: %v", err)
		}
		if len(claimRuntimeEvents) != 1 {
			t.Fatalf("expected 1 claim runtime event, got %+v", claimRuntimeEvents)
		}
		expectedPreviousStatus := "PENDING"
		if request.RequestID == timedOutRequestID {
			expectedPreviousStatus = "TIMEOUT"
		}
		assertValidEventPayload(t, claimRuntimeEvents[0].PayloadJSON, map[string]string{
			"request_id":      request.RequestID,
			"from":            "agent-a",
			"from_agent_id":   "agent-a",
			"to_agent_id":     "agent-b",
			"method":          request.Method,
			"status":          "PROCESSING",
			"previous_status": expectedPreviousStatus,
		})
		assertAgentRequestRuntimePromptContext(t, claimRuntimeEvents[0], "agent.request.list", workspaceID, "agent-b", request.RequestID, "agent-a", "agent-b", request.Method, "PROCESSING")
		assertAgentRequestRuntimePromptContextExtraField(t, claimRuntimeEvents[0], "previous_status", expectedPreviousStatus)
		assertLiveEventMirrorsRuntimeEventWithAgentID(t, claimEvent, claimRuntimeEvents[0], "agent.request.claimed", "agent-a")
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, claimEvent.PayloadJSON), claimRuntimeEvents[0].PayloadJSON)
		assertSSEAliasTypeAndOptionalCanonicalEventType(t, claimEvent, "agent_request.claimed")
	}

	result, rpcErr = callAgentRequestListRaw(t, h, listCtx, rawList)
	if rpcErr != nil {
		t.Fatalf("agentRequestList second rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second list result type %T", result)
	}
	requests, ok = payload["requests"].([]sqlite.AgentRequestRecord)
	if !ok {
		t.Fatalf("unexpected second requests type %T", payload["requests"])
	}
	if len(requests) != 0 {
		t.Fatalf("expected claimed requests to disappear from second list, got %+v", requests)
	}
}

func TestAgentRespondNormalizesTrimmedRequestIDAcrossResultAndEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-respond-trimmed-request-id"
	requestCtx := testAuthContext(workspaceID, "agent", "agent-a")
	respondCtx := testAuthContext(workspaceID, "agent", "agent-b")
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", "agent-b")
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawRequest, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "trimmed.respond",
		Payload:     `{"ok":true}`,
		TimeoutSec:  30,
	})
	if err != nil {
		t.Fatalf("marshal request params: %v", err)
	}
	result, rpcErr := callAgentRequestRaw(t, h, requestCtx, rawRequest)
	if rpcErr != nil {
		t.Fatalf("agentRequest rpc error: %+v", rpcErr)
	}
	requestPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected request result type %T", result)
	}
	requestID, ok := requestPayload["request_id"].(string)
	if !ok || requestID == "" {
		t.Fatalf("unexpected request_id payload %#v", requestPayload["request_id"])
	}
	_ = nextEvent(t, ch)

	rawRespond, err := json.Marshal(agentRespondParams{RequestID: "  " + requestID + "  ", Response: `done`})
	if err != nil {
		t.Fatalf("marshal respond params: %v", err)
	}
	result, rpcErr = callAgentRespondRaw(t, h, respondCtx, rawRespond)
	if rpcErr != nil {
		t.Fatalf("agentRespond rpc error: %+v", rpcErr)
	}
	respondPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected respond result type %T", result)
	}
	if gotRequestID, ok := respondPayload["request_id"].(string); !ok || gotRequestID != requestID {
		t.Fatalf("expected trimmed request_id %q, got %#v", requestID, respondPayload["request_id"])
	}

	evt := nextEvent(t, ch)
	if evt.Type != "agent.response" {
		t.Fatalf("expected agent.response event, got %+v", evt)
	}
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{"request_id": requestID, "from": "agent-b"})
}

func TestAgentRequestResultNormalizesTrimmedRequestID(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-request-result-trimmed-request-id"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", "agent-b")

	rawRequest, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "trimmed.result",
		Payload:     `{"ok":true}`,
		TimeoutSec:  30,
	})
	if err != nil {
		t.Fatalf("marshal request params: %v", err)
	}
	result, rpcErr := callAgentRequestRaw(t, h, ctx, rawRequest)
	if rpcErr != nil {
		t.Fatalf("agentRequest rpc error: %+v", rpcErr)
	}
	requestPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected request result type %T", result)
	}
	requestID, ok := requestPayload["request_id"].(string)
	if !ok || requestID == "" {
		t.Fatalf("unexpected request_id payload %#v", requestPayload["request_id"])
	}

	rawResult, err := json.Marshal(agentRequestResultParams{RequestID: "  " + requestID + "  "})
	if err != nil {
		t.Fatalf("marshal request result params: %v", err)
	}
	result, rpcErr = callAgentRequestResultRaw(t, h, ctx, rawResult)
	if rpcErr != nil {
		t.Fatalf("agentRequestResult rpc error: %+v", rpcErr)
	}
	stored, ok := result.(sqlite.AgentRequestRecord)
	if !ok {
		t.Fatalf("unexpected request result payload type %T", result)
	}
	if stored.RequestID != requestID {
		t.Fatalf("expected trimmed request_id %q, got %+v", requestID, stored)
	}
}

func TestAgentRequestResultRequiresAgentPrincipalType(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-request-result-principal-type"
	agentCtx := testAuthContext(workspaceID, "agent", "agent-a")
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", "agent-b")

	rawRequest, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "principal.type.result",
		Payload:     `{"ok":true}`,
		TimeoutSec:  30,
	})
	if err != nil {
		t.Fatalf("marshal request params: %v", err)
	}
	result, rpcErr := callAgentRequestRaw(t, h, agentCtx, rawRequest)
	if rpcErr != nil {
		t.Fatalf("agentRequest rpc error: %+v", rpcErr)
	}
	requestPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected request result type %T", result)
	}
	requestID, ok := requestPayload["request_id"].(string)
	if !ok || requestID == "" {
		t.Fatalf("unexpected request_id payload %#v", requestPayload["request_id"])
	}

	humanCtx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   workspaceID,
		PrincipalType: "human",
		PrincipalID:   "agent-a",
	})
	rawResult, err := json.Marshal(agentRequestResultParams{RequestID: requestID})
	if err != nil {
		t.Fatalf("marshal request result params: %v", err)
	}
	if _, rpcErr = callAgentRequestResultRaw(t, h, humanCtx, rawResult); rpcErr == nil ||
		rpcErr.Code != errCodePermissionDenied ||
		rpcErr.Message != "agent principal required" {
		t.Fatalf("expected agent-principal permission error, got %+v", rpcErr)
	}
}

func TestAgentRequestLookupErrorsMapToRPCBoundaryFailures(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-1",
		PrincipalType: "agent",
		PrincipalID:   "agent-1",
	})

	for _, tc := range []struct {
		name       string
		call       func() (any, *RPCError)
		wantCode   int
		wantErrMsg string
	}{
		{
			name: "respond missing request lookup",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRespondParams{RequestID: "areq-missing", Response: "done"})
				return callAgentRespondRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "request not found",
		},
		{
			name: "request result missing lookup",
			call: func() (any, *RPCError) {
				raw, _ := json.Marshal(agentRequestResultParams{RequestID: "areq-missing"})
				return callAgentRequestResultRaw(t, h, ctx, raw)
			},
			wantCode:   errCodeInvalidParams,
			wantErrMsg: "request not found",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rpcErr := tc.call()
			if rpcErr == nil {
				t.Fatal("expected rpc error")
			}
			if rpcErr.Code != tc.wantCode || rpcErr.Message != tc.wantErrMsg {
				t.Fatalf("expected (%d, %q), got (%d, %q)", tc.wantCode, tc.wantErrMsg, rpcErr.Code, rpcErr.Message)
			}
		})
	}
	_ = store
}

func TestAgentMessageSendRejectsMissingWorkspace(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-message-missing-workspace", "agent", "agent-a")

	raw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID: "ws-message-missing-workspace",
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "hello",
	})
	if err != nil {
		t.Fatalf("marshal send params: %v", err)
	}

	_, rpcErr := callAgentMessageSendRaw(t, h, ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected rpc error")
	}
	if rpcErr.Code != errCodeInvalidParams || rpcErr.Message != "workspace not found" {
		t.Fatalf("expected workspace not found invalid params error, got %+v", rpcErr)
	}
}

func TestMessagingEventPayloadJSONIsEscaped(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-event-payload",
		PrincipalType: "agent",
		PrincipalID:   `agent"quoted`,
	})
	workspaceID := "ws-event-payload"
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, `agent"quoted`, "agent-b", `agent"sender`, `agent"target`)
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	sendTestMessage(t, h, ctx, agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: `agent"quoted`,
		ToAgentID:   "agent-b",
		Content:     "hello",
	})
	evt := nextEvent(t, ch)
	if evt.Type != "agent.message" {
		t.Fatalf("expected agent.message event, got %+v", evt)
	}
	assertValidEventTimestamp(t, evt.Timestamp)
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{"from": `agent"quoted`})

	ctx = context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   workspaceID,
		PrincipalType: "agent",
		PrincipalID:   `agent"sender`,
	})
	rawRequest, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: `agent"sender`,
		ToAgentID:   `agent"target`,
		Method:      `method"quoted`,
		Payload:     `{"ok":true}`,
		TimeoutSec:  30,
	})
	if err != nil {
		t.Fatalf("marshal request params: %v", err)
	}
	result, rpcErr := callAgentRequestRaw(t, h, ctx, rawRequest)
	if rpcErr != nil {
		t.Fatalf("agentRequest rpc error: %+v", rpcErr)
	}
	requestPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected request result type %T", result)
	}
	requestID, ok := requestPayload["request_id"].(string)
	if !ok || requestID == "" {
		t.Fatalf("unexpected request_id payload %#v", requestPayload["request_id"])
	}
	evt = nextEvent(t, ch)
	if evt.Type != "agent.request" {
		t.Fatalf("expected agent.request event, got %+v", evt)
	}
	assertValidEventTimestamp(t, evt.Timestamp)
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{"from": `agent"sender`, "method": `method"quoted`})

	ctx = context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   workspaceID,
		PrincipalType: "agent",
		PrincipalID:   `agent"target`,
	})
	rawRespond, err := json.Marshal(agentRespondParams{RequestID: requestID, Response: `done`})
	if err != nil {
		t.Fatalf("marshal respond params: %v", err)
	}
	result, rpcErr = callAgentRespondRaw(t, h, ctx, rawRespond)
	if rpcErr != nil {
		t.Fatalf("agentRespond rpc error: %+v", rpcErr)
	}
	if _, ok := result.(map[string]any); !ok {
		t.Fatalf("unexpected respond result type %T", result)
	}
	evt = nextEvent(t, ch)
	if evt.Type != "agent.response" {
		t.Fatalf("expected agent.response event, got %+v", evt)
	}
	assertValidEventTimestamp(t, evt.Timestamp)
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{"from": `agent"target`})
}

func TestAgentMessageSendAppendsAlignedRuntimeEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-message-runtime-aligned"
		fromAgentID = "agent-a"
		toAgentID   = "agent-b"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Message Runtime Alignment",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     fromAgentID,
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register sender: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     toAgentID,
		OwnerUserID: "developer",
		DisplayName: "Agent B",
	}); err != nil {
		t.Fatalf("register recipient: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Channel:     "ops",
		ContentType: "application/json",
		Content:     `{"hello":"world"}`,
	})
	if err != nil {
		t.Fatalf("marshal message send params: %v", err)
	}

	result, rpcErr := callAgentMessageSendRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessageSend rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected message result type %T", result)
	}
	messageID, ok := payload["message_id"].(string)
	if !ok || messageID == "" {
		t.Fatalf("unexpected message_id payload %#v", payload["message_id"])
	}

	evt := nextEvent(t, ch)
	if evt.Type != "agent.message" {
		t.Fatalf("expected agent.message event, got %+v", evt)
	}
	assertValidEventPayload(t, evt.PayloadJSON, map[string]string{
		"message_id":    messageID,
		"from":          fromAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"channel":       "ops",
		"content_type":  "application/json",
		"status":        "SENT",
	})

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    messageID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 runtime event, got %+v", events)
	}
	if events[0].AgentID != fromAgentID || events[0].ActorID != fromAgentID {
		t.Fatalf("expected sender runtime event ids, got %+v", events[0])
	}
	assertValidEventPayload(t, events[0].PayloadJSON, map[string]string{
		"message_id":    messageID,
		"from":          fromAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"channel":       "ops",
		"content_type":  "application/json",
		"status":        "SENT",
	})
	assertAgentMessageRuntimePromptContext(t, events[0], "agent.message.send", workspaceID, fromAgentID, toAgentID, "ops")
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, evt, events[0], "agent.message", toAgentID)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, evt.PayloadJSON), events[0].PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, evt, "agent_message.sent")

	secondRaw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Channel:     "ops",
		ContentType: "application/json",
		Content:     `{"hello":"again"}`,
	})
	if err != nil {
		t.Fatalf("marshal second message send params: %v", err)
	}

	secondResult, rpcErr := callAgentMessageSendRaw(t, h, ctx, secondRaw)
	if rpcErr != nil {
		t.Fatalf("second agentMessageSend rpc error: %+v", rpcErr)
	}
	secondPayload, ok := secondResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second message result type %T", secondResult)
	}
	secondMessageID, ok := secondPayload["message_id"].(string)
	if !ok || secondMessageID == "" {
		t.Fatalf("unexpected second message_id payload %#v", secondPayload["message_id"])
	}

	secondEvt := nextEvent(t, ch)
	if secondEvt.Type != "agent.message" {
		t.Fatalf("expected second agent.message event, got %+v", secondEvt)
	}
	assertValidEventPayload(t, secondEvt.PayloadJSON, map[string]string{
		"message_id":    secondMessageID,
		"from":          fromAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"channel":       "ops",
		"content_type":  "application/json",
		"status":        "SENT",
	})

	secondEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    secondMessageID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second runtime events: %v", err)
	}
	if len(secondEvents) != 1 {
		t.Fatalf("expected 1 second runtime event, got %+v", secondEvents)
	}
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, secondEvt, secondEvents[0], "agent.message", toAgentID)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondEvt.PayloadJSON), secondEvents[0].PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, secondEvt, "agent_message.sent")
	if secondEvents[0].EventID == events[0].EventID || secondEvents[0].IngestSeq <= events[0].IngestSeq {
		t.Fatalf("expected second send to mirror a newly appended runtime row, got first=%+v second=%+v", events[0], secondEvents[0])
	}
}

func TestAgentRequestAndResponseAppendAlignedRuntimeEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-request-runtime-aligned"
		fromAgentID = "agent-a"
		toAgentID   = "agent-b"
	)
	requestCtx := testAuthContext(workspaceID, "agent", fromAgentID)
	responseCtx := testAuthContext(workspaceID, "agent", toAgentID)

	if err := store.CreateWorkspace(requestCtx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Request Runtime Alignment",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{fromAgentID, toAgentID} {
		if err := store.RegisterAgent(requestCtx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	claimServerTestWorkspaceAuthority(t, requestCtx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawRequest, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Method:      "call.runtime",
		Payload:     `{"ok":true}`,
		TimeoutSec:  45,
	})
	if err != nil {
		t.Fatalf("marshal request params: %v", err)
	}

	result, rpcErr := callAgentRequestRaw(t, h, requestCtx, rawRequest)
	if rpcErr != nil {
		t.Fatalf("agentRequest rpc error: %+v", rpcErr)
	}
	requestPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected request result type %T", result)
	}
	requestID, ok := requestPayload["request_id"].(string)
	if !ok || requestID == "" {
		t.Fatalf("unexpected request_id payload %#v", requestPayload["request_id"])
	}

	requestEvent := nextEvent(t, ch)
	if requestEvent.Type != "agent.request" {
		t.Fatalf("expected agent.request event, got %+v", requestEvent)
	}
	assertValidEventPayload(t, requestEvent.PayloadJSON, map[string]string{
		"request_id":    requestID,
		"from":          fromAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"method":        "call.runtime",
		"status":        "PENDING",
	})

	requestRuntimeEvents, err := store.ListRuntimeEvents(requestCtx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_request.sent",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list request runtime events: %v", err)
	}
	if len(requestRuntimeEvents) != 1 {
		t.Fatalf("expected 1 request runtime event, got %+v", requestRuntimeEvents)
	}
	assertValidEventPayload(t, requestRuntimeEvents[0].PayloadJSON, map[string]string{
		"request_id":    requestID,
		"from":          fromAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"method":        "call.runtime",
		"status":        "PENDING",
	})
	assertAgentRequestRuntimePromptContext(t, requestRuntimeEvents[0], "agent.request", workspaceID, fromAgentID, requestID, fromAgentID, toAgentID, "call.runtime", "PENDING")
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, requestEvent, requestRuntimeEvents[0], "agent.request", toAgentID)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, requestEvent.PayloadJSON), requestRuntimeEvents[0].PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, requestEvent, "agent_request.sent")

	rawRespond, err := json.Marshal(agentRespondParams{RequestID: requestID, Response: `{"done":true}`})
	if err != nil {
		t.Fatalf("marshal respond params: %v", err)
	}
	if _, rpcErr := callAgentRespondRaw(t, h, responseCtx, rawRespond); rpcErr != nil {
		t.Fatalf("agentRespond rpc error: %+v", rpcErr)
	}

	responseEvent := nextEvent(t, ch)
	if responseEvent.Type != "agent.response" {
		t.Fatalf("expected agent.response event, got %+v", responseEvent)
	}
	assertValidEventPayload(t, responseEvent.PayloadJSON, map[string]string{
		"request_id":    requestID,
		"from":          toAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"method":        "call.runtime",
		"status":        "COMPLETED",
	})

	responseRuntimeEvents, err := store.ListRuntimeEvents(requestCtx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_response.recorded",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list response runtime events: %v", err)
	}
	if len(responseRuntimeEvents) != 1 {
		t.Fatalf("expected 1 response runtime event, got %+v", responseRuntimeEvents)
	}
	assertValidEventPayload(t, responseRuntimeEvents[0].PayloadJSON, map[string]string{
		"request_id":    requestID,
		"from":          toAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"method":        "call.runtime",
		"status":        "COMPLETED",
	})
	assertAgentRequestRuntimePromptContext(t, responseRuntimeEvents[0], "agent.respond", workspaceID, toAgentID, requestID, fromAgentID, toAgentID, "call.runtime", "COMPLETED")
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, responseEvent, responseRuntimeEvents[0], "agent.response", fromAgentID)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, responseEvent.PayloadJSON), responseRuntimeEvents[0].PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, responseEvent, "agent_response.recorded")

	secondRawRequest, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Method:      "call.runtime",
		Payload:     `{"ok":"second"}`,
		TimeoutSec:  45,
	})
	if err != nil {
		t.Fatalf("marshal second request params: %v", err)
	}

	secondResult, rpcErr := callAgentRequestRaw(t, h, requestCtx, secondRawRequest)
	if rpcErr != nil {
		t.Fatalf("second agentRequest rpc error: %+v", rpcErr)
	}
	secondRequestPayload, ok := secondResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second request result type %T", secondResult)
	}
	secondRequestID, ok := secondRequestPayload["request_id"].(string)
	if !ok || secondRequestID == "" {
		t.Fatalf("unexpected second request_id payload %#v", secondRequestPayload["request_id"])
	}

	secondRequestEvent := nextEvent(t, ch)
	if secondRequestEvent.Type != "agent.request" {
		t.Fatalf("expected second agent.request event, got %+v", secondRequestEvent)
	}
	assertValidEventPayload(t, secondRequestEvent.PayloadJSON, map[string]string{
		"request_id":    secondRequestID,
		"from":          fromAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"method":        "call.runtime",
		"status":        "PENDING",
	})

	secondRequestRuntimeEvents, err := store.ListRuntimeEvents(requestCtx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_request.sent",
		EntityType:  "agent_request",
		EntityID:    secondRequestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second request runtime events: %v", err)
	}
	if len(secondRequestRuntimeEvents) != 1 {
		t.Fatalf("expected 1 second request runtime event, got %+v", secondRequestRuntimeEvents)
	}
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, secondRequestEvent, secondRequestRuntimeEvents[0], "agent.request", toAgentID)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondRequestEvent.PayloadJSON), secondRequestRuntimeEvents[0].PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, secondRequestEvent, "agent_request.sent")
	if secondRequestRuntimeEvents[0].EventID == requestRuntimeEvents[0].EventID || secondRequestRuntimeEvents[0].IngestSeq <= requestRuntimeEvents[0].IngestSeq {
		t.Fatalf("expected second request to mirror a newly appended runtime row, got first=%+v second=%+v", requestRuntimeEvents[0], secondRequestRuntimeEvents[0])
	}

	secondRawRespond, err := json.Marshal(agentRespondParams{RequestID: secondRequestID, Response: `{"done":"again"}`})
	if err != nil {
		t.Fatalf("marshal second respond params: %v", err)
	}
	if _, rpcErr := callAgentRespondRaw(t, h, responseCtx, secondRawRespond); rpcErr != nil {
		t.Fatalf("second agentRespond rpc error: %+v", rpcErr)
	}

	secondResponseEvent := nextEvent(t, ch)
	if secondResponseEvent.Type != "agent.response" {
		t.Fatalf("expected second agent.response event, got %+v", secondResponseEvent)
	}
	assertValidEventPayload(t, secondResponseEvent.PayloadJSON, map[string]string{
		"request_id":    secondRequestID,
		"from":          toAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"method":        "call.runtime",
		"status":        "COMPLETED",
	})

	secondResponseRuntimeEvents, err := store.ListRuntimeEvents(requestCtx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_response.recorded",
		EntityType:  "agent_request",
		EntityID:    secondRequestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second response runtime events: %v", err)
	}
	if len(secondResponseRuntimeEvents) != 1 {
		t.Fatalf("expected 1 second response runtime event, got %+v", secondResponseRuntimeEvents)
	}
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, secondResponseEvent, secondResponseRuntimeEvents[0], "agent.response", fromAgentID)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondResponseEvent.PayloadJSON), secondResponseRuntimeEvents[0].PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, secondResponseEvent, "agent_response.recorded")
	if secondResponseRuntimeEvents[0].EventID == responseRuntimeEvents[0].EventID || secondResponseRuntimeEvents[0].IngestSeq <= responseRuntimeEvents[0].IngestSeq {
		t.Fatalf("expected second response to mirror a newly appended runtime row, got first=%+v second=%+v", responseRuntimeEvents[0], secondResponseRuntimeEvents[0])
	}
}

func sendTestMessage(t *testing.T, h *Handler, ctx context.Context, params agentMessageSendParams) string {
	t.Helper()

	ensureMessagingTestWorkspaceAndAgents(t, h.store, params.WorkspaceID, params.FromAgentID, params.ToAgentID)

	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal send params: %v", err)
	}
	result, rpcErr := callAgentMessageSendRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessageSend rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected send result type %T", result)
	}
	messageID, ok := payload["message_id"].(string)
	if !ok || messageID == "" {
		t.Fatalf("unexpected message_id payload %#v", payload["message_id"])
	}
	return messageID
}

func pollTestMessages(t *testing.T, h *Handler, ctx context.Context, params agentMessagePollParams) []agentPollMessage {
	t.Helper()

	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal poll params: %v", err)
	}
	result, rpcErr := callAgentMessagePollRaw(t, h, ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected poll result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	return messages
}

func nextEvent(t *testing.T, ch <-chan EventMessage) EventMessage {
	t.Helper()

	select {
	case evt := <-ch:
		return evt
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return EventMessage{}
	}
}

func assertValidEventTimestamp(t *testing.T, timestamp string) {
	t.Helper()

	if strings.TrimSpace(timestamp) == "" {
		t.Fatal("expected non-empty event timestamp")
	}
	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		t.Fatalf("invalid event timestamp %q: %v", timestamp, err)
	}
}

func assertValidEventPayload(t *testing.T, payload string, want map[string]string) {
	t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("invalid event payload JSON %q: %v", payload, err)
	}
	for key, expected := range want {
		if decoded[key] != expected {
			t.Fatalf("expected payload[%q]=%q, got %v in %+v", key, expected, decoded[key], decoded)
		}
	}
}

func assertAgentMessageRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantFromAgentID, wantToAgentID, wantChannel string) {
	t.Helper()

	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected agent message prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	assertAgentMessagePromptContextField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertAgentMessagePromptContextField(t, envelope, "context_kind", "authority_bearing_agent_message")
	assertAgentMessagePromptContextField(t, envelope, "surface", wantSurface)
	assertAgentMessagePromptContextField(t, envelope, "origin", "server_rpc")
	assertAgentMessagePromptContextField(t, envelope, "workspace_id", wantWorkspaceID)
	assertAgentMessagePromptContextField(t, envelope, "principal_type", "agent")
	assertAgentMessagePromptContextField(t, envelope, "principal_id", wantFromAgentID)
	assertAgentMessagePromptContextField(t, envelope, "actor_agent_id", wantFromAgentID)
	assertAgentMessagePromptContextField(t, envelope, "from_agent_id", wantFromAgentID)
	assertAgentMessagePromptContextField(t, envelope, "to_agent_id", wantToAgentID)
	assertAgentMessagePromptContextField(t, envelope, "channel", wantChannel)
	assertAgentMessagePromptContextField(t, envelope, "authority_model", "workspace_authority")
	assertAgentMessagePromptContextField(t, envelope, "compiler_status", "non_daemon_context_envelope")
	assertAgentMessagePromptContextField(t, envelope, "daemon_prompt_compiler_convergence", "not_claimed")
	assertAgentMessagePromptContextField(t, envelope, "prompt_capability_evidence", "not_present")
}

func assertAgentRequestRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantPrincipalID, wantRequestID, wantFromAgentID, wantToAgentID, wantMethod, wantStatus string) {
	t.Helper()

	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected agent request prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	assertAgentRequestPromptContextField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertAgentRequestPromptContextField(t, envelope, "context_kind", "authority_bearing_agent_request")
	assertAgentRequestPromptContextField(t, envelope, "surface", wantSurface)
	assertAgentRequestPromptContextField(t, envelope, "origin", "server_rpc")
	assertAgentRequestPromptContextField(t, envelope, "workspace_id", wantWorkspaceID)
	assertAgentRequestPromptContextField(t, envelope, "principal_type", "agent")
	assertAgentRequestPromptContextField(t, envelope, "principal_id", wantPrincipalID)
	assertAgentRequestPromptContextField(t, envelope, "actor_agent_id", wantPrincipalID)
	assertAgentRequestPromptContextField(t, envelope, "request_id", wantRequestID)
	assertAgentRequestPromptContextField(t, envelope, "from_agent_id", wantFromAgentID)
	assertAgentRequestPromptContextField(t, envelope, "to_agent_id", wantToAgentID)
	assertAgentRequestPromptContextField(t, envelope, "method", wantMethod)
	assertAgentRequestPromptContextField(t, envelope, "status", wantStatus)
	assertAgentRequestPromptContextField(t, envelope, "authority_model", "workspace_authority")
	assertAgentRequestPromptContextField(t, envelope, "compiler_status", "non_daemon_context_envelope")
	assertAgentRequestPromptContextField(t, envelope, "daemon_prompt_compiler_convergence", "not_claimed")
	assertAgentRequestPromptContextField(t, envelope, "prompt_capability_evidence", "not_present")
}

func assertAgentRequestRuntimePromptContextExtraField(t *testing.T, event sqlite.RuntimeEventRecord, key, want string) {
	t.Helper()

	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected agent request prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	assertAgentRequestPromptContextField(t, envelope, key, want)
}

func assertAgentMessagePromptContextField(t *testing.T, envelope map[string]any, key, want string) {
	t.Helper()
	got, ok := envelope[key].(string)
	if !ok {
		t.Fatalf("agent message prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
	}
	if got != want {
		t.Fatalf("agent message prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
	}
}

func assertAgentRequestPromptContextField(t *testing.T, envelope map[string]any, key, want string) {
	t.Helper()
	got, ok := envelope[key].(string)
	if !ok {
		t.Fatalf("agent request prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
	}
	if got != want {
		t.Fatalf("agent request prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
	}
}

func recentPollBaseTime(offset time.Duration) time.Time {
	return time.Now().UTC().Add(offset).Truncate(time.Second)
}

func formatPollTime(ts time.Time) string {
	return ts.Format(time.RFC3339Nano)
}

func recentPollTimestamp(offset time.Duration) string {
	return formatPollTime(recentPollBaseTime(offset))
}

func intPtr(v int) *int { return &v }

var (
	testDBCacheBytes []byte
	testDBCacheOnce  sync.Once
)

func newServerTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	os.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")
	t.Cleanup(func() { os.Unsetenv("RHIZOME_ALLOW_PLAINTEXT_VAULT") })

	testDBCacheOnce.Do(func() {
		dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("rhizome-server-master-cache-%d.db", os.Getpid()))
		_ = os.Remove(dbPath)
		masterStore, err := sqlite.NewStore(dbPath)
		if err != nil {
			panic("NewStore failed for server master cache: " + err.Error())
		}
		if err := masterStore.ApplyMigrations(context.Background()); err != nil {
			_ = masterStore.Close()
			panic("ApplyMigrations failed for server master cache: " + err.Error())
		}
		_ = masterStore.Close()

		bytes, err := os.ReadFile(dbPath)
		if err != nil {
			panic("ReadFile failed for server master cache: " + err.Error())
		}
		testDBCacheBytes = bytes
	})

	dbPath := filepath.Join(t.TempDir(), "rhizome-server-test.db")
	if err := os.WriteFile(dbPath, testDBCacheBytes, 0644); err != nil {
		t.Fatalf("WriteFile failed to copy cache to temp dir: %v", err)
	}

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	store.AllowLegacyPatchOnlySubmitsForTesting()
	node, err := store.EnsureLocalAuthorityNode(context.Background())
	if err != nil {
		_ = store.Close()
		t.Fatalf("EnsureLocalAuthorityNode failed: %v", err)
	}
	authorityNodeIDLiteral := strings.ReplaceAll(node.AuthorityNodeID, `'`, `''`)
	triggerSQL := fmt.Sprintf(`
CREATE TRIGGER IF NOT EXISTS test_seed_workspace_authority_after_insert
AFTER INSERT ON workspaces
WHEN instr(lower(NEW.workspace_id), 'memory') > 0
BEGIN
	INSERT INTO workspace_authority(
		workspace_id,
		scope,
		holder_authority_node_id,
		lease_token,
		term,
		lease_expires_at,
		commit_watermark,
		applied_watermark,
		status,
		updated_at
	) VALUES (
		NEW.workspace_id,
		'workspace',
		'%s',
		'lease-server-test-auto-' || NEW.workspace_id,
		1,
		strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ','now','+1 hour'),
		1,
		1,
		'ACTIVE',
		strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ','now')
	)
	ON CONFLICT(workspace_id, scope) DO NOTHING;
END
`, authorityNodeIDLiteral)
	if _, err := store.DB().ExecContext(context.Background(), triggerSQL); err != nil {
		_ = store.Close()
		t.Fatalf("install workspace authority seed trigger failed: %v", err)
	}

	t.Cleanup(func() {
		_ = store.Close()
	})

	return store
}
