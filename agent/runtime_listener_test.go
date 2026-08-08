package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestProcessMessageBatchAdvancesCursorAfterPartialFailure(t *testing.T) {
	var mu sync.Mutex
	var updateNotes []string
	var ackIDs []string
	updateCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.update.post":
			mu.Lock()
			updateCount++
			call := updateCount
			mu.Unlock()

			var payload map[string]any
			if raw := rpcString(req.Params, "payload_json"); raw != "" {
				if err := json.Unmarshal([]byte(raw), &payload); err != nil {
					t.Fatalf("decode update payload: %v", err)
				}
				mu.Lock()
				updateNotes = append(updateNotes, rpcStringMap(payload, "notes"))
				mu.Unlock()
			}
			if call == 2 {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.message.ack":
			rawIDs, ok := req.Params["message_ids"].([]any)
			if !ok {
				t.Fatalf("expected message_ids array, got %#v", req.Params["message_ids"])
			}
			ackIDs = ackIDs[:0]
			for _, rawID := range rawIDs {
				id, _ := rawID.(string)
				ackIDs = append(ackIDs, id)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during message batch handling: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client:  NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	result := PollMessagesResult{
		Messages: []MessageRecord{
			{MessageID: "msg-b", CreatedAt: "2026-03-23T10:00:01Z", FromAgentID: "agent-b", Content: "later message"},
			{MessageID: "msg-a", CreatedAt: "2026-03-23T10:00:00Z", FromAgentID: "agent-a", Content: "earlier message", MetadataJSON: "{\"task_id\":\"task-1\"}"},
		},
	}

	outcome, err := runtime.processMessageBatch(context.Background(), result)
	if err == nil {
		t.Fatal("expected partial failure from second message")
	}
	if !outcome.hadError {
		t.Fatalf("expected hadError to be true, got %+v", outcome)
	}
	if outcome.handled != 1 {
		t.Fatalf("expected one handled message, got %+v", outcome)
	}
	if got := outcome.nextCursor; got != "2026-03-23T10:00:01Z|msg-b" {
		t.Fatalf("unexpected cursor: %q", got)
	}
	if strings.TrimSpace(err.Error()) == "" || !strings.Contains(err.Error(), "msg-b") {
		t.Fatalf("expected error to mention failing message, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ackIDs) != 1 || ackIDs[0] != "msg-a" {
		t.Fatalf("unexpected ack ids: %#v", ackIDs)
	}
	if len(updateNotes) != 2 {
		t.Fatalf("expected both messages to be processed before failure, got notes %#v", updateNotes)
	}
	if updateNotes[0] != "earlier message" || updateNotes[1] != "later message" {
		t.Fatalf("unexpected update order: %#v", updateNotes)
	}
}

func TestProcessRequestBatchContinuesAfterFailure(t *testing.T) {
	var mu sync.Mutex
	var respondedIDs []string
	callCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.respond":
			mu.Lock()
			callCount++
			call := callCount
			respondedIDs = append(respondedIDs, rpcString(req.Params, "request_id"))
			mu.Unlock()

			if call == 1 {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during request batch handling: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client:  NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	requests := []AgentRequestRecord{
		{RequestID: "req-b", Method: "runtime.status", CreatedAt: "2026-03-23T10:00:01Z"},
		{RequestID: "req-a", Method: "runtime.status", CreatedAt: "2026-03-23T10:00:00Z"},
	}

	err := runtime.processRequestBatch(context.Background(), requests)
	if err == nil {
		t.Fatal("expected batch error from first request response failure")
	}
	if !strings.Contains(err.Error(), "req-a") {
		t.Fatalf("expected error to mention failing request, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(respondedIDs) != 2 {
		t.Fatalf("expected both requests to be attempted, got %#v", respondedIDs)
	}
	if respondedIDs[0] != "req-a" || respondedIDs[1] != "req-b" {
		t.Fatalf("unexpected request order: %#v", respondedIDs)
	}
}
