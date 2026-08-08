package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRuntimeProcessNewsBatchQueuesSystemNewsTriggerAndAdvancesCursor(t *testing.T) {
	var updatePayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.update.post":
			raw := rpcString(req.Params, "payload_json")
			if raw != "" {
				if err := json.Unmarshal([]byte(raw), &updatePayload); err != nil {
					t.Fatalf("decode update payload: %v", err)
				}
			}
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during news batch handling: %s", req.Method)
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
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "task-1",
			ActiveSessionID: "sess-1",
		},
	}

	outcome, err := runtime.processNewsBatch(context.Background(), PollNewsResult{
		Items: []NewsRecord{
			{
				NewsID:     "news-1",
				Title:      "Platform Update",
				Content:    "The native listener now reacts to system news.",
				AuthorID:   "system",
				AuthorType: "agent",
				CreatedAt:  "2026-03-23T10:00:00Z",
			},
		},
		NextCursorCreatedAt: "2026-03-23T10:00:00Z",
		NextCursorNewsID:    "news-1",
	})
	if err != nil {
		t.Fatalf("processNewsBatch() error: %v", err)
	}
	if outcome.handled != 1 || outcome.hadError {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	trigger := runtime.currentPendingWorkTrigger()
	if trigger.Trigger != "system_news" || trigger.TaskID != "task-1" || trigger.SessionID != "sess-1" {
		t.Fatalf("unexpected pending trigger: %+v", trigger)
	}
	createdAt, newsID := runtime.newsCursor()
	if createdAt != "2026-03-23T10:00:00Z" || newsID != "news-1" {
		t.Fatalf("unexpected news cursor created_at=%q news_id=%q", createdAt, newsID)
	}
	if updatePayload["status"] != "SYSTEM_NEWS" || updatePayload["news_id"] != "news-1" {
		t.Fatalf("unexpected update payload: %+v", updatePayload)
	}
}

func TestRuntimeProcessNewsBatchDoesNotWakeBlockedSessionFromGenericNews(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.update.post", "agent.state.set":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during blocked news handling: %s", req.Method)
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
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "task-1",
			ActiveSessionID: "sess-1",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "sess-1",
			TaskID:    "task-1",
			AgentID:   "agent-1",
			Status:    "BLOCKED",
			Summary:   "waiting on alpha artifact",
		},
	}

	_, err := runtime.processNewsBatch(context.Background(), PollNewsResult{
		Items: []NewsRecord{{
			NewsID:     "news-1",
			Title:      "Unrelated update",
			Content:    "A generic workspace note changed.",
			AuthorID:   "system",
			AuthorType: "agent",
			CreatedAt:  "2026-03-23T10:00:00Z",
		}},
		NextCursorCreatedAt: "2026-03-23T10:00:00Z",
		NextCursorNewsID:    "news-1",
	})
	if err != nil {
		t.Fatalf("processNewsBatch() error: %v", err)
	}
	if trigger := runtime.currentPendingWorkTrigger(); trigger.Trigger != "" || trigger.TaskID != "" || trigger.SessionID != "" {
		t.Fatalf("generic news must not wake blocked session, got %+v", trigger)
	}
}

func TestHandleInboundMessageSkipsGenericNewsBroadcast(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			t.Fatal("news broadcast should not post a generic inbound message update")
		default:
			t.Fatalf("unexpected method during inbound news handling: %s", req.Method)
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

	if err := runtime.handleInboundMessage(context.Background(), MessageRecord{
		MessageID: "msg-news-1",
		Channel:   "news",
		Content:   "System news broadcast",
	}); err != nil {
		t.Fatalf("handleInboundMessage() error: %v", err)
	}
}

func TestEnsureRunnableTaskConsultsWorkNextWhenTriggerPending(t *testing.T) {
	var workNextCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			workNextCalls++
			if got := rpcString(req.Params, "trigger"); got != "system_news" {
				t.Fatalf("expected system_news trigger, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-03-23T10:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-1",
				"has_work":     false,
				"reason":       "idle",
			})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during ensureRunnableTask: %s", req.Method)
		}
	}))
	defer server.Close()

	activeTask := WorkspaceTaskRecord{
		TaskID:       "task-1",
		OwnerUserID:  "owner",
		Priority:     "HIGH",
		Status:       "PENDING",
		TaskKind:     "general",
		TaskTemplate: "default",
		LinkedBy:     "system",
		LinkedAt:     "2026-03-23T09:00:00Z",
		ClaimAgentID: stringPtr("agent-1"),
		ClaimStatus:  stringPtr("CLAIMED"),
	}
	activeSession := AgentSessionStateRecord{
		SessionID:   "sess-1",
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		TaskID:      "task-1",
		Status:      "ACTIVE",
		Summary:     "working",
		UpdatedAt:   "2026-03-23T09:00:00Z",
		StartedAt:   "2026-03-23T09:00:00Z",
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			ActiveTaskID:          "task-1",
			ActiveSessionID:       "sess-1",
			PendingTrigger:        "system_news",
			PendingTriggerTask:    "task-1",
			PendingTriggerSession: "sess-1",
		},
		activeTask:    &activeTask,
		activeSession: &activeSession,
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error: %v", err)
	}
	if task != nil {
		t.Fatalf("expected work.next to control selection, got %+v", task)
	}
	if workNextCalls != 1 {
		t.Fatalf("expected exactly one work.next call, got %d", workNextCalls)
	}
	if trigger := runtime.currentPendingWorkTrigger(); trigger.Trigger != "system_news" || trigger.TaskID != "task-1" || trigger.SessionID != "sess-1" {
		t.Fatalf("expected pending trigger to survive a no-work scheduler response, got %+v", trigger)
	}
}
