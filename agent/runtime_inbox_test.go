package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMessageInboxPersistsAndReloadsState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	inbox, err := OpenMessageInbox("ws-1", "agent-1")
	if err != nil {
		t.Fatalf("OpenMessageInbox() error = %v", err)
	}

	startedAt := time.Date(2026, 3, 23, 10, 0, 0, 0, time.UTC)
	if err := inbox.MarkRuntimeStarted(startedAt); err != nil {
		t.Fatalf("MarkRuntimeStarted() error = %v", err)
	}

	messages := []MessageRecord{
		{
			MessageID:   "msg-a",
			WorkspaceID: "ws-1",
			FromAgentID: "agent-a",
			ToAgentID:   "agent-1",
			Channel:     "default",
			Content:     "first",
			CreatedAt:   "2026-03-23T09:59:58Z",
		},
		{
			MessageID:   "msg-b",
			WorkspaceID: "ws-1",
			FromAgentID: "agent-b",
			ToAgentID:   "agent-1",
			Channel:     "default",
			Content:     "second",
			CreatedAt:   "2026-03-23T09:59:59Z",
		},
	}
	if err := inbox.RecordBatch(messages, startedAt.Add(2*time.Second), "2026-03-23T10:00:02Z|msg-b"); err != nil {
		t.Fatalf("RecordBatch() error = %v", err)
	}
	if err := inbox.MarkDeliveryAttempt("msg-a", startedAt.Add(3*time.Second)); err != nil {
		t.Fatalf("MarkDeliveryAttempt() error = %v", err)
	}
	if err := inbox.MarkHandled("msg-a", startedAt.Add(4*time.Second)); err != nil {
		t.Fatalf("MarkHandled() error = %v", err)
	}
	if err := inbox.MarkAckAttempt([]string{"msg-a"}, startedAt.Add(5*time.Second)); err != nil {
		t.Fatalf("MarkAckAttempt() error = %v", err)
	}
	if err := inbox.MarkAcked([]string{"msg-a"}, startedAt.Add(6*time.Second)); err != nil {
		t.Fatalf("MarkAcked() error = %v", err)
	}

	reloaded, err := OpenMessageInbox("ws-1", "agent-1")
	if err != nil {
		t.Fatalf("OpenMessageInbox() reload error = %v", err)
	}

	stats := reloaded.Stats()
	if stats.Total != 2 || stats.Pending != 1 || stats.Unread != 1 || stats.Unacked != 0 {
		t.Fatalf("unexpected inbox stats: %+v", stats)
	}
	if stats.MissedSinceStart != 2 {
		t.Fatalf("expected both messages to be counted as missed since start, got %+v", stats)
	}
	if stats.CarryoverPending != 0 {
		t.Fatalf("expected no carryover pending messages, got %+v", stats)
	}
	if got := reloaded.LastSyncedCursor(); got != "2026-03-23T10:00:02Z|msg-b" {
		t.Fatalf("unexpected synced cursor: %q", got)
	}
	handled, acked, ok := reloaded.MessageStatus("msg-a")
	if !ok || !handled || !acked {
		t.Fatalf("expected msg-a to be handled and acked, got handled=%v acked=%v ok=%v", handled, acked, ok)
	}
	handled, acked, ok = reloaded.MessageStatus("msg-b")
	if !ok || handled || acked {
		t.Fatalf("expected msg-b to remain pending, got handled=%v acked=%v ok=%v", handled, acked, ok)
	}
	if summary := reloaded.Summary(); !strings.Contains(summary, "pending=1") || !strings.Contains(summary, "missed=2") {
		t.Fatalf("unexpected summary: %q", summary)
	}
}

func TestMessageInboxCompactsOldEntriesPredictably(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	inbox, err := OpenMessageInbox("ws-retention", "agent-retention")
	if err != nil {
		t.Fatalf("OpenMessageInbox() error = %v", err)
	}
	base := time.Date(2026, 3, 23, 9, 0, 0, 0, time.UTC)
	inbox.state.Messages = []messageInboxEntry{
		{
			Message:     MessageRecord{MessageID: "pending-1", WorkspaceID: "ws-retention", ToAgentID: "agent-retention", CreatedAt: base.Add(1 * time.Minute).Format(time.RFC3339Nano)},
			FirstSeenAt: base.Add(1 * time.Minute).Format(time.RFC3339Nano),
			LastSeenAt:  base.Add(2 * time.Minute).Format(time.RFC3339Nano),
		},
		{
			Message:     MessageRecord{MessageID: "pending-2", WorkspaceID: "ws-retention", ToAgentID: "agent-retention", CreatedAt: base.Add(2 * time.Minute).Format(time.RFC3339Nano)},
			FirstSeenAt: base.Add(2 * time.Minute).Format(time.RFC3339Nano),
			LastSeenAt:  base.Add(3 * time.Minute).Format(time.RFC3339Nano),
		},
		{
			Message:     MessageRecord{MessageID: "pending-3", WorkspaceID: "ws-retention", ToAgentID: "agent-retention", CreatedAt: base.Add(3 * time.Minute).Format(time.RFC3339Nano)},
			FirstSeenAt: base.Add(3 * time.Minute).Format(time.RFC3339Nano),
			LastSeenAt:  base.Add(4 * time.Minute).Format(time.RFC3339Nano),
			HandledAt:   base.Add(4 * time.Minute).Format(time.RFC3339Nano),
		},
		{
			Message:     MessageRecord{MessageID: "archived-1", WorkspaceID: "ws-retention", ToAgentID: "agent-retention", CreatedAt: base.Add(4 * time.Minute).Format(time.RFC3339Nano)},
			FirstSeenAt: base.Add(4 * time.Minute).Format(time.RFC3339Nano),
			LastSeenAt:  base.Add(5 * time.Minute).Format(time.RFC3339Nano),
			HandledAt:   base.Add(5 * time.Minute).Format(time.RFC3339Nano),
			AckedAt:     base.Add(6 * time.Minute).Format(time.RFC3339Nano),
		},
		{
			Message:     MessageRecord{MessageID: "archived-2", WorkspaceID: "ws-retention", ToAgentID: "agent-retention", CreatedAt: base.Add(5 * time.Minute).Format(time.RFC3339Nano)},
			FirstSeenAt: base.Add(5 * time.Minute).Format(time.RFC3339Nano),
			LastSeenAt:  base.Add(6 * time.Minute).Format(time.RFC3339Nano),
			HandledAt:   base.Add(6 * time.Minute).Format(time.RFC3339Nano),
			AckedAt:     base.Add(7 * time.Minute).Format(time.RFC3339Nano),
		},
	}

	inbox.compactWithLimitsLocked(2, 1)

	ids := make([]string, 0, len(inbox.state.Messages))
	for _, entry := range inbox.state.Messages {
		ids = append(ids, entry.Message.MessageID)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 retained messages, got %+v", ids)
	}
	if ids[0] != "pending-2" || ids[1] != "pending-3" || ids[2] != "archived-2" {
		t.Fatalf("expected newest live and archived entries to be retained deterministically, got %+v", ids)
	}
	if _, ok := inbox.index["pending-1"]; ok {
		t.Fatalf("expected oldest live message to be pruned, got index %+v", inbox.index)
	}
	if _, ok := inbox.index["archived-1"]; ok {
		t.Fatalf("expected oldest archived message to be pruned, got index %+v", inbox.index)
	}
}

func TestRuntimeReconcilesLocalInboxBeforeRemotePolling(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	inbox, err := OpenMessageInbox("ws-1", "agent-1")
	if err != nil {
		t.Fatalf("OpenMessageInbox() error = %v", err)
	}
	startedAt := time.Date(2026, 3, 23, 11, 0, 0, 0, time.UTC)
	if err := inbox.MarkRuntimeStarted(startedAt); err != nil {
		t.Fatalf("MarkRuntimeStarted() error = %v", err)
	}
	if err := inbox.RecordBatch([]MessageRecord{
		{
			MessageID:   "msg-new",
			WorkspaceID: "ws-1",
			FromAgentID: "agent-a",
			ToAgentID:   "agent-1",
			Channel:     "default",
			Content:     "new message",
			CreatedAt:   "2026-03-23T10:59:59Z",
		},
		{
			MessageID:   "msg-old",
			WorkspaceID: "ws-1",
			FromAgentID: "agent-b",
			ToAgentID:   "agent-1",
			Channel:     "default",
			Content:     "old unacked",
			CreatedAt:   "2026-03-23T10:59:58Z",
		},
	}, startedAt.Add(1*time.Second), "2026-03-23T10:59:59Z|msg-new"); err != nil {
		t.Fatalf("RecordBatch() error = %v", err)
	}
	if err := inbox.MarkHandled("msg-old", startedAt.Add(2*time.Second)); err != nil {
		t.Fatalf("MarkHandled() error = %v", err)
	}

	var updateCalls int
	var ackBatches [][]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			updateCalls++
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, nil)
		case "agent.message.ack":
			rawIDs, ok := req.Params["message_ids"].([]any)
			if !ok {
				t.Fatalf("expected message_ids array, got %#v", req.Params["message_ids"])
			}
			ids := make([]string, 0, len(rawIDs))
			for _, rawID := range rawIDs {
				id, _ := rawID.(string)
				ids = append(ids, id)
			}
			ackBatches = append(ackBatches, ids)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during inbox reconcile: %s", req.Method)
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
		inbox:   inbox,
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	if err := runtime.reconcileLocalInbox(context.Background()); err != nil {
		t.Fatalf("reconcileLocalInbox() error = %v", err)
	}

	if updateCalls != 1 {
		t.Fatalf("expected exactly one state update for the newly replayed message, got %d", updateCalls)
	}
	if len(ackBatches) != 2 {
		t.Fatalf("expected two ack batches, got %#v", ackBatches)
	}
	if len(ackBatches[0]) != 1 || ackBatches[0][0] != "msg-new" {
		t.Fatalf("unexpected first ack batch: %#v", ackBatches[0])
	}
	if len(ackBatches[1]) != 1 || ackBatches[1][0] != "msg-old" {
		t.Fatalf("unexpected second ack batch: %#v", ackBatches[1])
	}

	stats := inbox.Stats()
	if stats.Pending != 0 || stats.Unread != 0 || stats.Unacked != 0 {
		t.Fatalf("expected inbox to be fully drained, got %+v", stats)
	}
	if got := runtime.messageCursor(); got != "2026-03-23T10:59:59Z|msg-new" {
		t.Fatalf("unexpected runtime cursor after reconcile: %q", got)
	}
}

func TestRuntimeInitializeAdoptsInboxCursor(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	inbox, err := OpenMessageInbox("ws-1", "agent-1")
	if err != nil {
		t.Fatalf("OpenMessageInbox() error = %v", err)
	}
	if err := inbox.SetLastSyncedCursor("cursor-from-inbox", time.Date(2026, 3, 23, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("SetLastSyncedCursor() error = %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.profile.update":
			writeRPCResult(w, req, map[string]any{"agent_id": "agent-1", "status": "UPDATED"})
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-03-23T12:00:00Z",
				"agent": map[string]any{
					"agent_id":         "agent-1",
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "online",
					"created_at":       "2026-03-23T12:00:00Z",
					"updated_at":       "2026-03-23T12:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{
						"workspace_id": "ws-1",
						"title":        "Workspace One",
						"status":       "ACTIVE",
					},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.get":
			writeRPCResult(w, req, map[string]any{"value": `{"doc_shas":{}}`})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.heartbeat":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during initialize: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:         t.TempDir(),
			RhizomeRPC:      server.URL,
			RhizomeToken:    "token",
			WorkspaceID:     "ws-1",
			AgentID:         "agent-1",
			DisplayName:     "Agent One",
			OwnerUserID:     "owner-1",
			ProtocolVersion: "rnar/v1",
			Mode:            RuntimeModeDaemon,
		},
		client: NewRhizomeClient(server.URL, "token"),
		agent:  &Agent{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.initialize(context.Background()); err != nil {
		t.Fatalf("initialize() error = %v", err)
	}
	if got := runtime.messageCursor(); got != "cursor-from-inbox" {
		t.Fatalf("expected runtime to adopt inbox cursor, got %q", got)
	}
}

func TestRuntimeClearMessageCursorClearsInboxAndScratch(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	inbox, err := OpenMessageInbox("ws-1", "agent-1")
	if err != nil {
		t.Fatalf("OpenMessageInbox() error = %v", err)
	}
	if err := inbox.SetLastSyncedCursor("2026-05-09T13:01:02.995990958Z|msg-stale", time.Now().UTC()); err != nil {
		t.Fatalf("SetLastSyncedCursor() error = %v", err)
	}

	var savedScratch string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			savedScratch = rpcString(req.Params, "value")
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during cursor clear: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			MessageCursor: "2026-05-09T13:01:02.995990958Z|msg-stale",
			DocSHAs:       map[string]string{},
		},
		inbox: inbox,
	}

	if !messagePollInvalidCursorError(errors.New("rpc agent.message.poll: after_created_at must be a valid poll cursor")) {
		t.Fatal("expected invalid cursor detector to match poll cursor rejection")
	}
	if !messagePollInvalidCursorError(&RhizomeRPCError{Method: "agent.message.poll", Code: rhizomeRPCCodeInvalidPollCursor, Message: "cursor anchor rejected"}) {
		t.Fatal("expected invalid cursor detector to match rpc code despite message drift")
	}
	if messagePollInvalidCursorError(&RhizomeRPCError{Method: "agent.message.poll", Code: -32602, Message: "cursor anchor rejected"}) {
		t.Fatal("did not expect invalid cursor detector to match unrelated rpc code without legacy text")
	}
	if err := runtime.clearMessageCursor(context.Background()); err != nil {
		t.Fatalf("clearMessageCursor() error = %v", err)
	}
	if got := runtime.messageCursor(); got != "" {
		t.Fatalf("expected runtime message cursor to be cleared, got %q", got)
	}
	reloaded, err := OpenMessageInbox("ws-1", "agent-1")
	if err != nil {
		t.Fatalf("OpenMessageInbox() reload error = %v", err)
	}
	if got := reloaded.LastSyncedCursor(); got != "" {
		t.Fatalf("expected inbox cursor to be cleared, got %q", got)
	}
	if strings.TrimSpace(savedScratch) == "" || strings.Contains(savedScratch, "msg-stale") {
		t.Fatalf("expected scratch save to clear message cursor, got %s", savedScratch)
	}
}

func TestOpenMessageInboxQuarantinesCorruptState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	path := messageInboxPath("ws-1", "agent-1")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(path, []byte("{broken"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	inbox, err := OpenMessageInbox("ws-1", "agent-1")
	if err != nil {
		t.Fatalf("OpenMessageInbox() error = %v", err)
	}
	if inbox == nil {
		t.Fatal("expected inbox to be returned after corrupt state quarantine")
	}
	if stats := inbox.Stats(); stats.Total != 0 || stats.Pending != 0 || stats.Unread != 0 {
		t.Fatalf("expected empty inbox after quarantine, got %+v", stats)
	}
	matches, err := filepath.Glob(path + ".corrupt-*")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected exactly one quarantined inbox file, got %#v", matches)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected original corrupt inbox to be removed, got err=%v", err)
	}
}
