package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestTransientBackoffProgression(t *testing.T) {
	var backoff transientBackoff

	if got := backoff.Next(2*time.Second, 30*time.Second); got != 2*time.Second {
		t.Fatalf("first backoff = %s, want 2s", got)
	}
	if got := backoff.Next(2*time.Second, 30*time.Second); got != 4*time.Second {
		t.Fatalf("second backoff = %s, want 4s", got)
	}
	if got := backoff.Next(2*time.Second, 30*time.Second); got != 8*time.Second {
		t.Fatalf("third backoff = %s, want 8s", got)
	}
	if got := backoff.Next(2*time.Second, 30*time.Second); got != 16*time.Second {
		t.Fatalf("fourth backoff = %s, want 16s", got)
	}
	if got := backoff.Next(2*time.Second, 30*time.Second); got != 30*time.Second {
		t.Fatalf("fifth backoff = %s, want 30s", got)
	}
	backoff.Reset()
	if got := backoff.Next(2*time.Second, 30*time.Second); got != 2*time.Second {
		t.Fatalf("backoff after reset = %s, want 2s", got)
	}
}

func TestBootstrapNeedsRefreshLockedUsesScratchReconciliation(t *testing.T) {
	freshNow := time.Now()
	ownedTask := WorkspaceTaskRecord{
		TaskID:       "task-owned",
		OwnerUserID:  "owner-1",
		Priority:     "HIGH",
		Status:       "RUNNING",
		TaskKind:     "general",
		TaskTemplate: "default",
		LinkedBy:     "system",
		LinkedAt:     freshNow.Format(time.RFC3339Nano),
		ClaimAgentID: stringPtr("agent-1"),
		ClaimStatus:  stringPtr("CLAIMED"),
	}
	r := &Runtime{
		cfg: RuntimeConfig{
			AgentID:        "agent-1",
			BootstrapEvery: 5 * time.Minute,
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Tasks: []WorkspaceTaskRecord{ownedTask},
				Sessions: []AgentSessionStateRecord{
					{
						SessionID: "session-owned",
						AgentID:   "agent-1",
						TaskID:    "task-owned",
						Status:    "ACTIVE",
					},
				},
			},
		},
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-missing",
			ActiveSessionID: "session-owned",
			ActiveRunID:     "run-1",
		},
		lastBootstrap: freshNow,
	}

	r.mu.Lock()
	if !r.bootstrapNeedsRefreshLocked() {
		r.mu.Unlock()
		t.Fatal("expected refresh when scratch references a missing task")
	}
	r.scratch.ActiveTaskID = "task-owned"
	if r.bootstrapNeedsRefreshLocked() {
		r.mu.Unlock()
		t.Fatal("expected fresh snapshot with owned runnable task to stay valid")
	}
	r.mu.Unlock()
}

func TestReconcileScratchAfterBootstrapClearsStaleState(t *testing.T) {
	r := &Runtime{
		cfg: RuntimeConfig{
			AgentID: "agent-1",
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Tasks:    []WorkspaceTaskRecord{},
				Sessions: []AgentSessionStateRecord{},
			},
		},
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-stale",
			ActiveSessionID: "session-stale",
			ActiveRunID:     "run-stale",
		},
	}

	now := time.Date(2026, 3, 23, 12, 34, 56, 789000000, time.UTC)

	r.mu.Lock()
	changed := r.reconcileScratchAfterBootstrapLocked(now)
	state := r.scratch
	r.mu.Unlock()

	if !changed {
		t.Fatal("expected reconcileScratchAfterBootstrapLocked to report changes")
	}
	if state.ActiveTaskID != "" || state.ActiveSessionID != "" || state.ActiveRunID != "" {
		t.Fatalf("expected stale runtime state to be cleared, got %+v", state)
	}
	if state.DocSHAs == nil {
		t.Fatal("expected DocSHAs map to be initialized")
	}
	if state.LastBootstrapAt != now.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("unexpected LastBootstrapAt %q", state.LastBootstrapAt)
	}
}

func TestReconcileScratchAfterBootstrapClearsReleasedResumeState(t *testing.T) {
	const taskID = "task-patchq-submit-stale"
	r := &Runtime{
		cfg: RuntimeConfig{AgentID: "agent-beta"},
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{
			Tasks: []WorkspaceTaskRecord{
				{
					TaskID:       taskID,
					Status:       "PENDING",
					Title:        "Submit READY_FOR_REVIEW branch to patch queue",
					ClaimAgentID: stringPtr("agent-beta"),
					ClaimStatus:  stringPtr("RELEASED"),
				},
			},
			Sessions: []AgentSessionStateRecord{},
		}},
		scratch: RuntimeScratchState{
			ActiveTaskID:          taskID,
			ActiveSessionID:       "session-stale",
			ActiveRunID:           "run-stale",
			PendingTrigger:        "request_resume",
			PendingTriggerTask:    taskID,
			PendingTriggerSession: "session-stale",
			PendingTriggerAt:      "2026-06-08T15:25:10Z",
			LastSummary:           "Submit READY_FOR_REVIEW branch to patch queue",
		},
	}
	now := time.Date(2026, 6, 9, 12, 57, 36, 0, time.UTC)

	r.mu.Lock()
	changed := r.reconcileScratchAfterBootstrapLocked(now)
	state := r.scratch
	r.recoverActiveStateLocked()
	activeTask := r.activeTask
	r.mu.Unlock()

	if !changed {
		t.Fatal("expected released active resume state to be reconciled")
	}
	if state.ActiveTaskID != "" || state.ActiveSessionID != "" || state.ActiveRunID != "" {
		t.Fatalf("expected released active state to be cleared, got %+v", state)
	}
	if state.PendingTrigger != "" || state.PendingTriggerTask != "" || state.PendingTriggerSession != "" || state.PendingTriggerAt != "" {
		t.Fatalf("expected stale request_resume trigger to be cleared, got %+v", state)
	}
	if activeTask != nil {
		t.Fatalf("released task must not recover as active task, got %+v", activeTask)
	}
}

func TestReconcileScratchAfterBootstrapClearsSystemSelfPause(t *testing.T) {
	r := &Runtime{
		cfg: RuntimeConfig{AgentID: "agent-beta"},
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{
			Tasks:    []WorkspaceTaskRecord{},
			Sessions: []AgentSessionStateRecord{},
		}},
		scratch: RuntimeScratchState{
			ControlPaused:       true,
			ControlMode:         "paused",
			ControlAction:       "pause",
			ControlActionReason: "Planner self-paused after 3 repeated identical failures: rpc workspace.ops.resolve: operator queue item is not open",
			DocSHAs:             map[string]string{},
		},
	}
	now := time.Date(2026, 5, 12, 9, 30, 0, 0, time.UTC)

	r.mu.Lock()
	changed := r.reconcileScratchAfterBootstrapLocked(now)
	state := r.scratch
	r.mu.Unlock()

	if !changed {
		t.Fatal("expected planner self-pause reconciliation to persist resume")
	}
	if state.ControlPaused || state.ControlMode != "live" || state.ControlAction != "resume" {
		t.Fatalf("expected system self-pause to be cleared, got %+v", state)
	}
	if !strings.Contains(state.ControlActionReason, "auto-resume") {
		t.Fatalf("expected auto-resume reason, got %q", state.ControlActionReason)
	}
	if !strings.Contains(strings.Join(state.AdvisorySignals, "\n"), "prior planner self-pause was cleared") {
		t.Fatalf("expected recovery advisory signal, got %+v", state.AdvisorySignals)
	}
}

func TestReconcileScratchAfterBootstrapPreservesManualPause(t *testing.T) {
	r := &Runtime{
		cfg: RuntimeConfig{AgentID: "agent-beta"},
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{
			Tasks:    []WorkspaceTaskRecord{},
			Sessions: []AgentSessionStateRecord{},
		}},
		scratch: RuntimeScratchState{
			ControlPaused:       true,
			ControlMode:         "paused",
			ControlAction:       "pause",
			ControlActionReason: "operator pause for inspection",
			DocSHAs:             map[string]string{},
		},
	}
	now := time.Date(2026, 5, 12, 9, 45, 0, 0, time.UTC)

	r.mu.Lock()
	_ = r.reconcileScratchAfterBootstrapLocked(now)
	state := r.scratch
	r.mu.Unlock()

	if !state.ControlPaused || state.ControlMode != "paused" || state.ControlAction != "pause" {
		t.Fatalf("manual pause should survive bootstrap reconciliation, got %+v", state)
	}
	if state.ControlActionReason != "operator pause for inspection" {
		t.Fatalf("manual pause reason changed: %q", state.ControlActionReason)
	}
}

func TestReconcileScratchAfterBootstrapQueuesReviewReadyResume(t *testing.T) {
	r := &Runtime{
		cfg: RuntimeConfig{
			AgentID: "agent-beta",
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Tasks: []WorkspaceTaskRecord{
					{
						TaskID:       "task-review",
						Status:       "RUNNING",
						ClaimAgentID: stringPtr("agent-beta"),
						ClaimStatus:  stringPtr("BLOCKED"),
					},
				},
			},
		},
		scratch: RuntimeScratchState{
			CompletionCoordinationTaskID:    "task-review",
			CompletionCoordinationSessionID: "session-beta",
			CompletionCoordinationState:     completionCoordinationStateReviewReady,
		},
	}

	now := time.Date(2026, 5, 5, 11, 0, 0, 123000000, time.UTC)

	r.mu.Lock()
	changed := r.reconcileScratchAfterBootstrapLocked(now)
	state := r.scratch
	r.mu.Unlock()

	if !changed {
		t.Fatal("expected review-ready reconciliation to persist a resume trigger")
	}
	if state.PendingTrigger != "request_resume" || state.PendingTriggerTask != "task-review" || state.PendingTriggerSession != "session-beta" {
		t.Fatalf("unexpected pending trigger after review-ready reconciliation: %+v", state)
	}
	if state.PendingTriggerAt != now.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("unexpected pending trigger timestamp: %q", state.PendingTriggerAt)
	}
}

func TestRecoverFromLoopFailureRefreshesBootstrapAndScratch(t *testing.T) {
	var methods []string
	var persisted RuntimeScratchState
	bootstrapCalls := 0
	registerCalls := 0
	hostURL := ""
	currentContextDocWrites := 0
	claimedWorkDocWrites := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.bootstrap":
			bootstrapCalls++
			if bootstrapCalls == 1 {
				if got := r.Header.Get("Authorization"); got != "Bearer stale-token" {
					t.Fatalf("expected stale token on first bootstrap, got %q", got)
				}
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte("unauthorized"))
				return
			}
			if got := r.Header.Get("Authorization"); got != "Bearer fresh-token" {
				t.Fatalf("expected refreshed token on retry bootstrap, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-03-23T00:00:00Z",
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
					"created_at":       "2026-03-23T00:00:00Z",
					"updated_at":       "2026-03-23T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{
						"workspace_id": "ws-1",
						"title":        "Workspace One",
						"status":       "ACTIVE",
					},
					"tasks":    []any{},
					"sessions": []any{},
				},
			})
		case "workspace.auth.agent.register":
			registerCalls++
			if got := rpcString(req.Params, "workspace_password"); got != testWorkspacePassword {
				t.Fatalf("expected default workspace password, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"agent_id":       "agent-1",
				"display_name":   "Agent One",
				"token":          "fresh-token",
				"workspace_id":   "ws-1",
				"workspace_name": "Workspace One",
				"host_url":       hostURL,
				"agent": map[string]any{
					"agent_id":         "agent-1",
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "REGISTERED",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "registered",
					"created_at":       "2026-03-23T00:00:00Z",
					"updated_at":       "2026-03-23T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.set":
			rawValue := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(rawValue), &persisted); err != nil {
				t.Fatalf("decode persisted scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			switch rpcString(req.Params, "doc_key") {
			case agentContextDocKey("agent-1"):
				currentContextDocWrites++
				content := rpcString(req.Params, "content")
				if !strings.Contains(content, "- outcome: idle") || !strings.Contains(content, "- task_id: (none)") {
					t.Fatalf("expected bootstrap recovery to clear current context doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-recovery-context-cleared"})
			case claimedWorkDocKey("agent-1"):
				claimedWorkDocWrites++
				if !strings.Contains(rpcString(req.Params, "content"), "active_claimed_work: none") {
					t.Fatalf("expected bootstrap recovery to clear claimed work doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-recovery-claimed-cleared"})
			default:
				t.Fatalf("bootstrap recovery should not materialize doc %q: %+v", rpcString(req.Params, "doc_key"), req.Params)
			}
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()
	hostURL = server.URL

	r := &Runtime{
		cfg: RuntimeConfig{
			RhizomeRPC:        server.URL,
			RhizomeHost:       server.URL,
			RhizomeToken:      "stale-token",
			WorkspaceID:       "ws-1",
			WorkspaceName:     "Workspace One",
			WorkspacePassword: testWorkspacePassword,
			AgentID:           "agent-1",
			DisplayName:       "Agent One",
			OwnerUserID:       "owner-1",
			UpdatesLimit:      5,
		},
		client: NewRhizomeClient(server.URL, "stale-token"),
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-stale",
			ActiveSessionID: "session-stale",
			ActiveRunID:     "run-stale",
			LastSummary:     "stale",
			DocSHAs:         map[string]string{"doc-1": "sha-old"},
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Tasks: []WorkspaceTaskRecord{
					{
						TaskID:       "task-stale",
						OwnerUserID:  "owner-1",
						Priority:     "HIGH",
						Status:       "RUNNING",
						TaskKind:     "general",
						TaskTemplate: "default",
						LinkedBy:     "system",
						LinkedAt:     time.Now().Format(time.RFC3339Nano),
						ClaimAgentID: stringPtr("agent-1"),
						ClaimStatus:  stringPtr("CLAIMED"),
					},
				},
				Sessions: []AgentSessionStateRecord{
					{
						SessionID:   "session-stale",
						WorkspaceID: "ws-1",
						AgentID:     "agent-1",
						TaskID:      "task-stale",
						Status:      "ACTIVE",
						Summary:     "stale",
						UpdatedAt:   "2026-03-23T00:00:00Z",
						StartedAt:   "2026-03-23T00:00:00Z",
					},
				},
			},
		},
	}

	recovered := r.recoverFromLoopFailure(context.Background(), "planner", fmt.Errorf("rpc agent.bootstrap http 401: unauthorized"))
	if !recovered {
		t.Fatal("expected auth failure to trigger bootstrap recovery")
	}

	if bootstrapCalls != 2 {
		t.Fatalf("expected 2 bootstrap calls, got %d", bootstrapCalls)
	}
	if registerCalls != 1 {
		t.Fatalf("expected 1 registration call, got %d", registerCalls)
	}
	if r.cfg.RhizomeToken != "fresh-token" {
		t.Fatalf("expected runtime token to refresh, got %q", r.cfg.RhizomeToken)
	}
	if r.client.token != "fresh-token" {
		t.Fatalf("expected client token to refresh, got %q", r.client.token)
	}
	if r.lastBootstrap.IsZero() {
		t.Fatal("expected lastBootstrap to be refreshed")
	}
	if r.scratch.ActiveTaskID != "" || r.scratch.ActiveSessionID != "" || r.scratch.ActiveRunID != "" {
		t.Fatalf("expected stale runtime state to be cleared after recovery, got %+v", r.scratch)
	}
	if r.scratch.LastBootstrapAt == "" {
		t.Fatal("expected LastBootstrapAt to be recorded")
	}
	if persisted.LastBootstrapAt == "" {
		t.Fatal("expected scratch state to be persisted after recovery")
	}
	if persisted.ActiveTaskID != "" || persisted.ActiveSessionID != "" || persisted.ActiveRunID != "" {
		t.Fatalf("expected persisted scratch to be reconciled, got %+v", persisted)
	}
	if persisted.DocSHAs == nil || persisted.DocSHAs["doc-1"] != "sha-old" {
		t.Fatalf("expected persisted scratch doc sha map to survive recovery, got %+v", persisted.DocSHAs)
	}
	if currentContextDocWrites != 1 || claimedWorkDocWrites != 1 {
		t.Fatalf("expected bootstrap recovery to publish one current-context and one claimed-work cleanup doc, got current=%d claimed=%d", currentContextDocWrites, claimedWorkDocWrites)
	}
	expected := []string{
		"agent.bootstrap",
		"workspace.auth.agent.register",
		"agent.bootstrap",
		"agent.limits.get",
		"agent.state.set",
		"workspace.doc.put",
		"agent.state.set",
		"workspace.doc.put",
		"agent.state.set",
	}
	if !reflect.DeepEqual(methods, expected) {
		t.Fatalf("unexpected recovery call sequence: %+v", methods)
	}
}
