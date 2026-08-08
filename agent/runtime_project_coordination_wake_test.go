package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPrepareStartupPlannerWakeQueuesRecoveredActiveResume(t *testing.T) {
	now := time.Date(2026, 5, 4, 10, 30, 0, 0, time.UTC)
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-alpha",
		},
		scratch: RuntimeScratchState{
			ActiveTaskID:              "task-active",
			ActiveSessionID:           "session-alpha",
			PendingTrigger:            "task_project_fields_updated",
			PendingTriggerTask:        "task-active",
			PendingTriggerSession:     "session-alpha",
			PendingTriggerAt:          now.Add(-time.Hour).Format(time.RFC3339Nano),
			ContinuationHoldTaskID:    "task-active",
			ContinuationHoldSessionID: "session-alpha",
			ContinuationHoldRunID:     "run-alpha",
			ContinuationHoldUntil:     now.Add(time.Hour).Format(time.RFC3339Nano),
			DocSHAs:                   map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:    "task-active",
			ProjectID: "project-active",
			Status:    "RUNNING",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-alpha",
			TaskID:    "task-active",
			AgentID:   "agent-alpha",
			Status:    "BLOCKED",
		},
		activeHydration:  &TaskHydrationBundle{Task: TaskStatus{TaskID: "task-active"}},
		activeWorkPacket: &AgentWorkPacket{WorkType: "resume_session"},
	}

	runtime.mu.Lock()
	wake := runtime.prepareStartupPlannerWakeLocked(now)
	state := runtime.scratch
	hydration := runtime.activeHydration
	packet := runtime.activeWorkPacket
	runtime.mu.Unlock()

	if !wake {
		t.Fatal("expected recovered active state to wake planner")
	}
	if state.PendingTrigger != "request_resume" || state.PendingTriggerTask != "task-active" || state.PendingTriggerSession != "session-alpha" {
		t.Fatalf("unexpected startup pending trigger: %+v", state)
	}
	if state.PendingTriggerAt != now.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected pending trigger timestamp: %q", state.PendingTriggerAt)
	}
	if state.ContinuationHoldTaskID != "" || state.ContinuationHoldSessionID != "" || state.ContinuationHoldRunID != "" || state.ContinuationHoldUntil != "" {
		t.Fatalf("startup resume should clear continuation hold, got %+v", state)
	}
	if hydration != nil || packet != nil {
		t.Fatalf("startup resume should clear stale hydration and packet, hydration=%+v packet=%+v", hydration, packet)
	}
}

func TestPrepareStartupPlannerWakeClearsStaleRequestResumeWithoutActivePresence(t *testing.T) {
	now := time.Date(2026, 5, 4, 11, 15, 0, 0, time.UTC)
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-gamma",
		},
		scratch: RuntimeScratchState{
			PendingTrigger:            "request_resume",
			PendingTriggerTask:        "task-stale",
			PendingTriggerSession:     "session-stale",
			PendingTriggerAt:          now.Add(-time.Hour).Format(time.RFC3339Nano),
			ContinuationHoldTaskID:    "task-stale",
			ContinuationHoldSessionID: "session-stale",
			ContinuationHoldRunID:     "run-stale",
			ContinuationHoldUntil:     now.Add(time.Hour).Format(time.RFC3339Nano),
			DocSHAs:                   map[string]string{},
		},
		activeHydration:  &TaskHydrationBundle{Task: TaskStatus{TaskID: "task-stale"}},
		activeWorkPacket: &AgentWorkPacket{WorkType: "resume_session"},
	}

	runtime.mu.Lock()
	wake := runtime.prepareStartupPlannerWakeLocked(now)
	state := runtime.scratch
	hydration := runtime.activeHydration
	packet := runtime.activeWorkPacket
	runtime.mu.Unlock()

	if !wake {
		t.Fatal("expected clean startup with stale request_resume to wake planner")
	}
	if state.PendingTrigger != "" || state.PendingTriggerTask != "" || state.PendingTriggerSession != "" || state.PendingTriggerAt != "" {
		t.Fatalf("expected stale request_resume trigger to be cleared, got %+v", state)
	}
	if state.ContinuationHoldTaskID != "" || state.ContinuationHoldSessionID != "" || state.ContinuationHoldRunID != "" || state.ContinuationHoldUntil != "" {
		t.Fatalf("expected stale continuation hold to be cleared, got %+v", state)
	}
	if hydration != nil || packet != nil {
		t.Fatalf("expected stale hydration and packet to be cleared, hydration=%+v packet=%+v", hydration, packet)
	}
}

func TestProjectCoordinationRuntimeEventQueuesActiveProjectResume(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if got := rpcString(req.Params, "key"); got != runtimeScratchStateKey {
				t.Fatalf("agent.state.set key = %q, want %q", got, runtimeScratchStateKey)
			}
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode saved scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during project coordination wake: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-alpha",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs:                   map[string]string{},
			ActiveTaskID:              "task-active",
			ActiveSessionID:           "session-alpha",
			ContinuationHoldTaskID:    "task-active",
			ContinuationHoldSessionID: "session-alpha",
			ContinuationHoldRunID:     "run-alpha",
			ContinuationHoldUntil:     time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:    "task-active",
			ProjectID: "project-active",
			Status:    "RUNNING",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-alpha",
			TaskID:    "task-active",
			AgentID:   "agent-alpha",
			Status:    "BLOCKED",
		},
		activeHydration:  &TaskHydrationBundle{Task: TaskStatus{TaskID: "task-active"}},
		activeWorkPacket: &AgentWorkPacket{WorkType: "resume_session"},
		lastBootstrap:    time.Now().UTC(),
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "workspace.ops.resolved",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"source_kind":"project","source_id":"project-active","status":"RESOLVED"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}

	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-active" || saved.PendingTriggerSession != "session-alpha" {
		t.Fatalf("unexpected saved pending trigger: %+v", saved)
	}
	if saved.ContinuationHoldTaskID != "" || saved.ContinuationHoldSessionID != "" || saved.ContinuationHoldRunID != "" || saved.ContinuationHoldUntil != "" {
		t.Fatalf("project coordination wake should clear continuation hold, got %+v", saved)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "request_resume" || got.TaskID != "task-active" || got.SessionID != "session-alpha" {
		t.Fatalf("unexpected runtime pending trigger: %+v", got)
	}

	runtime.mu.Lock()
	hydration := runtime.activeHydration
	packet := runtime.activeWorkPacket
	lastBootstrap := runtime.lastBootstrap
	runtime.mu.Unlock()
	if hydration != nil || packet != nil {
		t.Fatalf("expected stale hydration and packet to be cleared, hydration=%+v packet=%+v", hydration, packet)
	}
	if !lastBootstrap.IsZero() {
		t.Fatalf("expected bootstrap cache invalidation, got %s", lastBootstrap.Format(time.RFC3339Nano))
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected project coordination wake to notify planner")
	}
}

func TestProjectPhaseTransitionEventWakesIdlePlanner(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		t.Fatalf("unexpected RPC during phase transition idle wake: %s", req.Method)
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-strategist",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
		lastBootstrap:    time.Now().UTC(),
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "project.phase.transitioned",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"project_id":"project-active","from_phase":"PLANNING","to_phase":"IMPLEMENTATION"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}

	runtime.mu.Lock()
	lastBootstrap := runtime.lastBootstrap
	runtime.mu.Unlock()
	if !lastBootstrap.IsZero() {
		t.Fatalf("expected phase transition wake to invalidate bootstrap, got %s", lastBootstrap.Format(time.RFC3339Nano))
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected phase transition event to wake idle planner")
	}
}

func TestGovernanceVoteOpenEventAppendsAdvisoryAndWakesPlanner(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if got := rpcString(req.Params, "key"); got != runtimeScratchStateKey {
				t.Fatalf("agent.state.set key = %q, want %q", got, runtimeScratchStateKey)
			}
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode saved scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected RPC during governance vote wake: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-delta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
			AdvisorySignals: []string{
				"old-1",
				"old-2",
				"old-3",
				"old-4",
				"old-5",
			},
		},
		lastBootstrap: time.Now().UTC(),
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "governance.challenge.defended",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"workspace_id":"ws-1","project_id":"project-clearpress","challenge_id":"govchal-1","state":"VOTING","current_round":1,"argument_doc_key":"docs/gov/arg","defense_doc_key":"docs/gov/def","nominated_successor_agent_id":"agent-gamma","voting_deadline_at":"2026-05-31T12:00:00Z"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}

	if len(saved.AdvisorySignals) != 5 {
		t.Fatalf("expected capped advisory ring, got %+v", saved.AdvisorySignals)
	}
	last := saved.AdvisorySignals[len(saved.AdvisorySignals)-1]
	if !strings.Contains(last, "GOVERNANCE VOTE OPEN: challenge govchal-1") ||
		!strings.Contains(last, "project_governance_challenge action=list") ||
		!strings.Contains(last, "action=vote") ||
		!strings.Contains(last, "docs/gov/arg") ||
		!strings.Contains(last, "docs/gov/def") {
		t.Fatalf("unexpected governance advisory: %q", last)
	}
	runtime.mu.Lock()
	lastBootstrap := runtime.lastBootstrap
	runtime.mu.Unlock()
	if !lastBootstrap.IsZero() {
		t.Fatalf("expected governance vote event to invalidate bootstrap, got %s", lastBootstrap.Format(time.RFC3339Nano))
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected governance vote event to wake planner")
	}
}

func TestProjectTaskLifecycleEventQueuesActiveProjectResumeForSiblingTask(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode saved scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during project task wake: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-alpha",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-root", ProjectID: "project-active", Status: "RUNNING"},
			{TaskID: "task-impl", ProjectID: "project-active", Status: "RESOLVED"},
		}}},
		scratch: RuntimeScratchState{
			DocSHAs:                   map[string]string{},
			ActiveTaskID:              "task-root",
			ActiveSessionID:           "session-alpha",
			ContinuationHoldTaskID:    "task-root",
			ContinuationHoldSessionID: "session-alpha",
			ContinuationHoldRunID:     "run-alpha",
			ContinuationHoldUntil:     time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:    "task-root",
			ProjectID: "project-active",
			Status:    "RUNNING",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-alpha",
			TaskID:    "task-root",
			AgentID:   "agent-alpha",
			Status:    "BLOCKED",
		},
		activeHydration:  &TaskHydrationBundle{Task: TaskStatus{TaskID: "task-root"}},
		activeWorkPacket: &AgentWorkPacket{WorkType: "resume_session"},
		lastBootstrap:    time.Now().UTC(),
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "task.completed",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"workspace_id":"ws-1","task_id":"task-impl","agent_id":"agent-beta","claim_status":"COMPLETED"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}

	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-root" || saved.PendingTriggerSession != "session-alpha" {
		t.Fatalf("unexpected saved pending trigger: %+v", saved)
	}
	if saved.ContinuationHoldTaskID != "" || saved.ContinuationHoldSessionID != "" || saved.ContinuationHoldRunID != "" || saved.ContinuationHoldUntil != "" {
		t.Fatalf("sibling project task wake should clear continuation hold, got %+v", saved)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected sibling project task event to notify planner")
	}
}

func TestProjectTaskLifecycleEventQueuesParkedBlockedProjectClaimForSiblingTask(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode saved scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during parked blocked claim wake: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-gamma",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-revision", ProjectID: "project-active", Status: "RUNNING", ClaimAgentID: stringPtr("agent-gamma"), ClaimStatus: stringPtr("BLOCKED")},
			{TaskID: "task-smoke", ProjectID: "project-active", Status: "RESOLVED", ClaimAgentID: stringPtr("agent-epsilon"), ClaimStatus: stringPtr("COMPLETED")},
		}}},
		scratch: RuntimeScratchState{
			DocSHAs:                  map[string]string{},
			ContinuationHoldTaskID:   "task-revision",
			ContinuationHoldRunID:    "run-stale",
			ContinuationHoldUntil:    time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
			WorkspaceDocWakeVersions: map[string]string{"old": "version"},
		},
		activeHydration:  &TaskHydrationBundle{Task: TaskStatus{TaskID: "task-revision"}},
		activeWorkPacket: &AgentWorkPacket{WorkType: "blocked_claim"},
		lastBootstrap:    time.Now().UTC(),
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "task.completed",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"workspace_id":"ws-1","task_id":"task-smoke","agent_id":"agent-epsilon","claim_status":"COMPLETED"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}

	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-revision" || saved.PendingTriggerSession != "" {
		t.Fatalf("unexpected saved pending trigger for parked blocked claim: %+v", saved)
	}
	if saved.ContinuationHoldTaskID != "" || saved.ContinuationHoldSessionID != "" || saved.ContinuationHoldRunID != "" || saved.ContinuationHoldUntil != "" {
		t.Fatalf("parked blocked claim wake should clear continuation hold, got %+v", saved)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "request_resume" || got.TaskID != "task-revision" || got.SessionID != "" {
		t.Fatalf("unexpected runtime pending trigger: %+v", got)
	}
	runtime.mu.Lock()
	hydration := runtime.activeHydration
	packet := runtime.activeWorkPacket
	lastBootstrap := runtime.lastBootstrap
	runtime.mu.Unlock()
	if hydration != nil || packet != nil {
		t.Fatalf("expected stale hydration and packet to be cleared, hydration=%+v packet=%+v", hydration, packet)
	}
	if !lastBootstrap.IsZero() {
		t.Fatalf("expected bootstrap cache invalidation, got %s", lastBootstrap.Format(time.RFC3339Nano))
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected parked blocked claim wake to notify planner")
	}
}

func TestProjectCoordinationRuntimeEventIgnoresSelfAuthoredTaskBlocked(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		t.Fatalf("unexpected RPC during self-authored task.blocked wake: %s", req.Method)
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-gamma",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-revision", ProjectID: "project-active", Status: "RUNNING", ClaimAgentID: stringPtr("agent-gamma"), ClaimStatus: stringPtr("BLOCKED")},
		}}},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "task.blocked",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"workspace_id":"ws-1","task_id":"task-revision","agent_id":"agent-gamma","claim_status":"BLOCKED"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("self-authored task.blocked should not queue resume trigger, got %+v", got)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("self-authored task.blocked should not wake planner")
	default:
	}
}

func TestProjectPatchQueueEventQueuesActiveProjectResume(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode saved scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during project patch queue wake: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-alpha",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "task-root",
			ActiveSessionID: "session-alpha",
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:    "task-root",
			ProjectID: "project-active",
			Status:    "RUNNING",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-alpha",
			TaskID:    "task-root",
			AgentID:   "agent-alpha",
			Status:    "BLOCKED",
		},
		lastBootstrap: time.Now().UTC(),
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "project.patch_queue.submitted",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"project_id":"project-active","queue_id":"queue-1","item_id":"item-1","branch_id":"branch-alpha"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}

	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-root" || saved.PendingTriggerSession != "session-alpha" {
		t.Fatalf("unexpected saved pending trigger: %+v", saved)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected patch queue project event to notify planner")
	}
}

func TestProjectMergeCoordinationEventsQueueActiveProjectResume(t *testing.T) {
	for _, eventType := range []string{"project.branch.changed", "project.patch_queue.changed"} {
		t.Run(eventType, func(t *testing.T) {
			var saved RuntimeScratchState
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req := decodeRPCRequest(t, r)
				switch req.Method {
				case "agent.state.set":
					if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
						t.Fatalf("decode saved scratch: %v", err)
					}
					writeRPCResult(w, req, nil)
				default:
					t.Fatalf("unexpected method during merge coordination wake: %s", req.Method)
				}
			}))
			defer server.Close()

			runtime := &Runtime{
				cfg: RuntimeConfig{
					WorkspaceID: "ws-1",
					AgentID:     "agent-gamma",
				},
				client:           NewRhizomeClient(server.URL, "token"),
				eventWakePlanner: make(chan struct{}, 1),
				scratch: RuntimeScratchState{
					DocSHAs:                   map[string]string{},
					ActiveTaskID:              "task-gamma-waiting",
					ActiveSessionID:           "session-gamma",
					ContinuationHoldTaskID:    "task-gamma-waiting",
					ContinuationHoldSessionID: "session-gamma",
					ContinuationHoldUntil:     time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
				},
				activeTask: &WorkspaceTaskRecord{
					TaskID:    "task-gamma-waiting",
					ProjectID: "project-active",
					Status:    "RUNNING",
				},
				activeSession: &AgentSessionStateRecord{
					SessionID: "session-gamma",
					TaskID:    "task-gamma-waiting",
					AgentID:   "agent-gamma",
					Status:    "BLOCKED",
				},
				activeHydration:  &TaskHydrationBundle{Task: TaskStatus{TaskID: "task-gamma-waiting"}},
				activeWorkPacket: &AgentWorkPacket{WorkType: "blocked_claim"},
				lastBootstrap:    time.Now().UTC(),
			}

			if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
				Type:        eventType,
				WorkspaceID: "ws-1",
				PayloadJSON: `{"project_id":"project-active","branch_id":"branch-beta","queue_id":"queue-beta","item_id":"item-beta","status":"MERGED","target_head_after":"` + strings.Repeat("a", 40) + `"}`,
			}); err != nil {
				t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
			}

			if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-gamma-waiting" || saved.PendingTriggerSession != "session-gamma" {
				t.Fatalf("unexpected saved pending trigger: %+v", saved)
			}
			if saved.ContinuationHoldTaskID != "" || saved.ContinuationHoldSessionID != "" || saved.ContinuationHoldUntil != "" {
				t.Fatalf("merge coordination wake should clear continuation hold, got %+v", saved)
			}
			runtime.mu.Lock()
			hydration := runtime.activeHydration
			packet := runtime.activeWorkPacket
			runtime.mu.Unlock()
			if hydration != nil || packet != nil {
				t.Fatalf("expected stale hydration and packet to be cleared, hydration=%+v packet=%+v", hydration, packet)
			}
			select {
			case <-runtime.eventWakePlanner:
			default:
				t.Fatal("expected merge coordination event to notify planner")
			}
		})
	}
}

func TestProjectTaskLifecycleEventIgnoresDifferentProjectSiblingTask(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		t.Fatalf("unexpected RPC during unrelated project task wake: %s", req.Method)
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-alpha",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-root", ProjectID: "project-active", Status: "RUNNING"},
			{TaskID: "task-other", ProjectID: "project-other", Status: "RESOLVED"},
		}}},
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "task-root",
			ActiveSessionID: "session-alpha",
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:    "task-root",
			ProjectID: "project-active",
			Status:    "RUNNING",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-alpha",
			TaskID:    "task-root",
			AgentID:   "agent-alpha",
			Status:    "BLOCKED",
		},
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "task.completed",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"workspace_id":"ws-1","task_id":"task-other","agent_id":"agent-beta","claim_status":"COMPLETED"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("different-project task event should not queue resume trigger, got %+v", got)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected unrelated project task event to still wake planner for scheduler refresh")
	}
}

func TestProjectCoordinationRuntimeEventIgnoresUnrelatedProjectForActiveResume(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		t.Fatalf("unexpected RPC during unrelated project wake: %s", req.Method)
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-alpha",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "task-active",
			ActiveSessionID: "session-alpha",
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:    "task-active",
			ProjectID: "project-active",
			Status:    "RUNNING",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-alpha",
			TaskID:    "task-active",
			AgentID:   "agent-alpha",
			Status:    "ACTIVE",
		},
		lastBootstrap: time.Now().UTC(),
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "project.repository.changed",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"project_id":"project-other","repo_id":"repo-other","repo_status":"READY"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}

	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("unrelated project should not queue resume trigger, got %+v", got)
	}
	runtime.mu.Lock()
	lastBootstrap := runtime.lastBootstrap
	runtime.mu.Unlock()
	if !lastBootstrap.IsZero() {
		t.Fatalf("expected bootstrap invalidation even for project coordination event, got %s", lastBootstrap.Format(time.RFC3339Nano))
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected unrelated project event to still wake planner for scheduler refresh")
	}
}

func TestProjectRoleAssignedClearsTargetAgentClaimHoldAndWakesPlanner(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if got := rpcString(req.Params, "key"); got != runtimeScratchStateKey {
				t.Fatalf("agent.state.set key = %q, want %q", got, runtimeScratchStateKey)
			}
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode saved scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected RPC during role assigned hold clear: %s", req.Method)
		}
	}))
	defer server.Close()

	wake := make(chan struct{}, 1)
	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws-1", AgentID: "delta"},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: wake,
		scratch: RuntimeScratchState{
			ProjectClaimHoldKind:      "project_claim_overlap",
			ProjectClaimHoldTaskID:    "task-evaluator",
			ProjectClaimHoldProjectID: "project-rq",
			ProjectClaimHoldUntil:     time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
			ProjectClaimHoldSummary:   "overlap before repair",
			LastWakeReason:            "frontier_self_selected_after_hold",
			DocSHAs:                   map[string]string{},
		},
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "project.role.assigned",
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		PayloadJSON: `{"project_id":"project-rq","agent_id":"delta","role_id":"role-delta-evaluator"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}
	if runtime.scratch.ProjectClaimHoldKind != "" || runtime.scratch.ProjectClaimHoldTaskID != "" || runtime.scratch.ProjectClaimHoldUntil != "" {
		t.Fatalf("target role assignment should clear held project claim, scratch=%+v", runtime.scratch)
	}
	if saved.ProjectClaimHoldKind != "" || saved.ProjectClaimHoldTaskID != "" || saved.ProjectClaimHoldUntil != "" {
		t.Fatalf("target role assignment should persist cleared project claim hold, saved=%+v", saved)
	}
	select {
	case <-wake:
	default:
		t.Fatalf("target role assignment should wake idle planner")
	}
}

func TestProjectRoleAssignedForOtherAgentKeepsClaimHold(t *testing.T) {
	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws-1", AgentID: "delta"},
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			ProjectClaimHoldKind:      "project_claim_overlap",
			ProjectClaimHoldTaskID:    "task-evaluator",
			ProjectClaimHoldProjectID: "project-rq",
			ProjectClaimHoldUntil:     time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
			ProjectClaimHoldSummary:   "overlap before repair",
			LastWakeReason:            "project_claim_overlap",
			DocSHAs:                   map[string]string{},
		},
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "project.role.assigned",
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		PayloadJSON: `{"project_id":"project-rq","agent_id":"gamma","role_id":"role-gamma-parser"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}
	if runtime.scratch.ProjectClaimHoldTaskID != "task-evaluator" {
		t.Fatalf("other agent role assignment must not clear delta hold, scratch=%+v", runtime.scratch)
	}
}

func TestProjectRoleAssignedKeepsTaskClaimConflictHold(t *testing.T) {
	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws-1", AgentID: "delta"},
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			ProjectClaimHoldKind:      "task_claim_conflict",
			ProjectClaimHoldTaskID:    "task-parser",
			ProjectClaimHoldProjectID: "project-rq",
			ProjectClaimHoldUntil:     time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
			ProjectClaimHoldSummary:   "another worker won the task claim",
			LastWakeReason:            "project_claim_overlap",
			DocSHAs:                   map[string]string{},
		},
	}

	if err := runtime.handleProjectCoordinationRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "project.role.assigned",
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		PayloadJSON: `{"project_id":"project-rq","agent_id":"delta","role_id":"role-delta-parser"}`,
	}); err != nil {
		t.Fatalf("handleProjectCoordinationRuntimeEvent() error = %v", err)
	}
	if runtime.scratch.ProjectClaimHoldKind != "task_claim_conflict" || runtime.scratch.ProjectClaimHoldTaskID != "task-parser" {
		t.Fatalf("role assignment must not clear generic claim-conflict hold, scratch=%+v", runtime.scratch)
	}
}
