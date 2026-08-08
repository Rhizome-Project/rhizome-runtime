package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleWorkspaceDocRuntimeEventQueuesBlockedSessionTrigger(t *testing.T) {
	var saved RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during workspace doc wake: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "beta",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "task-beta",
			ActiveSessionID: "sess-beta",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID:   "sess-beta",
			WorkspaceID: "ws-1",
			AgentID:     "beta",
			TaskID:      "task-beta",
			Status:      "BLOCKED",
			Summary:     "waiting for alpha handoff",
			BlockedOn: []BlockedRef{{
				Kind:   "dependency",
				Detail: "workspace doc pilot.code-error.kvparser.alpha.20260426-084136 is required before review",
			}},
		},
	}

	err := runtime.handleWorkspaceDocRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "workspace_doc.upserted",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"doc_key":"pilot.code-error.kvparser.alpha.20260426-084136","title":"Alpha implementation"}`,
	})
	if err != nil {
		t.Fatalf("handleWorkspaceDocRuntimeEvent() error = %v", err)
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-beta" || saved.PendingTriggerSession != "sess-beta" {
		t.Fatalf("unexpected pending trigger: %+v", saved)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "request_resume" || got.TaskID != "task-beta" || got.SessionID != "sess-beta" {
		t.Fatalf("runtime scratch not updated: %+v", got)
	}
}

func TestHandleWorkspaceDocRuntimeEventIgnoresUnrelatedDoc(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		t.Fatalf("unexpected RPC for unrelated workspace doc wake: %s", req.Method)
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "gamma",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "task-gamma",
			ActiveSessionID: "sess-gamma",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID:   "sess-gamma",
			WorkspaceID: "ws-1",
			AgentID:     "gamma",
			TaskID:      "task-gamma",
			Status:      "BLOCKED",
			BlockedOn: []BlockedRef{{
				Kind:   "dependency",
				Detail: "needs workspace doc pilot.code-error.kvparser.alpha.20260426-084136",
			}},
		},
	}

	err := runtime.handleWorkspaceDocRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "workspace_doc.upserted",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"doc_key":"pilot.code-error.kvparser.unrelated.20260426-084136"}`,
	})
	if err != nil {
		t.Fatalf("handleWorkspaceDocRuntimeEvent() error = %v", err)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("unexpected pending trigger for unrelated doc: %+v", got)
	}
}

func TestHandleWorkspaceDocRuntimeEventIgnoresOwnPresenceDoc(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		t.Fatalf("unexpected RPC for own presence doc wake: %s", req.Method)
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "alpha",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "task-alpha",
			ActiveSessionID: "sess-alpha",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID:      "sess-alpha",
			WorkspaceID:    "ws-1",
			AgentID:        "alpha",
			TaskID:         "task-alpha",
			Status:         "BLOCKED",
			RelatedDocKeys: []string{agentContextDocKey("alpha"), claimedWorkDocKey("alpha")},
			BlockedOn: []BlockedRef{{
				Kind:   "tool",
				Detail: "waiting for external browser smoke evidence",
			}},
		},
	}

	for _, docKey := range []string{agentContextDocKey("alpha"), claimedWorkDocKey("alpha")} {
		err := runtime.handleWorkspaceDocRuntimeEvent(context.Background(), RhizomeEvent{
			Type:        "workspace_doc.upserted",
			WorkspaceID: "ws-1",
			PayloadJSON: `{"doc_key":"` + docKey + `","sha":"self-presence"}`,
		})
		if err != nil {
			t.Fatalf("handleWorkspaceDocRuntimeEvent(%s) error = %v", docKey, err)
		}
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("own presence docs should not queue request_resume, got %+v", got)
	}
}

func TestHandleWorkspaceDocRuntimeEventIgnoresSelfAuthoredDoc(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		t.Fatalf("unexpected RPC for self-authored workspace doc wake: %s", req.Method)
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "epsilon",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "task-review",
			ActiveSessionID: "sess-review",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID:   "sess-review",
			WorkspaceID: "ws-1",
			AgentID:     "epsilon",
			TaskID:      "task-review",
			Status:      "BLOCKED",
			RelatedDocKeys: []string{
				"task.task-review.blocker",
			},
			BlockedOn: []BlockedRef{{
				Kind:   "dependency",
				Detail: "waiting on task.task-review.blocker",
			}},
		},
	}

	err := runtime.handleWorkspaceDocRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "workspace_doc.upserted",
		WorkspaceID: "ws-1",
		AgentID:     "epsilon",
		PayloadJSON: `{"doc_key":"task.task-review.blocker","sha":"self-blocker-v1"}`,
	})
	if err != nil {
		t.Fatalf("handleWorkspaceDocRuntimeEvent() error = %v", err)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("self-authored blocker doc should not queue request_resume, got %+v", got)
	}
}

func TestHandleWorkspaceDocRuntimeEventWakesBlockedBootstrapSession(t *testing.T) {
	var saved RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during bootstrap workspace doc wake: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Tasks: []WorkspaceTaskRecord{{
					TaskID:      "task-beta",
					Title:       "Review Alpha parser",
					Description: "review pilot.code-error.kvparser.alpha.20260426-084136 after alpha lands it",
				}},
				Sessions: []AgentSessionStateRecord{{
					SessionID:   "sess-beta",
					WorkspaceID: "ws-1",
					AgentID:     "beta",
					TaskID:      "task-beta",
					Status:      "BLOCKED",
					BlockedOn: []BlockedRef{{
						Kind:   "dependency",
						Detail: "needs workspace doc pilot.code-error.kvparser.alpha.20260426-084136",
					}},
				}},
			},
		},
	}

	err := runtime.handleWorkspaceDocRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "workspace_doc.upserted",
		WorkspaceID: "ws-1",
		PayloadJSON: `{"doc_key":"pilot.code-error.kvparser.alpha.20260426-084136"}`,
	})
	if err != nil {
		t.Fatalf("handleWorkspaceDocRuntimeEvent() error = %v", err)
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-beta" || saved.PendingTriggerSession != "sess-beta" {
		t.Fatalf("unexpected pending trigger from bootstrap session: %+v", saved)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected workspace doc wake to notify planner")
	}
}

func TestSetPendingWorkTriggerWakesPlanner(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during pending trigger wake: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "system_news", "task-beta", "sess-beta"); err != nil {
		t.Fatalf("setPendingWorkTrigger() error = %v", err)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected setPendingWorkTrigger to notify planner")
	}
}

func TestEnsureRunnableTaskQueuesSatisfiedWorkspaceDocBlockerFromSnapshot(t *testing.T) {
	const docKey = "pilot.code-error.kvparser.alpha.20260426-084136"
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.work.next":
			if got := rpcString(req.Params, "trigger"); got != "request_resume" {
				t.Fatalf("expected request_resume trigger, got %q", got)
			}
			if got := rpcString(req.Params, "candidate_task_id"); got != "task-beta" {
				t.Fatalf("expected candidate task task-beta, got %q", got)
			}
			if got := rpcString(req.Params, "candidate_session_id"); got != "sess-beta" {
				t.Fatalf("expected candidate session sess-beta, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-26T09:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "beta",
				"has_work":     false,
				"reason":       "idle",
			})
		default:
			t.Fatalf("unexpected method during snapshot doc blocker reconciliation: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Docs: []WorkspaceDocRecord{{DocKey: docKey, Title: "Alpha output"}},
				Tasks: []WorkspaceTaskRecord{{
					TaskID:      "task-beta",
					Title:       "Review Alpha output",
					Description: "Review " + docKey,
				}},
				Sessions: []AgentSessionStateRecord{{
					SessionID:   "sess-beta",
					WorkspaceID: "ws-1",
					AgentID:     "beta",
					TaskID:      "task-beta",
					Status:      "BLOCKED",
					BlockedOn:   []BlockedRef{{Kind: "dependency", Detail: "waiting for workspace doc " + docKey}},
				}},
			},
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("expected no selected work from idle scheduler response, got %+v", task)
	}
	if len(methods) != 3 || methods[0] != "agent.state.set" || methods[1] != "agent.state.set" || methods[2] != "agent.work.next" {
		t.Fatalf("unexpected method order: %#v", methods)
	}
}

func TestEnsureRunnableTaskRefreshesBootstrapForMissedWorkspaceDocWake(t *testing.T) {
	const docKey = "pilot.code-error.kvparser.alpha.20260426-084136"
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.bootstrap":
			writeRPCResult(w, req, BootstrapResult{
				GeneratedAt: "2026-04-26T09:01:00Z",
				Agent: AgentRecord{
					AgentID:         "beta",
					WorkspaceID:     "ws-1",
					OwnerUserID:     "owner-1",
					DisplayName:     "Beta",
					Role:            "reviewer",
					Status:          "ACTIVE",
					ProtocolVersion: "rnar/v1",
					Capabilities:    []string{"tool.call"},
					CreatedAt:       "2026-04-26T09:00:00Z",
					UpdatedAt:       "2026-04-26T09:01:00Z",
				},
				Snapshot: WorkspaceSnapshot{
					Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Workspace", Status: "ACTIVE"},
					Docs:      []WorkspaceDocRecord{{DocKey: docKey, Title: "Alpha output"}},
					Tasks: []WorkspaceTaskRecord{{
						TaskID:      "task-beta",
						Title:       "Review Alpha output",
						Description: "Review " + docKey,
					}},
					Sessions: []AgentSessionStateRecord{{
						SessionID:   "sess-beta",
						WorkspaceID: "ws-1",
						AgentID:     "beta",
						TaskID:      "task-beta",
						Status:      "BLOCKED",
						BlockedOn:   []BlockedRef{{Kind: "dependency", Detail: "waiting for workspace doc " + docKey}},
					}},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.work.next":
			if got := rpcString(req.Params, "trigger"); got != "request_resume" {
				t.Fatalf("expected request_resume trigger after missed event reconciliation, got %q", got)
			}
			if got := rpcString(req.Params, "candidate_session_id"); got != "sess-beta" {
				t.Fatalf("expected candidate session sess-beta, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-26T09:01:01Z",
				"workspace_id": "ws-1",
				"agent_id":     "beta",
				"has_work":     false,
				"reason":       "idle",
			})
		default:
			t.Fatalf("unexpected method during missed doc wake reconciliation: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "beta",
			Workdir:     t.TempDir(),
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Tasks: []WorkspaceTaskRecord{{TaskID: "task-beta", Description: "Review " + docKey}},
				Sessions: []AgentSessionStateRecord{{
					SessionID: "sess-beta",
					AgentID:   "beta",
					TaskID:    "task-beta",
					Status:    "BLOCKED",
					BlockedOn: []BlockedRef{{Kind: "dependency", Detail: "waiting for workspace doc " + docKey}},
				}},
			},
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("expected no selected work from idle scheduler response, got %+v", task)
	}
	if !containsAll(methods, []string{"agent.bootstrap", "agent.limits.get", "agent.state.set", "agent.work.next"}) {
		t.Fatalf("expected bootstrap reconciliation before work.next, got %#v", methods)
	}
	if runtime.bootstrap.Snapshot.Docs[0].DocKey != docKey {
		t.Fatalf("expected refreshed bootstrap to contain doc %q, got %+v", docKey, runtime.bootstrap.Snapshot.Docs)
	}
}

func TestSatisfiedWorkspaceDocBlockerSkipsAlreadyQueuedDocVersion(t *testing.T) {
	const docKey = "pilot.code-error.kvparser.alpha.20260426-084136"
	const docSHA = "sha-alpha-v1"

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "beta",
		},
		scratch: RuntimeScratchState{
			DocSHAs:                  map[string]string{},
			WorkspaceDocWakeVersions: map[string]string{},
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Docs: []WorkspaceDocRecord{{DocKey: docKey, SHA: docSHA, Title: "Alpha output"}},
				Tasks: []WorkspaceTaskRecord{{
					TaskID:      "task-beta",
					Title:       "Review Alpha output",
					Description: "Review " + docKey,
				}},
				Sessions: []AgentSessionStateRecord{{
					SessionID:   "sess-beta",
					WorkspaceID: "ws-1",
					AgentID:     "beta",
					TaskID:      "task-beta",
					Status:      "BLOCKED",
					BlockedOn:   []BlockedRef{{Kind: "dependency", Detail: "waiting for workspace doc " + docKey}},
				}},
			},
		},
	}

	target, ok := runtime.satisfiedWorkspaceDocBlockerTarget()
	if !ok {
		t.Fatal("expected first unseen doc version to wake blocked session")
	}
	if target.TaskID != "task-beta" || target.SessionID != "sess-beta" || target.DocKey != docKey || target.Version != docSHA {
		t.Fatalf("unexpected wake target: %+v", target)
	}

	runtime.scratch.WorkspaceDocWakeVersions[workspaceDocWakeStateKey(target)] = target.Version
	if target, ok := runtime.satisfiedWorkspaceDocBlockerTarget(); ok {
		t.Fatalf("expected already queued doc version to stay quiet, got %+v", target)
	}

	runtime.bootstrap.Snapshot.Docs[0].SHA = "sha-alpha-v2"
	if target, ok := runtime.satisfiedWorkspaceDocBlockerTarget(); !ok || target.Version != "sha-alpha-v2" {
		t.Fatalf("expected changed doc version to wake again, got target=%+v ok=%v", target, ok)
	}
}

func TestSatisfiedWorkspaceDocBlockerIgnoresOwnPresenceDocsFromSnapshot(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "alpha",
		},
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Docs: []WorkspaceDocRecord{
					{DocKey: agentContextDocKey("alpha"), SHA: "ctx-v1"},
					{DocKey: claimedWorkDocKey("alpha"), SHA: "claim-v1"},
				},
				Sessions: []AgentSessionStateRecord{{
					SessionID:      "sess-alpha",
					WorkspaceID:    "ws-1",
					AgentID:        "alpha",
					TaskID:         "task-alpha",
					Status:         "BLOCKED",
					RelatedDocKeys: []string{agentContextDocKey("alpha"), claimedWorkDocKey("alpha")},
					BlockedOn:      []BlockedRef{{Kind: "tool", Detail: "waiting for external browser smoke evidence"}},
				}},
			},
		},
	}

	if target, ok := runtime.satisfiedWorkspaceDocBlockerTarget(); ok {
		t.Fatalf("own presence docs should not wake blocked session, got %+v", target)
	}
}

func TestWorkspaceDocEventKeysDedupesPayloadKeys(t *testing.T) {
	keys := workspaceDocEventKeys(RhizomeEvent{
		Type:        "workspace_doc.upserted",
		PayloadJSON: `{"doc_key":"runbook.alpha","doc_keys":["runbook.alpha","runbook.beta"],"related_doc_keys":["runbook.beta"]}`,
	})
	if len(keys) != 2 || keys[0] != "runbook.alpha" || keys[1] != "runbook.beta" {
		t.Fatalf("unexpected doc keys: %#v", keys)
	}
}
