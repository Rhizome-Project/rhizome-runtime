package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type rpcRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      any            `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

func decodeRPCRequest(t *testing.T, r *http.Request) rpcRequest {
	t.Helper()

	defer r.Body.Close()
	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.Fatalf("decode rpc request: %v", err)
	}
	return req
}

func writeRPCResult(w http.ResponseWriter, req rpcRequest, result any) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"result":  result,
	})
}

func rpcString(params map[string]any, key string) string {
	value, _ := params[key].(string)
	return value
}

func TestRuntimeInitializeRegistersBootstrapsAndRestoresScratchState(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	scratch := RuntimeScratchState{
		MessageCursor:   "cursor-7",
		ActiveSessionID: "session-1",
		ActiveTaskID:    "task-1",
		ActiveRunID:     "run-1",
		LastSummary:     "steady",
		DocSHAs:         map[string]string{"agent.agent-1.current_context": "sha-old"},
	}

	methods := make([]string, 0, 4)
	heartbeats := make([]string, 0, 1)
	workspaceHost := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.auth.agent.register":
			if rpcString(req.Params, "workspace_password") != testWorkspacePassword {
				t.Fatalf("expected workspace password to be forwarded, got %+v", req.Params)
			}
			if rpcString(req.Params, "workspace_name") != "" {
				t.Fatalf("expected empty workspace_name on first registration, got %+v", req.Params)
			}
			if rpcString(req.Params, "host_url") != workspaceHost {
				t.Fatalf("expected host_url to be forwarded, got %+v", req.Params)
			}
			if rpcString(req.Params, "status") != "REGISTERED" {
				t.Fatalf("expected registration status to be forwarded, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"agent_id":       "agent-1",
				"display_name":   "Agent One",
				"token":          "agent-token-new",
				"workspace_id":   "ws-1",
				"workspace_name": "Workspace One",
				"host_url":       workspaceHost,
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
		case "agent.bootstrap":
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
					"tasks": []any{
						map[string]any{
							"task_id":        "task-1",
							"title":          "Task One",
							"description":    "First task",
							"owner_user_id":  "owner-1",
							"priority":       "HIGH",
							"status":         "RUNNING",
							"task_kind":      "general",
							"task_template":  "default",
							"linked_by":      "system",
							"linked_at":      "2026-03-23T00:00:00Z",
							"claim_agent_id": "agent-1",
							"claim_status":   "CLAIMED",
						},
					},
					"sessions": []any{
						map[string]any{
							"session_id":          "session-1",
							"workspace_id":        "ws-1",
							"agent_id":            "agent-1",
							"task_id":             "task-1",
							"status":              "ACTIVE",
							"summary":             "session summary",
							"updated_at":          "2026-03-23T00:00:00Z",
							"started_at":          "2026-03-23T00:00:00Z",
							"keep_session_active": true,
						},
					},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.profile.update":
			if got := rpcString(req.Params, "actor_id"); got != "agent-1" {
				t.Fatalf("expected profile sync actor_id agent-1, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"agent_id": "agent-1", "status": "UPDATED"})
		case "agent.state.get":
			raw, err := json.Marshal(scratch)
			if err != nil {
				t.Fatalf("marshal scratch: %v", err)
			}
			writeRPCResult(w, req, map[string]any{"value": string(raw)})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			switch rpcString(req.Params, "doc_key") {
			case agentContextDocKey("agent-1"):
				if !strings.Contains(rpcString(req.Params, "content"), "- task_id: task-1") {
					t.Fatalf("expected startup current context doc to restore task-1, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-context-startup"})
			case claimedWorkDocKey("agent-1"):
				if !strings.Contains(rpcString(req.Params, "content"), "- task_id: task-1") {
					t.Fatalf("expected startup claimed work doc to restore task-1, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-claimed-startup"})
			default:
				t.Fatalf("unexpected startup doc key: %+v", req.Params)
			}
		case "agent.heartbeat":
			heartbeats = append(heartbeats, rpcString(req.Params, "summary"))
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during initialize: %s", req.Method)
		}
	}))
	defer server.Close()
	workspaceHost = server.URL

	cfg := RuntimeConfig{
		Workdir:           workdir,
		RhizomeRPC:        server.URL,
		WorkspaceID:       "ws-1",
		WorkspacePassword: testWorkspacePassword,
		AgentID:           "agent-1",
		DisplayName:       "Agent One",
		OwnerUserID:       "owner-1",
		ProtocolVersion:   "rnar/v1",
		Mode:              RuntimeModeDaemon,
	}
	cfg.ApplyDefaults()

	runtime := &Runtime{
		cfg:    cfg,
		client: NewRhizomeClient(server.URL, ""),
		agent:  &Agent{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.initialize(context.Background()); err != nil {
		t.Fatalf("initialize() error = %v", err)
	}

	expected := []string{
		"workspace.auth.agent.register",
		"agent.profile.update",
		"agent.bootstrap",
		"agent.limits.get",
		"agent.state.get",
		"agent.state.set",
		"workspace.doc.put",
		"agent.state.set",
		"workspace.doc.put",
		"agent.state.set",
		"agent.heartbeat",
	}
	if !reflect.DeepEqual(methods, expected) {
		t.Fatalf("unexpected call order: %#v", methods)
	}
	if len(heartbeats) != 1 || !strings.Contains(heartbeats[0], "agent=agent-1") || !strings.Contains(heartbeats[0], "display=Agent One") {
		t.Fatalf("expected startup heartbeat summary to include effective identity, got %#v", heartbeats)
	}
	if runtime.activeTask == nil || runtime.activeTask.TaskID != "task-1" {
		t.Fatalf("expected active task to be restored, got %+v", runtime.activeTask)
	}
	if runtime.activeSession == nil || runtime.activeSession.SessionID != "session-1" {
		t.Fatalf("expected active session to be restored, got %+v", runtime.activeSession)
	}
	if runtime.activeRunID != "run-1" {
		t.Fatalf("expected active run id run-1, got %q", runtime.activeRunID)
	}
	if runtime.scratch.LastSummary != "steady" {
		t.Fatalf("expected scratch summary to survive initialize, got %q", runtime.scratch.LastSummary)
	}
	if runtime.cfg.RhizomeToken != "agent-token-new" {
		t.Fatalf("expected runtime token to update from registration, got %q", runtime.cfg.RhizomeToken)
	}
	if runtime.cfg.WorkspaceID != "ws-1" {
		t.Fatalf("expected runtime workspace id to update from registration, got %q", runtime.cfg.WorkspaceID)
	}
	if runtime.cfg.WorkspaceName != "Workspace One" {
		t.Fatalf("expected runtime workspace name to update from registration, got %q", runtime.cfg.WorkspaceName)
	}
	if runtime.cfg.RhizomeHost != server.URL {
		t.Fatalf("expected runtime host to update from registration, got %q", runtime.cfg.RhizomeHost)
	}
	if runtime.cfg.RhizomeRPC != defaultRPCEndpoint(server.URL) {
		t.Fatalf("expected runtime rpc endpoint to switch to workspace host rpc, got %q", runtime.cfg.RhizomeRPC)
	}
	if runtime.client.token != "agent-token-new" {
		t.Fatalf("expected client token to update from registration, got %q", runtime.client.token)
	}
	if runtime.client.endpoint != defaultRPCEndpoint(server.URL) {
		t.Fatalf("expected client endpoint to switch to workspace host rpc, got %q", runtime.client.endpoint)
	}
	if runtime.agent.Client != runtime.client {
		t.Fatalf("expected agent client to be rebound before tool registration, got %+v", runtime.agent.Client)
	}
	if runtime.agent.WorkspaceID != runtime.cfg.WorkspaceID {
		t.Fatalf("expected agent workspace id to be rebound, got %q", runtime.agent.WorkspaceID)
	}
	if runtime.agent.AgentID != runtime.cfg.AgentID {
		t.Fatalf("expected agent id to be rebound, got %q", runtime.agent.AgentID)
	}
	if runtime.agent.registry == nil {
		t.Fatal("expected agent registry to be initialized")
	}
	for _, name := range []string{"tension_attach", "tension_detach", "tension_lifecycle_update"} {
		if _, ok := runtime.agent.registry.Get(name); !ok {
			t.Fatalf("expected %s to be registered after early agent binding", name)
		}
	}
	if runtime.currentSummary() != "steady" {
		t.Fatalf("currentSummary() = %q, want steady", runtime.currentSummary())
	}
	if runtime.lastBootstrap.IsZero() {
		t.Fatal("expected lastBootstrap to be populated")
	}
	if time.Since(runtime.lastBootstrap) > time.Minute {
		t.Fatalf("lastBootstrap is too old: %s", runtime.lastBootstrap)
	}

	profile := LoadRhizomeProfile()
	if profile.AgentToken != "agent-token-new" {
		t.Fatalf("expected saved profile token to match registration token, got %+v", profile)
	}
	if profile.WorkspaceID != "ws-1" || profile.WorkspaceName != "Workspace One" {
		t.Fatalf("unexpected saved workspace profile: %+v", profile)
	}
	if profile.WorkspacePassword != testWorkspacePassword {
		t.Fatalf("expected configured workspace password to be saved, got %+v", profile)
	}
	if profile.HostURL != server.URL {
		t.Fatalf("expected saved host url to match registration host, got %+v", profile)
	}
	if profile.RPCEndpoint != defaultRPCEndpoint(server.URL) {
		t.Fatalf("expected saved rpc endpoint to match runtime rpc, got %+v", profile)
	}
	if profile.OwnerUserID != "owner-1" {
		t.Fatalf("expected saved owner user id to match registration owner, got %+v", profile)
	}
}

func TestRecoverActiveStateDropsClosedTaskSessionAndIdleSummary(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "beta",
		},
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-old",
			ActiveSessionID: "session-old",
			ActiveRunID:     "run-old",
			LastSummary:     "stale kvparser review blocked on old alpha output",
			DocSHAs:         map[string]string{},
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Tasks: []WorkspaceTaskRecord{{
					TaskID:       "task-old",
					Title:        "Old task",
					Status:       "CANCELLED",
					ClaimAgentID: stringPtr("beta"),
					ClaimStatus:  stringPtr("CLAIMED"),
				}},
				Sessions: []AgentSessionStateRecord{{
					SessionID: "session-old",
					AgentID:   "beta",
					TaskID:    "task-old",
					Status:    "BLOCKED",
					Summary:   "blocked on old alpha output",
				}},
			},
		},
		activeTask:    &WorkspaceTaskRecord{TaskID: "task-old"},
		activeSession: &AgentSessionStateRecord{SessionID: "session-old"},
		activeRunID:   "run-old",
	}

	runtime.recoverActiveStateLocked()

	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeRunID != "" {
		t.Fatalf("expected closed task/session to be discarded, task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
	if runtime.scratch.ActiveTaskID != "" || runtime.scratch.ActiveSessionID != "" || runtime.scratch.ActiveRunID != "" {
		t.Fatalf("expected scratch active ids to be cleared, got %+v", runtime.scratch)
	}
	if runtime.scratch.LastSummary != "idle" {
		t.Fatalf("expected stale summary to reset to idle, got %q", runtime.scratch.LastSummary)
	}
}

func TestRuntimeInitializeUsesSavedTokenWithoutReRegistering(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	scratch := RuntimeScratchState{
		MessageCursor: "cursor-1",
		LastSummary:   "from saved token",
		DocSHAs:       map[string]string{},
	}
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		if r.Header.Get("Authorization") != "Bearer saved-token" {
			t.Fatalf("expected saved token auth header, got %q", r.Header.Get("Authorization"))
		}

		switch req.Method {
		case "agent.profile.update":
			writeRPCResult(w, req, map[string]any{"agent_id": "agent-1", "status": "UPDATED"})
		case "agent.bootstrap":
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
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.get":
			raw, err := json.Marshal(scratch)
			if err != nil {
				t.Fatalf("marshal scratch: %v", err)
			}
			writeRPCResult(w, req, map[string]any{"value": string(raw)})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.heartbeat":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during saved-token initialize: %s", req.Method)
		}
	}))
	defer server.Close()

	cfg := RuntimeConfig{
		Workdir:         workdir,
		RhizomeRPC:      server.URL,
		RhizomeToken:    "saved-token",
		WorkspaceID:     "ws-1",
		WorkspaceName:   "Workspace One",
		AgentID:         "agent-1",
		DisplayName:     "Agent One",
		ProtocolVersion: "rnar/v1",
		Mode:            RuntimeModeDaemon,
	}
	cfg.ApplyDefaults()

	runtime := &Runtime{
		cfg:    cfg,
		client: NewRhizomeClient(server.URL, "saved-token"),
		agent:  &Agent{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.initialize(context.Background()); err != nil {
		t.Fatalf("initialize() error = %v", err)
	}

	expected := []string{"agent.profile.update", "agent.bootstrap", "agent.limits.get", "agent.state.get", "agent.state.set", "agent.heartbeat"}
	if !reflect.DeepEqual(methods, expected) {
		t.Fatalf("unexpected call order: %#v", methods)
	}
	if runtime.cfg.RhizomeToken != "saved-token" || runtime.client.token != "saved-token" {
		t.Fatalf("expected saved token to remain in use, got cfg=%q client=%q", runtime.cfg.RhizomeToken, runtime.client.token)
	}
	profile := LoadRhizomeProfile()
	if profile.AgentToken != "saved-token" || profile.WorkspaceID != "ws-1" || profile.WorkspaceName != "Workspace One" {
		t.Fatalf("unexpected saved profile after token reuse: %+v", profile)
	}
}

func TestRuntimeInitializeRefreshesRegistrationAfterBootstrapAuthFailure(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	scratch := RuntimeScratchState{
		MessageCursor: "cursor-refresh",
		LastSummary:   "after refresh",
		DocSHAs:       map[string]string{},
	}
	var methods []string
	bootstrapCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.profile.update":
			if r.Header.Get("Authorization") != "Bearer stale-token" && r.Header.Get("Authorization") != "Bearer refreshed-token" {
				t.Fatalf("expected profile sync to use stale or refreshed token, got %q", r.Header.Get("Authorization"))
			}
			writeRPCResult(w, req, map[string]any{"agent_id": "agent-1", "status": "UPDATED"})
		case "agent.bootstrap":
			bootstrapCalls++
			if bootstrapCalls == 1 {
				if r.Header.Get("Authorization") != "Bearer stale-token" {
					t.Fatalf("expected first bootstrap to use stale token, got %q", r.Header.Get("Authorization"))
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req.ID,
					"error": map[string]any{
						"code":    -32000,
						"message": "invalid access token",
					},
				})
				return
			}
			if r.Header.Get("Authorization") != "Bearer refreshed-token" {
				t.Fatalf("expected retry bootstrap to use refreshed token, got %q", r.Header.Get("Authorization"))
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
				},
			})
		case "workspace.auth.agent.register":
			if got := r.Header.Get("Authorization"); got != "" {
				t.Fatalf("expected refresh registration to omit stale auth header, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"agent_id":       "agent-1",
				"display_name":   "Agent One",
				"token":          "refreshed-token",
				"workspace_id":   "ws-1",
				"workspace_name": "Workspace One",
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
		case "agent.state.get":
			raw, err := json.Marshal(scratch)
			if err != nil {
				t.Fatalf("marshal scratch: %v", err)
			}
			if r.Header.Get("Authorization") != "Bearer refreshed-token" {
				t.Fatalf("expected scratch load to use refreshed token, got %q", r.Header.Get("Authorization"))
			}
			writeRPCResult(w, req, map[string]any{"value": string(raw)})
		case "agent.state.set":
			if r.Header.Get("Authorization") != "Bearer refreshed-token" {
				t.Fatalf("expected scratch save to use refreshed token, got %q", r.Header.Get("Authorization"))
			}
			writeRPCResult(w, req, nil)
		case "agent.heartbeat":
			if r.Header.Get("Authorization") != "Bearer refreshed-token" {
				t.Fatalf("expected heartbeat to use refreshed token, got %q", r.Header.Get("Authorization"))
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during token refresh initialize: %s", req.Method)
		}
	}))
	defer server.Close()

	cfg := RuntimeConfig{
		Workdir:           workdir,
		RhizomeRPC:        server.URL,
		RhizomeToken:      "stale-token",
		WorkspaceID:       "ws-1",
		WorkspaceName:     "Workspace One",
		WorkspacePassword: testWorkspacePassword,
		AgentID:           "agent-1",
		DisplayName:       "Agent One",
		OwnerUserID:       "owner-1",
		ProtocolVersion:   "rnar/v1",
		Mode:              RuntimeModeDaemon,
	}
	cfg.ApplyDefaults()

	runtime := &Runtime{
		cfg:    cfg,
		client: NewRhizomeClient(server.URL, "stale-token"),
		agent:  &Agent{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.initialize(context.Background()); err != nil {
		t.Fatalf("initialize() error = %v", err)
	}

	expected := []string{"agent.profile.update", "agent.bootstrap", "workspace.auth.agent.register", "agent.bootstrap", "agent.limits.get", "agent.state.get", "agent.state.set", "agent.heartbeat"}
	if !reflect.DeepEqual(methods, expected) {
		t.Fatalf("unexpected call order: %#v", methods)
	}
	if runtime.cfg.RhizomeToken != "refreshed-token" || runtime.client.token != "refreshed-token" {
		t.Fatalf("expected refreshed token to replace stale one, got cfg=%q client=%q", runtime.cfg.RhizomeToken, runtime.client.token)
	}
}

func TestRuntimeRecoverActiveStateLockedFallsBackToOwnedWork(t *testing.T) {
	self := "agent-1"
	claimed := "CLAIMED"
	r := &Runtime{
		cfg: RuntimeConfig{AgentID: self},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Tasks: []WorkspaceTaskRecord{
					{TaskID: "task-owned", Priority: "HIGH", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
					{TaskID: "task-other", Priority: "CRITICAL", Status: "PENDING"},
				},
				Sessions: []AgentSessionStateRecord{
					{SessionID: "session-owned", AgentID: self, TaskID: "task-owned", Status: "ACTIVE", Summary: "running"},
				},
			},
		},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	r.recoverActiveStateLocked()

	if r.activeTask == nil || r.activeTask.TaskID != "task-owned" {
		t.Fatalf("expected owned task to be recovered, got %+v", r.activeTask)
	}
	if r.activeSession == nil || r.activeSession.SessionID != "session-owned" {
		t.Fatalf("expected owned session to be recovered, got %+v", r.activeSession)
	}
}

func TestRuntimeRecoverActiveStateClearsStaleRunIDWhenFallbackSessionDiffers(t *testing.T) {
	self := "agent-1"
	claimed := "CLAIMED"
	r := &Runtime{
		cfg: RuntimeConfig{AgentID: self},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Tasks: []WorkspaceTaskRecord{
					{TaskID: "task-new", Priority: "HIGH", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
				},
				Sessions: []AgentSessionStateRecord{
					{SessionID: "session-new", AgentID: self, TaskID: "task-new", Status: "ACTIVE", Summary: "running"},
				},
			},
		},
		activeRunID: "run-stale",
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-old",
			ActiveSessionID: "session-old",
			ActiveRunID:     "run-stale",
			LastSummary:     "old task summary",
			DocSHAs:         map[string]string{},
		},
	}

	r.recoverActiveStateLocked()

	if r.activeTask == nil || r.activeTask.TaskID != "task-new" {
		t.Fatalf("expected fallback task recovery, got %+v", r.activeTask)
	}
	if r.activeSession == nil || r.activeSession.SessionID != "session-new" {
		t.Fatalf("expected fallback session recovery, got %+v", r.activeSession)
	}
	if r.activeRunID != "" || r.scratch.ActiveRunID != "" {
		t.Fatalf("expected stale ActiveRunID to be cleared when recovered task/session differ, run=%q scratch=%+v", r.activeRunID, r.scratch)
	}
}

func TestRuntimeEnsureRunnableTaskClaimsSessionAndPersistsScratch(t *testing.T) {
	var methods []string
	var sessionStartSessionID string
	var sessionStartTaskID string
	var runWriteRunID string
	var persistedScratch RuntimeScratchState
	var claimedWorkContent string
	var currentContextContent string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-03-23T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-1",
				"has_work":     true,
				"reason":       "next_pending",
				"task": map[string]any{
					"task_id":       "task-claim",
					"title":         "Claim me",
					"description":   "make progress",
					"owner_user_id": "owner-1",
					"priority":      "HIGH",
					"status":        "PENDING",
					"task_kind":     "general",
					"task_template": "default",
					"linked_by":     "system",
					"linked_at":     "2026-03-23T00:00:00Z",
				},
				"hydration": map[string]any{
					"generated_at": "2026-03-23T00:00:00Z",
					"workspace_task": map[string]any{
						"task_id":       "task-claim",
						"title":         "Claim me",
						"description":   "make progress",
						"owner_user_id": "owner-1",
						"priority":      "HIGH",
						"status":        "PENDING",
						"task_kind":     "general",
						"task_template": "default",
						"linked_by":     "system",
						"linked_at":     "2026-03-23T00:00:00Z",
					},
					"task": map[string]any{
						"task_id":       "task-claim",
						"title":         "Claim me",
						"description":   "make progress",
						"owner_user_id": "owner-1",
						"priority":      "HIGH",
						"status":        "PENDING",
						"task_kind":     "general",
						"task_template": "default",
						"node_counts":   map[string]any{},
						"nodes":         []any{},
					},
					"docs":          []any{},
					"task_links":    []any{},
					"related_tasks": []any{},
					"artifacts":     []any{},
					"updates":       []any{},
				},
			})
		case "agent.task.claim":
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			sessionStartSessionID = rpcString(req.Params, "session_id")
			sessionStartTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{
				"state": map[string]any{
					"session_id":          sessionStartSessionID,
					"workspace_id":        "ws-1",
					"agent_id":            "agent-1",
					"task_id":             sessionStartTaskID,
					"status":              "ACTIVE",
					"summary":             "session started",
					"updated_at":          "2026-03-23T00:00:00Z",
					"started_at":          "2026-03-23T00:00:00Z",
					"keep_session_active": true,
				},
			})
		case "workspace.execution.run.write":
			runWriteRunID = rpcString(req.Params, "run_id")
			writeRPCResult(w, req, map[string]any{
				"run": map[string]any{
					"run_id":       runWriteRunID,
					"workspace_id": "ws-1",
					"task_id":      rpcString(req.Params, "task_id"),
					"session_id":   rpcString(req.Params, "session_id"),
					"agent_id":     "agent-1",
					"title":        rpcString(req.Params, "title"),
					"summary":      rpcString(req.Params, "summary"),
					"status":       rpcString(req.Params, "status"),
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &persistedScratch); err != nil {
				t.Fatalf("decode persisted scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			if rpcString(req.Params, "doc_key") == agentContextDocKey("agent-1") {
				currentContextContent = rpcString(req.Params, "content")
			}
			if rpcString(req.Params, "doc_key") == claimedWorkDocKey("agent-1") {
				claimedWorkContent = rpcString(req.Params, "content")
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-claimed-work"})
		default:
			t.Fatalf("unexpected method during task ensure: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{
		TaskID:       "task-claim",
		Title:        "Claim me",
		Description:  "make progress",
		OwnerUserID:  "owner-1",
		Priority:     "HIGH",
		Status:       "PENDING",
		TaskKind:     "general",
		TaskTemplate: "default",
		LinkedBy:     "system",
		LinkedAt:     "2026-03-23T00:00:00Z",
	}

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
		},
		client: NewRhizomeClient(server.URL, "token"),
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{Tasks: []WorkspaceTaskRecord{task}},
		},
		scratch: RuntimeScratchState{
			ActiveTaskID: "task-stale",
			LastSummary:  "stale blocked summary",
			DocSHAs:      map[string]string{},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	got, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if got == nil || got.TaskID != task.TaskID {
		t.Fatalf("ensureRunnableTask() = %+v, want task %q", got, task.TaskID)
	}
	if got.ClaimAgentID == nil || *got.ClaimAgentID != "agent-1" {
		t.Fatalf("expected task to be claimed by the agent, got %+v", got.ClaimAgentID)
	}
	if runtime.activeTask == nil || runtime.activeTask.TaskID != task.TaskID {
		t.Fatalf("expected runtime active task to be updated, got %+v", runtime.activeTask)
	}
	if runtime.activeHydration == nil || hydrationTaskID(runtime.activeHydration) != task.TaskID {
		t.Fatalf("expected runtime hydration cache for task %q, got %+v", task.TaskID, runtime.activeHydration)
	}
	if runtime.activeSession == nil || runtime.activeSession.SessionID == "" {
		t.Fatalf("expected runtime active session to be created, got %+v", runtime.activeSession)
	}
	if runtime.activeRunID == "" {
		t.Fatal("expected active run id to be set")
	}
	if sessionStartSessionID == "" || sessionStartTaskID != task.TaskID {
		t.Fatalf("unexpected session start inputs: session_id=%q task_id=%q", sessionStartSessionID, sessionStartTaskID)
	}
	if runWriteRunID == "" {
		t.Fatal("expected execution run to be written with a run id")
	}
	if persistedScratch.ActiveTaskID != task.TaskID || persistedScratch.ActiveSessionID != runtime.activeSession.SessionID || persistedScratch.ActiveRunID != runtime.activeRunID {
		t.Fatalf("persisted scratch mismatch: %+v", persistedScratch)
	}
	if persistedScratch.LastSummary != "Claim me" {
		t.Fatalf("expected fresh task-local summary, got %+v", persistedScratch)
	}
	if !strings.Contains(claimedWorkContent, "- last_summary: Claim me") {
		t.Fatalf("expected claimed work doc to use fresh summary, got %q", claimedWorkContent)
	}
	if strings.Contains(claimedWorkContent, "stale blocked summary") {
		t.Fatalf("expected stale summary to be removed from claimed work doc, got %q", claimedWorkContent)
	}
	if !strings.Contains(currentContextContent, "- task_id: task-claim") || !strings.Contains(currentContextContent, "- outcome: active") {
		t.Fatalf("expected current context doc to reflect active claim, got %q", currentContextContent)
	}
	if strings.Contains(currentContextContent, "stale blocked summary") {
		t.Fatalf("expected stale summary to be removed from current context doc, got %q", currentContextContent)
	}
	expectedMethods := []string{
		"agent.work.next",
		"agent.task.claim",
		"agent.session.start",
		"workspace.execution.run.write",
		"agent.state.set",
		"workspace.doc.put",
		"agent.state.set",
		"workspace.doc.put",
		"agent.state.set",
	}
	if !reflect.DeepEqual(methods, expectedMethods) {
		t.Fatalf("unexpected call order: %#v", methods)
	}
}

func TestRuntimeEnsureRunnableTaskReclaimsReleasedSelfOwnedTaskBeforeStartingSession(t *testing.T) {
	var methods []string
	var sessionStartTaskID string
	var persistedScratch RuntimeScratchState
	var currentContextContent string
	claimed := "CLAIMED"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-03-23T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-1",
				"has_work":     true,
				"reason":       "next_pending",
				"task": map[string]any{
					"task_id":        "task-reclaim",
					"title":          "Reclaim me",
					"description":    "claim drifted to released",
					"owner_user_id":  "owner-1",
					"priority":       "HIGH",
					"status":         "PENDING",
					"task_kind":      "general",
					"task_template":  "default",
					"linked_by":      "system",
					"linked_at":      "2026-03-23T00:00:00Z",
					"claim_agent_id": "agent-1",
					"claim_status":   "RELEASED",
				},
				"hydration": map[string]any{
					"generated_at": "2026-03-23T00:00:00Z",
					"workspace_task": map[string]any{
						"task_id":        "task-reclaim",
						"title":          "Reclaim me",
						"description":    "claim drifted to released",
						"owner_user_id":  "owner-1",
						"priority":       "HIGH",
						"status":         "PENDING",
						"task_kind":      "general",
						"task_template":  "default",
						"linked_by":      "system",
						"linked_at":      "2026-03-23T00:00:00Z",
						"claim_agent_id": "agent-1",
						"claim_status":   "RELEASED",
					},
					"task": map[string]any{
						"task_id":       "task-reclaim",
						"title":         "Reclaim me",
						"description":   "claim drifted to released",
						"owner_user_id": "owner-1",
						"priority":      "HIGH",
						"status":        "PENDING",
						"task_kind":     "general",
						"task_template": "default",
						"node_counts":   map[string]any{},
						"nodes":         []any{},
					},
					"docs":          []any{},
					"task_links":    []any{},
					"related_tasks": []any{},
					"artifacts":     []any{},
					"updates":       []any{},
				},
			})
		case "agent.task.claim":
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			sessionStartTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{
				"state": map[string]any{
					"session_id":          rpcString(req.Params, "session_id"),
					"workspace_id":        "ws-1",
					"agent_id":            "agent-1",
					"task_id":             sessionStartTaskID,
					"status":              "ACTIVE",
					"summary":             "session started",
					"updated_at":          "2026-03-23T00:00:00Z",
					"started_at":          "2026-03-23T00:00:00Z",
					"keep_session_active": true,
				},
			})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{
				"run": map[string]any{
					"run_id":       rpcString(req.Params, "run_id"),
					"workspace_id": "ws-1",
					"task_id":      rpcString(req.Params, "task_id"),
					"session_id":   rpcString(req.Params, "session_id"),
					"agent_id":     "agent-1",
					"title":        rpcString(req.Params, "title"),
					"summary":      rpcString(req.Params, "summary"),
					"status":       rpcString(req.Params, "status"),
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &persistedScratch); err != nil {
				t.Fatalf("decode persisted scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			if rpcString(req.Params, "doc_key") == agentContextDocKey("agent-1") {
				currentContextContent = rpcString(req.Params, "content")
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-claimed-work"})
		default:
			t.Fatalf("unexpected method during released self-claim reclaim: %s", req.Method)
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
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-reclaim",
			ActiveSessionID: "session-stale",
			ActiveRunID:     "run-stale",
			LastSummary:     "stale blocked summary",
			DocSHAs:         map[string]string{},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	got, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if got == nil || got.TaskID != "task-reclaim" {
		t.Fatalf("expected reclaimed task, got %+v", got)
	}
	if got.ClaimAgentID == nil || *got.ClaimAgentID != "agent-1" || got.ClaimStatus == nil || *got.ClaimStatus != claimed {
		t.Fatalf("expected live claimed ownership after re-claim, got %+v", got)
	}
	if sessionStartTaskID != "task-reclaim" {
		t.Fatalf("expected session to start on reclaimed task, got %q", sessionStartTaskID)
	}
	expectedMethods := []string{
		"agent.work.next",
		"agent.task.claim",
		"agent.session.start",
		"workspace.execution.run.write",
		"agent.state.set",
		"workspace.doc.put",
		"agent.state.set",
		"workspace.doc.put",
		"agent.state.set",
	}
	if !reflect.DeepEqual(methods, expectedMethods) {
		t.Fatalf("unexpected call order: %#v", methods)
	}
	if persistedScratch.ActiveTaskID != "task-reclaim" {
		t.Fatalf("expected scratch to track reclaimed task, got %+v", persistedScratch)
	}
	if persistedScratch.LastSummary != "Reclaim me" {
		t.Fatalf("expected reclaimed task to replace stale summary, got %+v", persistedScratch)
	}
	if !strings.Contains(currentContextContent, "- summary: Reclaim me") {
		t.Fatalf("expected current context doc to use fresh activation summary, got %q", currentContextContent)
	}
}

func TestRuntimeEnsureRunnableTaskClearsReleasedActiveClaimBeforeReclaim(t *testing.T) {
	var methods []string
	var persistedScratch RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &persistedScratch); err != nil {
				t.Fatalf("decode persisted scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-presence"})
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-25T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-1",
				"has_work":     false,
				"reason":       "no_runnable_work",
			})
		case "agent.task.claim", "agent.session.start":
			t.Fatalf("released active claim must not be treated as already runnable before work.next reclaim, got %s", req.Method)
		default:
			t.Fatalf("unexpected method during released active cleanup: %s", req.Method)
		}
	}))
	defer server.Close()

	released := "RELEASED"
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
			DisplayName:  "Agent One",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-released",
			ActiveSessionID: "session-stale",
			ActiveRunID:     "run-stale",
			DocSHAs:         map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:       "task-released",
			Title:        "Released owner-bound lane",
			Status:       "PENDING",
			ClaimAgentID: stringPtr("agent-1"),
			ClaimStatus:  &released,
			ProjectID:    "project-1",
			ProjectLane:  "implementation",
			TaskKind:     "EXECUTION",
			TaskTemplate: "generic",
			OwnerUserID:  "owner-1",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-stale",
			TaskID:    "task-released",
			AgentID:   "agent-1",
			Status:    "ACTIVE",
		},
		activeRunID: "run-stale",
	}
	t.Cleanup(func() { _ = runtime.Close() })

	got, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if got != nil {
		t.Fatalf("released active claim must not be returned as runnable before reclaim, got %+v", got)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeRunID != "" {
		t.Fatalf("released active state should be cleared locally, task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
	if persistedScratch.ActiveTaskID != "" || persistedScratch.ActiveSessionID != "" || persistedScratch.ActiveRunID != "" {
		t.Fatalf("released active state should be cleared in persisted scratch, got %+v", persistedScratch)
	}
	if !containsAll(methods, []string{"agent.state.set", "workspace.doc.put", "agent.work.next"}) {
		t.Fatalf("expected cleanup plus work.next reclaim opportunity, got %#v", methods)
	}
}

func TestRuntimeEnsureRunnableTaskPreservesProfileGateClosedWorkNext(t *testing.T) {
	var methods []string
	currentContextDocWrites := 0
	claimedWorkDocWrites := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at":                 "2026-03-23T00:00:00Z",
				"workspace_id":                 "ws-1",
				"agent_id":                     "agent-1",
				"has_work":                     false,
				"reason":                       "profile_gate_closed",
				"autonomous_execution_allowed": false,
				"profile_gate_reason":          "default_work_mode_observer",
				"profile_gate_summary":         "Agent profile default_work_mode is observer.",
				"profile_gate_blocked_work":    true,
				"packet": map[string]any{
					"work_type":            "profile_gate_closed",
					"coordination_state":   "profile_gate_closed",
					"preferred_transition": "agent_profile_update",
					"why_now":              "default_work_mode_observer",
					"gate": map[string]any{
						"gate_state":  "closed",
						"gate_type":   "profile_autonomous_execution",
						"needed_from": "agent.profile.update",
						"summary":     "Agent profile default_work_mode is observer.",
					},
				},
			})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			switch rpcString(req.Params, "doc_key") {
			case agentContextDocKey("agent-1"):
				currentContextDocWrites++
				content := rpcString(req.Params, "content")
				if !strings.Contains(content, "- outcome: idle") || !strings.Contains(content, "- task_id: (none)") {
					t.Fatalf("expected profile gate to clear current context doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-profile-context-cleared"})
			case claimedWorkDocKey("agent-1"):
				claimedWorkDocWrites++
				if !strings.Contains(rpcString(req.Params, "content"), "active_claimed_work: none") {
					t.Fatalf("expected profile gate to clear claimed work doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-profile-claimed-cleared"})
			default:
				t.Fatalf("profile gate should not materialize doc %q: %+v", rpcString(req.Params, "doc_key"), req.Params)
			}
		default:
			t.Fatalf("profile gate should not claim, start a session, or use bootstrap fallback; unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	bootstrapTask := WorkspaceTaskRecord{
		TaskID:       "task-bootstrap",
		Title:        "Should stay unclaimed",
		OwnerUserID:  "owner-1",
		Priority:     "HIGH",
		Status:       "PENDING",
		TaskKind:     "general",
		TaskTemplate: "default",
		LinkedBy:     "system",
		LinkedAt:     "2026-03-23T00:00:00Z",
	}

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
		},
		client: NewRhizomeClient(server.URL, "token"),
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{Tasks: []WorkspaceTaskRecord{bootstrapTask}},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:       "task-stale",
			Title:        "Stale task",
			OwnerUserID:  "owner-1",
			Priority:     "HIGH",
			Status:       "PENDING",
			TaskKind:     "general",
			TaskTemplate: "default",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID:   "session-stale",
			WorkspaceID: "ws-1",
			AgentID:     "agent-1",
			TaskID:      "task-stale",
			Status:      "ENDED",
		},
		activeRunID: "run-stale",
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-stale",
			ActiveSessionID: "session-stale",
			ActiveRunID:     "run-stale",
			LastSummary:     "stale active summary",
			DocSHAs:         map[string]string{},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	got, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if got != nil {
		t.Fatalf("expected profile gate to leave runtime without a task, got %+v", got)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeHydration != nil {
		t.Fatalf("expected no active task/session/hydration, got task=%+v session=%+v hydration=%+v", runtime.activeTask, runtime.activeSession, runtime.activeHydration)
	}
	if runtime.activeRunID != "" || runtime.scratch.ActiveTaskID != "" || runtime.scratch.ActiveSessionID != "" || runtime.scratch.ActiveRunID != "" {
		t.Fatalf("expected profile gate to clear stale active ids, run=%q scratch=%+v", runtime.activeRunID, runtime.scratch)
	}
	if runtime.activeWorkPacket == nil || runtime.activeWorkPacket.Gate == nil {
		t.Fatalf("expected profile gate work packet to be preserved, got %+v", runtime.activeWorkPacket)
	}
	if runtime.activeWorkPacket.WorkType != "profile_gate_closed" || runtime.activeWorkPacket.Gate.GateType != "profile_autonomous_execution" {
		t.Fatalf("unexpected preserved profile gate packet: %+v", runtime.activeWorkPacket)
	}
	if runtime.scratch.LastWakeReason != "profile_gate_closed" || runtime.scratch.LastWakeSummary != "Agent profile default_work_mode is observer." {
		t.Fatalf("expected scratch wake diagnostic to reflect profile gate, got %+v", runtime.scratch)
	}

	status := runtime.runtimeStatusPayload(time.Now().UTC())
	packet, ok := status["work_packet"].(*AgentWorkPacket)
	if !ok || packet == nil || packet.Gate == nil {
		t.Fatalf("runtime status did not expose profile gate packet: %+v", status["work_packet"])
	}
	if packet.Gate.GateState != "closed" || packet.Gate.GateType != "profile_autonomous_execution" || packet.WhyNow != "default_work_mode_observer" {
		t.Fatalf("runtime status exposed wrong gate packet: %+v", packet)
	}
	if status["work_type"] != "profile_gate_closed" || status["work_gate_state"] != "closed" || status["work_gate_type"] != "profile_autonomous_execution" {
		t.Fatalf("runtime status did not expose top-level profile gate fields: %+v", status)
	}
	fallback, ok := status["bootstrap_work_fallback"].(map[string]any)
	if !ok || fallback["posture"] != "disabled" || fallback["can_consume_work"] != false {
		t.Fatalf("runtime status did not expose closed daemon bootstrap fallback posture: %+v", status["bootstrap_work_fallback"])
	}
	if status["task_id"] != "" || status["session_id"] != "" {
		t.Fatalf("runtime status should not expose stale task/session, got task=%v session=%v", status["task_id"], status["session_id"])
	}

	if currentContextDocWrites != 1 || claimedWorkDocWrites != 1 {
		t.Fatalf("expected profile gate to publish one current-context and one claimed-work cleanup doc, got current=%d claimed=%d", currentContextDocWrites, claimedWorkDocWrites)
	}
	expectedMethods := []string{"agent.work.next", "agent.state.set", "workspace.doc.put", "agent.state.set", "workspace.doc.put", "agent.state.set"}
	if !reflect.DeepEqual(methods, expectedMethods) {
		t.Fatalf("unexpected call order: %#v", methods)
	}
}

func TestRuntimeEnsureRunnableTaskPreservesProjectGateClosedWorkNext(t *testing.T) {
	var methods []string
	currentContextDocWrites := 0
	claimedWorkDocWrites := 0
	var gateUpdatePayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at":                 "2026-03-23T00:00:00Z",
				"workspace_id":                 "ws-1",
				"agent_id":                     "agent-1",
				"has_work":                     false,
				"reason":                       "project_gate_closed",
				"autonomous_execution_allowed": true,
				"packet": map[string]any{
					"work_type":            "project_gate_closed",
					"coordination_state":   "project_gate_closed",
					"preferred_transition": "renew_strategic_lead",
					"why_now":              "implementation gate blocked",
					"gate": map[string]any{
						"gate_state":  "BLOCKED",
						"gate_type":   "project_implementation_gate",
						"needed_from": "project.lead.claim",
						"summary":     "strategic_lead_active: Active strategic lead lease is required before implementation work",
					},
				},
			})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			switch rpcString(req.Params, "doc_key") {
			case agentContextDocKey("agent-1"):
				currentContextDocWrites++
				if !strings.Contains(rpcString(req.Params, "content"), "- outcome: idle") {
					t.Fatalf("expected project gate to clear current context doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-project-gate-context-cleared"})
			case claimedWorkDocKey("agent-1"):
				claimedWorkDocWrites++
				if !strings.Contains(rpcString(req.Params, "content"), "active_claimed_work: none") {
					t.Fatalf("expected project gate to clear claimed work doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-project-gate-claimed-cleared"})
			default:
				t.Fatalf("project gate should not materialize doc %q: %+v", rpcString(req.Params, "doc_key"), req.Params)
			}
		case "agent.update.post":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &gateUpdatePayload); err != nil {
				t.Fatalf("decode delegated project gate update: %v", err)
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-project-gate"})
		default:
			t.Fatalf("project gate should not claim, start a session, or use bootstrap fallback; unexpected method %s", req.Method)
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
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			ActiveTaskID:       "task-pending",
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-pending",
			LastSummary:        "stale active summary",
			DocSHAs:            map[string]string{},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	got, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if got != nil {
		t.Fatalf("expected project gate to leave runtime without a task, got %+v", got)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeHydration != nil {
		t.Fatalf("expected no active task/session/hydration, got task=%+v session=%+v hydration=%+v", runtime.activeTask, runtime.activeSession, runtime.activeHydration)
	}
	if runtime.scratch.PendingTrigger != "" || runtime.scratch.ActiveTaskID != "" {
		t.Fatalf("expected project gate to clear trigger and active task, got scratch=%+v", runtime.scratch)
	}
	if runtime.activeWorkPacket == nil || runtime.activeWorkPacket.Gate == nil {
		t.Fatalf("expected project gate work packet to be preserved, got %+v", runtime.activeWorkPacket)
	}
	if runtime.activeWorkPacket.WorkType != "project_gate_closed" || runtime.activeWorkPacket.Gate.GateType != "project_implementation_gate" {
		t.Fatalf("unexpected preserved project gate packet: %+v", runtime.activeWorkPacket)
	}
	if !strings.Contains(runtime.scratch.LastWakeSummary, "strategic_lead_active") {
		t.Fatalf("expected scratch wake diagnostic to reflect project gate, got %+v", runtime.scratch)
	}

	status := runtime.runtimeStatusPayload(time.Now().UTC())
	if status["work_type"] != "project_gate_closed" || status["work_gate_state"] != "BLOCKED" || status["work_gate_type"] != "project_implementation_gate" {
		t.Fatalf("runtime status did not expose top-level project gate fields: %+v", status)
	}
	if currentContextDocWrites != 1 || claimedWorkDocWrites != 1 {
		t.Fatalf("expected project gate to publish cleanup docs, got current=%d claimed=%d", currentContextDocWrites, claimedWorkDocWrites)
	}
	if gateUpdatePayload["delegation_state"] != "blocked_project_gate" || gateUpdatePayload["task_id"] != "task-pending" {
		t.Fatalf("expected delegated project gate blocker update, got %+v", gateUpdatePayload)
	}
	expectedMethods := []string{"agent.work.next", "agent.state.set", "workspace.doc.put", "agent.state.set", "workspace.doc.put", "agent.state.set", "agent.update.post"}
	if !reflect.DeepEqual(methods, expectedMethods) {
		t.Fatalf("unexpected call order: %#v", methods)
	}
}

func TestRuntimeEnsureRunnableTaskNonDaemonFallsBackWhenAgentWorkNextIsUnavailable(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "")

	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.work.next":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error": map[string]any{
					"code":    -32601,
					"message": "unknown method: agent.work.next",
				},
			})
		case "agent.task.claim":
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			writeRPCResult(w, req, map[string]any{
				"state": map[string]any{
					"session_id":          rpcString(req.Params, "session_id"),
					"workspace_id":        "ws-1",
					"agent_id":            "agent-1",
					"task_id":             "task-fallback",
					"status":              "ACTIVE",
					"summary":             "session started",
					"updated_at":          "2026-03-23T00:00:00Z",
					"started_at":          "2026-03-23T00:00:00Z",
					"keep_session_active": true,
				},
			})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{
				"run": map[string]any{
					"run_id":       rpcString(req.Params, "run_id"),
					"workspace_id": "ws-1",
					"task_id":      "task-fallback",
					"session_id":   rpcString(req.Params, "session_id"),
					"agent_id":     "agent-1",
					"title":        rpcString(req.Params, "title"),
					"summary":      rpcString(req.Params, "summary"),
					"status":       rpcString(req.Params, "status"),
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-fallback"})
		default:
			t.Fatalf("unexpected method during fallback ensure: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{
		TaskID:       "task-fallback",
		Title:        "Fallback task",
		Description:  "use bootstrap fallback",
		OwnerUserID:  "owner-1",
		Priority:     "HIGH",
		Status:       "PENDING",
		TaskKind:     "general",
		TaskTemplate: "default",
		LinkedBy:     "system",
		LinkedAt:     "2026-03-23T00:00:00Z",
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:            RuntimeModeTUI,
			Workdir:         t.TempDir(),
			RhizomeRPC:      server.URL,
			RhizomeToken:    "token",
			WorkspaceID:     "ws-1",
			AgentID:         "agent-1",
			DisplayName:     "Agent One",
			OwnerUserID:     "owner-1",
			ProtocolVersion: "rnar/v1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{Tasks: []WorkspaceTaskRecord{task}},
		},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	got, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if got == nil || got.TaskID != task.TaskID {
		t.Fatalf("ensureRunnableTask() = %+v, want fallback task %q", got, task.TaskID)
	}
	if runtime.activeHydration != nil {
		t.Fatalf("expected no hydration cache on bootstrap fallback, got %+v", runtime.activeHydration)
	}
	if len(methods) == 0 || methods[0] != "agent.work.next" {
		t.Fatalf("expected agent.work.next to be attempted first, got %#v", methods)
	}
	status := runtime.runtimeStatusPayload(time.Now().UTC())
	fallback, ok := status["bootstrap_work_fallback"].(map[string]any)
	if !ok || fallback["posture"] != "compatibility_enabled" || fallback["can_consume_work"] != true {
		t.Fatalf("non-daemon runtime status should show compatibility fallback can consume work, got %+v", status["bootstrap_work_fallback"])
	}
}

func TestRuntimeEnsureRunnableTaskDaemonDegradesWhenAgentWorkNextIsUnavailable(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "")
	t.Setenv(managedAgentAllowLocalShellFlag, "")
	t.Setenv(managedAgentAllowLocalMutationFlag, "")

	var methods []string
	var persistedScratch RuntimeScratchState
	currentContextDocWrites := 0
	claimedWorkDocWrites := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.work.next":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error": map[string]any{
					"code":    -32601,
					"message": "unknown method: agent.work.next",
				},
			})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &persistedScratch); err != nil {
				t.Fatalf("decode persisted scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			switch rpcString(req.Params, "doc_key") {
			case agentContextDocKey("agent-1"):
				currentContextDocWrites++
				content := rpcString(req.Params, "content")
				if !strings.Contains(content, "- outcome: idle") || !strings.Contains(content, "- task_id: (none)") {
					t.Fatalf("expected work-next degraded to clear current context doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-degraded-context-cleared"})
			case claimedWorkDocKey("agent-1"):
				claimedWorkDocWrites++
				if !strings.Contains(rpcString(req.Params, "content"), "active_claimed_work: none") {
					t.Fatalf("expected work-next degraded to clear claimed work doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-degraded-claimed-cleared"})
			default:
				t.Fatalf("work-next degraded should not materialize doc %q: %+v", rpcString(req.Params, "doc_key"), req.Params)
			}
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			t.Fatalf("daemon must not use bootstrap fallback after WorkNext failure; unexpected method %s", req.Method)
		default:
			t.Fatalf("unexpected method during managed daemon degraded ensure: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{
		TaskID:       "task-bootstrap",
		Title:        "Bootstrap must not win",
		Description:  "would have been selected by legacy fallback",
		OwnerUserID:  "owner-1",
		Priority:     "HIGH",
		Status:       "PENDING",
		TaskKind:     "general",
		TaskTemplate: "default",
		LinkedBy:     "system",
		LinkedAt:     "2026-03-23T00:00:00Z",
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:            RuntimeModeDaemon,
			Workdir:         t.TempDir(),
			RhizomeRPC:      server.URL,
			RhizomeToken:    "token",
			WorkspaceID:     "ws-1",
			AgentID:         "agent-1",
			DisplayName:     "Agent One",
			OwnerUserID:     "owner-1",
			ProtocolVersion: "rnar/v1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{Tasks: []WorkspaceTaskRecord{task}},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-stale",
			Title:       "Stale task",
			OwnerUserID: "owner-1",
			Priority:    "HIGH",
			Status:      "PENDING",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID:   "session-stale",
			WorkspaceID: "ws-1",
			AgentID:     "agent-1",
			TaskID:      "task-stale",
			Status:      "ENDED",
		},
		activeRunID: "run-stale",
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-stale",
			ActiveSessionID: "session-stale",
			ActiveRunID:     "run-stale",
			LastSummary:     "stale summary",
			DocSHAs:         map[string]string{},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	got, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if got != nil {
		t.Fatalf("expected degraded no-work result, got %+v", got)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeHydration != nil || runtime.activeRunID != "" {
		t.Fatalf("expected degraded state to clear active work, task=%+v session=%+v hydration=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeHydration, runtime.activeRunID)
	}
	if runtime.activeWorkPacket == nil || runtime.activeWorkPacket.WorkType != "work_next_degraded" {
		t.Fatalf("expected degraded work packet, got %+v", runtime.activeWorkPacket)
	}
	if runtime.activeWorkPacket.Gate == nil || runtime.activeWorkPacket.Gate.GateType != "scheduler_contract" || runtime.activeWorkPacket.Gate.NeededFrom != "agent.work.next" {
		t.Fatalf("expected scheduler-contract gate packet, got %+v", runtime.activeWorkPacket)
	}
	if runtime.scratch.ActiveTaskID != "" || runtime.scratch.ActiveSessionID != "" || runtime.scratch.ActiveRunID != "" {
		t.Fatalf("expected degraded scratch to clear active ids, got %+v", runtime.scratch)
	}
	if runtime.scratch.LastWakeReason != "work_next_degraded" || !strings.Contains(runtime.scratch.LastWakeSummary, "bootstrap compatibility fallback disabled") {
		t.Fatalf("expected degraded scratch diagnostics, got %+v", runtime.scratch)
	}
	if persistedScratch.LastWakeReason != "work_next_degraded" || persistedScratch.ActiveTaskID != "" || persistedScratch.ActiveSessionID != "" || persistedScratch.ActiveRunID != "" {
		t.Fatalf("expected persisted degraded scratch, got %+v", persistedScratch)
	}
	status := runtime.runtimeStatusPayload(time.Now().UTC())
	if status["work_type"] != "work_next_degraded" || status["work_gate_state"] != "closed" || status["work_gate_type"] != "scheduler_contract" {
		t.Fatalf("runtime status did not expose top-level work-next degraded gate: %+v", status)
	}
	fallback, ok := status["bootstrap_work_fallback"].(map[string]any)
	if !ok || fallback["posture"] != "disabled" || fallback["can_consume_work"] != false {
		t.Fatalf("daemon runtime status should show bootstrap fallback cannot consume work, got %+v", status["bootstrap_work_fallback"])
	}
	if currentContextDocWrites != 1 || claimedWorkDocWrites != 1 {
		t.Fatalf("expected work-next degraded to publish one current-context and one claimed-work cleanup doc, got current=%d claimed=%d", currentContextDocWrites, claimedWorkDocWrites)
	}

	expectedMethods := []string{"agent.work.next", "agent.state.set", "workspace.doc.put", "agent.state.set", "workspace.doc.put", "agent.state.set"}
	if !reflect.DeepEqual(methods, expectedMethods) {
		t.Fatalf("unexpected call order: %#v", methods)
	}
}

func TestRuntimeHandleInboundMessagePostsUpdateAndQueuesWakeWithoutActiveBinding(t *testing.T) {
	var updatePayload map[string]any
	var persistedScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.update.post":
			if raw := rpcString(req.Params, "payload_json"); raw != "" {
				if err := json.Unmarshal([]byte(raw), &updatePayload); err != nil {
					t.Fatalf("decode update payload: %v", err)
				}
			}
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &persistedScratch); err != nil {
				t.Fatalf("decode persisted scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during inbound message handling: %s", req.Method)
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
	t.Cleanup(func() { _ = runtime.Close() })

	message := MessageRecord{
		MessageID:    "msg-1",
		WorkspaceID:  "ws-1",
		FromAgentID:  "agent-b",
		ToAgentID:    "agent-1",
		Channel:      "default",
		Content:      "Need\nreview  now",
		MetadataJSON: "{\"task_id\":\"task-55\"}",
	}

	if err := runtime.handleInboundMessage(context.Background(), message); err != nil {
		t.Fatalf("handleInboundMessage() error = %v", err)
	}
	if runtime.scratch.ActiveTaskID != "" || runtime.scratch.ActiveSessionID != "" || runtime.scratch.ActiveRunID != "" {
		t.Fatalf("expected inbound message not to materialize active runtime binding, got %+v", runtime.scratch)
	}
	if persistedScratch.ActiveTaskID != "" || persistedScratch.ActiveSessionID != "" || persistedScratch.ActiveRunID != "" {
		t.Fatalf("expected persisted scratch not to include active binding, got %+v", persistedScratch)
	}
	if persistedScratch.PendingTrigger != "inbound_message" || persistedScratch.PendingTriggerTask != "task-55" || persistedScratch.LastWakeTaskID != "task-55" {
		t.Fatalf("expected inbound message to queue wake evidence instead of active binding, got %+v", persistedScratch)
	}
	if got := rpcStringMap(updatePayload, "status"); got != "INBOUND_MESSAGE" {
		t.Fatalf("unexpected status in update payload: %q", got)
	}
	if got := rpcStringMap(updatePayload, "notes"); got != "Need review now" {
		t.Fatalf("unexpected notes in update payload: %q", got)
	}
	if got := rpcStringMap(updatePayload, "next_action"); got != "replan after inbound message" {
		t.Fatalf("unexpected next_action in update payload: %q", got)
	}
	taskIDs, _ := updatePayload["task_ids"].([]any)
	if len(taskIDs) != 1 || taskIDs[0] != "task-55" {
		t.Fatalf("unexpected task_ids in update payload: %#v", updatePayload["task_ids"])
	}
}

func TestRuntimeHandleRequestUsesCurrentSessionAndRejectsUnsupportedMethods(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var responseJSON string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "agent.respond" {
			t.Fatalf("unexpected method during request handling: %s", req.Method)
		}
		responseJSON = rpcString(req.Params, "response")
		writeRPCResult(w, req, nil)
	}))
	defer server.Close()

	self := "agent-1"
	task := WorkspaceTaskRecord{TaskID: "task-1", Title: "Task One", Priority: "HIGH", Status: "RUNNING"}
	session := AgentSessionStateRecord{SessionID: "session-1", AgentID: self, TaskID: task.TaskID, Status: "ACTIVE", Summary: "working"}
	inbox, err := OpenMessageInbox("ws-1", self)
	if err != nil {
		t.Fatalf("OpenMessageInbox() error = %v", err)
	}
	startedAt := time.Date(2026, 3, 23, 10, 0, 0, 0, time.UTC)
	if err := inbox.MarkRuntimeStarted(startedAt); err != nil {
		t.Fatalf("MarkRuntimeStarted() error = %v", err)
	}
	if err := inbox.RecordBatch([]MessageRecord{{
		MessageID:   "msg-1",
		WorkspaceID: "ws-1",
		FromAgentID: "agent-b",
		ToAgentID:   self,
		Channel:     "default",
		Content:     "pending",
		CreatedAt:   "2026-03-23T09:59:59Z",
	}}, startedAt.Add(2*time.Second), "2026-03-23T09:59:59Z|msg-1"); err != nil {
		t.Fatalf("RecordBatch() error = %v", err)
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      self,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		inbox:         inbox,
		activeTask:    &task,
		activeSession: &session,
		scratch:       RuntimeScratchState{LastSummary: "steady", DocSHAs: map[string]string{}},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{RequestID: "req-1", Method: "runtime.status"}); err != nil {
		t.Fatalf("handleRequest(runtime.status) error = %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode runtime.status response: %v", err)
	}
	if parsed["status"] != "ok" {
		t.Fatalf("unexpected status: %#v", parsed)
	}
	if parsed["agent_id"] != self || parsed["session_id"] != "session-1" || parsed["task_id"] != "task-1" {
		t.Fatalf("unexpected response fields: %#v", parsed)
	}
	if parsed["summary"] != "steady" {
		t.Fatalf("unexpected summary: %#v", parsed)
	}
	if parsed["paused"] != false {
		t.Fatalf("expected runtime.status to include paused=false, got %#v", parsed["paused"])
	}
	if parsed["attachable"] != true {
		t.Fatalf("expected runtime.status to include attachable=true, got %#v", parsed["attachable"])
	}
	control, ok := parsed["control"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtime.status control block, got %#v", parsed["control"])
	}
	if control["paused"] != false || control["mode"] == "" {
		t.Fatalf("unexpected control snapshot: %#v", control)
	}
	if activeTask, ok := parsed["active_task"].(map[string]any); !ok || activeTask["task_id"] != "task-1" {
		t.Fatalf("expected active_task block, got %#v", parsed["active_task"])
	}
	if activeSession, ok := parsed["active_session"].(map[string]any); !ok || activeSession["session_id"] != "session-1" {
		t.Fatalf("expected active_session block, got %#v", parsed["active_session"])
	}
	inboxStatus, ok := parsed["message_inbox"].(map[string]any)
	if !ok {
		t.Fatalf("expected message_inbox block, got %#v", parsed["message_inbox"])
	}
	if inboxStatus["pending"] != float64(1) || inboxStatus["unread"] != float64(1) || inboxStatus["missed_since_start"] != float64(1) {
		t.Fatalf("unexpected message_inbox counters: %#v", inboxStatus)
	}
	if inboxStatus["last_synced_cursor"] != "2026-03-23T09:59:59Z|msg-1" {
		t.Fatalf("unexpected message_inbox cursor: %#v", inboxStatus)
	}

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{RequestID: "req-2", Method: "not-implemented"}); err != nil {
		t.Fatalf("handleRequest(not-implemented) error = %v", err)
	}
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode unsupported response: %v", err)
	}
	if parsed["status"] != "unsupported" {
		t.Fatalf("unexpected unsupported status: %#v", parsed)
	}
	if parsed["details"] == "" {
		t.Fatalf("expected unsupported response details, got %#v", parsed)
	}

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{RequestID: "req-3", Method: "not-implemented", Payload: `{"prompt":"hello"}`}); err != nil {
		t.Fatalf("handleRequest(not-implemented with payload) error = %v", err)
	}
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode unsupported payload response: %v", err)
	}
	if parsed["status"] != "unsupported" {
		t.Fatalf("expected unsupported status for arbitrary payload request, got %#v", parsed)
	}
	if !strings.Contains(strings.ToLower(rpcStringMap(parsed, "details")), "model.ask") {
		t.Fatalf("expected unsupported payload response to mention model.ask, got %#v", parsed)
	}
}

func TestRuntimeHeartbeatSnapshotReflectsSessionState(t *testing.T) {
	r := &Runtime{
		scratch: RuntimeScratchState{LastSummary: "steady"},
	}

	status, summary := r.heartbeatSnapshot()
	if status != "ACTIVE" || summary != "steady" {
		t.Fatalf("unexpected idle heartbeat snapshot: status=%q summary=%q", status, summary)
	}

	r.busy = true
	status, _ = r.heartbeatSnapshot()
	if status != "ACTIVE" {
		t.Fatalf("expected busy runtime to report ACTIVE, got %q", status)
	}

	r.busy = false
	r.activeSession = &AgentSessionStateRecord{Status: "BLOCKED"}
	status, _ = r.heartbeatSnapshot()
	if status != "BLOCKED" {
		t.Fatalf("expected blocked session to report BLOCKED, got %q", status)
	}

	r.activeSession = &AgentSessionStateRecord{Status: "ENDED"}
	status, _ = r.heartbeatSnapshot()
	if status != "ACTIVE" {
		t.Fatalf("expected ended session to still report ACTIVE until cleanup, got %q", status)
	}
}

func TestRuntimeHandleRequestMemoryStatusAndPacket(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var responseJSON string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "agent.respond" {
			t.Fatalf("unexpected method during request handling: %s", req.Method)
		}
		responseJSON = rpcString(req.Params, "response")
		writeRPCResult(w, req, nil)
	}))
	defer server.Close()

	service := openTestAgentMemoryService(t, "ws-memory", "agent-memory")
	for idx := 0; idx < 2; idx++ {
		if err := service.appendEvent(LocalMemoryEvent{
			NodeType:       localMemoryNodeArtifactDelta,
			EventKind:      "task_cycle_result",
			Summary:        "Stabilize deliverable before sync",
			Details:        "Stabilize deliverable before sync",
			TaskID:         "task-memory",
			SessionID:      "session-memory",
			TensionID:      "tension-memory",
			ProtoClusterID: "cluster-memory",
			DocKeys:        []string{"task.task-memory"},
			Outcome:        "completed",
			MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
				Outcome:    "completed",
				NextAction: "Stabilize deliverable before sync",
				Materialize: TaskMaterialization{
					DocKey: "task.task-memory",
				},
			}),
		}); err != nil {
			t.Fatalf("appendEvent(%d) error = %v", idx, err)
		}
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:            t.TempDir(),
			RhizomeRPC:         server.URL,
			RhizomeToken:       "token",
			WorkspaceID:        "ws-memory",
			AgentID:            "agent-memory",
			MaxPromptSpecChars: 9000,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		memory:        service,
		activeTask:    &WorkspaceTaskRecord{TaskID: "task-memory", Title: "Task Memory", Priority: "HIGH", Status: "RUNNING"},
		activeSession: &AgentSessionStateRecord{SessionID: "session-memory", TaskID: "task-memory", Status: "ACTIVE"},
		focus:         &RuntimeFocusState{TaskID: "task-memory", ProtoClusterID: "cluster-memory", FocusTensionID: "tension-memory"},
		scratch:       RuntimeScratchState{LastSummary: "steady", DocSHAs: map[string]string{"task.task-memory": "sha-1"}},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{RequestID: "req-memory-1", Method: "runtime.memory.status"}); err != nil {
		t.Fatalf("handleRequest(runtime.memory.status) error = %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode runtime.memory.status response: %v", err)
	}
	if parsed["status"] != "ok" {
		t.Fatalf("unexpected status: %#v", parsed)
	}
	control, ok := parsed["memory_control"].(map[string]any)
	if !ok {
		t.Fatalf("expected memory_control block, got %#v", parsed["memory_control"])
	}
	if control["packet_cache_entries"] == nil {
		t.Fatalf("expected memory control packet cache data, got %#v", control)
	}
	if _, ok := parsed["memory_body"]; ok {
		t.Fatalf("did not expect runtime.memory.status to include packet preview, got %#v", parsed["memory_body"])
	}
	if stats, ok := control["stats"].(map[string]any); !ok || stats["packet_builds"] != float64(0) {
		t.Fatalf("expected memory.status to stay read-only for packet builds, got %#v", control["stats"])
	}

	requestPayload, _ := json.Marshal(runtimeMemoryPacketRequest{MaxChars: 240})
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID: "req-memory-2",
		Method:    "runtime.memory.packet",
		Payload:   string(requestPayload),
	}); err != nil {
		t.Fatalf("handleRequest(runtime.memory.packet) error = %v", err)
	}
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode runtime.memory.packet response: %v", err)
	}
	if parsed["summary"] != "memory packet preview" {
		t.Fatalf("unexpected packet preview summary: %#v", parsed)
	}
	if body, _ := parsed["memory_body"].(string); body == "" || !strings.Contains(body, "## Agent Memory Body") {
		t.Fatalf("expected packet preview body, got %#v", parsed["memory_body"])
	}
	if control, ok := parsed["memory_control"].(map[string]any); !ok {
		t.Fatalf("expected memory_control in packet preview, got %#v", parsed["memory_control"])
	} else if stats, ok := control["stats"].(map[string]any); !ok || stats["packet_builds"] == float64(0) {
		t.Fatalf("expected packet preview to increment packet builds, got %#v", control["stats"])
	}
}

func TestRuntimeHandleRequestMemoryInvalidate(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var responseJSON string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "agent.respond" {
			t.Fatalf("unexpected method during request handling: %s", req.Method)
		}
		responseJSON = rpcString(req.Params, "response")
		writeRPCResult(w, req, nil)
	}))
	defer server.Close()

	service := openTestAgentMemoryService(t, "ws-memory-2", "agent-memory-2")
	if err := service.rememberDocVersions(map[string]string{"task.task-memory-2": "sha-1"}); err != nil {
		t.Fatalf("rememberDocVersions() error = %v", err)
	}
	for idx := 0; idx < 2; idx++ {
		if err := service.appendEvent(LocalMemoryEvent{
			NodeType:       localMemoryNodeArtifactDelta,
			EventKind:      "task_cycle_result",
			Summary:        "Prepare proof before publish",
			Details:        "Prepare proof before publish",
			TaskID:         "task-memory-2",
			SessionID:      "session-memory-2",
			TensionID:      "tension-memory-2",
			ProtoClusterID: "cluster-memory-2",
			DocKeys:        []string{"task.task-memory-2"},
			Outcome:        "completed",
			MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
				Outcome:    "completed",
				NextAction: "Prepare proof before publish",
				Materialize: TaskMaterialization{
					DocKey: "task.task-memory-2",
				},
			}),
		}); err != nil {
			t.Fatalf("appendEvent(%d) error = %v", idx, err)
		}
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:            t.TempDir(),
			RhizomeRPC:         server.URL,
			RhizomeToken:       "token",
			WorkspaceID:        "ws-memory-2",
			AgentID:            "agent-memory-2",
			MaxPromptSpecChars: 9000,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		memory:        service,
		activeTask:    &WorkspaceTaskRecord{TaskID: "task-memory-2", Title: "Task Memory Two", Priority: "HIGH", Status: "RUNNING"},
		activeSession: &AgentSessionStateRecord{SessionID: "session-memory-2", TaskID: "task-memory-2", Status: "ACTIVE"},
		focus:         &RuntimeFocusState{TaskID: "task-memory-2", ProtoClusterID: "cluster-memory-2", FocusTensionID: "tension-memory-2"},
		scratch:       RuntimeScratchState{LastSummary: "steady", DocSHAs: map[string]string{"task.task-memory-2": "sha-2"}},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	payload, _ := json.Marshal(buildCanonicalVersionInvalidation(
		map[string]string{"task.task-memory-2": "sha-2"},
		map[string]string{},
		map[string]string{"task.task-memory-2": "sha-1"},
		map[string]string{},
	))
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID: "req-memory-invalidate",
		Method:    "runtime.memory.invalidate",
		Payload:   string(payload),
	}); err != nil {
		t.Fatalf("handleRequest(runtime.memory.invalidate) error = %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode invalidate response: %v", err)
	}
	if parsed["status"] != "ok" || parsed["summary"] != "memory invalidated and rebuilt" {
		t.Fatalf("unexpected invalidate response: %#v", parsed)
	}
	if body, _ := parsed["memory_body"].(string); strings.Contains(body, "Repeatedly Works Here:") {
		t.Fatalf("expected invalidation to drop stale procedural hint, got %#v", parsed["memory_body"])
	}
	control, ok := parsed["memory_control"].(map[string]any)
	if !ok {
		t.Fatalf("expected memory_control after invalidation, got %#v", parsed["memory_control"])
	}
	stats, ok := control["stats"].(map[string]any)
	if !ok {
		t.Fatalf("expected memory_control.stats block, got %#v", control["stats"])
	}
	if stats["procedures"] != float64(0) {
		t.Fatalf("expected zero procedures after invalidate, got %#v", stats)
	}
}

func TestRuntimeHeartbeatSnapshotAppendsMemorySummary(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-memory-3", "agent-memory-3")
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeProcedure,
		EventKind:      "task_cycle_result",
		Summary:        "Publish verified delta",
		Details:        "Publish verified delta",
		TaskID:         "task-memory-3",
		SessionID:      "session-memory-3",
		TensionID:      "tension-memory-3",
		ProtoClusterID: "cluster-memory-3",
		Outcome:        "completed",
		MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
			Outcome:    "completed",
			MemoryType: "PROCEDURE",
			NextAction: "Publish verified delta",
		}),
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}

	r := &Runtime{
		scratch: RuntimeScratchState{LastSummary: "steady"},
		memory:  service,
	}

	status, summary := r.heartbeatSnapshot()
	if status != "ACTIVE" {
		t.Fatalf("expected active heartbeat status, got %q", status)
	}
	if !strings.Contains(summary, "memory proc=1") {
		t.Fatalf("expected heartbeat summary to include memory suffix, got %q", summary)
	}
}

func rpcStringMap(params map[string]any, key string) string {
	value, _ := params[key].(string)
	return value
}

func TestDaemonCapabilityPosturePackAdvertisesDisabledAuthoritySurfaces(t *testing.T) {
	defaultPack := daemonCapabilityPosturePack(RuntimeConfig{Mode: RuntimeModeDaemon})
	for _, want := range []string{
		"smoke.cycles.per_agent: 3",
		"smoke.cycles.per_task: 6",
		"tool.retry.ceiling: 20",
		"provider.retry.ceiling: 2",
	} {
		if !strings.Contains(defaultPack, want) {
			t.Fatalf("expected default posture pack to contain %q, got:\n%s", want, defaultPack)
		}
	}

	pack := daemonCapabilityPosturePack(RuntimeConfig{
		Mode:                     RuntimeModeDaemon,
		MaxSmokeCyclesPerAgent:   4,
		MaxSmokeCyclesPerTask:    7,
		MaxToolLoopIterations:    11,
		MaxProviderRetryAttempts: 5,
	})
	if pack == "" {
		t.Fatal("expected daemon capability posture pack")
	}
	for _, want := range []string{
		"smoke.cycles.per_agent: 4",
		"smoke.cycles.per_task: 7",
		"smoke.budget: no full budget ledger",
		"tool.retry.ceiling: 11",
		"provider.retry.ceiling: 5",
		"memory.local: disabled",
		"memory.workspace.write: disabled",
		"memory.promotion: disabled",
		"mcp: disabled unless hardened",
		"bridge: inspection_only for Program A unless represented by a hardened capability snapshot",
		"executor: disabled unless wrapped by an operation ledger",
		"tui: inspection_only",
		"web: inspection_only",
	} {
		if !strings.Contains(pack, want) {
			t.Fatalf("expected posture pack to contain %q, got:\n%s", want, pack)
		}
	}
	if got := daemonCapabilityPosturePack(RuntimeConfig{Mode: RuntimeModeTUI}); got != "" {
		t.Fatalf("expected non-daemon posture pack to be empty, got %q", got)
	}
}

func TestDaemonRuntimeToolPlaneKeepsMCPRefreshDisabled(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		WorkspaceID:  "ws-daemon-tools",
		AgentID:      "agent-daemon-tools",
		DisplayName:  "Daemon Tools Agent",
		OwnerUserID:  "owner-daemon-tools",
		RhizomeToken: "token",
	}, nil)
	if runtime == nil || runtime.agent == nil || runtime.agent.registry == nil {
		t.Fatal("expected runtime agent registry")
	}
	if _, ok := runtime.agent.registry.Get("memory_reinforce"); ok {
		t.Fatal("expected daemon registry to omit memory_reinforce")
	}

	hitServer := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitServer = true
		t.Fatalf("daemon refresh should not call remote tool plane, got %s", r.Method)
	}))
	defer server.Close()

	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.cfg.RhizomeRPC = server.URL
	runtime.bindAgentRuntimeState()

	if err := runtime.refreshToolPlane(context.Background()); err != nil {
		t.Fatalf("refreshToolPlane() error = %v", err)
	}
	if hitServer {
		t.Fatal("expected daemon refreshToolPlane to stay inspection-only")
	}
}

func TestDaemonRuntimeToolPlaneLoadsWorkspaceToolsWhenExplicitlyEnabled(t *testing.T) {
	var hitServer bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		hitServer = true
		if req.Method != "tool.list" {
			t.Fatalf("unexpected rpc method: %s", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"tools": []map[string]any{
				{
					"tool_id":       "fake_ledger_tool",
					"display_name":  "Fake Ledger Tool",
					"description":   "Deterministic deployment ledger completion tool",
					"status":        "ACTIVE",
					"manifest_json": `{"input_schema":{"type":"object","properties":{"action":{"type":"string"},"scenario":{"type":"string"}}}}`,
				},
			},
			"count": 1,
		})
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                 RuntimeModeDaemon,
		Workdir:              t.TempDir(),
		WorkspaceID:          "ws-daemon-tools",
		AgentID:              "agent-daemon-tools",
		DisplayName:          "Daemon Tools Agent",
		OwnerUserID:          "owner-daemon-tools",
		RhizomeToken:         "token",
		RhizomeRPC:           server.URL,
		DaemonWorkspaceTools: true,
	}, nil)
	runtime.client = NewRhizomeClient(server.URL, "token")

	if err := runtime.refreshToolPlane(context.Background()); err != nil {
		t.Fatalf("refreshToolPlane() error = %v", err)
	}
	if !hitServer {
		t.Fatal("expected daemon workspace tool opt-in to call remote tool plane")
	}
	if _, ok := runtime.agent.registry.Get("fake_ledger_tool"); !ok {
		t.Fatal("expected fake_ledger_tool to be registered as a dynamic workspace tool")
	}
}

func TestDaemonMaterializeCycleSkipsCanonicalMemoryWrites(t *testing.T) {
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.ops.get":
			writeRPCError(w, req, -32602, "operator queue item not found")
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": req.Method + "-sha"})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, nil)
		case "workspace.ops.resolve":
			writeRPCResult(w, req, nil)
		case "workspace.memory.write":
			t.Fatalf("daemon materialization should not write canonical memory: %+v", req.Params)
		case "workspace.claim.write":
			t.Fatalf("daemon materialization should not write canonical claims: %+v", req.Params)
		default:
			t.Fatalf("unexpected rpc method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:                RuntimeModeDaemon,
			Workdir:             t.TempDir(),
			WorkspaceID:         "ws-daemon-memory",
			AgentID:             "agent-daemon-memory",
			OwnerUserID:         "owner-daemon-memory",
			MaxResultMemoryBody: 2048,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}

	task := WorkspaceTaskRecord{
		TaskID: "task-daemon-memory",
		Title:  "Daemon Memory Task",
	}
	session := AgentSessionStateRecord{
		SessionID: "session-daemon-memory",
		TaskID:    task.TaskID,
		Status:    "ACTIVE",
	}
	result := StructuredTaskResult{
		Outcome:     "completed",
		Summary:     "daemon observation only",
		MemoryType:  "NOTE",
		MemoryTitle: "Daemon Memory Note",
		MemoryBody:  "This must remain observation-only.",
	}

	if err := runtime.materializeCycle(context.Background(), task, session, "run-daemon-memory", result); err != nil {
		t.Fatalf("materializeCycle() error = %v", err)
	}

	for _, forbidden := range []string{"workspace.memory.write", "workspace.claim.write"} {
		for _, method := range methods {
			if method == forbidden {
				t.Fatalf("expected %s to stay disabled in daemon mode, got methods=%v", forbidden, methods)
			}
		}
	}
	if got := strings.Count(strings.Join(methods, ","), "workspace.doc.put"); got == 0 {
		t.Fatalf("expected daemon materialization to keep coordination doc writes, got methods=%v", methods)
	}
	if got := strings.Count(strings.Join(methods, ","), "agent.state.set"); got == 0 {
		t.Fatalf("expected daemon materialization to keep scratch/state writes, got methods=%v", methods)
	}
}

func TestRuntimeStartupIdentityLeaseQuarantinesDuplicateLiveConsumer(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	cfg := RuntimeConfig{
		Workdir:     t.TempDir(),
		RhizomeRPC:  "https://rhizome.test/rpc",
		WorkspaceID: "ws-duplicate",
		AgentID:     "agent-duplicate",
		DisplayName: "Agent Duplicate",
		OwnerUserID: "owner-duplicate",
		Mode:        RuntimeModeDaemon,
	}
	cfg.ApplyDefaults()

	origMatches := runtimeIdentityLeaseProcessMatchesFunc
	runtimeIdentityLeaseProcessMatchesFunc = func(runtimeIdentityLeaseInfo) (bool, error) {
		return true, nil
	}
	t.Cleanup(func() {
		runtimeIdentityLeaseProcessMatchesFunc = origMatches
	})

	first := NewRuntime(cfg, nil)
	t.Cleanup(func() { _ = first.Close() })
	if err := first.acquireStartupIdentityLease(); err != nil {
		t.Fatalf("first acquireStartupIdentityLease() error = %v", err)
	}

	leasePath := runtimeIdentityLeasePath(cfg.WorkspaceID, cfg.AgentID)
	if !pathExists(leasePath) {
		t.Fatalf("expected runtime identity lease at %s", leasePath)
	}

	second := NewRuntime(cfg, nil)
	err := second.acquireStartupIdentityLease()
	if err == nil || !strings.Contains(err.Error(), "already has an active consumer") {
		t.Fatalf("expected duplicate lease acquisition to fail closed, got %v", err)
	}

	quarantineRoot := filepath.Join(runtimeIdentityQuarantineRoot(), sanitizePathComponent(cfg.WorkspaceID), sanitizePathComponent(cfg.AgentID))
	matches, globErr := filepath.Glob(filepath.Join(quarantineRoot, "*.json"))
	if globErr != nil {
		t.Fatalf("Glob() error: %v", globErr)
	}
	if len(matches) == 0 {
		t.Fatalf("expected duplicate acquisition to emit quarantine evidence under %s", quarantineRoot)
	}
}

func TestRuntimeStartupIdentityLeaseReclaimsLivePIDWithMismatchedCommandLine(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	cfg := RuntimeConfig{
		Workdir:     t.TempDir(),
		RhizomeRPC:  "https://rhizome.test/rpc",
		WorkspaceID: "ws-live-reused-pid",
		AgentID:     "agent-live-reused-pid",
		DisplayName: "Agent Live Reused PID",
		OwnerUserID: "owner-live-reused-pid",
		Mode:        RuntimeModeDaemon,
	}
	cfg.ApplyDefaults()

	origMatches := runtimeIdentityLeaseProcessMatchesFunc
	runtimeIdentityLeaseProcessMatchesFunc = func(runtimeIdentityLeaseInfo) (bool, error) {
		return false, nil
	}
	t.Cleanup(func() {
		runtimeIdentityLeaseProcessMatchesFunc = origMatches
	})

	leasePath := runtimeIdentityLeasePath(cfg.WorkspaceID, cfg.AgentID)
	if err := os.MkdirAll(filepath.Dir(leasePath), 0o700); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	now := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	reusedPIDLease := runtimeIdentityLeaseInfo{
		PID:         os.Getpid(),
		StartedAt:   now,
		LastSeenAt:  now,
		Mode:        string(RuntimeModeDaemon),
		Workdir:     cfg.Workdir,
		WorkspaceID: cfg.WorkspaceID,
		AgentID:     cfg.AgentID,
		DisplayName: cfg.DisplayName,
	}
	raw, err := json.MarshalIndent(reusedPIDLease, "", "  ")
	if err != nil {
		t.Fatalf("marshal reused-pid lease: %v", err)
	}
	if err := os.WriteFile(leasePath, raw, 0o600); err != nil {
		t.Fatalf("WriteFile(reused-pid lease) error: %v", err)
	}

	runtime := NewRuntime(cfg, nil)
	t.Cleanup(func() { _ = runtime.Close() })
	if err := runtime.acquireStartupIdentityLease(); err != nil {
		t.Fatalf("expected live PID with mismatched command line to be reclaimed, got %v", err)
	}

	current, err := loadRuntimeIdentityLease(leasePath)
	if err != nil {
		t.Fatalf("load fresh lease: %v", err)
	}
	if current.PID != os.Getpid() || current.LastSeenAt == now {
		t.Fatalf("expected fresh lease after reclaim, got %+v", current)
	}
	matches, globErr := filepath.Glob(leasePath + ".stale-*")
	if globErr != nil {
		t.Fatalf("Glob() error: %v", globErr)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived mismatched live-PID lease, got %v", matches)
	}
}

func TestRuntimeStartupIdentityLeaseReclaimsStaleLease(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	cfg := RuntimeConfig{
		Workdir:     t.TempDir(),
		RhizomeRPC:  "https://rhizome.test/rpc",
		WorkspaceID: "ws-stale",
		AgentID:     "agent-stale",
		DisplayName: "Agent Stale",
		OwnerUserID: "owner-stale",
		Mode:        RuntimeModeDaemon,
	}
	cfg.ApplyDefaults()

	leasePath := runtimeIdentityLeasePath(cfg.WorkspaceID, cfg.AgentID)
	if err := os.MkdirAll(filepath.Dir(leasePath), 0o700); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	staleLease := runtimeIdentityLeaseInfo{
		PID:         2147483647,
		StartedAt:   "2026-04-12T00:00:00Z",
		LastSeenAt:  "2026-04-12T00:00:00Z",
		Mode:        string(RuntimeModeDaemon),
		Workdir:     cfg.Workdir,
		WorkspaceID: cfg.WorkspaceID,
		AgentID:     cfg.AgentID,
		DisplayName: cfg.DisplayName,
	}
	raw, err := json.MarshalIndent(staleLease, "", "  ")
	if err != nil {
		t.Fatalf("marshal stale lease: %v", err)
	}
	if err := os.WriteFile(leasePath, raw, 0o600); err != nil {
		t.Fatalf("WriteFile(stale lease) error: %v", err)
	}

	runtime := NewRuntime(cfg, nil)
	t.Cleanup(func() { _ = runtime.Close() })
	if err := runtime.acquireStartupIdentityLease(); err != nil {
		t.Fatalf("acquireStartupIdentityLease() error = %v", err)
	}

	if !pathExists(leasePath) {
		t.Fatalf("expected stale lease to be replaced at %s", leasePath)
	}
	data, err := os.ReadFile(leasePath)
	if err != nil {
		t.Fatalf("ReadFile(leasePath) error: %v", err)
	}
	if !strings.Contains(string(data), "\"agent_id\": \"agent-stale\"") {
		t.Fatalf("expected fresh lease contents, got %s", string(data))
	}
	matches, globErr := filepath.Glob(leasePath + ".stale-*")
	if globErr != nil {
		t.Fatalf("Glob() error: %v", globErr)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived stale lease, got %v", matches)
	}
}

func TestRuntimeStartupIdentityLeaseReclaimsExpiredLivePIDLease(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	cfg := RuntimeConfig{
		Workdir:     t.TempDir(),
		RhizomeRPC:  "https://rhizome.test/rpc",
		WorkspaceID: "ws-reused-pid",
		AgentID:     "agent-reused-pid",
		DisplayName: "Agent Reused PID",
		OwnerUserID: "owner-reused-pid",
		Mode:        RuntimeModeDaemon,
	}
	cfg.ApplyDefaults()

	leasePath := runtimeIdentityLeasePath(cfg.WorkspaceID, cfg.AgentID)
	if err := os.MkdirAll(filepath.Dir(leasePath), 0o700); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	old := time.Now().UTC().Add(-runtimeIdentityLeaseStaleAfter - time.Minute).Format(time.RFC3339Nano)
	staleLease := runtimeIdentityLeaseInfo{
		PID:         os.Getpid(),
		StartedAt:   old,
		LastSeenAt:  old,
		Mode:        string(RuntimeModeDaemon),
		Workdir:     cfg.Workdir,
		WorkspaceID: cfg.WorkspaceID,
		AgentID:     cfg.AgentID,
		DisplayName: cfg.DisplayName,
	}
	raw, err := json.MarshalIndent(staleLease, "", "  ")
	if err != nil {
		t.Fatalf("marshal stale lease: %v", err)
	}
	if err := os.WriteFile(leasePath, raw, 0o600); err != nil {
		t.Fatalf("WriteFile(stale lease) error: %v", err)
	}

	runtime := NewRuntime(cfg, nil)
	t.Cleanup(func() { _ = runtime.Close() })
	if err := runtime.acquireStartupIdentityLease(); err != nil {
		t.Fatalf("expected expired live-PID lease to be reclaimed, got %v", err)
	}

	current, err := loadRuntimeIdentityLease(leasePath)
	if err != nil {
		t.Fatalf("load fresh lease: %v", err)
	}
	if current.PID != os.Getpid() || current.LastSeenAt == old {
		t.Fatalf("expected fresh lease after reclaim, got %+v", current)
	}
	matches, globErr := filepath.Glob(leasePath + ".stale-*")
	if globErr != nil {
		t.Fatalf("Glob() error: %v", globErr)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived stale live-PID lease, got %v", matches)
	}
}

func TestRuntimeStartupIdentityLeaseRefreshUpdatesLastSeen(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	cfg := RuntimeConfig{
		Workdir:     t.TempDir(),
		RhizomeRPC:  "https://rhizome.test/rpc",
		WorkspaceID: "ws-refresh-lease",
		AgentID:     "agent-refresh-lease",
		DisplayName: "Agent Refresh Lease",
		OwnerUserID: "owner-refresh-lease",
		Mode:        RuntimeModeDaemon,
	}
	cfg.ApplyDefaults()

	runtime := NewRuntime(cfg, nil)
	t.Cleanup(func() { _ = runtime.Close() })
	if err := runtime.acquireStartupIdentityLease(); err != nil {
		t.Fatalf("acquireStartupIdentityLease() error = %v", err)
	}
	leasePath := runtimeIdentityLeasePath(cfg.WorkspaceID, cfg.AgentID)
	before, err := loadRuntimeIdentityLease(leasePath)
	if err != nil {
		t.Fatalf("load lease before refresh: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := runtime.refreshStartupIdentityLease(); err != nil {
		t.Fatalf("refreshStartupIdentityLease() error = %v", err)
	}
	after, err := loadRuntimeIdentityLease(leasePath)
	if err != nil {
		t.Fatalf("load lease after refresh: %v", err)
	}
	if after.LastSeenAt == "" || after.LastSeenAt == before.LastSeenAt {
		t.Fatalf("expected last_seen_at to advance, before=%+v after=%+v", before, after)
	}
}

func TestRuntimeStartupIdentityLeaseReleaseDoesNotDeleteReplacementLease(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	cfg := RuntimeConfig{
		Workdir:     t.TempDir(),
		RhizomeRPC:  "https://rhizome.test/rpc",
		WorkspaceID: "ws-release-lease",
		AgentID:     "agent-release-lease",
		DisplayName: "Agent Release Lease",
		OwnerUserID: "owner-release-lease",
		Mode:        RuntimeModeDaemon,
	}
	cfg.ApplyDefaults()

	runtime := NewRuntime(cfg, nil)
	if err := runtime.acquireStartupIdentityLease(); err != nil {
		t.Fatalf("acquireStartupIdentityLease() error = %v", err)
	}
	leasePath := runtimeIdentityLeasePath(cfg.WorkspaceID, cfg.AgentID)
	replacement := runtimeIdentityLeaseInfo{
		PID:         os.Getpid() + 100000,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		LastSeenAt:  time.Now().UTC().Format(time.RFC3339Nano),
		Mode:        string(RuntimeModeDaemon),
		Workdir:     cfg.Workdir,
		WorkspaceID: cfg.WorkspaceID,
		AgentID:     cfg.AgentID,
	}
	raw, err := json.MarshalIndent(replacement, "", "  ")
	if err != nil {
		t.Fatalf("marshal replacement lease: %v", err)
	}
	if err := atomicWriteFile(leasePath, raw, 0o600); err != nil {
		t.Fatalf("write replacement lease: %v", err)
	}

	runtime.releaseStartupIdentityLease()

	current, err := loadRuntimeIdentityLease(leasePath)
	if err != nil {
		t.Fatalf("expected replacement lease to survive release: %v", err)
	}
	if current.PID != replacement.PID {
		t.Fatalf("expected replacement lease owner to survive, got %+v", current)
	}
}

func TestRuntimeHeartbeatLoopFailsClosedOnLostIdentityLease(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	cfg := RuntimeConfig{
		Workdir:        t.TempDir(),
		RhizomeRPC:     "https://rhizome.test/rpc",
		WorkspaceID:    "ws-lost-lease",
		AgentID:        "agent-lost-lease",
		DisplayName:    "Agent Lost Lease",
		OwnerUserID:    "owner-lost-lease",
		Mode:           RuntimeModeDaemon,
		HeartbeatEvery: time.Millisecond,
	}
	cfg.ApplyDefaults()

	runtime := NewRuntime(cfg, nil)
	t.Cleanup(func() { _ = runtime.Close() })
	if err := runtime.acquireStartupIdentityLease(); err != nil {
		t.Fatalf("acquireStartupIdentityLease() error = %v", err)
	}
	leasePath := runtimeIdentityLeasePath(cfg.WorkspaceID, cfg.AgentID)
	current, err := loadRuntimeIdentityLease(leasePath)
	if err != nil {
		t.Fatalf("load lease: %v", err)
	}
	current.AgentID = "different-agent"
	raw, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		t.Fatalf("marshal mutated lease: %v", err)
	}
	if err := atomicWriteFile(leasePath, raw, 0o600); err != nil {
		t.Fatalf("overwrite lease: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err = runtime.runHeartbeatLoop(ctx)
	if err == nil || !strings.Contains(err.Error(), "runtime identity lease lost") {
		t.Fatalf("expected heartbeat loop to fail closed on lost lease, got %v", err)
	}
}

func TestHeartbeatInactiveSessionKeepaliveClearsActiveState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	savedStates := []RuntimeScratchState{}
	currentContextDocWrites := 0
	claimedWorkDocWrites := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedStates = append(savedStates, state)
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			docKey := rpcString(req.Params, "doc_key")
			content := rpcString(req.Params, "content")
			switch docKey {
			case agentContextDocKey("agent-ended"):
				currentContextDocWrites++
				for _, want := range []string{"- session_id: (none)", "- task_id: (none)", "- outcome: idle", "Session keepalive stopped for inactive session session-ended"} {
					if !strings.Contains(content, want) {
						t.Fatalf("current context doc missing %q: %s", want, content)
					}
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-current"})
			case claimedWorkDocKey("agent-ended"):
				claimedWorkDocWrites++
				if !strings.Contains(content, "active_claimed_work: none") {
					t.Fatalf("claimed work doc was not cleared: %s", content)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-claimed"})
			default:
				t.Fatalf("unexpected doc key: %s", docKey)
			}
		default:
			t.Fatalf("unexpected method while clearing inactive session: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{
		TaskID: "task-ended",
		Title:  "Ended Task",
		Status: "RUNNING",
	}
	session := AgentSessionStateRecord{
		SessionID:   "session-ended",
		WorkspaceID: "ws-ended",
		AgentID:     "agent-ended",
		TaskID:      "task-ended",
		Status:      "ACTIVE",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-ended",
			AgentID:      "agent-ended",
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-ended",
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-ended",
			ActiveSessionID: "session-ended",
			ActiveRunID:     "run-ended",
			DocSHAs:         map[string]string{},
		},
	}

	ok := runtime.handleInactiveSessionKeepalive(context.Background(), &session, errors.New("rpc agent.session.keepalive: session is not active: session-ended (ENDED)"))
	if !ok {
		t.Fatal("expected inactive keepalive error to be handled")
	}

	runtime.mu.Lock()
	activeTask := runtime.activeTask
	activeSession := runtime.activeSession
	activeRunID := runtime.activeRunID
	scratch := runtime.scratch
	runtime.mu.Unlock()

	if activeTask != nil || activeSession != nil || activeRunID != "" {
		t.Fatalf("expected active state cleared, got task=%+v session=%+v run=%q", activeTask, activeSession, activeRunID)
	}
	if scratch.ActiveTaskID != "" || scratch.ActiveSessionID != "" || scratch.ActiveRunID != "" {
		t.Fatalf("expected scratch active ids cleared, got %+v", scratch)
	}
	if !strings.Contains(scratch.LastSummary, "session-ended") {
		t.Fatalf("expected scratch summary to mention ended session, got %q", scratch.LastSummary)
	}
	if currentContextDocWrites != 1 || claimedWorkDocWrites != 1 {
		t.Fatalf("expected one current-context and one claimed-work reset, got current=%d claimed=%d", currentContextDocWrites, claimedWorkDocWrites)
	}
	if len(savedStates) != 3 {
		t.Fatalf("expected three scratch saves, got %d", len(savedStates))
	}
	if savedStates[0].ActiveTaskID != "" || savedStates[0].ActiveSessionID != "" || savedStates[0].ActiveRunID != "" {
		t.Fatalf("expected first saved state to clear active ids, got %+v", savedStates[0])
	}
}
