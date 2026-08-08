package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceAuthAgentRegisterHandlerIssuesTokenAndStoresPrincipal(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "203.0.113.10",
		UserAgent: "rhizome-agent-handler/1.0",
	})

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-handler-agent",
		Title:       "Handler Agent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceAgentRegisterParams{
		WorkspaceID:       "ws-handler-agent",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-handler-1",
		DisplayName:       "Agent Handler 1",
		OwnerUserID:       "developer",
		Role:              "worker",
		ProtocolVersion:   "1.0",
		Summary:           "handler auth agent",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.workspaceAuthAgentRegister(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceAuthAgentRegister rpc error: %+v", rpcErr)
	}
	auth, ok := result.(sqlite.AgentRegistrationResult)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if auth.Token == "" {
		t.Fatal("expected token from agent registration")
	}
	if auth.AgentID != "agent-handler-1" || auth.DisplayName != "Agent Handler 1" {
		t.Fatalf("unexpected agent registration result: %+v", auth)
	}
	if auth.Agent.AgentID != "agent-handler-1" || auth.Agent.OwnerUserID == "" || len(auth.Agent.Capabilities) != 0 {
		t.Fatalf("expected registration result to include canonical agent identity, got %+v", auth.Agent)
	}
	if auth.Agent.LastSeenAt != nil {
		t.Fatalf("expected fresh registration not to set last_seen_at, got %+v", auth.Agent)
	}
	if auth.Agent.IsOnline {
		t.Fatalf("expected fresh registration to remain offline until heartbeat, got %+v", auth.Agent)
	}

	agent, err := store.GetAgent(ctx, "ws-handler-agent", "agent-handler-1")
	if err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if agent.DisplayName != "Agent Handler 1" {
		t.Fatalf("expected handler agent display name to persist, got %+v", agent)
	}
	if agent.LastSeenAt != nil || agent.IsOnline {
		t.Fatalf("expected persisted agent to remain offline until heartbeat, got %+v", agent)
	}
	authority, err := store.GetWorkspaceAuthority(ctx, "ws-handler-agent", "workspace")
	if err != nil {
		t.Fatalf("expected local workspace authority to be ensured, got %v", err)
	}
	if authority.HolderAuthorityNodeID == "" || authority.Status != sqlite.WorkspaceAuthorityStatusActive {
		t.Fatalf("expected active local workspace authority, got %+v", authority)
	}

	events, err := store.ListSecurityEvents(ctx, "ws-handler-agent", 10)
	if err != nil {
		t.Fatalf("list security events: %v", err)
	}
	var found bool
	for _, evt := range events {
		if evt.EventType == "agent_registered" {
			found = true
			if evt.IPAddress != "203.0.113.10" || evt.UserAgent != "rhizome-agent-handler/1.0" {
				t.Fatalf("expected request metadata capture in event, got %+v", evt)
			}
		}
	}
	if !found {
		t.Fatalf("expected agent_registered event in %+v", events)
	}

	if _, err := store.AuthenticateAccessToken(ctx, auth.Token); err != nil {
		t.Fatalf("authenticate issued token: %v", err)
	}
}

func TestWorkspaceAuthAgentRegisterHandlerBindsExplicitSharedLimitGroup(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "203.0.113.40",
		UserAgent: "rhizome-agent-handler/limit-group",
	})

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-handler-agent-group",
		Title:       "Handler Agent Group",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceAgentRegisterParams{
		WorkspaceID:       "ws-handler-agent-group",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-handler-group",
		DisplayName:       "Agent Handler Group",
		GroupID:           "codex",
		OwnerUserID:       "developer",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	if _, rpcErr := h.workspaceAuthAgentRegister(ctx, raw); rpcErr != nil {
		t.Fatalf("workspaceAuthAgentRegister rpc error: %+v", rpcErr)
	}

	group, err := store.GetAgentLimitGroup(ctx, "ws-handler-agent-group", "agent-handler-group")
	if err != nil {
		t.Fatalf("get assigned limit group: %v", err)
	}
	if group == nil || group.GroupID != "codex" {
		t.Fatalf("expected shared codex limit group, got %+v", group)
	}
	if len(group.Agents) != 1 || group.Agents[0] != "agent-handler-group" {
		t.Fatalf("expected codex membership for agent-handler-group, got %+v", group)
	}
}

func TestWorkspaceAuthAgentRegisterHandlerPreservesMetadataOnPartialReregister(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "203.0.113.12",
		UserAgent: "rhizome-agent-handler/2.0",
	})

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-handler-agent-reregister",
		Title:       "Handler Agent Reregister",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-handler-agent-reregister",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "203.0.113.13",
		UserAgent:         "rhizome-web-handler/2.0",
	})
	if err != nil {
		t.Fatalf("register human owner: %v", err)
	}

	initialRaw, err := json.Marshal(workspaceAgentRegisterParams{
		WorkspaceID:       "ws-handler-agent-reregister",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-handler-reregister",
		DisplayName:       "Partner Handler",
		OwnerUserID:       human.UserID,
		Role:              "reviewer",
		ProtocolVersion:   "partner-runtime/v8",
		Capabilities:      mustJSONRaw([]string{"analysis", "tool.call"}),
		Summary:           "partner handler inventory",
	})
	if err != nil {
		t.Fatalf("marshal initial params: %v", err)
	}

	initialResult, rpcErr := h.workspaceAuthAgentRegister(ctx, initialRaw)
	if rpcErr != nil {
		t.Fatalf("initial workspaceAuthAgentRegister rpc error: %+v", rpcErr)
	}
	initialAuth, ok := initialResult.(sqlite.AgentRegistrationResult)
	if !ok {
		t.Fatalf("unexpected initial result type %T", initialResult)
	}
	if initialAuth.Agent.DisplayName != "Partner Handler" || initialAuth.Agent.Role != "reviewer" {
		t.Fatalf("expected initial registration to persist identity metadata, got %+v", initialAuth.Agent)
	}

	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: "ws-handler-agent-reregister",
		AgentID:     "agent-handler-reregister",
		Status:      "active",
		Summary:     "still alive",
	}); err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}

	before, err := store.GetAgent(ctx, "ws-handler-agent-reregister", "agent-handler-reregister")
	if err != nil {
		t.Fatalf("get agent before partial reregister: %v", err)
	}
	if before.LastSeenAt == nil || *before.LastSeenAt == "" {
		t.Fatalf("expected heartbeat-backed presence before partial reregister, got %+v", before)
	}
	beforeLastSeenAt := *before.LastSeenAt
	beforeSummary := before.Summary

	partialRaw, err := json.Marshal(workspaceAgentRegisterParams{
		WorkspaceID:       "ws-handler-agent-reregister",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-handler-reregister",
	})
	if err != nil {
		t.Fatalf("marshal partial params: %v", err)
	}

	partialResult, rpcErr := h.workspaceAuthAgentRegister(ctx, partialRaw)
	if rpcErr != nil {
		t.Fatalf("partial workspaceAuthAgentRegister rpc error: %+v", rpcErr)
	}
	partialAuth, ok := partialResult.(sqlite.AgentRegistrationResult)
	if !ok {
		t.Fatalf("unexpected partial result type %T", partialResult)
	}
	if partialAuth.DisplayName != "Partner Handler" {
		t.Fatalf("expected partial reregister result to preserve display name, got %+v", partialAuth)
	}
	if partialAuth.Agent.OwnerUserID != human.UserID {
		t.Fatalf("expected partial reregister to preserve owner %q, got %+v", human.UserID, partialAuth.Agent)
	}
	if partialAuth.Agent.DisplayName != "Partner Handler" || partialAuth.Agent.Role != "reviewer" {
		t.Fatalf("expected partial reregister to preserve display/role, got %+v", partialAuth.Agent)
	}
	if partialAuth.Agent.ProtocolVersion != "partner-runtime/v8" {
		t.Fatalf("expected partial reregister to preserve protocol version, got %+v", partialAuth.Agent)
	}
	if len(partialAuth.Agent.Capabilities) != 2 || partialAuth.Agent.Capabilities[0] != "analysis" || partialAuth.Agent.Capabilities[1] != "tool.call" {
		t.Fatalf("expected partial reregister to preserve capabilities, got %+v", partialAuth.Agent)
	}
	if partialAuth.Agent.Summary != beforeSummary {
		t.Fatalf("expected partial reregister to preserve current summary %q, got %+v", beforeSummary, partialAuth.Agent)
	}
	if partialAuth.Agent.LastSeenAt == nil || *partialAuth.Agent.LastSeenAt != beforeLastSeenAt || !partialAuth.Agent.IsOnline {
		t.Fatalf("expected partial reregister to preserve liveness evidence, got %+v", partialAuth.Agent)
	}

	principal, err := store.AuthenticateAccessToken(ctx, human.Token)
	if err != nil {
		t.Fatalf("authenticate human token: %v", err)
	}
	authCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   principal.WorkspaceID,
		PrincipalType: principal.SubjectType,
		PrincipalID:   principal.SubjectID,
		TokenID:       principal.TokenID,
		TokenPrefix:   principal.TokenPrefix,
		DisplayName:   principal.DisplayName,
	})
	profileResult, rpcErr := h.workspaceAuthHumanProfileGet(authCtx, mustJSONRaw(map[string]any{}))
	if rpcErr != nil {
		t.Fatalf("workspaceAuthHumanProfileGet rpc error: %+v", rpcErr)
	}
	profilePayload, ok := profileResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected profile result type %T", profileResult)
	}
	agentItems, ok := profilePayload["agents"].([]map[string]any)
	if !ok {
		t.Fatalf("unexpected agents payload type %T", profilePayload["agents"])
	}
	var preserved map[string]any
	for _, agent := range agentItems {
		if agent["agent_id"] == "agent-handler-reregister" {
			preserved = agent
			break
		}
	}
	if preserved == nil {
		t.Fatalf("expected preserved agent in profile payload, got %+v", agentItems)
	}
	if preserved["display_name"] != "Partner Handler" || preserved["role"] != "reviewer" {
		t.Fatalf("expected profile inventory to preserve display/role, got %+v", preserved)
	}
	if preserved["protocol_version"] != "partner-runtime/v8" || preserved["summary"] != beforeSummary {
		t.Fatalf("expected profile inventory to preserve protocol/summary, got %+v", preserved)
	}
	switch capabilities := preserved["capabilities"].(type) {
	case []any:
		if len(capabilities) != 2 || capabilities[0] != "analysis" || capabilities[1] != "tool.call" {
			t.Fatalf("expected profile inventory to preserve capabilities, got %+v", preserved)
		}
	case []string:
		if len(capabilities) != 2 || capabilities[0] != "analysis" || capabilities[1] != "tool.call" {
			t.Fatalf("expected profile inventory to preserve capabilities, got %+v", preserved)
		}
	default:
		t.Fatalf("unexpected capabilities payload type %T in %+v", preserved["capabilities"], preserved)
	}
	if preserved["liveness_status"] != "ONLINE" {
		t.Fatalf("expected profile inventory to preserve online liveness after partial reregister, got %+v", preserved)
	}
}

func TestWorkspaceAuthHumanRegisterAndLoginHandler(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "203.0.113.11",
		UserAgent: "rhizome-web-handler/1.0",
	})

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-handler-human",
		Title:       "Handler Human",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	rawRegister, err := json.Marshal(workspaceHumanRegisterParams{
		WorkspaceID:       "ws-handler-human",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
	})
	if err != nil {
		t.Fatalf("marshal human register params: %v", err)
	}
	registerResult, rpcErr := h.workspaceAuthHumanRegister(ctx, rawRegister)
	if rpcErr != nil {
		t.Fatalf("workspaceAuthHumanRegister rpc error: %+v", rpcErr)
	}
	reg, ok := registerResult.(sqlite.HumanAuthResult)
	if !ok {
		t.Fatalf("unexpected register result type %T", registerResult)
	}
	if reg.Token == "" {
		t.Fatalf("expected registration to issue token, got %+v", reg)
	}

	rawLogin, err := json.Marshal(workspaceHumanLoginParams{
		WorkspaceID: "ws-handler-human",
		Username:    "alice",
		Password:    "alice-password",
	})
	if err != nil {
		t.Fatalf("marshal human login params: %v", err)
	}
	loginResult, rpcErr := h.workspaceAuthHumanLogin(ctx, rawLogin)
	if rpcErr != nil {
		t.Fatalf("workspaceAuthHumanLogin rpc error: %+v", rpcErr)
	}
	login, ok := loginResult.(sqlite.HumanAuthResult)
	if !ok {
		t.Fatalf("unexpected login result type %T", loginResult)
	}
	if login.Token == "" || login.Token == reg.Token {
		t.Fatalf("expected distinct login token, got reg=%+v login=%+v", reg, login)
	}
	if _, err := store.AuthenticateAccessToken(ctx, reg.Token); err != nil {
		t.Fatalf("authenticate original human token: %v", err)
	}

	events, err := store.ListSecurityEvents(ctx, "ws-handler-human", 10)
	if err != nil {
		t.Fatalf("list security events: %v", err)
	}
	var registered, loggedIn int
	for _, evt := range events {
		switch evt.EventType {
		case "human_registered":
			registered++
		case "human_login":
			loggedIn++
		}
	}
	if registered != 1 || loggedIn < 1 {
		t.Fatalf("expected human registration/login events, got reg=%d login=%d events=%+v", registered, loggedIn, events)
	}
	if _, err := store.AuthenticateAccessToken(ctx, login.Token); err != nil {
		t.Fatalf("authenticate human token: %v", err)
	}
}

func TestWorkspaceAuthHumanLoginAliasesUseUsernameNotDisplayName(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "203.0.113.12",
		UserAgent: "rhizome-web-handler/1.1",
	})

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-handler-human-login-alias",
		Title:       "Handler Human Login Alias",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	rawRegister, err := json.Marshal(workspaceHumanRegisterParams{
		WorkspaceID:       "ws-handler-human-login-alias",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice-login",
		DisplayName:       "Alice Display",
		Password:          "alice-password",
	})
	if err != nil {
		t.Fatalf("marshal human register params: %v", err)
	}
	if _, rpcErr := h.workspaceAuthHumanRegister(ctx, rawRegister); rpcErr != nil {
		t.Fatalf("workspaceAuthHumanRegister rpc error: %+v", rpcErr)
	}

	for _, tc := range []struct {
		name   string
		params workspaceHumanLoginParams
	}{
		{
			name: "login_name alias",
			params: workspaceHumanLoginParams{
				WorkspaceID: "ws-handler-human-login-alias",
				LoginName:   "alice-login",
				Password:    "alice-password",
			},
		},
		{
			name: "legacy name alias",
			params: workspaceHumanLoginParams{
				WorkspaceID: "ws-handler-human-login-alias",
				Name:        "alice-login",
				Password:    "alice-password",
			},
		},
	} {
		rawLogin, err := json.Marshal(tc.params)
		if err != nil {
			t.Fatalf("%s: marshal human login params: %v", tc.name, err)
		}
		loginResult, rpcErr := h.workspaceAuthHumanLogin(ctx, rawLogin)
		if rpcErr != nil {
			t.Fatalf("%s: workspaceAuthHumanLogin rpc error: %+v", tc.name, rpcErr)
		}
		login, ok := loginResult.(sqlite.HumanAuthResult)
		if !ok {
			t.Fatalf("%s: unexpected login result type %T", tc.name, loginResult)
		}
		if login.Username != "alice-login" || login.DisplayName != "Alice Display" {
			t.Fatalf("%s: unexpected login identity %+v", tc.name, login)
		}
	}

	rawDisplayNameLogin, err := json.Marshal(workspaceHumanLoginParams{
		WorkspaceID: "ws-handler-human-login-alias",
		Name:        "Alice Display",
		Password:    "alice-password",
	})
	if err != nil {
		t.Fatalf("marshal display-name login params: %v", err)
	}
	if _, rpcErr := h.workspaceAuthHumanLogin(ctx, rawDisplayNameLogin); rpcErr == nil {
		t.Fatal("expected display name login to be rejected once username differs")
	} else if !strings.Contains(rpcErr.Message, "invalid human credentials") {
		t.Fatalf("expected invalid credentials for display-name login, got %+v", rpcErr)
	}
}

func TestWorkspaceSecurityPasswordUpdateHandlerUpdatesPasswordAndAudits(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "198.51.100.12",
		UserAgent: "rhizome-admin/1.0",
	})
	authCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-handler-password",
		PrincipalType: "human",
		PrincipalID:   "developer",
	})

	if err := store.CreateWorkspace(authCtx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-handler-password",
		Title:       "Handler Password",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceSecurityPasswordUpdateParams{
		WorkspaceID: "ws-handler-password",
		Password:    "rotated-workspace-pass",
		UpdatedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("marshal password update params: %v", err)
	}
	result, rpcErr := h.workspaceSecurityPasswordUpdate(authCtx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceSecurityPasswordUpdate rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected password update result type %T", result)
	}
	if payload["status"] != "UPDATED" {
		t.Fatalf("expected updated status, got %#v", payload["status"])
	}

	updated, err := store.GetWorkspaceSecuritySettings(authCtx, "ws-handler-password")
	if err != nil {
		t.Fatalf("get updated workspace settings: %v", err)
	}
	if updated.PasswordUpdatedAt == "" {
		t.Fatal("expected password update timestamp")
	}

	events, err := store.ListSecurityEvents(authCtx, "ws-handler-password", 10)
	if err != nil {
		t.Fatalf("list security events: %v", err)
	}
	var found bool
	for _, evt := range events {
		if evt.EventType == "workspace_settings_updated" {
			found = true
			if evt.IPAddress != "198.51.100.12" || evt.UserAgent != "rhizome-admin/1.0" {
				t.Fatalf("expected password update metadata capture, got %+v", evt)
			}
		}
	}
	if !found {
		t.Fatalf("expected workspace update event in %+v", events)
	}
}

func TestWorkspaceSecurityAuditListHandlerReturnsSecurityEvents(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "198.51.100.13",
		UserAgent: "rhizome-audit/1.0",
	})
	authCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-handler-audit",
		PrincipalType: "human",
		PrincipalID:   "developer",
	})

	if err := store.CreateWorkspace(authCtx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-handler-audit",
		Title:       "Handler Audit",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.UpdateWorkspaceSecuritySettings(authCtx, sqlite.WorkspaceSecuritySettingsInput{
		WorkspaceID:       "ws-handler-audit",
		WorkspacePassword: "audit-password",
		UpdatedByType:     "human",
		UpdatedByID:       "developer",
		IPAddress:         "198.51.100.13",
		UserAgent:         "rhizome-audit/1.0",
	}); err != nil {
		t.Fatalf("seed password update: %v", err)
	}

	raw, err := json.Marshal(workspaceSecurityAuditListParams{
		WorkspaceID: "ws-handler-audit",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal audit list params: %v", err)
	}
	result, rpcErr := h.workspaceSecurityAuditList(authCtx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceSecurityAuditList rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected audit list result type %T", result)
	}
	if count, ok := payload["count"].(int); !ok || count == 0 {
		t.Fatalf("expected audit list to return events, got %#v", payload["count"])
	}
	items, ok := payload["items"].([]map[string]any)
	if !ok {
		t.Fatalf("unexpected items type %T", payload["items"])
	}
	if len(items) == 0 || items[0]["event_type"] == "" {
		t.Fatalf("expected populated security events, got %+v", items)
	}
}

func TestAuthMiddlewareWithStoreAuthenticatesPerTokenAndCapturesMetadata(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-middleware",
		Title:       "Middleware",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	auth, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-middleware",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-middleware",
		DisplayName:       "Agent Middleware",
		OwnerUserID:       "developer",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	var capturedPrincipal AuthPrincipal
	var capturedMeta RequestMetadata
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := authPrincipalFromContext(r.Context())
		if !ok {
			t.Fatal("expected auth principal in context")
		}
		meta, ok := requestMetadataFromContext(r.Context())
		if !ok {
			t.Fatal("expected request metadata in context")
		}
		capturedPrincipal = principal
		capturedMeta = meta
		_ = json.NewEncoder(w).Encode(map[string]any{
			"workspace_id": principal.WorkspaceID,
			"principal":    principal.PrincipalID,
			"client_ip":    meta.ClientIP,
			"user_agent":   meta.UserAgent,
		})
	})

	req := httptest.NewRequest(http.MethodPost, "/rpc", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer "+auth.Token)
	req.Header.Set("User-Agent", "rhizome-middleware/1.0")
	req.Header.Set("X-Forwarded-For", "203.0.113.14, 10.0.0.1")
	resp := httptest.NewRecorder()

	AuthMiddlewareWithStore(store, next).ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected middleware to allow request, got status %d body=%s", resp.Code, resp.Body.String())
	}
	if capturedPrincipal.WorkspaceID != "ws-middleware" || capturedPrincipal.PrincipalType != "agent" || capturedPrincipal.PrincipalID != "agent-middleware" {
		t.Fatalf("unexpected captured principal: %+v", capturedPrincipal)
	}
	if capturedMeta.ClientIP != "203.0.113.14" || capturedMeta.UserAgent != "rhizome-middleware/1.0" {
		t.Fatalf("unexpected captured metadata: %+v", capturedMeta)
	}

	resp = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/rpc", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer invalid-token")
	AuthMiddlewareWithStore(store, next).ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected rpc error response, got status %d", resp.Code)
	}
	if !bytes.Contains(resp.Body.Bytes(), []byte("invalid token")) {
		t.Fatalf("expected invalid token error, got %s", resp.Body.String())
	}
}

func TestAuthMiddlewareWithStoreRejectsQueryTokenForEventsAndAPIs(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-middleware-events",
		Title:       "Middleware Events",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	auth, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-middleware-events",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-events",
		DisplayName:       "Agent Events",
		OwnerUserID:       "developer",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := authPrincipalFromContext(r.Context())
		if !ok {
			t.Fatal("expected auth principal in context")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"principal": principal.PrincipalID})
	})
	handler := AuthMiddlewareWithStore(store, next)

	eventReq := httptest.NewRequest(http.MethodGet, "/events?workspace_id=ws-middleware-events&token="+auth.Token, nil)
	eventResp := httptest.NewRecorder()
	handler.ServeHTTP(eventResp, eventReq)
	if eventResp.Code != http.StatusUnauthorized {
		t.Fatalf("expected event query token to be rejected, got %d body=%s", eventResp.Code, eventResp.Body.String())
	}
	if !bytes.Contains(eventResp.Body.Bytes(), []byte("missing Authorization header")) {
		t.Fatalf("expected missing authorization error for events, got %s", eventResp.Body.String())
	}

	apiReq := httptest.NewRequest(http.MethodPost, "/api/workspace/security/logs?token="+auth.Token, bytes.NewReader([]byte(`{}`)))
	apiResp := httptest.NewRecorder()
	handler.ServeHTTP(apiResp, apiReq)
	if apiResp.Code != http.StatusUnauthorized {
		t.Fatalf("expected api query token to be rejected, got %d body=%s", apiResp.Code, apiResp.Body.String())
	}
	if !bytes.Contains(apiResp.Body.Bytes(), []byte("missing Authorization header")) {
		t.Fatalf("expected missing authorization error, got %s", apiResp.Body.String())
	}
}

func TestAuthMiddlewareWithStoreRejectsLegacyServerTokenFallback(t *testing.T) {
	t.Setenv("RHIZOME_API_TOKEN", "legacy-server-token")

	store := newServerTestStore(t)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/api/workspace/security/logs", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer legacy-server-token")
	resp := httptest.NewRecorder()

	AuthMiddlewareWithStore(store, next).ServeHTTP(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected legacy server token fallback to be rejected, got %d body=%s", resp.Code, resp.Body.String())
	}
	if !bytes.Contains(resp.Body.Bytes(), []byte("invalid token")) {
		t.Fatalf("expected invalid token message, got %s", resp.Body.String())
	}
}

func TestServeHumanRegisterHTTPBootstrapsWithoutExistingToken(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID:       "ws-http-human",
		Title:             "HTTP Human Workspace",
		CreatedBy:         "developer",
		WorkspacePassword: "test-workspace-password",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/auth/human/register", bytes.NewReader([]byte(`{
		"workspace_name":"HTTP Human Workspace",
		"workspace_password":"test-workspace-password",
		"name":"Alice",
		"password":"alice-password"
	}`)))
	req.Header.Set("User-Agent", "rhizome-dashboard/1.0")
	req.Header.Set("X-Forwarded-For", "203.0.113.21")
	resp := httptest.NewRecorder()

	h.ServeHumanRegisterHTTP().ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.Code, resp.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload["workspace_id"] != "ws-http-human" || payload["workspace_name"] != "HTTP Human Workspace" {
		t.Fatalf("unexpected workspace payload: %+v", payload)
	}
	if token, _ := payload["access_token"].(string); token == "" {
		t.Fatalf("expected access token, got %+v", payload)
	}
}

func TestServeHumanRegisterHTTPRejectsWeakPassword(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID:       "ws-http-human-weak-password",
		Title:             "HTTP Human Weak Password",
		CreatedBy:         "developer",
		WorkspacePassword: "test-workspace-password",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/auth/human/register", bytes.NewReader([]byte(`{
		"workspace_id":"ws-http-human-weak-password",
		"workspace_password":"test-workspace-password",
		"username":"alice",
		"display_name":"Alice",
		"password":"too-short"
	}`)))
	resp := httptest.NewRecorder()

	h.ServeHumanRegisterHTTP().ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), "at least 12 characters") {
		t.Fatalf("expected password policy message, got %s", resp.Body.String())
	}
}

func TestServeWorkspaceSecurityLogsHTTPScopesToAuthenticatedWorkspace(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	for _, workspace := range []sqlite.WorkspaceCreateInput{
		{WorkspaceID: "ws-scope-a", Title: "Workspace A", CreatedBy: "developer"},
		{WorkspaceID: "ws-scope-b", Title: "Workspace B", CreatedBy: "developer"},
	} {
		if err := store.CreateWorkspace(ctx, workspace); err != nil {
			t.Fatalf("create workspace %s: %v", workspace.WorkspaceID, err)
		}
	}

	auth, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-scope-a",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "203.0.113.22",
		UserAgent:         "rhizome-dashboard/1.0",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	handler := AuthMiddlewareWithStore(store, h.ServeWorkspaceSecurityLogsHTTP())

	req := httptest.NewRequest(http.MethodPost, "/api/workspace/security/logs", bytes.NewReader([]byte(`{"workspace_id":"ws-scope-a","limit":10}`)))
	req.Header.Set("Authorization", "Bearer "+auth.Token)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected scoped request to succeed, got %d body=%s", resp.Code, resp.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/workspace/security/logs", bytes.NewReader([]byte(`{"workspace_id":"ws-scope-b","limit":10}`)))
	req.Header.Set("Authorization", "Bearer "+auth.Token)
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected cross-workspace access to be denied, got %d body=%s", resp.Code, resp.Body.String())
	}
	if !bytes.Contains(resp.Body.Bytes(), []byte("workspace access denied")) {
		t.Fatalf("expected workspace access denied message, got %s", resp.Body.String())
	}
}

func TestAuthMiddlewareWithStoreReturnsJSONErrorForAPIRequests(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	req := httptest.NewRequest(http.MethodPost, "/api/workspace/security/logs", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer invalid-token")
	resp := httptest.NewRecorder()

	AuthMiddlewareWithStore(store, next).ServeHTTP(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for API request, got %d body=%s", resp.Code, resp.Body.String())
	}
	if !bytes.Contains(resp.Body.Bytes(), []byte("invalid token")) {
		t.Fatalf("expected invalid token message, got %s", resp.Body.String())
	}
}

func TestWorkspaceAuthHumanProfileHTTPAndRPCContract(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-human-profile-handler",
		Title:       "Human Profile Handler",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	auth, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-profile-handler",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "203.0.113.30",
		UserAgent:         "rhizome-web/3.0",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}
	for _, agent := range []sqlite.AgentRegisterInput{
		{
			WorkspaceID:     "ws-human-profile-handler",
			AgentID:         "agent-one",
			OwnerUserID:     auth.UserID,
			DisplayName:     "One Agent",
			Status:          "REGISTERED",
			ProtocolVersion: "workspace-bootstrap/v2",
			Capabilities:    []string{"analysis", "review"},
			Summary:         "partner agent one",
		},
		{
			WorkspaceID:     "ws-human-profile-handler",
			AgentID:         "agent-two",
			OwnerUserID:     auth.UserID,
			DisplayName:     "Two Agent",
			Status:          "PAUSED",
			ProtocolVersion: "workspace-bootstrap/v3",
			Capabilities:    []string{"tool.call"},
			Summary:         "partner agent two",
		},
	} {
		if err := store.RegisterAgent(ctx, agent); err != nil {
			t.Fatalf("register agent %s: %v", agent.AgentID, err)
		}
	}

	getReq := httptest.NewRequest(http.MethodPost, "/api/auth/human/profile/get", bytes.NewReader([]byte(`{}`)))
	getReq.Header.Set("Authorization", "Bearer "+auth.Token)
	getReq.Header.Set("User-Agent", "rhizome-web/3.0")
	getReq.Header.Set("X-Forwarded-For", "203.0.113.30")
	getResp := httptest.NewRecorder()
	AuthMiddlewareWithStore(store, h.ServeHumanProfileGetHTTP()).ServeHTTP(getResp, getReq)

	if getResp.Code != http.StatusOK {
		t.Fatalf("expected profile get to succeed, got %d body=%s", getResp.Code, getResp.Body.String())
	}
	var getPayload map[string]any
	if err := json.Unmarshal(getResp.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("decode profile get response: %v", err)
	}
	profile, ok := getPayload["profile"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected profile payload: %+v", getPayload)
	}
	if profile["user_id"] != auth.UserID {
		t.Fatalf("expected immutable user_id %q, got %+v", auth.UserID, profile["user_id"])
	}
	if profile["username"] != "alice" {
		t.Fatalf("expected immutable username alice, got %+v", profile["username"])
	}
	if profile["display_name"] != "Alice" {
		t.Fatalf("expected profile display_name Alice, got %+v", profile["display_name"])
	}
	agents, ok := profile["agents"].([]any)
	if !ok || len(agents) != 2 {
		t.Fatalf("expected two owned agents, got %+v", profile["agents"])
	}
	for _, rawAgent := range agents {
		agent, ok := rawAgent.(map[string]any)
		if !ok {
			t.Fatalf("unexpected agent payload in human profile: %+v", rawAgent)
		}
		if lastSeenAt, exists := agent["last_seen_at"]; !exists || lastSeenAt != nil {
			t.Fatalf("expected just-registered profile agent to expose null last_seen_at, got %+v", agent)
		}
		if isOnline, ok := agent["is_online"].(bool); !ok || isOnline {
			t.Fatalf("expected just-registered profile agent to remain offline until heartbeat, got %+v", agent)
		}
		if livenessStatus := agent["liveness_status"]; livenessStatus != "REGISTERED_OFFLINE" {
			t.Fatalf("expected explicit registered-offline liveness status, got %+v", agent)
		}
		switch agent["agent_id"] {
		case "agent-one":
			if agent["protocol_version"] != "workspace-bootstrap/v2" {
				t.Fatalf("expected protocol_version for agent-one, got %+v", agent)
			}
			capabilities, ok := agent["capabilities"].([]any)
			if !ok || len(capabilities) != 2 || capabilities[0] != "analysis" || capabilities[1] != "review" {
				t.Fatalf("expected capability inventory for agent-one, got %+v", agent)
			}
		case "agent-two":
			if agent["status"] != "PAUSED" || agent["protocol_version"] != "workspace-bootstrap/v3" {
				t.Fatalf("expected declared registration metadata for agent-two, got %+v", agent)
			}
		}
	}

	updateReq := httptest.NewRequest(http.MethodPost, "/api/auth/human/profile/update", bytes.NewReader([]byte(`{
		"display_name":"Alice Updated",
		"password":"alice-new-password"
	}`)))
	updateReq.Header.Set("Authorization", "Bearer "+auth.Token)
	updateReq.Header.Set("User-Agent", "rhizome-web/3.0")
	updateReq.Header.Set("X-Forwarded-For", "203.0.113.31")
	updateResp := httptest.NewRecorder()
	AuthMiddlewareWithStore(store, h.ServeHumanProfileUpdateHTTP()).ServeHTTP(updateResp, updateReq)

	if updateResp.Code != http.StatusOK {
		t.Fatalf("expected profile update to succeed, got %d body=%s", updateResp.Code, updateResp.Body.String())
	}
	var updatePayload map[string]any
	if err := json.Unmarshal(updateResp.Body.Bytes(), &updatePayload); err != nil {
		t.Fatalf("decode profile update response: %v", err)
	}
	updatedProfile, ok := updatePayload["profile"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected profile update payload: %+v", updatePayload)
	}
	if updatedProfile["user_id"] != auth.UserID {
		t.Fatalf("expected immutable user_id after update, got %+v", updatedProfile["user_id"])
	}
	if updatedProfile["display_name"] != "Alice Updated" {
		t.Fatalf("expected updated display name, got %+v", updatedProfile["display_name"])
	}

	login, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-human-profile-handler",
		Username:    "alice",
		Password:    "alice-new-password",
	})
	if err != nil {
		t.Fatalf("login after profile update: %v", err)
	}
	if login.UserID != auth.UserID {
		t.Fatalf("expected login to resolve same user_id, got %q", login.UserID)
	}
}

func TestWorkspaceAuthHumanSessionsHandlers(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "203.0.113.40",
		UserAgent: "rhizome-web/4.0",
	})

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-human-sessions-handler",
		Title:       "Human Sessions Handler",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	reg, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-sessions-handler",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "203.0.113.40",
		UserAgent:         "rhizome-web/4.0",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}
	login, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-human-sessions-handler",
		Username:    "alice",
		Password:    "alice-password",
		IPAddress:   "203.0.113.40",
		UserAgent:   "rhizome-web/4.0",
	})
	if err != nil {
		t.Fatalf("login human: %v", err)
	}

	regPrincipal, err := store.AuthenticateAccessToken(ctx, reg.Token)
	if err != nil {
		t.Fatalf("authenticate registration token: %v", err)
	}
	loginPrincipal, err := store.AuthenticateAccessToken(ctx, login.Token)
	if err != nil {
		t.Fatalf("authenticate login token: %v", err)
	}

	authCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   loginPrincipal.WorkspaceID,
		PrincipalType: loginPrincipal.SubjectType,
		PrincipalID:   loginPrincipal.SubjectID,
		TokenID:       loginPrincipal.TokenID,
		TokenPrefix:   loginPrincipal.TokenPrefix,
		DisplayName:   loginPrincipal.DisplayName,
	})

	listResult, rpcErr := h.workspaceAuthHumanSessionsList(authCtx, mustJSONRaw(map[string]any{}))
	if rpcErr != nil {
		t.Fatalf("workspaceAuthHumanSessionsList rpc error: %+v", rpcErr)
	}
	listPayload, ok := listResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected sessions list result type %T", listResult)
	}
	if listPayload["current_token_id"] != loginPrincipal.TokenID {
		t.Fatalf("expected current_token_id %q, got %+v", loginPrincipal.TokenID, listPayload["current_token_id"])
	}
	sessions, ok := listPayload["sessions"].([]map[string]any)
	if !ok {
		t.Fatalf("unexpected sessions payload type %T", listPayload["sessions"])
	}
	if len(sessions) != 2 {
		t.Fatalf("expected two sessions, got %+v", sessions)
	}
	foundCurrent := false
	foundRegistration := false
	for _, session := range sessions {
		tokenID, _ := session["token_id"].(string)
		if tokenID == loginPrincipal.TokenID {
			foundCurrent = true
			if current, _ := session["current"].(bool); !current {
				t.Fatalf("expected login token to be marked current, got %+v", session)
			}
		}
		if tokenID == regPrincipal.TokenID {
			foundRegistration = true
		}
	}
	if !foundCurrent || !foundRegistration {
		t.Fatalf("expected both active sessions to be listed, got %+v", sessions)
	}

	revokeResult, rpcErr := h.workspaceAuthHumanSessionsRevoke(authCtx, mustJSONRaw(workspaceHumanSessionsRevokeParams{
		AllOtherSessions: true,
		Reason:           "dashboard logout",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceAuthHumanSessionsRevoke rpc error: %+v", rpcErr)
	}
	revokePayload, ok := revokeResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected revoke result type %T", revokeResult)
	}
	if revokePayload["revoked_count"] != 1 {
		t.Fatalf("expected one revoked session, got %+v", revokePayload["revoked_count"])
	}
	if _, err := store.AuthenticateAccessToken(ctx, reg.Token); err == nil {
		t.Fatal("expected registration token to be revoked by session logout")
	}
	if _, err := store.AuthenticateAccessToken(ctx, login.Token); err != nil {
		t.Fatalf("expected current token to stay valid: %v", err)
	}
}

func TestWorkspaceAuthHumanSessionsHandlersCanRevokeSpecificSessionAndExposeRevokedEntry(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "203.0.113.41",
		UserAgent: "rhizome-web/4.2",
	})

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-human-session-revoke-handler",
		Title:       "Human Session Revoke Handler",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	reg, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-session-revoke-handler",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "203.0.113.41",
		UserAgent:         "rhizome-web/4.2",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}
	staleLogin, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-human-session-revoke-handler",
		Username:    "alice",
		Password:    "alice-password",
		IPAddress:   "203.0.113.41",
		UserAgent:   "rhizome-web/4.2",
	})
	if err != nil {
		t.Fatalf("login human stale session: %v", err)
	}
	currentLogin, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-human-session-revoke-handler",
		Username:    "alice",
		Password:    "alice-password",
		IPAddress:   "203.0.113.41",
		UserAgent:   "rhizome-web/4.2",
	})
	if err != nil {
		t.Fatalf("login human current session: %v", err)
	}

	stalePrincipal, err := store.AuthenticateAccessToken(ctx, staleLogin.Token)
	if err != nil {
		t.Fatalf("authenticate stale token: %v", err)
	}
	currentPrincipal, err := store.AuthenticateAccessToken(ctx, currentLogin.Token)
	if err != nil {
		t.Fatalf("authenticate current token: %v", err)
	}
	authCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   currentPrincipal.WorkspaceID,
		PrincipalType: currentPrincipal.SubjectType,
		PrincipalID:   currentPrincipal.SubjectID,
		TokenID:       currentPrincipal.TokenID,
		TokenPrefix:   currentPrincipal.TokenPrefix,
		DisplayName:   currentPrincipal.DisplayName,
	})

	revokeResult, rpcErr := h.workspaceAuthHumanSessionsRevoke(authCtx, mustJSONRaw(workspaceHumanSessionsRevokeParams{
		TokenID: stalePrincipal.TokenID,
		Reason:  "manual revoke",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceAuthHumanSessionsRevoke rpc error: %+v", rpcErr)
	}
	revokePayload, ok := revokeResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected revoke result type %T", revokeResult)
	}
	if revokePayload["status"] != "REVOKED" || revokePayload["token_id"] != stalePrincipal.TokenID {
		t.Fatalf("unexpected revoke payload: %+v", revokePayload)
	}

	listResult, rpcErr := h.workspaceAuthHumanSessionsList(authCtx, mustJSONRaw(map[string]any{}))
	if rpcErr != nil {
		t.Fatalf("workspaceAuthHumanSessionsList rpc error: %+v", rpcErr)
	}
	listPayload, ok := listResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected sessions list result type %T", listResult)
	}
	sessions, ok := listPayload["sessions"].([]map[string]any)
	if !ok {
		t.Fatalf("unexpected sessions payload type %T", listPayload["sessions"])
	}
	if len(sessions) != 3 {
		t.Fatalf("expected all issued sessions to remain visible after revoke, got %+v", sessions)
	}

	var revokedEntry map[string]any
	var currentEntry map[string]any
	for _, session := range sessions {
		tokenID, _ := session["token_id"].(string)
		switch tokenID {
		case stalePrincipal.TokenID:
			revokedEntry = session
		case currentPrincipal.TokenID:
			currentEntry = session
		}
	}
	if revokedEntry == nil {
		t.Fatalf("expected revoked session entry for %s, got %+v", stalePrincipal.TokenID, sessions)
	}
	revokedAt, ok := revokedEntry["revoked_at"].(*string)
	if !ok || revokedAt == nil || *revokedAt == "" {
		t.Fatalf("expected revoked session to expose revoked_at, got %+v", revokedEntry)
	}
	if revokedReason, _ := revokedEntry["revoked_reason"].(string); revokedReason != "manual revoke" {
		t.Fatalf("expected revoked reason to round-trip, got %+v", revokedEntry)
	}
	if currentEntry == nil {
		t.Fatalf("expected current session entry for %s, got %+v", currentPrincipal.TokenID, sessions)
	}
	if current, _ := currentEntry["current"].(bool); !current {
		t.Fatalf("expected current session to stay marked current, got %+v", currentEntry)
	}

	if _, err := store.AuthenticateAccessToken(ctx, staleLogin.Token); err == nil {
		t.Fatal("expected specifically revoked token to become invalid")
	}
	if _, err := store.AuthenticateAccessToken(ctx, reg.Token); err != nil {
		t.Fatalf("expected untouched registration token to stay valid: %v", err)
	}
	if _, err := store.AuthenticateAccessToken(ctx, currentLogin.Token); err != nil {
		t.Fatalf("expected current token to stay valid: %v", err)
	}
}

func TestWorkspaceAuthAgentTokenRotateHandlerDeliversSecurityNoticeAndRedactsWorkspaceMessages(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "198.51.100.40",
		UserAgent: "rhizome-web/4.1",
	})
	authCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-agent-rotate-handler",
		PrincipalType: "human",
		PrincipalID:   "developer",
	})

	if err := store.CreateWorkspace(authCtx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-rotate-handler",
		Title:       "Agent Rotate Handler",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(authCtx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-agent-rotate-handler",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "198.51.100.40",
		UserAgent:         "rhizome-web/4.1",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}
	principal, err := store.AuthenticateAccessToken(authCtx, human.Token)
	if err != nil {
		t.Fatalf("authenticate human token: %v", err)
	}
	authCtx = context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   principal.WorkspaceID,
		PrincipalType: principal.SubjectType,
		PrincipalID:   principal.SubjectID,
		TokenID:       principal.TokenID,
		TokenPrefix:   principal.TokenPrefix,
		DisplayName:   principal.DisplayName,
	})

	initialAgent, err := store.RegisterAgentWithWorkspacePassword(authCtx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-agent-rotate-handler",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-rotate",
		DisplayName:       "Rotating Agent",
		Role:              "reviewer",
		ProtocolVersion:   "workspace-bootstrap/v9",
		Capabilities:      []string{"analysis", "tool.call"},
		Summary:           "partner rotate lifecycle",
		OwnerUserID:       human.UserID,
		IPAddress:         "198.51.100.41",
		UserAgent:         "rhizome-agent/2.0",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if initialAgent.Token == "" {
		t.Fatal("expected initial agent token")
	}
	authority := claimServerTestWorkspaceAuthority(t, authCtx, store, "ws-agent-rotate-handler")

	ch := h.GetEventBus().Subscribe("ws-agent-rotate-handler")
	defer h.GetEventBus().Unsubscribe("ws-agent-rotate-handler", ch)

	rotateResult, rpcErr := h.workspaceAuthAgentTokenRotate(authCtx, mustJSONRaw(workspaceAgentTokenRotateParams{
		AgentID: "agent-rotate",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceAuthAgentTokenRotate rpc error: %+v", rpcErr)
	}
	rotatePayload, ok := rotateResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected rotate result type %T", rotateResult)
	}
	newToken, _ := rotatePayload["token"].(string)
	messageID, _ := rotatePayload["message_id"].(string)
	if newToken == "" || messageID == "" {
		t.Fatalf("expected rotate payload to include token and message id, got %+v", rotatePayload)
	}
	if rotatePayload["rotation_mode"] != "grace_until_first_use" {
		t.Fatalf("expected staged rotation mode, got %+v", rotatePayload["rotation_mode"])
	}

	profileResult, rpcErr := h.workspaceAuthHumanProfileGet(authCtx, mustJSONRaw(map[string]any{}))
	if rpcErr != nil {
		t.Fatalf("workspaceAuthHumanProfileGet rpc error: %+v", rpcErr)
	}
	profilePayload, ok := profileResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected profile result type %T", profileResult)
	}
	agentItems, ok := profilePayload["agents"].([]map[string]any)
	if !ok {
		t.Fatalf("unexpected agents payload type %T", profilePayload["agents"])
	}
	var rotatedAgent map[string]any
	for _, agent := range agentItems {
		if agent["agent_id"] == "agent-rotate" {
			rotatedAgent = agent
			break
		}
	}
	if rotatedAgent == nil {
		t.Fatalf("expected rotated agent in profile payload, got %+v", agentItems)
	}
	authToken, ok := rotatedAgent["auth_token"].(map[string]any)
	if !ok {
		t.Fatalf("expected auth_token in rotated agent payload, got %+v", rotatedAgent)
	}
	if tokenPrefix, _ := authToken["token_prefix"].(string); tokenPrefix != newToken[:8] {
		t.Fatalf("expected rotated agent token prefix %q, got %+v", newToken[:8], authToken["token_prefix"])
	}
	if rotatedAgent["role"] != "reviewer" || rotatedAgent["protocol_version"] != "workspace-bootstrap/v9" {
		t.Fatalf("expected rotated agent to preserve declared role/protocol, got %+v", rotatedAgent)
	}
	if rotatedAgent["summary"] != "partner rotate lifecycle" {
		t.Fatalf("expected rotated agent summary to survive token rotation, got %+v", rotatedAgent)
	}
	if rotatedAgent["liveness_status"] != "REGISTERED_OFFLINE" {
		t.Fatalf("expected rotated agent to remain explicitly registered-offline until heartbeat, got %+v", rotatedAgent)
	}
	switch capabilities := rotatedAgent["capabilities"].(type) {
	case []any:
		if len(capabilities) != 2 || capabilities[0] != "analysis" || capabilities[1] != "tool.call" {
			t.Fatalf("expected rotated agent capability inventory to survive token rotation, got %+v", rotatedAgent)
		}
	case []string:
		if len(capabilities) != 2 || capabilities[0] != "analysis" || capabilities[1] != "tool.call" {
			t.Fatalf("expected rotated agent capability inventory to survive token rotation, got %+v", rotatedAgent)
		}
	default:
		t.Fatalf("unexpected capabilities payload type %T in %+v", rotatedAgent["capabilities"], rotatedAgent)
	}

	rawMessages, err := store.ListWorkspaceMessages(authCtx, "ws-agent-rotate-handler", "security", 10)
	if err != nil {
		t.Fatalf("list raw workspace messages: %v", err)
	}
	var storedNotice *sqlite.MessageRecord
	for i := range rawMessages {
		if rawMessages[i].MessageID == messageID {
			storedNotice = &rawMessages[i]
			break
		}
	}
	if storedNotice == nil {
		t.Fatalf("expected private system notice with message id %s in raw storage, got %+v", messageID, rawMessages)
	}
	if storedNotice.FromAgentID != "system" || storedNotice.ToAgentID != "agent-rotate" || storedNotice.ContentType != "application/vnd.rhizome.auth-token" {
		t.Fatalf("unexpected stored security notice metadata: %+v", storedNotice)
	}
	if !strings.Contains(storedNotice.Content, newToken) {
		t.Fatalf("expected raw security notice to contain rotated token, got %+v", storedNotice)
	}

	runtimeEvent := mustRuntimeEvent(t, authCtx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-agent-rotate-handler",
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    messageID,
		Limit:       1,
	})
	assertServerRuntimeEventAuthorityMetadata(t, runtimeEvent, authority)
	live := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, live, runtimeEvent, "agent.message", "agent-rotate")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, live.PayloadJSON), runtimeEvent.PayloadJSON)

	listed, rpcErr := h.workspaceMessagesList(authCtx, mustJSONRaw(workspaceMessagesListParams{
		WorkspaceID: "ws-agent-rotate-handler",
		Channel:     "security",
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMessagesList rpc error: %+v", rpcErr)
	}
	listedPayload, ok := listed.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspace messages result type %T", listed)
	}
	messages, ok := listedPayload["messages"].([]sqlite.MessageRecord)
	if !ok {
		t.Fatalf("unexpected messages payload type %T", listedPayload["messages"])
	}
	var redactedNotice *sqlite.MessageRecord
	for i := range messages {
		if messages[i].MessageID == messageID {
			redactedNotice = &messages[i]
			break
		}
	}
	if redactedNotice == nil {
		t.Fatalf("expected redacted notice in workspace list, got %+v", messages)
	}
	if redactedNotice.Content != "[redacted security notice]" || redactedNotice.MetadataJSON != "{}" {
		t.Fatalf("expected redacted security notice, got %+v", redactedNotice)
	}

	if _, err := store.AuthenticateAccessToken(authCtx, initialAgent.Token); err != nil {
		t.Fatalf("expected old agent token to stay valid until rotated token is used: %v", err)
	}
	if _, err := store.AuthenticateAccessToken(authCtx, newToken); err != nil {
		t.Fatalf("authenticate rotated agent token: %v", err)
	}
	if _, err := store.AuthenticateAccessToken(ctx, initialAgent.Token); err == nil {
		t.Fatal("expected old agent token to be revoked after rotated token use")
	}

	secondRotateResult, rpcErr := h.workspaceAuthAgentTokenRotate(authCtx, mustJSONRaw(workspaceAgentTokenRotateParams{
		AgentID: "agent-rotate",
	}))
	if rpcErr != nil {
		t.Fatalf("second workspaceAuthAgentTokenRotate rpc error: %+v", rpcErr)
	}
	secondRotatePayload, ok := secondRotateResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second rotate result type %T", secondRotateResult)
	}
	secondToken, _ := secondRotatePayload["token"].(string)
	secondMessageID, _ := secondRotatePayload["message_id"].(string)
	if secondToken == "" || secondMessageID == "" {
		t.Fatalf("expected second rotate payload to include token and message id, got %+v", secondRotatePayload)
	}

	secondRuntimeEvent := mustRuntimeEvent(t, authCtx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-agent-rotate-handler",
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    secondMessageID,
		Limit:       1,
	})
	assertServerRuntimeEventAuthorityMetadata(t, secondRuntimeEvent, authority)
	secondLive := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, secondLive, secondRuntimeEvent, "agent.message", "agent-rotate")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondLive.PayloadJSON), secondRuntimeEvent.PayloadJSON)
	if secondRuntimeEvent.EventID == runtimeEvent.EventID || secondRuntimeEvent.IngestSeq <= runtimeEvent.IngestSeq {
		t.Fatalf("expected second rotate notice to mirror a newly appended runtime row, got first=%+v second=%+v", runtimeEvent, secondRuntimeEvent)
	}

	secondRawMessages, err := store.ListWorkspaceMessages(authCtx, "ws-agent-rotate-handler", "security", 20)
	if err != nil {
		t.Fatalf("list raw workspace messages after second rotate: %v", err)
	}
	var secondStoredNotice *sqlite.MessageRecord
	for i := range secondRawMessages {
		if secondRawMessages[i].MessageID == secondMessageID {
			secondStoredNotice = &secondRawMessages[i]
			break
		}
	}
	if secondStoredNotice == nil {
		t.Fatalf("expected second private system notice with message id %s in raw storage, got %+v", secondMessageID, secondRawMessages)
	}
	if !strings.Contains(secondStoredNotice.Content, secondToken) {
		t.Fatalf("expected second raw security notice to contain rotated token, got %+v", secondStoredNotice)
	}

	if _, err := store.AuthenticateAccessToken(authCtx, newToken); err != nil {
		t.Fatalf("expected first rotated token to stay valid until second rotated token is used: %v", err)
	}
	if _, err := store.AuthenticateAccessToken(authCtx, secondToken); err != nil {
		t.Fatalf("authenticate second rotated agent token: %v", err)
	}
	if _, err := store.AuthenticateAccessToken(ctx, newToken); err == nil {
		t.Fatal("expected first rotated token to be revoked after second rotated token use")
	}
}

func TestResolveSecurityContextUsesAuthenticatedPrincipalActorAndWorkspace(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-profile-scope",
		Title:       "Profile Scope",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	authCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-profile-scope",
		PrincipalType: "human",
		PrincipalID:   "human-123",
		DisplayName:   "Alice",
	})

	workspace, actorType, actorID, err := h.resolveSecurityContext(authCtx, "ws-profile-scope", "Profile Scope", "forged-user")
	if err != nil {
		t.Fatalf("resolve security context: %v", err)
	}
	if workspace.WorkspaceID != "ws-profile-scope" {
		t.Fatalf("expected authenticated workspace, got %+v", workspace)
	}
	if actorType != "human" || actorID != "human-123" {
		t.Fatalf("expected principal identity from auth context, got actorType=%q actorID=%q", actorType, actorID)
	}

	if _, _, _, err := h.resolveSecurityContext(authCtx, "ws-other", "", "forged-user"); err == nil {
		t.Fatal("expected cross-workspace security resolution to fail")
	}

	agentCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-profile-scope",
		PrincipalType: "agent",
		PrincipalID:   "agent-123",
		TokenID:       "token-agent",
	})
	if _, _, _, err := h.resolveSecurityContext(agentCtx, "ws-profile-scope", "Profile Scope", "forged-user"); !errors.Is(err, errWorkspaceAccessDenied) {
		t.Fatalf("expected agent principal to be denied for workspace security, got %v", err)
	}
}

func TestResolveWorkspaceUsesExactIdentifierAndRejectsAliasFallback(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	for _, workspace := range []sqlite.WorkspaceCreateInput{
		{WorkspaceID: "ws-lookup-a", Title: "Lookup Workspace", CreatedBy: "developer"},
		{WorkspaceID: "ws-lookup-b", Title: "Lookup Workspace", CreatedBy: "developer"},
	} {
		if err := store.CreateWorkspace(ctx, workspace); err != nil {
			t.Fatalf("create workspace %s: %v", workspace.WorkspaceID, err)
		}
	}

	workspace, err := h.resolveWorkspace(ctx, "ws-lookup-a", "")
	if err != nil {
		t.Fatalf("resolve workspace by id: %v", err)
	}
	if workspace.WorkspaceID != "ws-lookup-a" {
		t.Fatalf("expected exact workspace id lookup, got %+v", workspace)
	}

	if _, err := h.resolveWorkspace(ctx, "", "Lookup Workspace"); !errors.Is(err, sqlite.ErrWorkspaceRefAmbiguous) {
		t.Fatalf("expected ambiguous title lookup to fail, got %v", err)
	}
	if _, err := h.resolveWorkspace(ctx, "ws-lookup-a", "Different Title"); !errors.Is(err, sqlite.ErrWorkspaceRefAmbiguous) {
		t.Fatalf("expected mismatched workspace alias to fail, got %v", err)
	}
	if _, err := h.resolveWorkspace(ctx, "ws-missing", "Lookup Workspace"); !errors.Is(err, sqlite.ErrWorkspaceNotFound) {
		t.Fatalf("expected missing id lookup to stay on id path, got %v", err)
	}
}

func TestResolveWorkspaceUnifiedRefPrefersSingleActiveTitleMatch(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	for _, workspace := range []sqlite.WorkspaceCreateInput{
		{
			WorkspaceID: "ws-rhizome-archived",
			Title:       "Rhizome Main",
			CreatedBy:   "developer",
			Status:      "ARCHIVED",
		},
		{
			WorkspaceID: "ws-rhizome-active",
			Title:       "Rhizome Main",
			CreatedBy:   "developer",
			Status:      "ACTIVE",
		},
	} {
		if err := store.CreateWorkspace(ctx, workspace); err != nil {
			t.Fatalf("create workspace %s: %v", workspace.WorkspaceID, err)
		}
	}

	workspace, err := h.resolveWorkspace(ctx, "Rhizome Main", "Rhizome Main")
	if err != nil {
		t.Fatalf("resolve unified workspace ref: %v", err)
	}
	if workspace.WorkspaceID != "ws-rhizome-active" {
		t.Fatalf("expected active workspace to win unified ref fallback, got %+v", workspace)
	}
}

func TestWorkspaceSecurityHandlersRejectAgentPrincipals(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-security-agent-deny",
		Title:       "Workspace Security Agent Deny",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	auth, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-security-agent-deny",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-security",
		DisplayName:       "Agent Security",
		OwnerUserID:       "developer",
		IPAddress:         "198.51.100.99",
		UserAgent:         "rhizome-agent/acl-test",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	logsHandler := AuthMiddlewareWithStore(store, h.ServeWorkspaceSecurityLogsHTTP())
	logsReq := httptest.NewRequest(http.MethodPost, "/api/workspace/security/logs", bytes.NewReader([]byte(`{"workspace_id":"ws-security-agent-deny","limit":5}`)))
	logsReq.Header.Set("Authorization", "Bearer "+auth.Token)
	logsResp := httptest.NewRecorder()
	logsHandler.ServeHTTP(logsResp, logsReq)
	if logsResp.Code != http.StatusForbidden {
		t.Fatalf("expected logs endpoint to reject agent principal, got %d body=%s", logsResp.Code, logsResp.Body.String())
	}
	if !bytes.Contains(logsResp.Body.Bytes(), []byte("workspace access denied")) {
		t.Fatalf("expected workspace access denied on logs endpoint, got %s", logsResp.Body.String())
	}

	updateHandler := AuthMiddlewareWithStore(store, h.ServeWorkspaceSecurityUpdateHTTP())
	updateReq := httptest.NewRequest(http.MethodPost, "/api/workspace/security/update", bytes.NewReader([]byte(`{"workspace_id":"ws-security-agent-deny","description":"forbidden"}`)))
	updateReq.Header.Set("Authorization", "Bearer "+auth.Token)
	updateResp := httptest.NewRecorder()
	updateHandler.ServeHTTP(updateResp, updateReq)
	if updateResp.Code != http.StatusForbidden {
		t.Fatalf("expected update endpoint to reject agent principal, got %d body=%s", updateResp.Code, updateResp.Body.String())
	}
	if !bytes.Contains(updateResp.Body.Bytes(), []byte("workspace access denied")) {
		t.Fatalf("expected workspace access denied on update endpoint, got %s", updateResp.Body.String())
	}
}
