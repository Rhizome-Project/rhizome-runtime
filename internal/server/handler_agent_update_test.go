package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceAuthAgentUpdatePreservesOwnershipAndPresence(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-agent-update-owned"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Update Owned",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	owner, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "203.0.113.30",
		UserAgent:         "rhizome-web-agent-update/1.0",
	})
	if err != nil {
		t.Fatalf("register human owner: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:     workspaceID,
		AgentID:         "agent-update-owned",
		OwnerUserID:     owner.UserID,
		DisplayName:     "Partner Worker",
		Role:            "reviewer",
		ProtocolVersion: "partner-runtime/v1",
		Capabilities:    []string{"analysis", "tool.call"},
		Summary:         "registered summary",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-update-owned",
		Status:      "active",
		Summary:     "live summary",
	}); err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}

	before, err := store.GetAgent(ctx, workspaceID, "agent-update-owned")
	if err != nil {
		t.Fatalf("get agent before update: %v", err)
	}
	if before.LastSeenAt == nil || *before.LastSeenAt == "" || !before.IsOnline {
		t.Fatalf("expected heartbeat-backed presence before update, got %+v", before)
	}
	beforeLastSeenAt := *before.LastSeenAt

	authPrincipal, err := store.AuthenticateAccessToken(ctx, owner.Token)
	if err != nil {
		t.Fatalf("authenticate human token: %v", err)
	}
	authCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   authPrincipal.WorkspaceID,
		PrincipalType: authPrincipal.SubjectType,
		PrincipalID:   authPrincipal.SubjectID,
		TokenID:       authPrincipal.TokenID,
		TokenPrefix:   authPrincipal.TokenPrefix,
		DisplayName:   authPrincipal.DisplayName,
	})

	result, rpcErr := h.workspaceAuthAgentUpdate(authCtx, mustJSONRaw(map[string]any{
		"agent_id":         "agent-update-owned",
		"display_name":     "Partner Planner",
		"role":             "planner",
		"protocol_version": "partner-runtime/v2",
		"capabilities":     []string{"analysis", "coordination"},
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceAuthAgentUpdate rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected update result type %T", result)
	}
	updated, ok := payload["agent"].(sqlite.AgentRecord)
	if !ok {
		t.Fatalf("unexpected update payload: %+v", payload)
	}
	if updated.OwnerUserID != owner.UserID {
		t.Fatalf("expected update to preserve owner %q, got %+v", owner.UserID, updated)
	}
	if updated.DisplayName != "Partner Planner" || updated.Role != "planner" {
		t.Fatalf("expected update to change display/role, got %+v", updated)
	}
	if updated.ProtocolVersion != "partner-runtime/v2" {
		t.Fatalf("expected update to change protocol version, got %+v", updated)
	}
	if got := updated.Capabilities; len(got) != 2 || got[0] != "analysis" || got[1] != "coordination" {
		t.Fatalf("expected update to change capabilities, got %+v", got)
	}
	if updated.Summary != "live summary" || updated.Status != "ACTIVE" {
		t.Fatalf("expected update to preserve live summary/status, got %+v", updated)
	}
	if updated.LastSeenAt == nil || *updated.LastSeenAt != beforeLastSeenAt || !updated.IsOnline {
		t.Fatalf("expected update to preserve live presence, got %+v", updated)
	}
	events, err := store.ListAuditEvents(ctx, sqlite.AuditEventFilter{
		EntityType: "agent",
		EntityID:   "agent-update-owned",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	var foundMetadataUpdate bool
	var registeredCount int
	for _, event := range events {
		switch event.EventType {
		case "agent_metadata_updated":
			foundMetadataUpdate = true
			if event.ActorID != owner.UserID {
				t.Fatalf("expected metadata update audit actor %q, got %+v", owner.UserID, event)
			}
		case "agent_registered":
			registeredCount++
		}
	}
	if !foundMetadataUpdate {
		t.Fatalf("expected agent_metadata_updated audit event, got %+v", events)
	}
	if registeredCount != 1 {
		t.Fatalf("expected exactly one agent_registered event from initial register, got %+v", events)
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
	var inventory map[string]any
	for _, item := range agentItems {
		if item["agent_id"] == "agent-update-owned" {
			inventory = item
			break
		}
	}
	if inventory == nil {
		t.Fatalf("expected updated agent in profile inventory, got %+v", agentItems)
	}
	if inventory["display_name"] != "Partner Planner" || inventory["role"] != "planner" {
		t.Fatalf("expected profile inventory to reflect updated display/role, got %+v", inventory)
	}
	if inventory["protocol_version"] != "partner-runtime/v2" {
		t.Fatalf("expected profile inventory to reflect updated protocol version, got %+v", inventory)
	}
	caps, ok := inventory["capabilities"].([]string)
	if !ok || len(caps) != 2 || caps[0] != "analysis" || caps[1] != "coordination" {
		t.Fatalf("expected profile inventory to reflect updated capabilities, got %+v", inventory["capabilities"])
	}
	if inventory["summary"] != "live summary" || inventory["liveness_status"] != "ONLINE" {
		t.Fatalf("expected profile inventory to preserve live summary/liveness, got %+v", inventory)
	}
}

func TestWorkspaceAuthAgentUpdateRejectsUnownedAgent(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-agent-update-unowned"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Update Unowned",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	owner, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "203.0.113.31",
		UserAgent:         "rhizome-web-agent-update/1.0",
	})
	if err != nil {
		t.Fatalf("register human owner: %v", err)
	}
	intruder, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "bob",
		DisplayName:       "Bob",
		Password:          "bob-password",
		IPAddress:         "203.0.113.32",
		UserAgent:         "rhizome-web-agent-update/1.0",
	})
	if err != nil {
		t.Fatalf("register human intruder: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:     workspaceID,
		AgentID:         "agent-update-locked",
		OwnerUserID:     owner.UserID,
		DisplayName:     "Partner Worker",
		Role:            "reviewer",
		ProtocolVersion: "partner-runtime/v1",
		Capabilities:    []string{"analysis", "tool.call"},
		Summary:         "registered summary",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	authPrincipal, err := store.AuthenticateAccessToken(ctx, intruder.Token)
	if err != nil {
		t.Fatalf("authenticate intruder token: %v", err)
	}
	authCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   authPrincipal.WorkspaceID,
		PrincipalType: authPrincipal.SubjectType,
		PrincipalID:   authPrincipal.SubjectID,
		TokenID:       authPrincipal.TokenID,
		TokenPrefix:   authPrincipal.TokenPrefix,
		DisplayName:   authPrincipal.DisplayName,
	})

	result, rpcErr := h.workspaceAuthAgentUpdate(authCtx, mustJSONRaw(map[string]any{
		"agent_id":     "agent-update-locked",
		"display_name": "Hijacked Name",
	}))
	if rpcErr == nil {
		t.Fatalf("expected unowned update to fail, got result %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != errWorkspaceAccessDenied.Error() {
		t.Fatalf("expected workspace access denied, got %+v", rpcErr)
	}

	agent, err := store.GetAgent(ctx, workspaceID, "agent-update-locked")
	if err != nil {
		t.Fatalf("get agent after rejected update: %v", err)
	}
	if agent.DisplayName != "Partner Worker" || agent.OwnerUserID != owner.UserID {
		t.Fatalf("expected rejected update to preserve agent truth, got %+v", agent)
	}
}
