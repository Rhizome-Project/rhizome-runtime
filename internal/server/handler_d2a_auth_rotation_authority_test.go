package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceAuthAgentTokenRotateRejectsMissingWorkspaceAuthorityWithNoSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	authCtx, workspaceID, agentID, initialToken := seedServerAgentTokenRotationScenario(t, store, "ws-d2a-auth-rotate-missing-authority-handler")

	if _, err := store.DB().ExecContext(authCtx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeTokens := countServerAgentTokens(t, authCtx, store, workspaceID, agentID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, authCtx, store, workspaceID)

	result, rpcErr := h.workspaceAuthAgentTokenRotate(authCtx, mustJSONRaw(workspaceAgentTokenRotateParams{
		AgentID: agentID,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on missing-authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.auth.agent_token.rotate")
	if after := countServerAgentTokens(t, authCtx, store, workspaceID, agentID); after != beforeTokens {
		t.Fatalf("expected token count to stay unchanged after missing-authority reject, before=%d after=%d", beforeTokens, after)
	}
	assertNoServerRotateAgentMessageSideEffects(t, authCtx, store, workspaceID, agentID)
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, authCtx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
	if _, err := store.AuthenticateAccessToken(authCtx, initialToken); err != nil {
		t.Fatalf("expected initial token to remain valid after missing-authority reject: %v", err)
	}
}

func TestWorkspaceAuthAgentTokenRotateRejectsStaleWorkspaceAuthorityWithNoSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	authCtx, workspaceID, agentID, initialToken := seedServerAgentTokenRotationScenario(t, store, "ws-d2a-auth-rotate-stale-authority-handler")

	current := claimServerTestWorkspaceAuthority(t, authCtx, store, workspaceID)
	beforeTokens := countServerAgentTokens(t, authCtx, store, workspaceID, agentID)
	transferServerTestWorkspaceAuthorityToPeer(t, authCtx, store, workspaceID, current, "authnode-998-1138")

	result, rpcErr := h.workspaceAuthAgentTokenRotate(authCtx, mustJSONRaw(workspaceAgentTokenRotateParams{
		AgentID: agentID,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale-authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.auth.agent_token.rotate")
	if after := countServerAgentTokens(t, authCtx, store, workspaceID, agentID); after != beforeTokens {
		t.Fatalf("expected token count to stay unchanged after stale-authority reject, before=%d after=%d", beforeTokens, after)
	}
	assertNoServerRotateAgentMessageSideEffects(t, authCtx, store, workspaceID, agentID)
	assertServerTaskAuthorityRejectEvent(t, authCtx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if _, err := store.AuthenticateAccessToken(authCtx, initialToken); err != nil {
		t.Fatalf("expected initial token to remain valid after stale-authority reject: %v", err)
	}
}

func seedServerAgentTokenRotationScenario(t *testing.T, store *sqlite.Store, workspaceID string) (context.Context, string, string, string) {
	t.Helper()

	ctx := context.WithValue(context.Background(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  "198.51.100.140",
		UserAgent: "rhizome-web/7.0",
	})
	authCtx := context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   workspaceID,
		PrincipalType: "human",
		PrincipalID:   "developer",
	})

	if err := store.CreateWorkspace(authCtx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Auth Rotate Authority",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	human, err := store.RegisterHuman(authCtx, sqlite.HumanRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "198.51.100.141",
		UserAgent:         "rhizome-web/7.0",
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
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-rotate",
		DisplayName:       "Rotating Agent",
		OwnerUserID:       human.UserID,
		IPAddress:         "198.51.100.142",
		UserAgent:         "rhizome-agent/7.0",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if initialAgent.Token == "" {
		t.Fatal("expected initial agent token")
	}

	return authCtx, workspaceID, "agent-rotate", initialAgent.Token
}

func countServerAgentTokens(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) int {
	t.Helper()

	tokens, err := store.ListAccessTokens(ctx, workspaceID, "agent", agentID, true, 20)
	if err != nil {
		t.Fatalf("list access tokens for %s/%s: %v", workspaceID, agentID, err)
	}
	return len(tokens)
}

func assertNoServerRotateAgentMessageSideEffects(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_message.sent events after reject: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no agent_message.sent events after reject, got %+v", events)
	}
	rawMessages, err := store.ListWorkspaceMessages(ctx, workspaceID, "security", 10)
	if err != nil {
		t.Fatalf("list workspace security messages after reject: %v", err)
	}
	if len(rawMessages) != 0 {
		t.Fatalf("expected no workspace security messages after reject, got %+v", rawMessages)
	}
	inbox, err := store.PollMessages(ctx, workspaceID, agentID, "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent inbox after reject: %v", err)
	}
	if len(inbox) != 0 {
		t.Fatalf("expected no agent inbox notices after reject, got %+v", inbox)
	}
}
