package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRotateAgentAccessTokenRejectsMissingWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	workspaceID, agentID, ownerUserID := seedAgentTokenRotationScenario(t, ctx, store, "ws-d2a-auth-rotate-missing-authority", "agent-rotate-missing-authority")
	beforeTokens := listAgentRotateAccessTokens(t, ctx, store, workspaceID, agentID)

	_, err := store.RotateAgentAccessToken(ctx, sqlite.AgentTokenRotateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ActorType:   "human",
		ActorID:     ownerUserID,
		IPAddress:   "198.51.100.61",
		UserAgent:   "rhizome-web/5.0",
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	var reject *sqlite.AuthorityRejectError
	if !errors.As(err, &reject) || reject == nil || reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %v", err)
	}

	afterTokens := listAgentRotateAccessTokens(t, ctx, store, workspaceID, agentID)
	if len(afterTokens) != len(beforeTokens) {
		t.Fatalf("expected token count to stay unchanged after reject, before=%d after=%d", len(beforeTokens), len(afterTokens))
	}
	assertNoRotationMessageSideEffects(t, ctx, store, workspaceID, agentID)
}

func TestRotateAgentAccessTokenRejectsStaleWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	workspaceID, agentID, ownerUserID := seedAgentTokenRotationScenario(t, ctx, store, "ws-d2a-auth-rotate-stale-authority", "agent-rotate-stale-authority")
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeTokens := listAgentRotateAccessTokens(t, ctx, store, workspaceID, agentID)
	beforeRejects := countAuthorityRejectEventsForRotation(t, ctx, store, workspaceID)

	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-995-5001")

	_, err := store.RotateAgentAccessToken(ctx, sqlite.AgentTokenRotateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ActorType:   "human",
		ActorID:     ownerUserID,
		IPAddress:   "198.51.100.62",
		UserAgent:   "rhizome-web/5.0",
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	var reject *sqlite.AuthorityRejectError
	if !errors.As(err, &reject) || reject == nil || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %v", err)
	}

	afterTokens := listAgentRotateAccessTokens(t, ctx, store, workspaceID, agentID)
	if len(afterTokens) != len(beforeTokens) {
		t.Fatalf("expected token count to stay unchanged after stale reject, before=%d after=%d", len(beforeTokens), len(afterTokens))
	}
	assertNoRotationMessageSideEffects(t, ctx, store, workspaceID, agentID)
	assertAuthorityRejectEventIncrementForRotation(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectStale)
}

func seedAgentTokenRotationScenario(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) (string, string, string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Agent Token Rotation Authority",
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
		IPAddress:         "198.51.100.60",
		UserAgent:         "rhizome-web/5.0",
	})
	if err != nil {
		t.Fatalf("register human owner: %v", err)
	}
	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           agentID,
		DisplayName:       "Rotating Agent",
		OwnerUserID:       owner.UserID,
		IPAddress:         "198.51.100.60",
		UserAgent:         "rhizome-agent/5.0",
	}); err != nil {
		t.Fatalf("register rotating agent: %v", err)
	}
	return workspaceID, agentID, owner.UserID
}

func listAgentRotateAccessTokens(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) []sqlite.AuthTokenRecord {
	t.Helper()

	tokens, err := store.ListAccessTokens(ctx, workspaceID, "agent", agentID, true, 20)
	if err != nil {
		t.Fatalf("list access tokens for %s/%s: %v", workspaceID, agentID, err)
	}
	return tokens
}

func assertNoRotationMessageSideEffects(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_message.sent events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no agent_message.sent events after authority reject, got %+v", events)
	}
	inbox, err := store.PollMessages(ctx, workspaceID, agentID, "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent inbox after authority reject: %v", err)
	}
	if len(inbox) != 0 {
		t.Fatalf("expected no inbox notice after authority reject, got %+v", inbox)
	}
}

func countAuthorityRejectEventsForRotation(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   sqlite.AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list authority rejected events: %v", err)
	}
	return len(events)
}

func assertAuthorityRejectEventIncrementForRotation(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, before int, want sqlite.AuthorityRejectCode) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   sqlite.AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list authority rejected events: %v", err)
	}
	if len(events) != before+1 {
		t.Fatalf("expected authority.rejected count to increment by 1, before=%d after=%d", before, len(events))
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority reject payload: %v", err)
	}
	if payload["reject_code"] != string(want) {
		t.Fatalf("expected authority reject code %q, got %+v", want, payload)
	}
}
