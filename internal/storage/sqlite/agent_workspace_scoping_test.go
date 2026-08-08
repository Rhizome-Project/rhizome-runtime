package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentScopingAcrossWorkspaces(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	for _, ws := range []string{"ws-alpha", "ws-beta"} {
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: ws,
			Title:       ws,
			CreatedBy:   "developer",
		}); err != nil {
			t.Fatalf("create workspace %s: %v", ws, err)
		}
	}

	for _, tc := range []struct {
		workspaceID string
		displayName string
		bio         string
		tag         string
	}{
		{workspaceID: "ws-alpha", displayName: "Shared Alpha", bio: "alpha bio", tag: "alpha"},
		{workspaceID: "ws-beta", displayName: "Shared Beta", bio: "beta bio", tag: "beta"},
	} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: tc.workspaceID,
			AgentID:     "agent-shared",
			OwnerUserID: "developer",
			DisplayName: tc.displayName,
		}); err != nil {
			t.Fatalf("register agent in %s: %v", tc.workspaceID, err)
		}
		if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
			WorkspaceID: tc.workspaceID,
			AgentID:     "agent-shared",
			Bio:         tc.bio,
			Tags:        []string{tc.tag},
		}); err != nil {
			t.Fatalf("upsert profile in %s: %v", tc.workspaceID, err)
		}
		if err := store.EnsureAgentLimitGroup(ctx, tc.workspaceID, "agent-shared", tc.displayName); err != nil {
			t.Fatalf("ensure limit group in %s: %v", tc.workspaceID, err)
		}
	}

	alphaProfile, err := store.GetAgentProfile(ctx, "ws-alpha", "agent-shared")
	if err != nil {
		t.Fatalf("get alpha profile: %v", err)
	}
	if alphaProfile.Bio != "alpha bio" {
		t.Fatalf("expected alpha bio, got %q", alphaProfile.Bio)
	}
	betaProfile, err := store.GetAgentProfile(ctx, "ws-beta", "agent-shared")
	if err != nil {
		t.Fatalf("get beta profile: %v", err)
	}
	if betaProfile.Bio != "beta bio" {
		t.Fatalf("expected beta bio, got %q", betaProfile.Bio)
	}

	alphaMatches, err := store.SearchAgentsByTags(ctx, "ws-alpha", []string{"alpha"})
	if err != nil {
		t.Fatalf("search alpha tags: %v", err)
	}
	if len(alphaMatches) != 1 || alphaMatches[0].DisplayName != "Shared Alpha" {
		t.Fatalf("expected alpha-scoped search result, got %+v", alphaMatches)
	}
	betaMatches, err := store.SearchAgentsByTags(ctx, "ws-beta", []string{"beta"})
	if err != nil {
		t.Fatalf("search beta tags: %v", err)
	}
	if len(betaMatches) != 1 || betaMatches[0].DisplayName != "Shared Beta" {
		t.Fatalf("expected beta-scoped search result, got %+v", betaMatches)
	}

	alphaGroup, err := store.GetAgentLimitGroup(ctx, "ws-alpha", "agent-shared")
	if err != nil {
		t.Fatalf("get alpha limit group: %v", err)
	}
	betaGroup, err := store.GetAgentLimitGroup(ctx, "ws-beta", "agent-shared")
	if err != nil {
		t.Fatalf("get beta limit group: %v", err)
	}
	if alphaGroup == nil || betaGroup == nil {
		t.Fatalf("expected limit groups in both workspaces, got alpha=%+v beta=%+v", alphaGroup, betaGroup)
	}
	if alphaGroup.GroupID == betaGroup.GroupID {
		t.Fatalf("expected workspace-scoped singleton limit groups, got duplicate id %q", alphaGroup.GroupID)
	}

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-alpha",
		AgentID:     "agent-shared",
		WorkspaceID: "ws-alpha",
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create alpha session: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-beta",
		AgentID:     "agent-shared",
		WorkspaceID: "ws-beta",
		StartedAt:   time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create beta session: %v", err)
	}

	alphaSessions, err := store.ListAgentSessions(ctx, "ws-alpha", "agent-shared", 10)
	if err != nil {
		t.Fatalf("list alpha sessions: %v", err)
	}
	if len(alphaSessions) != 1 || alphaSessions[0].SessionID != "sess-alpha" {
		t.Fatalf("expected only alpha session, got %+v", alphaSessions)
	}
	betaSessions, err := store.ListAgentSessions(ctx, "ws-beta", "agent-shared", 10)
	if err != nil {
		t.Fatalf("list beta sessions: %v", err)
	}
	if len(betaSessions) != 1 || betaSessions[0].SessionID != "sess-beta" {
		t.Fatalf("expected only beta session, got %+v", betaSessions)
	}
}
