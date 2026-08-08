package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentProfileSettingsUpdateAndGetRoundTrip(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-profile-roundtrip",
		Title:       "Profile Roundtrip",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-profile-roundtrip",
		AgentID:     "agent-profile",
		OwnerUserID: "human-owner-1",
		DisplayName: "Profile Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    "ws-profile-roundtrip",
		AgentID:        "agent-profile",
		Bio:            "Builds and reviews auth flows",
		Specialization: "auth",
		OwnerName:      "Alice",
		OwnerContact:   "alice@example.internal",
		AvatarURL:      "https://example.internal/avatar.png",
		Links:          []string{"https://example.internal/docs", "https://example.internal/runbook"},
		Tags:           []string{"auth", "security", "profiles"},
		ToolsAccess:    []string{"dashboard", "rpc.describe"},
		Metadata: map[string]any{
			"locale": "ru",
			"tier":   "trusted",
		},
	}); err != nil {
		t.Fatalf("upsert agent profile: %v", err)
	}

	profile, err := store.GetAgentProfile(ctx, "ws-profile-roundtrip", "agent-profile")
	if err != nil {
		t.Fatalf("get agent profile: %v", err)
	}

	if profile.WorkspaceID != "ws-profile-roundtrip" || profile.AgentID != "agent-profile" {
		t.Fatalf("unexpected profile identity: %+v", profile)
	}
	if profile.Bio != "Builds and reviews auth flows" || profile.Specialization != "auth" {
		t.Fatalf("unexpected profile core fields: %+v", profile)
	}
	if profile.OwnerName != "Alice" || profile.OwnerContact != "alice@example.internal" {
		t.Fatalf("unexpected owner fields: %+v", profile)
	}
	if len(profile.Links) != 2 || profile.Links[0] != "https://example.internal/docs" {
		t.Fatalf("unexpected links: %+v", profile.Links)
	}
	if len(profile.Tags) != 3 || profile.Tags[2] != "profiles" {
		t.Fatalf("unexpected tags: %+v", profile.Tags)
	}
	if len(profile.ToolsAccess) != 2 || profile.ToolsAccess[1] != "rpc.describe" {
		t.Fatalf("unexpected tools access: %+v", profile.ToolsAccess)
	}
	if profile.Metadata["locale"] != "ru" || profile.Metadata["tier"] != "trusted" {
		t.Fatalf("unexpected metadata: %+v", profile.Metadata)
	}
	if profile.UpdatedAt == "" {
		t.Fatal("expected updated_at to be populated")
	}
}

func TestWorkspaceHumanDisplayNameUniquenessIsScopedPerWorkspace(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	for _, workspace := range []sqlite.WorkspaceCreateInput{
		{WorkspaceID: "ws-profile-unique-a", Title: "Profile Unique A", CreatedBy: "developer"},
		{WorkspaceID: "ws-profile-unique-b", Title: "Profile Unique B", CreatedBy: "developer"},
	} {
		if err := store.CreateWorkspace(ctx, workspace); err != nil {
			t.Fatalf("create workspace %s: %v", workspace.WorkspaceID, err)
		}
	}

	if _, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-profile-unique-a",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice-smith",
		DisplayName:       "Alice Smith",
		Password:          "alice-password",
	}); err != nil {
		t.Fatalf("register initial human: %v", err)
	}

	if _, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-profile-unique-a",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice-smith-2",
		DisplayName:       "  alice   smith  ",
		Password:          "alice-password-2",
	}); err == nil {
		t.Fatal("expected duplicate normalized display name in one workspace to fail")
	}

	if _, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-profile-unique-b",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice-smith",
		DisplayName:       "alice smith",
		Password:          "alice-password-3",
	}); err != nil {
		t.Fatalf("expected same normalized display name in another workspace to succeed: %v", err)
	}

	login, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-profile-unique-a",
		Username:    " ALICE-SMITH ",
		Password:    "alice-password",
	})
	if err != nil {
		t.Fatalf("login with normalized username: %v", err)
	}
	if login.DisplayName != "Alice Smith" {
		t.Fatalf("expected canonical display name on login, got %+v", login)
	}
	if _, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-profile-unique-a",
		Username:    "Alice Smith",
		Password:    "alice-password",
	}); err == nil {
		t.Fatal("expected display name to be rejected as a login once username diverges")
	}
}

func TestWorkspaceOwnedAgentsCanBeDerivedFromOwnerUserID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-profile-owned-agents",
		Title:       "Owned Agents",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	alice, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-profile-owned-agents",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
	})
	if err != nil {
		t.Fatalf("register alice: %v", err)
	}
	bob, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-profile-owned-agents",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "bob",
		DisplayName:       "Bob",
		Password:          "bob-password",
	})
	if err != nil {
		t.Fatalf("register bob: %v", err)
	}

	for _, agent := range []sqlite.AgentRegisterInput{
		{WorkspaceID: "ws-profile-owned-agents", AgentID: "agent-alice-1", OwnerUserID: alice.UserID, DisplayName: "Alice One"},
		{WorkspaceID: "ws-profile-owned-agents", AgentID: "agent-alice-2", OwnerUserID: alice.UserID, DisplayName: "Alice Two"},
		{WorkspaceID: "ws-profile-owned-agents", AgentID: "agent-bob-1", OwnerUserID: bob.UserID, DisplayName: "Bob One"},
	} {
		if err := store.RegisterAgent(ctx, agent); err != nil {
			t.Fatalf("register agent %s: %v", agent.AgentID, err)
		}
	}

	agents, err := store.ListWorkspaceAgents(ctx, "ws-profile-owned-agents")
	if err != nil {
		t.Fatalf("list workspace agents: %v", err)
	}

	var aliceOwned []string
	var bobOwned []string
	for _, agent := range agents {
		switch agent.OwnerUserID {
		case alice.UserID:
			aliceOwned = append(aliceOwned, agent.AgentID)
		case bob.UserID:
			bobOwned = append(bobOwned, agent.AgentID)
		}
	}

	if len(aliceOwned) != 2 {
		t.Fatalf("expected 2 alice-owned agents, got %+v", aliceOwned)
	}
	if len(bobOwned) != 1 || bobOwned[0] != "agent-bob-1" {
		t.Fatalf("expected bob-owned agent slice, got %+v", bobOwned)
	}
}
