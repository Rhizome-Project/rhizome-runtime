package sqlite_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestHumanProfileGetUpdateAndOwnedAgents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-human-profile",
		Title:       "Human Profile",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	reg, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-profile",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "198.51.100.40",
		UserAgent:         "rhizome-web/1.0",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	for _, agent := range []sqlite.AgentRegisterInput{
		{WorkspaceID: "ws-human-profile", AgentID: "agent-a", OwnerUserID: reg.UserID, DisplayName: "Alpha Agent"},
		{WorkspaceID: "ws-human-profile", AgentID: "agent-b", OwnerUserID: reg.UserID, DisplayName: "Beta Agent"},
	} {
		if err := store.RegisterAgent(ctx, agent); err != nil {
			t.Fatalf("register agent %s: %v", agent.AgentID, err)
		}
	}

	profile, err := store.GetHumanProfile(ctx, "ws-human-profile", reg.UserID)
	if err != nil {
		t.Fatalf("get human profile: %v", err)
	}
	if profile.UserID != reg.UserID {
		t.Fatalf("expected immutable user_id %q, got %q", reg.UserID, profile.UserID)
	}
	if profile.Username != "alice" {
		t.Fatalf("expected immutable username alice, got %q", profile.Username)
	}
	if profile.DisplayName != "Alice" {
		t.Fatalf("expected display name Alice, got %q", profile.DisplayName)
	}
	if profile.AgentCount != 2 || len(profile.Agents) != 2 {
		t.Fatalf("expected 2 owned agents, got count=%d agents=%+v", profile.AgentCount, profile.Agents)
	}
	for _, agent := range profile.Agents {
		if agent.OwnerUserID != reg.UserID {
			t.Fatalf("expected owned agent to reference owner user id %q, got %+v", reg.UserID, agent)
		}
	}

	updated, err := store.UpdateHumanProfile(ctx, sqlite.HumanProfileUpdateInput{
		WorkspaceID: "ws-human-profile",
		UserID:      reg.UserID,
		DisplayName: "Alice Renamed",
		Password:    "alice-new-password",
		IPAddress:   "198.51.100.41",
		UserAgent:   "rhizome-web/2.0",
	})
	if err != nil {
		t.Fatalf("update human profile: %v", err)
	}
	if updated.UserID != reg.UserID {
		t.Fatalf("expected immutable user_id after update, got %q", updated.UserID)
	}
	if updated.DisplayName != "Alice Renamed" {
		t.Fatalf("expected updated display name, got %q", updated.DisplayName)
	}
	if len(updated.Agents) != 2 {
		t.Fatalf("expected owned agents to remain attached, got %+v", updated.Agents)
	}

	if _, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-human-profile",
		Username:    "Alice Renamed",
		Password:    "alice-password",
	}); err == nil {
		t.Fatal("expected display name to stay out of the login field")
	}
	login, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-human-profile",
		Username:    "alice",
		Password:    "alice-new-password",
	})
	if err != nil {
		t.Fatalf("login updated human: %v", err)
	}
	if login.UserID != reg.UserID {
		t.Fatalf("expected login to preserve user_id, got %q", login.UserID)
	}
	if login.DisplayName != "Alice Renamed" {
		t.Fatalf("expected login to return updated display name, got %+v", login)
	}
}

func TestHumanDisplayNameUniquenessInWorkspace(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-human-display-unique",
		Title:       "Human Display Unique",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	first, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-display-unique",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
	})
	if err != nil {
		t.Fatalf("register first human: %v", err)
	}
	if first.UserID == "" {
		t.Fatal("expected generated user id")
	}

	if _, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-display-unique",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice-2",
		DisplayName:       "  alice  ",
		Password:          "alice-password-2",
	}); !errors.Is(err, sqlite.ErrHumanDisplayNameConflict) {
		t.Fatalf("expected display name conflict on registration, got %v", err)
	}

	second, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-display-unique",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "bob",
		DisplayName:       "Bob",
		Password:          "bob-password",
	})
	if err != nil {
		t.Fatalf("register second human: %v", err)
	}

	if _, err := store.UpdateHumanProfile(ctx, sqlite.HumanProfileUpdateInput{
		WorkspaceID: "ws-human-display-unique",
		UserID:      second.UserID,
		DisplayName: "ALICE",
	}); !errors.Is(err, sqlite.ErrHumanDisplayNameConflict) {
		t.Fatalf("expected display name conflict on update, got %v", err)
	}
}

func TestHumanUsernameUniquenessInWorkspace(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-human-username-unique",
		Title:       "Human Username Unique",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-username-unique",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
	}); err != nil {
		t.Fatalf("register first human: %v", err)
	}

	if _, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-username-unique",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "  ALICE  ",
		DisplayName:       "Alice Two",
		Password:          "alice-password-2",
	}); !errors.Is(err, sqlite.ErrHumanUsernameConflict) {
		t.Fatalf("expected username conflict in one workspace, got %v", err)
	}

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-human-username-unique-2",
		Title:       "Human Username Unique 2",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create second workspace: %v", err)
	}

	if _, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-username-unique-2",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice Workspace Two",
		Password:          "alice-password-3",
	}); err != nil {
		t.Fatalf("expected same username in another workspace to succeed: %v", err)
	}
}
