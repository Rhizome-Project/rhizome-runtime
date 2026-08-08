package sqlite_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceSecurityDefaultPasswordAndRotation(t *testing.T) {
	t.Parallel()
	if sqlite.DefaultWorkspacePassword == "14"+"88" || strings.TrimSpace(sqlite.DefaultWorkspacePassword) == "" {
		t.Fatal("default workspace password must be a non-empty ephemeral value")
	}

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-security-password",
		Title:       "Workspace Security Password",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	settings, err := store.GetWorkspaceSecuritySettings(ctx, "ws-security-password")
	if err != nil {
		t.Fatalf("get workspace security settings: %v", err)
	}
	if settings.WorkspaceID != "ws-security-password" || settings.PasswordUpdatedAt == "" {
		t.Fatalf("expected seeded workspace security settings, got %+v", settings)
	}

	agent, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-security-password",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-default-pass",
		DisplayName:       "Agent Default Pass",
		OwnerUserID:       "developer",
		IPAddress:         "203.0.113.10",
		UserAgent:         "rhizome-agent/1.0",
	})
	if err != nil {
		t.Fatalf("register agent with default password: %v", err)
	}
	if agent.Token == "" {
		t.Fatal("expected agent registration to issue a token")
	}

	updated, err := store.UpdateWorkspaceSecuritySettings(ctx, sqlite.WorkspaceSecuritySettingsInput{
		WorkspaceID:       "ws-security-password",
		WorkspacePassword: "new-workspace-password",
		UpdatedByType:     "human",
		UpdatedByID:       "developer",
		IPAddress:         "198.51.100.10",
		UserAgent:         "rhizome-test/1.0",
	})
	if err != nil {
		t.Fatalf("update workspace security settings: %v", err)
	}
	if updated.PasswordUpdatedAt == "" {
		t.Fatal("expected password update timestamp")
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-security-password",
		WorkspacePassword: "new-workspace-password",
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "198.51.100.11",
		UserAgent:         "rhizome-web/1.0",
	})
	if err != nil {
		t.Fatalf("register human with rotated password: %v", err)
	}
	if human.Token == "" {
		t.Fatal("expected human registration to issue a token")
	}
}

func TestWorkspaceSecurityUsesExplicitCreationPassword(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const password = "explicit-test-workspace-password"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID:       "ws-explicit-password",
		Title:             "Explicit Password",
		CreatedBy:         "test",
		WorkspacePassword: password,
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-explicit-password",
		WorkspacePassword: "wrong-password",
		AgentID:           "wrong-agent",
	}); err == nil {
		t.Fatal("registration unexpectedly accepted the wrong workspace password")
	}
	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-explicit-password",
		WorkspacePassword: password,
		AgentID:           "expected-agent",
	}); err != nil {
		t.Fatalf("registration rejected the explicit workspace password: %v", err)
	}
}

func TestNewPasswordsEnforceCentralPolicy(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID:       "ws-short-password",
		Title:             "Short Password",
		CreatedBy:         "test",
		WorkspacePassword: "too-short",
	}); err == nil || !strings.Contains(err.Error(), "at least 12 characters") {
		t.Fatalf("short initial workspace password was not rejected: %v", err)
	}

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID:       "ws-password-policy",
		Title:             "Password Policy",
		CreatedBy:         "test",
		WorkspacePassword: "valid-workspace-password",
	}); err != nil {
		t.Fatalf("create workspace with valid password: %v", err)
	}
	if _, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-password-policy",
		WorkspacePassword: "valid-workspace-password",
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "too-short",
	}); err == nil || !strings.Contains(err.Error(), "at least 12 characters") {
		t.Fatalf("short initial human password was not rejected: %v", err)
	}
	if _, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-password-policy",
		WorkspacePassword: "valid-workspace-password",
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          strings.Repeat("a", 257),
	}); err == nil || !strings.Contains(err.Error(), "at most 256 bytes") {
		t.Fatalf("oversized initial human password was not rejected: %v", err)
	}
	if _, err := store.UpdateWorkspaceSecuritySettings(ctx, sqlite.WorkspaceSecuritySettingsInput{
		WorkspaceID:       "ws-password-policy",
		WorkspacePassword: "too-short",
		UpdatedByType:     "human",
		UpdatedByID:       "test",
	}); err == nil || !strings.Contains(err.Error(), "at least 12 characters") {
		t.Fatalf("short workspace password update was not rejected: %v", err)
	}
	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-password-policy",
		WorkspacePassword: "valid-workspace-password",
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "valid-human-password",
	})
	if err != nil {
		t.Fatalf("register human with valid password: %v", err)
	}
	if _, err := store.UpdateHumanProfile(ctx, sqlite.HumanProfileUpdateInput{
		WorkspaceID: "ws-password-policy",
		UserID:      human.UserID,
		DisplayName: "Alice",
		Password:    "too-short",
	}); err == nil || !strings.Contains(err.Error(), "at least 12 characters") {
		t.Fatalf("short human password update was not rejected: %v", err)
	}
}

func TestWorkspaceAuthRegistrationAndLoginIssueTokens(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-human-auth",
		Title:       "Human Auth",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	reg, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-auth",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "203.0.113.20",
		UserAgent:         "rhizome-web/1.0",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}
	if reg.Token == "" {
		t.Fatal("expected human registration token")
	}

	login, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-human-auth",
		Username:    "alice",
		Password:    "alice-password",
		IPAddress:   "203.0.113.20",
		UserAgent:   "rhizome-web/1.0",
	})
	if err != nil {
		t.Fatalf("login human: %v", err)
	}
	if login.Token == "" {
		t.Fatal("expected human login token")
	}
	if login.Token == reg.Token {
		t.Fatal("expected login to issue a distinct token")
	}
	if _, err := store.AuthenticateAccessToken(ctx, reg.Token); err != nil {
		t.Fatalf("authenticate original human token: %v", err)
	}

	principal, err := store.AuthenticateAccessToken(ctx, login.Token)
	if err != nil {
		t.Fatalf("authenticate access token: %v", err)
	}
	if principal.WorkspaceID != "ws-human-auth" || principal.SubjectType != "human" || principal.DisplayName != "Alice" {
		t.Fatalf("unexpected principal record: %+v", principal)
	}
}

func TestRegisterAgentWithWorkspacePasswordPreservesMetadataOnPartialReregister(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-partial-reregister",
		Title:       "Agent Partial Reregister",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	owner, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-agent-partial-reregister",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "198.51.100.70",
		UserAgent:         "rhizome-web/5.0",
	})
	if err != nil {
		t.Fatalf("register owner: %v", err)
	}

	initial, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-agent-partial-reregister",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-partner",
		DisplayName:       "Partner Executor",
		OwnerUserID:       owner.UserID,
		Role:              "reviewer",
		ProtocolVersion:   "partner-runtime/v7",
		Capabilities:      []string{"analysis", "review"},
		Summary:           "partner executor inventory",
		IPAddress:         "198.51.100.71",
		UserAgent:         "rhizome-agent/5.0",
	})
	if err != nil {
		t.Fatalf("initial agent registration: %v", err)
	}
	if initial.DisplayName != "Partner Executor" || initial.Agent.DisplayName != "Partner Executor" {
		t.Fatalf("expected initial registration display name to persist, got %+v", initial)
	}

	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: "ws-agent-partial-reregister",
		AgentID:     "agent-partner",
		Status:      "active",
		Summary:     "alive",
	}); err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}

	before, err := store.GetAgent(ctx, "ws-agent-partial-reregister", "agent-partner")
	if err != nil {
		t.Fatalf("get agent before partial reregister: %v", err)
	}
	if before.LastSeenAt == nil || *before.LastSeenAt == "" {
		t.Fatalf("expected heartbeat-backed presence before partial reregister, got %+v", before)
	}
	beforeLastSeenAt := *before.LastSeenAt
	beforeSummary := before.Summary

	partial, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-agent-partial-reregister",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-partner",
		IPAddress:         "198.51.100.72",
		UserAgent:         "rhizome-agent/5.1",
	})
	if err != nil {
		t.Fatalf("partial agent reregister: %v", err)
	}
	if partial.DisplayName != "Partner Executor" {
		t.Fatalf("expected partial reregister result to preserve display name, got %+v", partial)
	}
	if partial.Agent.OwnerUserID != owner.UserID {
		t.Fatalf("expected partial reregister to preserve owner %q, got %+v", owner.UserID, partial.Agent)
	}
	if partial.Agent.DisplayName != "Partner Executor" || partial.Agent.Role != "reviewer" {
		t.Fatalf("expected partial reregister to preserve display/role, got %+v", partial.Agent)
	}
	if partial.Agent.ProtocolVersion != "partner-runtime/v7" {
		t.Fatalf("expected partial reregister to preserve protocol version, got %+v", partial.Agent)
	}
	if strings.Join(partial.Agent.Capabilities, ",") != "analysis,review" {
		t.Fatalf("expected partial reregister to preserve capabilities, got %+v", partial.Agent)
	}
	if partial.Agent.Summary != beforeSummary {
		t.Fatalf("expected partial reregister to preserve current summary %q, got %+v", beforeSummary, partial.Agent)
	}
	if partial.Agent.LastSeenAt == nil || *partial.Agent.LastSeenAt != beforeLastSeenAt || !partial.Agent.IsOnline {
		t.Fatalf("expected partial reregister to preserve liveness evidence, got %+v", partial.Agent)
	}
}

func TestWorkspaceHumanSessionsListAndRevoke(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-human-sessions",
		Title:       "Human Sessions",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	reg, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-human-sessions",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "203.0.113.20",
		UserAgent:         "rhizome-web/1.0",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	login, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-human-sessions",
		Username:    "alice",
		Password:    "alice-password",
		IPAddress:   "203.0.113.20",
		UserAgent:   "rhizome-web/1.0",
	})
	if err != nil {
		t.Fatalf("login human: %v", err)
	}

	current, err := store.AuthenticateAccessToken(ctx, login.Token)
	if err != nil {
		t.Fatalf("authenticate current token: %v", err)
	}

	sessions, err := store.ListHumanSessions(ctx, "ws-human-sessions", reg.UserID, current.TokenID)
	if err != nil {
		t.Fatalf("list human sessions: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("expected two active sessions, got %+v", sessions)
	}
	currentCount := 0
	for _, session := range sessions {
		if session.IsCurrent {
			currentCount++
			if session.TokenID != current.TokenID {
				t.Fatalf("expected current session token %q, got %+v", current.TokenID, session)
			}
		}
	}
	if currentCount != 1 {
		t.Fatalf("expected exactly one current session, got %+v", sessions)
	}

	revoked, err := store.RevokeHumanSessions(ctx, sqlite.HumanSessionRevokeInput{
		WorkspaceID:    "ws-human-sessions",
		UserID:         reg.UserID,
		CurrentTokenID: current.TokenID,
		Scope:          "others",
		ActorType:      "human",
		ActorID:        reg.UserID,
		IPAddress:      "203.0.113.21",
		UserAgent:      "rhizome-web/1.0",
	})
	if err != nil {
		t.Fatalf("revoke other human sessions: %v", err)
	}
	if revoked != 1 {
		t.Fatalf("expected to revoke one old session, got %d", revoked)
	}

	if _, err := store.AuthenticateAccessToken(ctx, reg.Token); err == nil {
		t.Fatal("expected original registration token to be revoked")
	}
	if _, err := store.AuthenticateAccessToken(ctx, login.Token); err != nil {
		t.Fatalf("expected current login token to remain valid: %v", err)
	}

	remaining, err := store.ListHumanSessions(ctx, "ws-human-sessions", reg.UserID, current.TokenID)
	if err != nil {
		t.Fatalf("list remaining human sessions: %v", err)
	}
	if len(remaining) != 1 || !remaining[0].IsCurrent || remaining[0].TokenID != current.TokenID {
		t.Fatalf("expected only the current session to remain, got %+v", remaining)
	}
}

func TestWorkspaceSecurityEventsCaptureMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-security-events",
		Title:       "Security Events",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-security-events",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-events",
		DisplayName:       "Agent Events",
		OwnerUserID:       "developer",
		IPAddress:         "198.51.100.30",
		UserAgent:         "rhizome-agent/2.0",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.UpdateWorkspaceSecuritySettings(ctx, sqlite.WorkspaceSecuritySettingsInput{
		WorkspaceID:       "ws-security-events",
		WorkspacePassword: "events-password",
		UpdatedByType:     "human",
		UpdatedByID:       "developer",
		IPAddress:         "198.51.100.31",
		UserAgent:         "rhizome-admin/2.0",
	}); err != nil {
		t.Fatalf("update workspace settings: %v", err)
	}

	events, err := store.ListSecurityEvents(ctx, "ws-security-events", 10)
	if err != nil {
		t.Fatalf("list security events: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected at least 2 security events, got %+v", events)
	}

	foundAgent := false
	foundUpdate := false
	for _, evt := range events {
		switch evt.EventType {
		case "agent_registered":
			foundAgent = true
			if evt.IPAddress != "198.51.100.30" || evt.UserAgent != "rhizome-agent/2.0" {
				t.Fatalf("expected agent metadata capture, got %+v", evt)
			}
		case "workspace_settings_updated":
			foundUpdate = true
			if evt.IPAddress != "198.51.100.31" || evt.UserAgent != "rhizome-admin/2.0" {
				t.Fatalf("expected workspace update metadata capture, got %+v", evt)
			}
		}
	}
	if !foundAgent || !foundUpdate {
		t.Fatalf("expected agent_registered and workspace_settings_updated events, got %+v", events)
	}
}

func TestWorkspaceSecurityRotateAgentAccessTokenDeliversPrivateInboxNotice(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-rotate-inbox",
		Title:       "Agent Rotate Inbox",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	owner, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-agent-rotate-inbox",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
		IPAddress:         "198.51.100.50",
		UserAgent:         "rhizome-web/2.0",
	})
	if err != nil {
		t.Fatalf("register human owner: %v", err)
	}

	original, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-agent-rotate-inbox",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-rotate",
		DisplayName:       "Rotate Agent",
		OwnerUserID:       owner.UserID,
		IPAddress:         "198.51.100.51",
		UserAgent:         "rhizome-agent/2.0",
	})
	if err != nil {
		t.Fatalf("register rotating agent: %v", err)
	}
	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-agent-rotate-inbox",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-peer",
		DisplayName:       "Peer Agent",
		OwnerUserID:       owner.UserID,
		IPAddress:         "198.51.100.52",
		UserAgent:         "rhizome-agent/2.1",
	}); err != nil {
		t.Fatalf("register peer agent: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, "ws-agent-rotate-inbox")

	rotated, err := store.RotateAgentAccessToken(ctx, sqlite.AgentTokenRotateInput{
		WorkspaceID: "ws-agent-rotate-inbox",
		AgentID:     "agent-rotate",
		ActorType:   "human",
		ActorID:     owner.UserID,
		IPAddress:   "198.51.100.53",
		UserAgent:   "rhizome-web/2.0",
	})
	if err != nil {
		t.Fatalf("rotate agent access token: %v", err)
	}
	if rotated.Token == "" || rotated.MessageID == "" {
		t.Fatalf("expected rotate result to include token and message id, got %+v", rotated)
	}
	if rotated.MessageEvent.EventID == "" {
		t.Fatalf("expected rotate result to include runtime event row, got %+v", rotated)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-agent-rotate-inbox",
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    rotated.MessageID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events for rotated notice: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 runtime event for rotated notice, got %+v", events)
	}
	if events[0].EventID != rotated.MessageEvent.EventID || events[0].IngestSeq != rotated.MessageEvent.IngestSeq {
		t.Fatalf("expected rotate result runtime row to match persisted row, got returned=%+v persisted=%+v", rotated.MessageEvent, events[0])
	}
	assertRuntimeEventAuthorityMetadata(t, rotated.MessageEvent, authority)
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)

	inbox, err := store.PollMessages(ctx, "ws-agent-rotate-inbox", "agent-rotate", "", 10, 24)
	if err != nil {
		t.Fatalf("poll rotating agent inbox: %v", err)
	}
	var notice *sqlite.MessageRecord
	for i := range inbox {
		if inbox[i].MessageID == rotated.MessageID {
			notice = &inbox[i]
			break
		}
	}
	if notice == nil {
		t.Fatalf("expected rotated agent inbox to receive message %s, got %+v", rotated.MessageID, inbox)
	}
	if notice.FromAgentID != "system" || notice.ToAgentID != "agent-rotate" {
		t.Fatalf("expected private system notice for rotated agent, got %+v", notice)
	}
	if notice.Channel != "security" || notice.ContentType != "application/vnd.rhizome.auth-token" {
		t.Fatalf("expected auth-token security notice, got %+v", notice)
	}
	if !strings.Contains(notice.Content, rotated.Token) {
		t.Fatalf("expected inbox notice to contain rotated token, got %+v", notice)
	}
	if !strings.Contains(notice.MetadataJSON, rotated.Token[:8]) {
		t.Fatalf("expected inbox notice metadata to include rotated token prefix %q, got %+v", rotated.Token[:8], notice)
	}

	peerInbox, err := store.PollMessages(ctx, "ws-agent-rotate-inbox", "agent-peer", "", 10, 24)
	if err != nil {
		t.Fatalf("poll peer agent inbox: %v", err)
	}
	if len(peerInbox) != 0 {
		t.Fatalf("expected rotation notice to stay private to target agent inbox, got %+v", peerInbox)
	}

	if _, err := store.AuthenticateAccessToken(ctx, original.Token); err != nil {
		t.Fatalf("expected old token to remain valid before new token use: %v", err)
	}
	if _, err := store.AuthenticateAccessToken(ctx, rotated.Token); err != nil {
		t.Fatalf("authenticate rotated token: %v", err)
	}
	if _, err := store.AuthenticateAccessToken(ctx, original.Token); err == nil {
		t.Fatal("expected original token to be revoked after rotated token is used")
	}
}

func TestWorkspaceSecurityRejectsInvalidCredentialsAndInputs(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-security-invalid",
		Title:       "Security Invalid",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-security-invalid",
		WorkspacePassword: "wrong",
		AgentID:           "agent-invalid",
		DisplayName:       "Agent Invalid",
		OwnerUserID:       "developer",
	}); err == nil {
		t.Fatal("expected wrong workspace password to fail agent registration")
	}

	if _, err := store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: "ws-security-invalid",
		Username:    "Alice",
		Password:    "wrong",
	}); err == nil {
		t.Fatal("expected invalid human credentials to fail")
	}

	if _, err := store.UpdateWorkspaceSecuritySettings(ctx, sqlite.WorkspaceSecuritySettingsInput{
		WorkspaceID:       "ws-security-invalid",
		WorkspacePassword: "rotated-password",
		UpdatedByType:     "human",
		UpdatedByID:       "developer",
	}); err != nil {
		t.Fatalf("update workspace security settings: %v", err)
	}

	events, err := store.ListSecurityEvents(ctx, "ws-security-invalid", 0)
	if err != nil {
		t.Fatalf("list security events with default limit: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected default list limit to return events")
	}
}

func TestWorkspaceSecurityEventOrderingNewestFirst(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-security-order",
		Title:       "Security Order",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	for i := 0; i < 2; i++ {
		if _, err := store.UpdateWorkspaceSecuritySettings(ctx, sqlite.WorkspaceSecuritySettingsInput{
			WorkspaceID:       "ws-security-order",
			WorkspacePassword: "workspace-order-pass",
			UpdatedByType:     "human",
			UpdatedByID:       "developer",
		}); err != nil {
			t.Fatalf("update workspace settings round %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	events, err := store.ListSecurityEvents(ctx, "ws-security-order", 10)
	if err != nil {
		t.Fatalf("list security events: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected at least 2 security events, got %+v", events)
	}
	if events[0].CreatedAt < events[1].CreatedAt {
		t.Fatalf("expected newest-first ordering, got %+v", events[:2])
	}
}
