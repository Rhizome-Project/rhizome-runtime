package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRegisterAgentDoesNotMarkAgentOnlineUntilHeartbeat(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-register-presence",
		Title:       "Agent Register Presence",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-agent-register-presence",
		AgentID:     "agent-register-presence",
		OwnerUserID: "developer",
		DisplayName: "Register Presence Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	agent, err := store.GetAgent(ctx, "ws-agent-register-presence", "agent-register-presence")
	if err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if agent.LastSeenAt != nil {
		t.Fatalf("expected registration not to set last_seen_at, got %q", *agent.LastSeenAt)
	}
	if agent.IsOnline {
		t.Fatalf("expected newly registered agent to remain offline until heartbeat, got %+v", agent)
	}

	agents, err := store.ListWorkspaceAgents(ctx, "ws-agent-register-presence")
	if err != nil {
		t.Fatalf("list workspace agents: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("expected one agent in workspace list, got %d", len(agents))
	}
	if agents[0].LastSeenAt != nil {
		t.Fatalf("expected listed agent not to expose last_seen_at after registration, got %q", *agents[0].LastSeenAt)
	}
	if agents[0].IsOnline {
		t.Fatalf("expected listed agent to remain offline until heartbeat, got %+v", agents[0])
	}
}

func TestRegisterAgentPreservesLastSeenAtOnReregister(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-reregister-presence",
		Title:       "Agent Reregister Presence",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-agent-reregister-presence",
		AgentID:     "agent-reregister-presence",
		OwnerUserID: "developer",
		DisplayName: "Original Agent",
	}); err != nil {
		t.Fatalf("initial register agent: %v", err)
	}

	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: "ws-agent-reregister-presence",
		AgentID:     "agent-reregister-presence",
		Status:      "active",
		Summary:     "still alive",
	}); err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}

	before, err := store.GetAgent(ctx, "ws-agent-reregister-presence", "agent-reregister-presence")
	if err != nil {
		t.Fatalf("get agent before reregister: %v", err)
	}
	if before.LastSeenAt == nil || *before.LastSeenAt == "" {
		t.Fatalf("expected heartbeat to establish last_seen_at, got %+v", before)
	}
	beforeLastSeenAt := *before.LastSeenAt

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:     "ws-agent-reregister-presence",
		AgentID:         "agent-reregister-presence",
		OwnerUserID:     "developer",
		DisplayName:     "Updated Agent",
		ProtocolVersion: "2.0",
		Capabilities:    []string{"docs", "coordination"},
		Summary:         "refreshed registration",
	}); err != nil {
		t.Fatalf("reregister agent: %v", err)
	}

	after, err := store.GetAgent(ctx, "ws-agent-reregister-presence", "agent-reregister-presence")
	if err != nil {
		t.Fatalf("get agent after reregister: %v", err)
	}
	if after.DisplayName != "Updated Agent" {
		t.Fatalf("expected reregister to refresh display name, got %+v", after)
	}
	if after.LastSeenAt == nil || *after.LastSeenAt != beforeLastSeenAt {
		t.Fatalf("expected reregister to preserve last_seen_at %q, got %+v", beforeLastSeenAt, after.LastSeenAt)
	}
	if !after.IsOnline {
		t.Fatalf("expected reregistered agent to remain online from preserved heartbeat evidence, got %+v", after)
	}
}

func TestRegisterAgentPreservingOmittedKeepsExecutorMetadataOnPartialReregister(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-register-patch",
		Title:       "Agent Register Patch",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	owner := "developer"
	display := "Patched Agent"
	role := "reviewer"
	protocolVersion := "partner-runtime/v9"
	initialCapabilities := []string{"analysis", "review"}
	initialSummary := "registered summary"
	registered, err := store.RegisterAgentPreservingOmitted(ctx, sqlite.AgentRegisterPatchInput{
		WorkspaceID:     "ws-agent-register-patch",
		AgentID:         "agent-register-patch",
		OwnerUserID:     &owner,
		DisplayName:     &display,
		Role:            &role,
		ProtocolVersion: &protocolVersion,
		Capabilities:    &initialCapabilities,
		Summary:         &initialSummary,
	})
	if err != nil {
		t.Fatalf("initial register preserving omitted: %v", err)
	}
	if registered.Role != "reviewer" || registered.ProtocolVersion != "partner-runtime/v9" {
		t.Fatalf("expected initial register to persist executor metadata, got %+v", registered)
	}

	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: "ws-agent-register-patch",
		AgentID:     "agent-register-patch",
		Status:      "active",
		Summary:     "live summary",
	}); err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}

	before, err := store.GetAgent(ctx, "ws-agent-register-patch", "agent-register-patch")
	if err != nil {
		t.Fatalf("get agent before patch reregister: %v", err)
	}
	if before.LastSeenAt == nil || *before.LastSeenAt == "" {
		t.Fatalf("expected heartbeat-backed presence before patch reregister, got %+v", before)
	}
	beforeLastSeenAt := *before.LastSeenAt

	after, err := store.RegisterAgentPreservingOmitted(ctx, sqlite.AgentRegisterPatchInput{
		WorkspaceID: "ws-agent-register-patch",
		AgentID:     "agent-register-patch",
	})
	if err != nil {
		t.Fatalf("partial patch reregister: %v", err)
	}
	if after.OwnerUserID != "developer" || after.DisplayName != "Patched Agent" {
		t.Fatalf("expected partial patch reregister to preserve owner/display, got %+v", after)
	}
	if after.Role != "reviewer" || after.ProtocolVersion != "partner-runtime/v9" {
		t.Fatalf("expected partial patch reregister to preserve role/protocol, got %+v", after)
	}
	if len(after.Capabilities) != 2 || after.Capabilities[0] != "analysis" || after.Capabilities[1] != "review" {
		t.Fatalf("expected partial patch reregister to preserve capabilities, got %+v", after)
	}
	if after.Summary != "live summary" {
		t.Fatalf("expected partial patch reregister to preserve current summary, got %+v", after)
	}
	if after.LastSeenAt == nil || *after.LastSeenAt != beforeLastSeenAt || !after.IsOnline {
		t.Fatalf("expected partial patch reregister to preserve liveness evidence, got %+v", after)
	}
}

func TestEnsureAgentRegisteredPreservesExistingExecutorMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-ensure-preserve",
		Title:       "Agent Ensure Preserve",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:     "ws-agent-ensure-preserve",
		AgentID:         "agent-ensure-preserve",
		OwnerUserID:     "partner-owner",
		DisplayName:     "Partner Agent",
		Role:            "reviewer",
		Status:          "PAUSED",
		ProtocolVersion: "partner-runtime/v5",
		Capabilities:    []string{"review", "analysis"},
		Summary:         "partner summary",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: "ws-agent-ensure-preserve",
		AgentID:     "agent-ensure-preserve",
		Status:      "active",
		Summary:     "live partner summary",
	}); err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}

	before, err := store.GetAgent(ctx, "ws-agent-ensure-preserve", "agent-ensure-preserve")
	if err != nil {
		t.Fatalf("get agent before ensure: %v", err)
	}
	if before.LastSeenAt == nil || *before.LastSeenAt == "" {
		t.Fatalf("expected heartbeat-backed last_seen_at before ensure, got %+v", before)
	}
	beforeLastSeenAt := *before.LastSeenAt

	after, err := store.EnsureAgentRegistered(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:     "ws-agent-ensure-preserve",
		AgentID:         "agent-ensure-preserve",
		OwnerUserID:     "system",
		DisplayName:     "Bootstrap Agent",
		Role:            "generalist",
		Status:          "ACTIVE",
		ProtocolVersion: "workspace-bootstrap/v1",
		Capabilities:    []string{"tool-use"},
		Summary:         "bootstrap summary",
	})
	if err != nil {
		t.Fatalf("ensure agent registered: %v", err)
	}

	if after.OwnerUserID != "partner-owner" || after.DisplayName != "Partner Agent" {
		t.Fatalf("expected ensure to preserve owner/display, got %+v", after)
	}
	if after.Role != "reviewer" || after.Status != "ACTIVE" {
		t.Fatalf("expected ensure to preserve current role/status truth, got %+v", after)
	}
	if after.ProtocolVersion != "partner-runtime/v5" {
		t.Fatalf("expected ensure to preserve protocol version, got %+v", after)
	}
	if len(after.Capabilities) != 2 || after.Capabilities[0] != "review" || after.Capabilities[1] != "analysis" {
		t.Fatalf("expected ensure to preserve capabilities, got %+v", after)
	}
	if after.Summary != "live partner summary" {
		t.Fatalf("expected ensure to preserve current summary, got %+v", after)
	}
	if after.LastSeenAt == nil || *after.LastSeenAt != beforeLastSeenAt || !after.IsOnline {
		t.Fatalf("expected ensure to preserve liveness evidence, got %+v", after)
	}
}

func TestRegisterAndHeartbeatPreserveSharedAgentTruth(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-shared-truth",
		Title:       "Agent Shared Truth",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:     "ws-agent-shared-truth",
		AgentID:         "agent-shared-truth",
		OwnerUserID:     "developer",
		DisplayName:     "Partner Agent",
		Role:            "reviewer",
		ProtocolVersion: "partner-runtime/v2",
		Capabilities:    []string{"analysis", "coordination", "analysis", "  review  "},
		Summary:         "Registered for partner queue",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	registered, err := store.GetAgent(ctx, "ws-agent-shared-truth", "agent-shared-truth")
	if err != nil {
		t.Fatalf("get registered agent: %v", err)
	}
	if registered.Role != "reviewer" || registered.ProtocolVersion != "partner-runtime/v2" {
		t.Fatalf("expected registration to persist role/protocol, got %+v", registered)
	}
	if got := registered.Capabilities; len(got) != 3 || got[0] != "analysis" || got[1] != "coordination" || got[2] != "review" {
		t.Fatalf("expected normalized registration capabilities, got %+v", got)
	}
	if registered.Summary != "Registered for partner queue" {
		t.Fatalf("expected registration summary to persist, got %+v", registered)
	}
	if registered.LastSeenAt != nil || registered.IsOnline {
		t.Fatalf("expected registration to remain offline until heartbeat, got %+v", registered)
	}

	listed, err := store.ListWorkspaceAgents(ctx, "ws-agent-shared-truth")
	if err != nil {
		t.Fatalf("list workspace agents after registration: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected one registered agent, got %+v", listed)
	}
	if listed[0].ProtocolVersion != registered.ProtocolVersion || listed[0].Summary != registered.Summary {
		t.Fatalf("expected list view to mirror registered truth, got %+v", listed[0])
	}
	if len(listed[0].Capabilities) != len(registered.Capabilities) {
		t.Fatalf("expected list capabilities to mirror registered truth, got %+v", listed[0])
	}
	if listed[0].LastSeenAt != nil || listed[0].IsOnline {
		t.Fatalf("expected listed agent to remain offline before heartbeat, got %+v", listed[0])
	}

	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: "ws-agent-shared-truth",
		AgentID:     "agent-shared-truth",
		Status:      "active",
		Summary:     "Serving partner queue",
	}); err != nil {
		t.Fatalf("record agent heartbeat: %v", err)
	}

	afterHeartbeat, err := store.GetAgent(ctx, "ws-agent-shared-truth", "agent-shared-truth")
	if err != nil {
		t.Fatalf("get agent after heartbeat: %v", err)
	}
	if afterHeartbeat.LastSeenAt == nil || *afterHeartbeat.LastSeenAt == "" || !afterHeartbeat.IsOnline {
		t.Fatalf("expected heartbeat to establish online presence, got %+v", afterHeartbeat)
	}
	if afterHeartbeat.Role != "reviewer" || afterHeartbeat.ProtocolVersion != "partner-runtime/v2" {
		t.Fatalf("expected heartbeat not to rewrite identity metadata, got %+v", afterHeartbeat)
	}
	if got := afterHeartbeat.Capabilities; len(got) != 3 || got[0] != "analysis" || got[1] != "coordination" || got[2] != "review" {
		t.Fatalf("expected heartbeat to preserve capabilities, got %+v", got)
	}
	if afterHeartbeat.Summary != "Serving partner queue" || afterHeartbeat.Status != "ACTIVE" {
		t.Fatalf("expected heartbeat to refresh summary/status, got %+v", afterHeartbeat)
	}

	listedAfterHeartbeat, err := store.ListWorkspaceAgents(ctx, "ws-agent-shared-truth")
	if err != nil {
		t.Fatalf("list workspace agents after heartbeat: %v", err)
	}
	if len(listedAfterHeartbeat) != 1 {
		t.Fatalf("expected one listed agent after heartbeat, got %+v", listedAfterHeartbeat)
	}
	if listedAfterHeartbeat[0].LastSeenAt == nil || !listedAfterHeartbeat[0].IsOnline {
		t.Fatalf("expected listed agent to reflect heartbeat-backed presence, got %+v", listedAfterHeartbeat[0])
	}
	if listedAfterHeartbeat[0].ProtocolVersion != afterHeartbeat.ProtocolVersion || listedAfterHeartbeat[0].Role != afterHeartbeat.Role {
		t.Fatalf("expected listed agent to preserve identity metadata after heartbeat, got %+v", listedAfterHeartbeat[0])
	}
}
