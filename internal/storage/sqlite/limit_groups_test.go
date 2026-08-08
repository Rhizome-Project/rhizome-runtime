package sqlite_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestLimitGroupCRUD(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	// Create workspace + agent for membership tests
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-limits",
		Title:       "Limits Test",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{"agent-alpha", "agent-beta"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-limits",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	// ── Create ──
	if err := store.CreateLimitGroup(ctx, sqlite.LimitGroupCreateInput{
		GroupID:          "grp-openai",
		WorkspaceID:      "ws-limits",
		Title:            "OpenAI Plus",
		OwnerName:        "developer",
		SubscriptionTier: "openai-plus",
		DailyLimit:       100,
		WeeklyLimit:      500,
	}); err != nil {
		t.Fatalf("create limit group: %v", err)
	}

	// ── Get ──
	grp, err := store.GetLimitGroup(ctx, "ws-limits", "grp-openai")
	if err != nil {
		t.Fatalf("get limit group: %v", err)
	}
	if grp.Title != "OpenAI Plus" {
		t.Fatalf("expected title 'OpenAI Plus', got %q", grp.Title)
	}
	if grp.DailyLimit != 100 || grp.WeeklyLimit != 500 {
		t.Fatalf("expected limits 100/500, got %d/%d", grp.DailyLimit, grp.WeeklyLimit)
	}
	// Remaining should start at limit values
	if grp.DailyRemaining != 100 || grp.WeeklyRemaining != 500 {
		t.Fatalf("expected remaining 100/500, got %d/%d", grp.DailyRemaining, grp.WeeklyRemaining)
	}
	if len(grp.Agents) != 0 {
		t.Fatalf("expected 0 agents, got %d", len(grp.Agents))
	}

	// ── Update with agents ──
	agentIDs := []string{"agent-alpha", "agent-beta"}
	dailyLimit := 200
	if err := store.UpdateLimitGroup(ctx, sqlite.LimitGroupUpdateInput{
		WorkspaceID:      "ws-limits",
		GroupID:          "grp-openai",
		Title:            "OpenAI Plus Team",
		SubscriptionTier: "openai-team",
		DailyLimit:       &dailyLimit,
		AgentIDs:         agentIDs,
	}); err != nil {
		t.Fatalf("update limit group: %v", err)
	}

	grp, err = store.GetLimitGroup(ctx, "ws-limits", "grp-openai")
	if err != nil {
		t.Fatalf("get updated group: %v", err)
	}
	if grp.Title != "OpenAI Plus Team" {
		t.Fatalf("expected updated title, got %q", grp.Title)
	}
	if grp.DailyLimit != 200 {
		t.Fatalf("expected daily limit 200, got %d", grp.DailyLimit)
	}
	if len(grp.Agents) != 2 {
		t.Fatalf("expected 2 agents, got %d", len(grp.Agents))
	}

	// ── List ──
	groups, err := store.ListLimitGroups(ctx, "ws-limits")
	if err != nil {
		t.Fatalf("list limit groups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}

	// ── Delete ──
	if err := store.DeleteLimitGroup(ctx, "ws-limits", "grp-openai"); err != nil {
		t.Fatalf("delete limit group: %v", err)
	}
	groups, err = store.ListLimitGroups(ctx, "ws-limits")
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("expected 0 groups after delete, got %d", len(groups))
	}
}

func TestLimitGroupWithEventRejectsForgedPromptPrincipal(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-limit-group-forged-principal"
	actorID := "operator-a"
	forgedPrincipalID := "operator-b"

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Limit Group Forged Principal",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		Scope:       "workspace",
		ActorType:   "operator",
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("ensure local authority: %v", err)
	}

	forgedEnvelope := func(surface string) map[string]any {
		return sqlite.BuildLimitGroupPromptContextEnvelope(surface, "server_rpc", workspaceID, "human", forgedPrincipalID)
	}

	createGroupID := "grp-forged-create"
	_, _, err := store.CreateLimitGroupWithEvent(ctx, sqlite.LimitGroupCreateInput{
		WorkspaceID:           workspaceID,
		GroupID:               createGroupID,
		Title:                 "Forged Create",
		ActorID:               actorID,
		ActorType:             "human",
		PromptContextEnvelope: forgedEnvelope("limits.group.create"),
		PromptContextSurface:  "limits.group.create",
	})
	if err == nil || !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("expected forged create principal_id rejection, got %v", err)
	}
	if _, getErr := store.GetLimitGroup(ctx, workspaceID, createGroupID); getErr == nil {
		t.Fatalf("expected forged create to roll back limit group row")
	}
	assertNoLimitGroupRuntimeEvent(t, store, ctx, workspaceID, "limits.group.created", createGroupID)

	existingGroupID := "grp-forged-existing"
	if err := store.CreateLimitGroup(ctx, sqlite.LimitGroupCreateInput{
		WorkspaceID: workspaceID,
		GroupID:     existingGroupID,
		Title:       "Existing Group",
	}); err != nil {
		t.Fatalf("seed existing limit group: %v", err)
	}

	_, _, err = store.UpdateLimitGroupWithEvent(ctx, sqlite.LimitGroupUpdateInput{
		WorkspaceID:           workspaceID,
		GroupID:               existingGroupID,
		Title:                 "Forged Update",
		ActorID:               actorID,
		ActorType:             "human",
		PromptContextEnvelope: forgedEnvelope("limits.group.update"),
		PromptContextSurface:  "limits.group.update",
	})
	if err == nil || !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("expected forged update principal_id rejection, got %v", err)
	}
	group, err := store.GetLimitGroup(ctx, workspaceID, existingGroupID)
	if err != nil {
		t.Fatalf("get existing group after forged update: %v", err)
	}
	if group.Title != "Existing Group" {
		t.Fatalf("expected forged update rollback to keep title, got %q", group.Title)
	}
	assertNoLimitGroupRuntimeEvent(t, store, ctx, workspaceID, "limits.group.updated", existingGroupID)

	_, err = store.DeleteLimitGroupWithEvent(ctx, sqlite.LimitGroupDeleteInput{
		WorkspaceID:           workspaceID,
		GroupID:               existingGroupID,
		ActorID:               actorID,
		ActorType:             "human",
		PromptContextEnvelope: forgedEnvelope("limits.group.delete"),
		PromptContextSurface:  "limits.group.delete",
	})
	if err == nil || !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("expected forged delete principal_id rejection, got %v", err)
	}
	if _, err := store.GetLimitGroup(ctx, workspaceID, existingGroupID); err != nil {
		t.Fatalf("expected forged delete rollback to leave group row: %v", err)
	}
	assertNoLimitGroupRuntimeEvent(t, store, ctx, workspaceID, "limits.group.deleted", existingGroupID)
}

func TestLimitReport(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-report",
		Title:       "Report Test",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-report",
		AgentID:     "agent-reporter",
		OwnerUserID: "developer",
		DisplayName: "Reporter",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	// Create group
	if err := store.CreateLimitGroup(ctx, sqlite.LimitGroupCreateInput{
		GroupID:     "grp-report",
		WorkspaceID: "ws-report",
		Title:       "Report Group",
		DailyLimit:  100,
		WeeklyLimit: 700,
	}); err != nil {
		t.Fatalf("create limit group: %v", err)
	}

	// Assign agent
	if err := store.UpdateLimitGroup(ctx, sqlite.LimitGroupUpdateInput{
		WorkspaceID: "ws-report",
		GroupID:     "grp-report",
		AgentIDs:    []string{"agent-reporter"},
	}); err != nil {
		t.Fatalf("assign agent: %v", err)
	}

	// Report limits
	if err := store.ReportLimits(ctx, sqlite.LimitReportInput{
		GroupID:         "grp-report",
		AgentID:         "agent-reporter",
		DailyRemaining:  42,
		WeeklyRemaining: 350,
	}); err != nil {
		t.Fatalf("report limits: %v", err)
	}

	// Verify remaining updated
	grp, err := store.GetLimitGroup(ctx, "ws-report", "grp-report")
	if err != nil {
		t.Fatalf("get group after report: %v", err)
	}
	if grp.DailyRemaining != 42 {
		t.Fatalf("expected daily remaining 42, got %d", grp.DailyRemaining)
	}
	if grp.WeeklyRemaining != 350 {
		t.Fatalf("expected weekly remaining 350, got %d", grp.WeeklyRemaining)
	}
	if grp.LastReportedAt == "" {
		t.Fatalf("expected last_reported_at to be set")
	}

	// Verify snapshot created
	snaps, err := store.ListLimitSnapshots(ctx, "grp-report", 10)
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].AgentID != "agent-reporter" {
		t.Fatalf("expected snapshot agent_id 'agent-reporter', got %q", snaps[0].AgentID)
	}
	if snaps[0].DailyRemaining != 42 {
		t.Fatalf("expected snapshot daily remaining 42, got %d", snaps[0].DailyRemaining)
	}
}

func assertNoLimitGroupRuntimeEvent(t *testing.T, store *sqlite.Store, ctx context.Context, workspaceID, eventType, groupID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "limit_group",
		EntityID:    groupID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s events for %s: %v", eventType, groupID, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events for %s, got %+v", eventType, groupID, events)
	}
}

func TestGetAgentLimitGroup(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agentlg",
		Title:       "Agent LG Test",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-agentlg",
		AgentID:     "agent-findme",
		OwnerUserID: "developer",
		DisplayName: "FindMe",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	// Agent not in any group initially
	grp, err := store.GetAgentLimitGroup(ctx, "ws-agentlg", "agent-findme")
	if err != nil {
		t.Fatalf("get agent limit group (none): %v", err)
	}
	if grp != nil {
		t.Fatalf("expected nil group for unassigned agent, got %+v", grp)
	}

	// Create group and assign
	if err := store.CreateLimitGroup(ctx, sqlite.LimitGroupCreateInput{
		GroupID:          "grp-finder",
		WorkspaceID:      "ws-agentlg",
		Title:            "Finder Group",
		SubscriptionTier: "claude-pro",
		DailyLimit:       50,
		WeeklyLimit:      300,
	}); err != nil {
		t.Fatalf("create limit group: %v", err)
	}
	if err := store.UpdateLimitGroup(ctx, sqlite.LimitGroupUpdateInput{
		WorkspaceID: "ws-agentlg",
		GroupID:     "grp-finder",
		AgentIDs:    []string{"agent-findme"},
	}); err != nil {
		t.Fatalf("assign agent: %v", err)
	}

	// Now should find group
	grp, err = store.GetAgentLimitGroup(ctx, "ws-agentlg", "agent-findme")
	if err != nil {
		t.Fatalf("get agent limit group: %v", err)
	}
	if grp == nil {
		t.Fatalf("expected group for agent, got nil")
	}
	if grp.GroupID != "grp-finder" {
		t.Fatalf("expected group 'grp-finder', got %q", grp.GroupID)
	}
	if grp.SubscriptionTier != "claude-pro" {
		t.Fatalf("expected tier 'claude-pro', got %q", grp.SubscriptionTier)
	}
	if len(grp.Agents) != 1 || grp.Agents[0] != "agent-findme" {
		t.Fatalf("expected [agent-findme], got %v", grp.Agents)
	}
}

func TestAssignAgentLimitGroupRebindsCanonicalMembership(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-shared-limit-group",
		Title:       "Shared Limit Group",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-shared-limit-group",
		AgentID:     "agent-shared",
		OwnerUserID: "developer",
		DisplayName: "Shared Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.EnsureAgentLimitGroup(ctx, "ws-shared-limit-group", "agent-shared", "Shared Agent"); err != nil {
		t.Fatalf("seed singleton limit group: %v", err)
	}

	if err := store.AssignAgentLimitGroup(ctx, "ws-shared-limit-group", "agent-shared", "codex", "codex"); err != nil {
		t.Fatalf("assign shared limit group: %v", err)
	}

	group, err := store.GetAgentLimitGroup(ctx, "ws-shared-limit-group", "agent-shared")
	if err != nil {
		t.Fatalf("get canonical limit group: %v", err)
	}
	if group == nil || group.GroupID != "codex" {
		t.Fatalf("expected canonical group codex, got %+v", group)
	}
	if len(group.Agents) != 1 || group.Agents[0] != "agent-shared" {
		t.Fatalf("expected codex group membership for agent-shared, got %+v", group)
	}

	groups, err := store.ListLimitGroups(ctx, "ws-shared-limit-group")
	if err != nil {
		t.Fatalf("list limit groups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected both singleton and shared groups to exist, got %+v", groups)
	}
	for _, candidate := range groups {
		if candidate.GroupID == "codex" {
			continue
		}
		if len(candidate.Agents) != 0 {
			t.Fatalf("expected stale singleton membership to be cleared, got %+v", candidate)
		}
	}
}
