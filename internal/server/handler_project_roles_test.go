package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectRoleRPCsUseAuthorityStorageAndPublishEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-project-roles-rpc"
	const actorID = "operator-a"
	const projectID = "project-roles"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, actorID, "lead-a", "lead-b", "reviewer-a")

	if _, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Roles",
		CreatedBy:   actorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	claimResult, rpcErr := h.projectLeadClaim(ctx, mustJSONRaw(projectLeadClaimParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		AgentID:      "lead-a",
		LeaseSeconds: 120,
		LeaseToken:   "lease-a",
		Summary:      "lead claim",
	}))
	if rpcErr != nil {
		t.Fatalf("project.lead.claim rpc error: %+v", rpcErr)
	}
	leadRole := claimResult.(map[string]any)["role"].(sqlite.ProjectRoleRecord)
	if leadRole.AgentID != "lead-a" || leadRole.RoleType != sqlite.ProjectRoleStrategicLead || leadRole.Status != sqlite.ProjectRoleStatusActive {
		t.Fatalf("project.lead.claim returned unexpected role: %+v", leadRole)
	}
	claimRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.lead.claimed",
		EntityType:  "project_role",
		EntityID:    leadRole.RoleID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.lead.claimed"), claimRuntime, "project.lead.claimed")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.lead.changed"), claimRuntime, "project.lead.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, claimRuntime.PayloadJSON), "project.lead.claim", workspaceID, "human", actorID, projectID, actorID)

	renewResult, rpcErr := h.projectLeadRenew(ctx, mustJSONRaw(projectLeadRenewParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		RoleID:       leadRole.RoleID,
		LeaseSeconds: 180,
		LeaseToken:   "lease-a",
		Summary:      "lead renew",
	}))
	if rpcErr != nil {
		t.Fatalf("project.lead.renew rpc error: %+v", rpcErr)
	}
	renewedRole := renewResult.(map[string]any)["role"].(sqlite.ProjectRoleRecord)
	if renewedRole.RoleID != leadRole.RoleID || renewedRole.Summary != "lead renew" {
		t.Fatalf("project.lead.renew returned unexpected role: %+v", renewedRole)
	}
	renewRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.lead.renewed",
		EntityType:  "project_role",
		EntityID:    leadRole.RoleID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.lead.renewed"), renewRuntime, "project.lead.renewed")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.lead.changed"), renewRuntime, "project.lead.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, renewRuntime.PayloadJSON), "project.lead.renew", workspaceID, "human", actorID, projectID, actorID)

	transferResult, rpcErr := h.projectLeadTransfer(ctx, mustJSONRaw(projectLeadTransferParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		RoleID:       leadRole.RoleID,
		ToAgentID:    "lead-b",
		LeaseSeconds: 240,
		LeaseToken:   "lease-b",
		Summary:      "lead transfer",
	}))
	if rpcErr != nil {
		t.Fatalf("project.lead.transfer rpc error: %+v", rpcErr)
	}
	transferredRole := transferResult.(map[string]any)["role"].(sqlite.ProjectRoleRecord)
	if transferredRole.AgentID != "lead-b" || transferredRole.RoleID == leadRole.RoleID || transferredRole.Status != sqlite.ProjectRoleStatusActive {
		t.Fatalf("project.lead.transfer returned unexpected role: %+v", transferredRole)
	}
	transferRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.lead.transferred",
		EntityType:  "project_role",
		EntityID:    transferredRole.RoleID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.lead.transferred"), transferRuntime, "project.lead.transferred")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.lead.changed"), transferRuntime, "project.lead.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, transferRuntime.PayloadJSON), "project.lead.transfer", workspaceID, "human", actorID, projectID, actorID)

	assignResult, rpcErr := h.projectRoleAssign(ctx, mustJSONRaw(projectRoleAssignParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        actorID,
		AgentID:        "reviewer-a",
		RoleType:       "reviewer",
		WriteScopeJSON: `{"paths":["internal/server/**"]}`,
		Summary:        "review role",
	}))
	if rpcErr != nil {
		t.Fatalf("project.role.assign rpc error: %+v", rpcErr)
	}
	reviewerRole := assignResult.(map[string]any)["role"].(sqlite.ProjectRoleRecord)
	if reviewerRole.AgentID != "reviewer-a" || reviewerRole.RoleType != sqlite.ProjectRoleReviewer {
		t.Fatalf("project.role.assign returned unexpected role: %+v", reviewerRole)
	}
	assignRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.role.assigned",
		EntityType:  "project_role",
		EntityID:    reviewerRole.RoleID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.role.assigned"), assignRuntime, "project.role.assigned")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.role.changed"), assignRuntime, "project.role.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, assignRuntime.PayloadJSON), "project.role.assign", workspaceID, "human", actorID, projectID, actorID)

	activeList, rpcErr := h.projectRolesList(ctx, mustJSONRaw(projectRolesListParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.roles.list rpc error: %+v", rpcErr)
	}
	activeRoles := activeList.(map[string]any)["roles"].([]sqlite.ProjectRoleRecord)
	if len(activeRoles) != 2 {
		t.Fatalf("project.roles.list active count = %d, want 2: %+v", len(activeRoles), activeRoles)
	}

	if _, rpcErr := h.projectLeadRelease(ctx, mustJSONRaw(projectLeadReleaseParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
		RoleID:      transferredRole.RoleID,
		LeaseToken:  "wrong-lease",
		Summary:     "wrong release",
	})); rpcErr == nil {
		t.Fatal("expected project.lead.release with stale lease token to fail")
	}

	releaseResult, rpcErr := h.projectLeadRelease(ctx, mustJSONRaw(projectLeadReleaseParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
		RoleID:      transferredRole.RoleID,
		LeaseToken:  "lease-b",
		Summary:     "lead release",
	}))
	if rpcErr != nil {
		t.Fatalf("project.lead.release rpc error: %+v", rpcErr)
	}
	releasedRole := releaseResult.(map[string]any)["role"].(sqlite.ProjectRoleRecord)
	if releasedRole.Status != sqlite.ProjectRoleStatusReleased {
		t.Fatalf("project.lead.release returned unexpected role: %+v", releasedRole)
	}
	releaseRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.lead.released",
		EntityType:  "project_role",
		EntityID:    transferredRole.RoleID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.lead.released"), releaseRuntime, "project.lead.released")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.lead.changed"), releaseRuntime, "project.lead.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, releaseRuntime.PayloadJSON), "project.lead.release", workspaceID, "human", actorID, projectID, actorID)

	allList, rpcErr := h.projectRolesList(ctx, mustJSONRaw(projectRolesListParams{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		IncludeInactive: true,
	}))
	if rpcErr != nil {
		t.Fatalf("project.roles.list include inactive rpc error: %+v", rpcErr)
	}
	allRoles := allList.(map[string]any)["roles"].([]sqlite.ProjectRoleRecord)
	if len(allRoles) != 3 {
		t.Fatalf("project.roles.list include inactive count = %d, want 3: %+v", len(allRoles), allRoles)
	}
}

func TestProjectLeadClaimRejectsAgentDelegatingAnotherAgent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-project-lead-agent-self-claim"
	const projectID = "project-agent-self-claim"
	const leadA = "lead-a"
	const leadB = "lead-b"
	ctx := testAuthContext(workspaceID, "agent", leadA)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, leadA, leadA, leadB)
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Agent Self Claim",
		CreatedBy:   leadA,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}

	result, rpcErr := h.projectLeadClaim(ctx, mustJSONRaw(projectLeadClaimParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      leadA,
		AgentID:      leadB,
		LeaseSeconds: 120,
	}))
	if rpcErr == nil {
		t.Fatal("expected agent principal to be forbidden from claiming lead for another agent")
	}
	if rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied, got %+v result=%+v", rpcErr, result)
	}
}

func TestProjectRolesListRejectsMissingProject(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-project-roles-list-missing-project"
	const actorID = "operator-a"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, actorID, "lead-a")

	result, rpcErr := h.projectRolesList(ctx, mustJSONRaw(projectRolesListParams{
		WorkspaceID: workspaceID,
		ProjectID:   "missing-project",
	}))
	if rpcErr == nil {
		t.Fatal("expected missing project roles.list to fail")
	}
	if result != nil {
		t.Fatalf("expected no result for missing project, got %+v", result)
	}
}

func TestProjectRoleAssignRejectsInvalidWriteScopeBeforeStorage(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-project-roles-invalid-scope"
	const actorID = "operator-a"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, actorID, "reviewer-a")

	result, rpcErr := h.projectRoleAssign(ctx, mustJSONRaw(projectRoleAssignParams{
		WorkspaceID:    workspaceID,
		ProjectID:      "project-invalid-scope",
		ActorID:        actorID,
		AgentID:        "reviewer-a",
		RoleType:       "reviewer",
		WriteScopeJSON: `{"paths":`,
	}))
	if rpcErr == nil {
		t.Fatal("expected invalid write_scope_json to fail")
	}
	if result != nil {
		t.Fatalf("expected no result on invalid write_scope_json, got %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams || rpcErr.Message != "write_scope_json must be valid JSON" {
		t.Fatalf("unexpected invalid write_scope_json error: %+v", rpcErr)
	}
}

func seedProjectRolesWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string, agentIDs ...string) {
	t.Helper()
	seedProjectMutationWorkspace(t, ctx, store, workspaceID, actorID)
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: actorID,
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
}
