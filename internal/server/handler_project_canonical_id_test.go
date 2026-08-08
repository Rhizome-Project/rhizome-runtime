package server

import (
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectRPCsResolveCaseVariantProjectIDToCanonical(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-project-rpc-case-alias"
		actorID     = "operator-a"
		agentID     = "beta"
		projectID   = "project-signal01-checkpoint-fixture-20260602T235249Z"
	)
	projectAlias := strings.ToLower(projectID)
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, actorID, agentID)
	if _, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project RPC Case Alias",
		CreatedBy:   actorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}

	roleResult, rpcErr := h.projectRoleAssign(ctx, mustJSONRaw(projectRoleAssignParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectAlias,
		ActorID:     actorID,
		AgentID:     agentID,
		RoleType:    sqlite.ProjectRoleReviewer,
		Summary:     "case alias regression reviewer",
	}))
	if rpcErr != nil {
		t.Fatalf("project.role.assign case alias rpc error: %+v", rpcErr)
	}
	role := roleResult.(map[string]any)["role"].(sqlite.ProjectRoleRecord)
	if role.ProjectID != projectID {
		t.Fatalf("role project_id = %q, want canonical %q", role.ProjectID, projectID)
	}
	roleRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.role.assigned",
		EntityType:  "project_role",
		EntityID:    role.RoleID,
	})
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, roleRuntime.PayloadJSON), "project.role.assign", workspaceID, "human", actorID, projectID, actorID)

	coordinationResult, rpcErr := h.projectCoordinationGet(ctx, mustJSONRaw(projectCoordinationGetParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectAlias,
	}))
	if rpcErr != nil {
		t.Fatalf("project.coordination.get case alias rpc error: %+v", rpcErr)
	}
	coordination := coordinationResult.(map[string]any)["coordination"].(sqlite.ProjectCoordinationRecord)
	if coordination.Project.ProjectID != projectID || len(coordination.Roles) != 1 || coordination.Roles[0].ProjectID != projectID {
		t.Fatalf("coordination did not resolve canonical project id: %+v", coordination)
	}

	listResult, rpcErr := h.projectPatchQueueList(ctx, mustJSONRaw(projectPatchQueueListParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectAlias,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.list case alias rpc error: %+v", rpcErr)
	}
	if got := listResult.(map[string]any)["count"].(int); got != 0 {
		t.Fatalf("empty project patch queue count = %d, want 0", got)
	}
}
