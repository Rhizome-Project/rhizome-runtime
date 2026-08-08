package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceControlCommandRequestRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-control-command-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceControlCommandRequestParams{
		WorkspaceID: workspaceID,
		CommandType: sqlite.ControlCommandRefreshKernel,
		AgentID:     "agent-a",
		Reason:      "request should fail before authority is present",
		RequestedBy: "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal control command params: %v", err)
	}

	result, rpcErr := h.workspaceControlCommandRequest(testAuthContext(workspaceID, "human", "dashboard"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	if rpcErr.Message != "authority rejected" {
		t.Fatalf("expected authority rejected message, got %+v", rpcErr)
	}

	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["error_kind"] != "authority_reject" {
		t.Fatalf("expected authority reject kind, got %+v", details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectMissing) {
		t.Fatalf("expected authority_missing reject code, got %+v", details)
	}
	if details["workspace_id"] != workspaceID {
		t.Fatalf("expected workspace_id in authority reject details, got %+v", details)
	}
	if details["surface"] != "workspace.control.command.request" {
		t.Fatalf("expected request surface in authority reject details, got %+v", details)
	}
	if details["scope"] != "workspace" {
		t.Fatalf("expected workspace scope in authority reject details, got %+v", details)
	}

	commandEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "control.command.requested",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list canonical control command events: %v", err)
	}
	if len(commandEvents) != 0 {
		t.Fatalf("expected no control.command.requested event on missing authority, got %+v", commandEvents)
	}

	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list runtime events after missing-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected missing-authority request not to persist runtime events, got %+v", events)
	}
}
