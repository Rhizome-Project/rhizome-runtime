package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceRSPCapabilityGetAndPut(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-rsp-capability-handler"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Capability Handler",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	initialAny, rpcErr := h.workspaceRSPCapabilityGet(ctx, mustJSONRaw(workspaceRSPCapabilityGetParams{
		WorkspaceID: workspaceID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPCapabilityGet rpc error: %+v", rpcErr)
	}
	initial := initialAny.(sqlite.RSPCapabilityFlags)
	if !initial.AnomalyShadow || !initial.StateShadow {
		t.Fatalf("expected shipped shadow defaults, got %+v", initial)
	}

	enable := true
	updatedAny, rpcErr := h.workspaceRSPCapabilityPut(testAuthContext(workspaceID, "human", "operator-a"), mustJSONRaw(workspaceRSPCapabilityPutParams{
		WorkspaceID:             workspaceID,
		GovernedHintsLive:       &enable,
		SafeLocalAutonomicsLive: &enable,
		UpdatedBy:               "operator-a",
		Reason:                  "enable unified rsp gates for testing",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPCapabilityPut rpc error: %+v", rpcErr)
	}
	updated := updatedAny.(sqlite.RSPCapabilityFlags)
	if !updated.GovernedHintsLive || !updated.SafeLocalAutonomicsLive {
		t.Fatalf("expected updated capability flags, got %+v", updated)
	}
}

func TestWorkspaceRSPCapabilityPutRequiresUpdatedBy(t *testing.T) {
	t.Parallel()

	h := NewHandler(newServerTestStore(t))
	if _, rpcErr := h.workspaceRSPCapabilityPut(context.Background(), mustJSONRaw(workspaceRSPCapabilityPutParams{
		WorkspaceID: "ws-rsp-capability-missing-updated-by",
	})); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected missing updated_by invalid params error, got %+v", rpcErr)
	}
}
