package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceRSPStateSnapshotReturnsPermissionDeniedWhenCapabilityDisabled(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedHandlerLocusSidecarScenario(t, ctx, store, "rsp-state-policy")
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.state.shadow",
		ToolID:      "*",
		Effect:      "DENY",
		Reason:      "disable state shadow for handler test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("disable state shadow capability: %v", err)
	}

	_, rpcErr := callWorkspaceRSPStateSnapshotRaw(t, h, ctx, mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied rpc error, got %+v", rpcErr)
	}
}
