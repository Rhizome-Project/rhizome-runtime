package server

import (
	"context"
	"encoding/json"
	"testing"
)

func TestWorkspaceInstrumentationControlStateRejectsInvalidMode(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	tests := []struct {
		name string
		call func(context.Context, json.RawMessage) (any, *RPCError)
	}{
		{name: "report", call: h.workspaceInstrumentationControlStateReport},
		{name: "snapshot", call: h.workspaceInstrumentationControlStateSnapshot},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(workspaceInstrumentationControlStateParams{
				WorkspaceID: scenario.workspaceID,
				Mode:        "totally-invalid-mode",
				ActorID:     "dashboard",
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			if _, rpcErr := tc.call(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params for invalid mode, got %+v", rpcErr)
			}
		})
	}
}

func TestWorkspaceInstrumentationControlStateScopedOperationsRejectMissingCluster(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	tests := []struct {
		name string
		call func(context.Context, json.RawMessage) (any, *RPCError)
	}{
		{name: "tick", call: h.workspaceInstrumentationControlStateTick},
		{name: "snapshot", call: h.workspaceInstrumentationControlStateSnapshot},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(workspaceInstrumentationControlStateParams{
				WorkspaceID:    scenario.workspaceID,
				ProtoClusterID: "task:" + scenario.workspaceID + "/missing",
				ActorID:        "dashboard",
				Limit:          10,
			})
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			if _, rpcErr := tc.call(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params for missing scoped cluster, got %+v", rpcErr)
			}
		})
	}
}
