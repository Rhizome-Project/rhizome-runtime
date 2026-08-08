package server

import (
	"context"
	"encoding/json"
	"testing"
)

func TestRuntimeBuildInfoRequiresAuthenticatedWorkspacePrincipal(t *testing.T) {
	h := &Handler{}
	raw, err := json.Marshal(runtimeBuildInfoParams{WorkspaceID: "rhizome-main"})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	if _, rpcErr := h.runtimeBuildInfo(context.Background(), raw); rpcErr == nil {
		t.Fatal("expected unauthenticated runtime.build.info call to be rejected")
	} else if rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied, got %+v", rpcErr)
	}

	mismatchCtx := testAuthContext("other-workspace", "agent", "alpha")
	if _, rpcErr := h.runtimeBuildInfo(mismatchCtx, raw); rpcErr == nil {
		t.Fatal("expected workspace isolation violation for mismatched principal")
	} else if rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied for workspace mismatch, got %+v", rpcErr)
	}
}

func TestRuntimeBuildInfoReturnsBuildIdentity(t *testing.T) {
	h := &Handler{}
	raw, err := json.Marshal(runtimeBuildInfoParams{WorkspaceID: "rhizome-main"})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	ctx := testAuthContext("rhizome-main", "agent", "alpha")
	result, rpcErr := h.runtimeBuildInfo(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("runtime.build.info rpc error: %+v", rpcErr)
	}

	// Round-trip through JSON to assert the non-secret identity shape the
	// managed-runtime preflight depends on for build-parity (CT-01).
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if _, ok := decoded["go_version"]; !ok {
		t.Fatalf("expected go_version in runtime build info, got %s", string(payload))
	}
	if _, ok := decoded["workspace_id"]; ok {
		t.Fatalf("runtime build info must not echo request params, got %s", string(payload))
	}
}
