package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspacePolicyPutRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-policy-put-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Policy Put Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspacePolicyPutParams{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "should fail closed before policy write",
		CreatedBy:   "operator-a",
	})
	if err != nil {
		t.Fatalf("marshal policy put params: %v", err)
	}

	result, rpcErr := h.workspacePolicyPut(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectMissing) || details["surface"] != "workspace.policy.put" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if items, err := store.ListCapabilityPolicies(ctx, sqlite.CapabilityPolicyFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list capability policies after missing-authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no capability policy rows on missing-authority reject, got %+v", items)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		Limit:       10,
	}); err != nil {
		t.Fatalf("list capability policy runtime events after missing-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no capability policy runtime events on missing-authority reject, got %+v", events)
	}
}

func TestWorkspacePolicyPutRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-policy-put-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Policy Put Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-905")

	raw, err := json.Marshal(workspacePolicyPutParams{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "should fail closed on stale authority",
		CreatedBy:   "operator-b",
	})
	if err != nil {
		t.Fatalf("marshal policy put params: %v", err)
	}

	result, rpcErr := h.workspacePolicyPut(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "workspace.policy.put" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if items, err := store.ListCapabilityPolicies(ctx, sqlite.CapabilityPolicyFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list capability policies after stale-authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no capability policy rows on stale-authority reject, got %+v", items)
	}
}

func TestWorkspaceRSPCapabilityPutRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-rsp-capability-put-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C RSP Capability Put Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	enable := true
	raw, err := json.Marshal(workspaceRSPCapabilityPutParams{
		WorkspaceID:             workspaceID,
		GovernedHintsLive:       &enable,
		SafeLocalAutonomicsLive: &enable,
		UpdatedBy:               "operator-c",
		Reason:                  "should fail closed before capability write",
	})
	if err != nil {
		t.Fatalf("marshal rsp capability put params: %v", err)
	}

	result, rpcErr := h.workspaceRSPCapabilityPut(testAuthContext(workspaceID, "human", "operator-c"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectMissing) || details["surface"] != "workspace.rsp.capability.put" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if items, err := store.ListCapabilityPolicies(ctx, sqlite.CapabilityPolicyFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list capability policies after missing-authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no capability policy rows on missing-authority reject, got %+v", items)
	}
}

func TestWorkspaceRSPCapabilityPutRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-rsp-capability-put-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C RSP Capability Put Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-906")

	enable := true
	raw, err := json.Marshal(workspaceRSPCapabilityPutParams{
		WorkspaceID:       workspaceID,
		GovernedHintsLive: &enable,
		ForecastShadow:    &enable,
		UpdatedBy:         "operator-d",
		Reason:            "should fail closed on stale authority",
	})
	if err != nil {
		t.Fatalf("marshal rsp capability put params: %v", err)
	}

	result, rpcErr := h.workspaceRSPCapabilityPut(testAuthContext(workspaceID, "human", "operator-d"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "workspace.rsp.capability.put" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if items, err := store.ListCapabilityPolicies(ctx, sqlite.CapabilityPolicyFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list capability policies after stale-authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no capability policy rows on stale-authority reject, got %+v", items)
	}
}
