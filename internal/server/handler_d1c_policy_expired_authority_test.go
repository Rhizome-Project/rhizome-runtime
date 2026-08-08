package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspacePolicyPutRejectsExpiredWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-policy-put-expired-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Policy Put Expired Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	expireServerTestWorkspaceAuthority(t, ctx, store, workspaceID, current)

	raw, err := json.Marshal(workspacePolicyPutParams{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "should fail closed on expired authority",
		CreatedBy:   "operator-expired-a",
	})
	if err != nil {
		t.Fatalf("marshal policy put params: %v", err)
	}

	result, rpcErr := h.workspacePolicyPut(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for expired workspace authority")
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
	if details["reject_code"] != string(sqlite.AuthorityRejectLeaseExpired) || details["surface"] != "workspace.policy.put" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if items, err := store.ListCapabilityPolicies(ctx, sqlite.CapabilityPolicyFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list capability policies after expired-authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no capability policy rows on expired-authority reject, got %+v", items)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		Limit:       10,
	}); err != nil {
		t.Fatalf("list capability policy runtime events after expired-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no capability policy runtime events on expired-authority reject, got %+v", events)
	}
	assertServerAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectLeaseExpired))
}

func TestWorkspaceRSPCapabilityPutRejectsExpiredWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-rsp-capability-put-expired-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C RSP Capability Put Expired Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	expireServerTestWorkspaceAuthority(t, ctx, store, workspaceID, current)

	enable := true
	raw, err := json.Marshal(workspaceRSPCapabilityPutParams{
		WorkspaceID:             workspaceID,
		GovernedHintsLive:       &enable,
		SafeLocalAutonomicsLive: &enable,
		UpdatedBy:               "operator-expired-b",
		Reason:                  "should fail closed on expired authority",
	})
	if err != nil {
		t.Fatalf("marshal rsp capability put params: %v", err)
	}

	result, rpcErr := h.workspaceRSPCapabilityPut(testAuthContext(workspaceID, "human", "operator-expired-b"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for expired workspace authority")
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
	if details["reject_code"] != string(sqlite.AuthorityRejectLeaseExpired) || details["surface"] != "workspace.rsp.capability.put" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if items, err := store.ListCapabilityPolicies(ctx, sqlite.CapabilityPolicyFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list capability policies after expired-authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no capability policy rows on expired-authority reject, got %+v", items)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		Limit:       10,
	}); err != nil {
		t.Fatalf("list capability policy runtime events after expired-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no capability policy runtime events on expired-authority reject, got %+v", events)
	}
	assertServerAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectLeaseExpired))
}

func expireServerTestWorkspaceAuthority(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, current sqlite.WorkspaceAuthorityRecord) {
	t.Helper()

	referenceAt := time.Now().UTC().Round(0)
	if _, _, err := store.ExpireWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityExpireInput{
		WorkspaceID:           workspaceID,
		Scope:                 "workspace",
		HolderAuthorityNodeID: current.HolderAuthorityNodeID,
		LeaseToken:            current.LeaseToken,
		Term:                  current.Term,
		CommitWatermark:       current.CommitWatermark,
		AppliedWatermark:      current.AppliedWatermark,
		ActorType:             "system",
		ActorID:               "tests",
		ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("expire workspace authority: %v", err)
	}
}

func assertServerAuthorityRejectEvent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, wantRejectCode string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   sqlite.AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list authority rejected events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected authority.rejected event")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority reject payload: %v", err)
	}
	if payload["reject_code"] != wantRejectCode {
		t.Fatalf("expected authority reject code %q, got %+v", wantRejectCode, payload)
	}
}
