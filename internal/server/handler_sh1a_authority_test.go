package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceAuthorityStatusReturnsMissingLeaseState(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-authority-status-missing"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Authority Status Missing",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceAuthorityStatusParams{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("marshal authority status params: %v", err)
	}
	result, rpcErr := h.workspaceAuthorityStatus(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspace.authority.status returned rpc error: %+v", rpcErr)
	}
	status, ok := result.(sqlite.LocalWorkspaceAuthorityStatus)
	if !ok {
		t.Fatalf("expected authority status result, got %+v", result)
	}
	if status.LeaseState != "missing" || status.Authority != nil {
		t.Fatalf("expected missing lease state with nil authority row, got %+v", status)
	}
}

func TestWorkspaceAuthorityEnsureLocalClaimsMissingAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-authority-ensure-local-claim"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Authority Ensure Local Claim",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceAuthorityEnsureLocalParams{
		WorkspaceID: workspaceID,
		ActorType:   "operator",
		ActorID:     "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal authority ensure-local params: %v", err)
	}
	result, rpcErr := h.workspaceAuthorityEnsureLocal(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspace.authority.ensure_local returned rpc error: %+v", rpcErr)
	}
	response, ok := result.(sqlite.EnsureLocalWorkspaceAuthorityResult)
	if !ok {
		t.Fatalf("expected authority ensure-local result, got %+v", result)
	}
	if response.Action != "CLAIMED" {
		t.Fatalf("expected CLAIMED action, got %+v", response)
	}
	if response.RuntimeEvent == nil || response.RuntimeEvent.EventType != sqlite.AuthorityEventGranted {
		t.Fatalf("expected authority.granted runtime event, got %+v", response.RuntimeEvent)
	}
}

func TestWorkspaceAuthorityEnsureLocalRejectsForeignLiveHolderWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-authority-ensure-local-foreign-live"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Authority Ensure Local Foreign Live",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-7101")

	raw, err := json.Marshal(workspaceAuthorityEnsureLocalParams{
		WorkspaceID: workspaceID,
		ActorType:   "operator",
		ActorID:     "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal authority ensure-local params: %v", err)
	}
	result, rpcErr := h.workspaceAuthorityEnsureLocal(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for foreign live holder")
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
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) {
		t.Fatalf("expected authority_stale reject code, got %+v", details)
	}
	if details["surface"] != "workspace.authority.ensure_local" {
		t.Fatalf("expected ensure_local surface, got %+v", details)
	}
}

func TestWorkspaceAuthorityForceBreakExpiresCurrentHolder(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-authority-force-break"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Authority Force Break",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-7102")

	raw, err := json.Marshal(workspaceAuthorityForceBreakParams{
		WorkspaceID: workspaceID,
		ActorType:   "operator",
		ActorID:     "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal authority force-break params: %v", err)
	}
	result, rpcErr := h.workspaceAuthorityForceBreak(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspace.authority.force_break returned rpc error: %+v", rpcErr)
	}
	response, ok := result.(sqlite.ForceBreakWorkspaceAuthorityResult)
	if !ok {
		t.Fatalf("expected authority force-break result, got %+v", result)
	}
	if response.Action != "EXPIRED" {
		t.Fatalf("expected EXPIRED action, got %+v", response)
	}
	if response.RuntimeEvent == nil || response.RuntimeEvent.EventType != sqlite.AuthorityEventExpired {
		t.Fatalf("expected authority.expired runtime event, got %+v", response.RuntimeEvent)
	}
	if response.Status.Authority == nil || response.Status.Authority.Status != sqlite.WorkspaceAuthorityStatusExpired {
		t.Fatalf("expected expired authority row in response, got %+v", response.Status)
	}
}
