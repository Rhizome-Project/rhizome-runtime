package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestPutCapabilityPolicyRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-policy-put-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Policy Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	_, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "should fail closed before policy write",
		CreatedBy:   "operator-a",
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	assertCapabilityPolicyCount(t, ctx, store, workspaceID, 0)
	assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 0)
}

func TestPutCapabilityPolicyWithEventRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-policy-put-with-event-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Policy WithEvent Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	transferExternalWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1505")
	_, _, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "should fail closed on stale authority",
		CreatedBy:   "operator-b",
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject code, got %+v", reject)
	}

	assertCapabilityPolicyCount(t, ctx, store, workspaceID, 0)
	assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 0)
}

func TestSetRSPCapabilityFlagsRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-rsp-capability-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C RSP Capability Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	enable := true
	_, err := store.SetRSPCapabilityFlags(ctx, sqlite.SetRSPCapabilityFlagsInput{
		WorkspaceID:       workspaceID,
		GovernedHintsLive: &enable,
		ForecastShadow:    &enable,
		UpdatedBy:         "operator-c",
		Reason:            "should fail closed before rsp capability write",
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	assertCapabilityPolicyCount(t, ctx, store, workspaceID, 0)
	assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 0)
}

func TestSetRSPCapabilityFlagsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-rsp-capability-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C RSP Capability Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	transferExternalWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1506")
	enable := true
	_, err := store.SetRSPCapabilityFlags(ctx, sqlite.SetRSPCapabilityFlagsInput{
		WorkspaceID:             workspaceID,
		GovernedHintsLive:       &enable,
		SafeLocalAutonomicsLive: &enable,
		UpdatedBy:               "operator-d",
		Reason:                  "should fail closed on stale authority",
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject code, got %+v", reject)
	}

	assertCapabilityPolicyCount(t, ctx, store, workspaceID, 0)
	assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 0)
}

func assertCapabilityPolicyCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()
	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_capability_policies WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count capability policies: %v", err)
	}
	if got != want {
		t.Fatalf("capability policy count = %d, want %d", got, want)
	}
}

func assertCapabilityPolicyRuntimeEventCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()
	got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		Limit:       20,
	})
	if got != want {
		t.Fatalf("capability_policy.put runtime event count = %d, want %d", got, want)
	}
}
