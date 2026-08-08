package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestPutCapabilityPolicyWithEventRejectsExpiredWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-policy-put-with-event-expired-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Policy WithEvent Expired Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimWorkspaceAuthorityForTest(t, ctx, store, workspaceID)
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
		ReferenceAt:           time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("expire workspace authority: %v", err)
	}

	_, _, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "should fail closed after authority expiry",
		CreatedBy:   "operator-expired",
	})
	if err == nil {
		t.Fatal("expected expired workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease expired reject code, got %+v", reject)
	}

	assertCapabilityPolicyRowCount(t, ctx, store, workspaceID, 0)
	assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 0)
}

func TestPutCapabilityPolicyRejectsExpiredWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-policy-put-expired-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Policy Expired Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimWorkspaceAuthorityForTest(t, ctx, store, workspaceID)
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
		ReferenceAt:           time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("expire workspace authority: %v", err)
	}

	_, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "should fail closed after authority expiry",
		CreatedBy:   "operator-expired",
	})
	if err == nil {
		t.Fatal("expected expired workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease expired reject code, got %+v", reject)
	}

	assertCapabilityPolicyRowCount(t, ctx, store, workspaceID, 0)
	assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 0)
}

func TestSetRSPCapabilityFlagsRejectsExpiredWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-rsp-capability-expired-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C RSP Capability Expired Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimWorkspaceAuthorityForTest(t, ctx, store, workspaceID)
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
		ReferenceAt:           time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("expire workspace authority: %v", err)
	}

	enable := true
	_, err := store.SetRSPCapabilityFlags(ctx, sqlite.SetRSPCapabilityFlagsInput{
		WorkspaceID:       workspaceID,
		GovernedHintsLive: &enable,
		ForecastShadow:    &enable,
		UpdatedBy:         "operator-expired",
		Reason:            "should fail closed after authority expiry",
	})
	if err == nil {
		t.Fatal("expected expired workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease expired reject code, got %+v", reject)
	}

	assertCapabilityPolicyRowCount(t, ctx, store, workspaceID, 0)
	assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 0)
}

func claimWorkspaceAuthorityForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) sqlite.WorkspaceAuthorityRecord {
	t.Helper()

	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	now := time.Now().UTC()
	record, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           workspaceID,
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-" + workspaceID,
		Term:                  1,
		LeaseExpiresAt:        now.Add(time.Hour).Format(time.RFC3339Nano),
		ReferenceAt:           now.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}
	return record
}

func assertCapabilityPolicyRowCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_capability_policies WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count capability policies: %v", err)
	}
	if got != want {
		t.Fatalf("capability policy count = %d, want %d", got, want)
	}
}
