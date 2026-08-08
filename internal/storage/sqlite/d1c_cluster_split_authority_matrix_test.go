package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestFirstProtectedMutationClusterRejectsLoserUnderSplitWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	t.Run("capability policy put", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		workspaceID := "ws-d1c-split-capability-policy-put"
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "D1C Split Capability Policy Put",
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

		if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
			WorkspaceID: workspaceID,
			SubjectType: "workspace",
			SubjectID:   workspaceID,
			Capability:  "rsp.governed_hints.live",
			ToolID:      "*",
			Effect:      "ALLOW",
			Reason:      "winner write should succeed before split",
			CreatedBy:   "operator-winner",
		}); err != nil {
			t.Fatalf("winner put capability policy: %v", err)
		}
		assertCapabilityPolicyRowCount(t, ctx, store, workspaceID, 1)
		assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 1)

		beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
		transferExternalWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1601")

		_, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
			WorkspaceID: workspaceID,
			SubjectType: "workspace",
			SubjectID:   workspaceID,
			Capability:  "rsp.governed_hints.live",
			ToolID:      "*",
			Effect:      "DENY",
			Reason:      "loser should reject after split",
			CreatedBy:   "operator-loser",
		})
		assertStaleAuthorityReject(t, err)
		assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectStale)
		assertCapabilityPolicyRowCount(t, ctx, store, workspaceID, 1)
		assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 1)
	})

	t.Run("capability policy put with event", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		workspaceID := "ws-d1c-split-capability-policy-put-event"
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "D1C Split Capability Policy Put Event",
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

		firstPolicy, firstEvent, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
			WorkspaceID: workspaceID,
			SubjectType: "workspace",
			SubjectID:   workspaceID,
			Capability:  "rsp.governed_hints.live",
			ToolID:      "*",
			Effect:      "ALLOW",
			Reason:      "winner write should succeed before split",
			CreatedBy:   "operator-winner",
		})
		if err != nil {
			t.Fatalf("winner put capability policy with event: %v", err)
		}
		if firstEvent.EventType != "capability_policy.put" || firstPolicy.PolicyID == "" {
			t.Fatalf("unexpected winner capability policy result policy=%+v event=%+v", firstPolicy, firstEvent)
		}
		assertCapabilityPolicyRowCount(t, ctx, store, workspaceID, 1)
		assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 1)

		beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
		transferExternalWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1602")

		loserPolicy, loserEvent, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
			WorkspaceID: workspaceID,
			SubjectType: "workspace",
			SubjectID:   workspaceID,
			Capability:  "rsp.governed_hints.live",
			ToolID:      "*",
			Effect:      "DENY",
			Reason:      "loser should reject after split",
			CreatedBy:   "operator-loser",
		})
		assertStaleAuthorityReject(t, err)
		if loserPolicy != (sqlite.CapabilityPolicyRecord{}) || loserEvent != (sqlite.RuntimeEventRecord{}) {
			t.Fatalf("expected loser write to return empty records, got policy=%+v event=%+v", loserPolicy, loserEvent)
		}
		assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectStale)
		assertCapabilityPolicyRowCount(t, ctx, store, workspaceID, 1)
		assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 1)
	})

	t.Run("rsp capability flags", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		workspaceID := "ws-d1c-split-rsp-capability-flags"
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "D1C Split RSP Capability Flags",
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

		enabled := true
		disabled := false
		if _, err := store.SetRSPCapabilityFlags(ctx, sqlite.SetRSPCapabilityFlagsInput{
			WorkspaceID:       workspaceID,
			GovernedHintsLive: &enabled,
			ForecastShadow:    &disabled,
			UpdatedBy:         "operator-winner",
			Reason:            "winner write should succeed before split",
		}); err != nil {
			t.Fatalf("winner set rsp capability flags: %v", err)
		}
		assertCapabilityPolicyRowCount(t, ctx, store, workspaceID, 2)
		assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 2)

		beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
		transferExternalWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1603")

		_, err := store.SetRSPCapabilityFlags(ctx, sqlite.SetRSPCapabilityFlagsInput{
			WorkspaceID:       workspaceID,
			GovernedHintsLive: &disabled,
			ForecastShadow:    &enabled,
			UpdatedBy:         "operator-loser",
			Reason:            "loser should reject after split",
		})
		assertStaleAuthorityReject(t, err)
		assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectStale)
		assertCapabilityPolicyRowCount(t, ctx, store, workspaceID, 2)
		assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 2)
	})

	t.Run("control command request", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		workspaceID := "ws-d1c-split-control-command"
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "D1C Split Control Command",
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

		if _, _, err := store.RequestControlCommandWithEvent(ctx, sqlite.ControlCommandInput{
			WorkspaceID: workspaceID,
			CommandType: sqlite.ControlCommandRefreshKernel,
			AgentID:     "agent-winner",
			Reason:      "winner write should succeed before split",
			RequestedBy: "operator-winner",
		}); err != nil {
			t.Fatalf("winner request control command: %v", err)
		}
		if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "control.command.requested",
			EntityType:  "control_command",
			Limit:       10,
		}); got != 1 {
			t.Fatalf("expected 1 control.command.requested row after winner write, got %d", got)
		}

		beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
		transferExternalWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1604")

		_, _, err := store.RequestControlCommandWithEvent(ctx, sqlite.ControlCommandInput{
			WorkspaceID: workspaceID,
			CommandType: sqlite.ControlCommandRefreshKernel,
			AgentID:     "agent-loser",
			Reason:      "loser should reject after split",
			RequestedBy: "operator-loser",
		})
		assertStaleAuthorityReject(t, err)
		assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectStale)
		if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "control.command.requested",
			EntityType:  "control_command",
			Limit:       10,
		}); got != 1 {
			t.Fatalf("expected loser split to not add control.command.requested rows, got %d", got)
		}
	})
}

func assertStaleAuthorityReject(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("expected stale authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}
}
