package sqlite

import (
	"context"
	"testing"
)

func TestGetRSPCapabilityFlagsUsesWorkspacePolicies(t *testing.T) {
	t.Setenv("RHIZOME_RSP_LIVE_ACTUATION", "0")

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-capability-flags"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Capability Flags",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, capability := range []string{
		rspCapabilityGovernedHintsLive,
		rspCapabilitySafeLocalAutonomics,
		rspCapabilityBeliefLive,
	} {
		if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
			WorkspaceID: workspaceID,
			SubjectType: "workspace",
			SubjectID:   workspaceID,
			Capability:  capability,
			ToolID:      "*",
			Effect:      "ALLOW",
			Reason:      "enable for test",
			CreatedBy:   "tester",
		}); err != nil {
			t.Fatalf("put capability policy %s: %v", capability, err)
		}
	}

	flags := store.GetRSPCapabilityFlags(ctx, workspaceID)
	if !flags.GovernedHintsLive || !flags.SafeLocalAutonomicsLive || !flags.BeliefLive {
		t.Fatalf("expected policy-enabled flags, got %+v", flags)
	}
	if !flags.AnomalyShadow || !flags.StateShadow {
		t.Fatalf("expected shipped shadow defaults to remain enabled, got %+v", flags)
	}
}
