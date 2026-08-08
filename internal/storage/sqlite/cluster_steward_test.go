package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestElectClusterSteward_SuccessAndConflict(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	// 1. Elect first steward
	steward, err := store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   "cluster-1",
		EpochID:     "epoch-A",
		CandidateID: "agent-alpha",
		TTLSeconds:  60,
	})
	if err != nil {
		t.Fatalf("first election failed: %v", err)
	}
	if steward.StewardAgentID != "agent-alpha" {
		t.Errorf("expected agent-alpha to win election, got %s", steward.StewardAgentID)
	}

	// 2. Prevent second election for same cluster concurrently
	_, err = store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   "cluster-1",
		EpochID:     "epoch-B",
		CandidateID: "agent-beta",
		TTLSeconds:  60,
	})
	if err != ErrStewardshipActive {
		t.Errorf("expected ErrStewardshipActive, got %v", err)
	}

	// 3. Verify active steward is still alpha
	active, err := store.GetActiveSteward(ctx, "cluster-1")
	if err != nil {
		t.Fatalf("get active steward failed: %v", err)
	}
	if active.StewardAgentID != "agent-alpha" {
		t.Errorf("expected agent-alpha to remain active steward, got %s", active.StewardAgentID)
	}
}

func TestElectClusterSteward_LeaseExpirationAndRevocation(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	// Elect a short-lived steward, then force the row into an expired state
	// directly so the regression does not depend on real time passing.
	_, err := store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   "cluster-expiring",
		EpochID:     "epoch-1",
		CandidateID: "agent-alpha",
		TTLSeconds:  60,
	})
	if err != nil {
		t.Fatalf("election failed: %v", err)
	}

	expiredAt := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	_, err = store.DB().ExecContext(ctx, `
		UPDATE cluster_stewards
		SET expires_at = ?, status = 'ACTIVE'
		WHERE cluster_id = ? AND epoch_id = ?
	`, expiredAt, "cluster-expiring", "epoch-1")
	if err != nil {
		t.Fatalf("force expiry failed: %v", err)
	}

	// The active lookup should treat the lease as expired immediately.
	_, err = store.GetActiveSteward(ctx, "cluster-expiring")
	if err != ErrStewardNotFound {
		t.Fatalf("expected expired steward to be hidden, got %v", err)
	}

	// Beta should successfully elect because Alpha's lease organically expired inside ElectClusterSteward
	_, err = store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   "cluster-expiring",
		EpochID:     "epoch-2",
		CandidateID: "agent-beta",
		TTLSeconds:  60,
	})
	if err != nil {
		t.Fatalf("beta election should have succeeded after alpha expiration: %v", err)
	}

	// Validate beta is the active returning steward
	active, err := store.GetActiveSteward(ctx, "cluster-expiring")
	if err != nil {
		t.Fatalf("failed to get active steward: %v", err)
	}
	if active.StewardAgentID != "agent-beta" {
		t.Errorf("expected agent-beta, got %s", active.StewardAgentID)
	}

	// Revoke Beta's stewardship manually
	err = store.RevokeStewardship(ctx, "cluster-expiring", "epoch-2")
	if err != nil {
		t.Fatalf("revoke failed: %v", err)
	}

	// Verify no active steward
	_, err = store.GetActiveSteward(ctx, "cluster-expiring")
	if err != ErrStewardNotFound {
		t.Errorf("expected ErrStewardNotFound, got %v", err)
	}
}

func TestElectClusterStewardUsesWorkspaceReferenceTime(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	workspaceID := "ws-steward-authority"
	clusterID := "task:" + workspaceID + "/task-a"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Steward Authority",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	referenceAt := time.Now().UTC().Add(2 * time.Minute).Round(0)
	setWorkspaceControlEpochAnchor(t, store, workspaceID, referenceAt.Format(time.RFC3339Nano))

	steward, err := store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   clusterID,
		EpochID:     "epoch-1",
		CandidateID: "agent-alpha",
		TTLSeconds:  60,
	})
	if err != nil {
		t.Fatalf("elect steward with workspace authority failed: %v", err)
	}
	if !steward.GrantedAt.Equal(referenceAt) {
		t.Fatalf("expected granted_at %s, got %s", referenceAt.Format(time.RFC3339Nano), steward.GrantedAt.Format(time.RFC3339Nano))
	}
	if !steward.ExpiresAt.Equal(referenceAt.Add(time.Minute)) {
		t.Fatalf("expected expires_at %s, got %s", referenceAt.Add(time.Minute).Format(time.RFC3339Nano), steward.ExpiresAt.Format(time.RFC3339Nano))
	}

	expiredReferenceAt := referenceAt.Add(2 * time.Minute)
	setWorkspaceControlEpochAnchor(t, store, workspaceID, expiredReferenceAt.Format(time.RFC3339Nano))

	_, err = store.GetActiveSteward(ctx, clusterID)
	if err != ErrStewardNotFound {
		t.Fatalf("expected workspace authority anchor to expire steward, got %v", err)
	}
}

func TestElectClusterStewardRejectsSecondActiveEpochForSameAgent(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	input := ElectStewardInput{
		ClusterID:   "cluster-same-agent",
		EpochID:     "epoch-1",
		CandidateID: "agent-alpha",
		TTLSeconds:  60,
	}
	first, err := store.ElectClusterSteward(ctx, input)
	if err != nil {
		t.Fatalf("first election failed: %v", err)
	}

	replayed, err := store.ElectClusterSteward(ctx, input)
	if err != nil {
		t.Fatalf("same-epoch replay should be idempotent, got %v", err)
	}
	if !replayed.GrantedAt.Equal(first.GrantedAt) || !replayed.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("expected same-epoch replay to return original lease, first=%+v replayed=%+v", first, replayed)
	}

	_, err = store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   input.ClusterID,
		EpochID:     "epoch-2",
		CandidateID: input.CandidateID,
		TTLSeconds:  60,
	})
	if err != ErrStewardshipActive {
		t.Fatalf("expected second active epoch for same agent to be rejected, got %v", err)
	}

	active, err := store.GetActiveSteward(ctx, input.ClusterID)
	if err != nil {
		t.Fatalf("get active steward failed: %v", err)
	}
	if active.EpochID != input.EpochID {
		t.Fatalf("expected active epoch %s, got %s", input.EpochID, active.EpochID)
	}
}

func TestElectClusterSteward_CentralizationPenalty(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	// Agent "agent-greedy" will attempt to become steward of 4 clusters
	agentID := "agent-greedy"

	// 1. Elect for Cluster A
	_, err := store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   "cluster-A",
		EpochID:     "epoch-A1",
		CandidateID: agentID,
		TTLSeconds:  60,
	})
	if err != nil {
		t.Fatalf("first election failed: %v", err)
	}

	// 2. Elect for Cluster B
	_, err = store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   "cluster-B",
		EpochID:     "epoch-B1",
		CandidateID: agentID,
		TTLSeconds:  60,
	})
	if err != nil {
		t.Fatalf("second election failed: %v", err)
	}

	// 3. Elect for Cluster C
	_, err = store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   "cluster-C",
		EpochID:     "epoch-C1",
		CandidateID: agentID,
		TTLSeconds:  60,
	})
	if err != nil {
		t.Fatalf("third election failed: %v", err)
	}

	// 4. Attempt to elect for Cluster D (should fail due to centralization penalty)
	_, err = store.ElectClusterSteward(ctx, ElectStewardInput{
		ClusterID:   "cluster-D",
		EpochID:     "epoch-D1",
		CandidateID: agentID,
		TTLSeconds:  60,
	})
	if err == nil {
		t.Fatalf("expected centralization penalty to reject fourth lease, but it succeeded")
	}
	if !strings.Contains(err.Error(), "centralization penalty") {
		t.Fatalf("expected error indicating centralization penalty, got: %v", err)
	}
}
