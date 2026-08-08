package sqlite

import (
	"context"
	"fmt"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestCurrentReviewerScarcityHealthSnapshotMarksSaturatedAndScarceWorkspacesDegraded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)

	const saturatedWorkspaceID = "ws-reviewer-scarcity-health-saturated"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: saturatedWorkspaceID,
		Title:       "Reviewer Scarcity Health Saturated",
		Description: "Bounded reviewer scarcity health",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create saturated workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, saturatedWorkspaceID, "agent-gen", "reviewer-a", "reviewer-b", "reviewer-c")
	setReviewerRouteAgentRole(t, ctx, store, saturatedWorkspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, saturatedWorkspaceID, "reviewer-b", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, saturatedWorkspaceID, "reviewer-c", "reviewer")

	liveAt := "2026-04-12T11:00:00Z"
	for _, tensionID := range []string{"tension-a", "tension-b", "tension-c"} {
		insertReviewerRouteLoadTension(t, ctx, store, saturatedWorkspaceID, tensionID, liveAt)
	}
	insertCoalitionSurfaceCoalition(t, ctx, store, saturatedWorkspaceID, "tension-a", "coal-a", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, saturatedWorkspaceID, "coal-a", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, saturatedWorkspaceID, "coal-a", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, saturatedWorkspaceID, "tension-b", "coal-b", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, saturatedWorkspaceID, "coal-b", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, saturatedWorkspaceID, "coal-b", "reviewer-b", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, saturatedWorkspaceID, "tension-c", "coal-c", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, saturatedWorkspaceID, "coal-c", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, saturatedWorkspaceID, "coal-c", "reviewer-c", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	const scarceWorkspaceID = "ws-reviewer-scarcity-health-scarce"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: scarceWorkspaceID,
		Title:       "Reviewer Scarcity Health Scarce",
		Description: "Bounded reviewer scarcity health",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create scarce workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, scarceWorkspaceID, "agent-gen-a", "agent-gen-b", "typed-a", "typed-b", "generalist-c")
	setReviewerRouteAgentRole(t, ctx, store, scarceWorkspaceID, "typed-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, scarceWorkspaceID, "typed-b", "reviewer")
	insertReviewerRouteLoadTension(t, ctx, store, scarceWorkspaceID, "tension-typed-headroom-a", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, scarceWorkspaceID, "tension-typed-headroom-a", "coal-typed-headroom-a", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, scarceWorkspaceID, "coal-typed-headroom-a", "agent-gen-a", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, scarceWorkspaceID, "coal-typed-headroom-a", "typed-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)
	insertReviewerRouteLoadTension(t, ctx, store, scarceWorkspaceID, "tension-typed-headroom-b", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, scarceWorkspaceID, "tension-typed-headroom-b", "coal-typed-headroom-b", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, scarceWorkspaceID, "coal-typed-headroom-b", "agent-gen-b", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, scarceWorkspaceID, "coal-typed-headroom-b", "generalist-c", "FAR_REVIEWER", 0.8, 0.6, 4, liveAt)

	snapshot := store.CurrentReviewerScarcityHealthSnapshot(ctx)
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded reviewer scarcity health snapshot, got %+v", snapshot)
	}
	if snapshot.WorkspaceCount != 2 {
		t.Fatalf("expected two workspaces in reviewer scarcity health snapshot, got %+v", snapshot)
	}
	if snapshot.SaturatedWorkspaceCount != 1 {
		t.Fatalf("expected one saturated workspace, got %+v", snapshot)
	}
	if snapshot.ScarceWorkspaceCount != 1 {
		t.Fatalf("expected one scarce workspace, got %+v", snapshot)
	}
	if snapshot.UnknownWorkspaceCount != 0 {
		t.Fatalf("expected zero unknown workspaces in degraded fixture, got %+v", snapshot)
	}
	if len(snapshot.SaturatedWorkspaceExamples) != 1 || snapshot.SaturatedWorkspaceExamples[0] != saturatedWorkspaceID {
		t.Fatalf("expected saturated workspace example %q, got %+v", saturatedWorkspaceID, snapshot)
	}
	if len(snapshot.ScarceWorkspaceExamples) != 1 || snapshot.ScarceWorkspaceExamples[0] != scarceWorkspaceID {
		t.Fatalf("expected scarce workspace example %q, got %+v", scarceWorkspaceID, snapshot)
	}
	if len(snapshot.UnknownWorkspaceExamples) != 0 {
		t.Fatalf("expected no unknown workspace examples in degraded fixture, got %+v", snapshot)
	}
}

func TestCurrentReviewerScarcityHealthSnapshotIgnoresUnknownWorkspaceWithoutReviewerLoadEvidence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-health-unknown"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Health Unknown",
		Description: "Bounded reviewer scarcity health",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create unknown workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")

	snapshot := store.CurrentReviewerScarcityHealthSnapshot(ctx)
	if snapshot.State != "ok" {
		t.Fatalf("expected unknown workspace without reviewer load evidence to be ignored, got %+v", snapshot)
	}
	if snapshot.WorkspaceCount != 1 {
		t.Fatalf("expected one workspace in reviewer scarcity health snapshot, got %+v", snapshot)
	}
	if snapshot.UnknownWorkspaceCount != 0 || snapshot.SaturatedWorkspaceCount != 0 || snapshot.ScarceWorkspaceCount != 0 {
		t.Fatalf("expected unknown-only workspace without load evidence to stay green, got %+v", snapshot)
	}
}

func TestCurrentReviewerScarcityHealthSnapshotIgnoresUnknownWorkspaceWithOnlineTypedCapacityAndOnlyLiveCoalitionEvidence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-health-unknown-load"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Health Unknown With Load",
		Description: "Reviewer load evidence should still degrade unknown scarcity",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create unknown workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")

	const liveAt = "2026-04-15T18:00:00Z"
	insertReviewerRouteLoadTension(t, ctx, store, workspaceID, "tension-unknown-load", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-unknown-load", "coal-unknown-load", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-unknown-load", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)

	snapshot := store.CurrentReviewerScarcityHealthSnapshot(ctx)
	if snapshot.State != "ok" {
		t.Fatalf("expected live coalition without reviewer load to stay ok when online typed reviewer capacity exists, got %+v", snapshot)
	}
	if snapshot.WorkspaceCount != 1 {
		t.Fatalf("expected one workspace in reviewer scarcity health snapshot, got %+v", snapshot)
	}
	if snapshot.UnknownWorkspaceCount != 0 {
		t.Fatalf("expected no unknown workspace degradation when reviewer capacity exists, got %+v", snapshot)
	}
	if len(snapshot.UnknownWorkspaceExamples) != 0 {
		t.Fatalf("expected no unknown workspace examples when reviewer capacity exists, got %+v", snapshot)
	}
}

func TestCurrentReviewerScarcityHealthSnapshotIgnoresOfflineStoppedWorkspaceCoalitionResidue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-health-stopped-residue"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Health Stopped Residue",
		Description: "Stopped agents with old live coalitions should not degrade reviewer scarcity",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create stopped-residue workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	for _, agentID := range []string{"agent-gen", "reviewer-a"} {
		setReviewerRouteAgentLastSeen(t, ctx, store, workspaceID, agentID, "2026-01-01T00:00:00Z")
	}

	const liveAt = "2026-04-15T18:00:00Z"
	insertReviewerRouteLoadTension(t, ctx, store, workspaceID, "tension-stopped-residue", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-stopped-residue", "coal-stopped-residue", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-stopped-residue", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-stopped-residue", "reviewer-a", "FAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	scarcity, err := store.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		t.Fatalf("ReviewerMeshScarcitySnapshot failed: %v", err)
	}
	if scarcity.OnlineAgents != 0 || scarcity.LiveCoalitions == 0 || scarcity.ActiveReviewerAssignments == 0 {
		t.Fatalf("expected offline live coalition residue fixture, got %+v", scarcity)
	}

	snapshot := store.CurrentReviewerScarcityHealthSnapshot(ctx)
	if snapshot.State != "ok" {
		t.Fatalf("expected offline stopped workspace coalition residue to stay ok, got %+v", snapshot)
	}
	if snapshot.UnknownWorkspaceCount != 0 {
		t.Fatalf("expected no unknown workspace degradation for stopped residue, got %+v", snapshot)
	}
}

func TestCurrentReviewerScarcityHealthSnapshotMarksUnknownWorkspaceDegradedWhenLiveCoalitionHasNoReviewerCapacity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-health-unknown-no-capacity"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Health Unknown Without Capacity",
		Description: "Live coalition with no reviewer capacity should degrade unknown scarcity",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create unknown workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen")

	const liveAt = "2026-04-15T18:00:00Z"
	insertReviewerRouteLoadTension(t, ctx, store, workspaceID, "tension-unknown-no-capacity", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-unknown-no-capacity", "coal-unknown-no-capacity", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-unknown-no-capacity", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)

	snapshot := store.CurrentReviewerScarcityHealthSnapshot(ctx)
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded reviewer scarcity health snapshot for live coalition without reviewer capacity, got %+v", snapshot)
	}
	if snapshot.WorkspaceCount != 1 {
		t.Fatalf("expected one workspace in reviewer scarcity health snapshot, got %+v", snapshot)
	}
	if snapshot.UnknownWorkspaceCount != 1 {
		t.Fatalf("expected one unknown workspace in reviewer scarcity health snapshot, got %+v", snapshot)
	}
	if snapshot.Message != "reviewer scarcity health is partial: unknown_workspaces=1" {
		t.Fatalf("expected partial unknown reviewer scarcity message, got %+v", snapshot)
	}
	if len(snapshot.UnknownWorkspaceExamples) != 1 || snapshot.UnknownWorkspaceExamples[0] != workspaceID {
		t.Fatalf("expected unknown workspace example %q, got %+v", workspaceID, snapshot)
	}
}

func TestCurrentReviewerScarcityHealthSnapshotIgnoresEmptyActiveWorkspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: "ws-reviewer-scarcity-health-empty",
		Title:       "Reviewer Scarcity Health Empty",
		Description: "Fresh workspace without reviewer evidence",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create empty workspace: %v", err)
	}

	snapshot := store.CurrentReviewerScarcityHealthSnapshot(ctx)
	if snapshot.State != "ok" {
		t.Fatalf("expected empty active workspace to be ignored, got %+v", snapshot)
	}
	if snapshot.WorkspaceCount != 1 {
		t.Fatalf("expected workspace count to include the active workspace, got %+v", snapshot)
	}
	if snapshot.UnknownWorkspaceCount != 0 {
		t.Fatalf("expected no unknown workspaces for empty evidence, got %+v", snapshot)
	}
}

func TestCurrentReviewerScarcityHealthSnapshotIgnoresArchivedWorkspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-health-archived"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Health Archived",
		Description: "Archived workspace should not affect service health",
		CreatedBy:   "operator",
		Status:      model.WorkspaceStatusArchived,
	}); err != nil {
		t.Fatalf("create archived workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")

	snapshot := store.CurrentReviewerScarcityHealthSnapshot(ctx)
	if snapshot.State != "ok" {
		t.Fatalf("expected archived workspace to be ignored, got %+v", snapshot)
	}
	if snapshot.WorkspaceCount != 0 {
		t.Fatalf("expected archived workspace to be excluded from active health scan, got %+v", snapshot)
	}
}

func TestCurrentReviewerScarcityHealthSnapshotCapsWorkspaceExamplesPerStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)

	liveAt := "2026-04-12T11:00:00Z"
	for i := 1; i <= 4; i++ {
		workspaceID := fmt.Sprintf("ws-reviewer-scarcity-health-cap-%d", i)
		if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "Reviewer Scarcity Health Cap",
			Description: "Bounded reviewer scarcity health",
			CreatedBy:   "operator",
		}); err != nil {
			t.Fatalf("create saturated workspace %d: %v", i, err)
		}

		registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", fmt.Sprintf("reviewer-%d", i))
		setReviewerRouteAgentRole(t, ctx, store, workspaceID, fmt.Sprintf("reviewer-%d", i), "reviewer")
		insertReviewerRouteLoadTension(t, ctx, store, workspaceID, fmt.Sprintf("tension-%d", i), liveAt)
		insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, fmt.Sprintf("tension-%d", i), fmt.Sprintf("coal-%d", i), "ACTIVE", 4, liveAt)
		insertCoalitionSurfaceMember(t, ctx, store, workspaceID, fmt.Sprintf("coal-%d", i), "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
		insertCoalitionSurfaceMember(t, ctx, store, workspaceID, fmt.Sprintf("coal-%d", i), fmt.Sprintf("reviewer-%d", i), "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)
	}

	snapshot := store.CurrentReviewerScarcityHealthSnapshot(ctx)
	if snapshot.SaturatedWorkspaceCount != 4 {
		t.Fatalf("expected four saturated workspaces, got %+v", snapshot)
	}
	if got := len(snapshot.SaturatedWorkspaceExamples); got != reviewerScarcityHealthWorkspaceExampleLimit {
		t.Fatalf("expected saturated workspace examples capped at %d, got %+v", reviewerScarcityHealthWorkspaceExampleLimit, snapshot)
	}
	if snapshot.SaturatedWorkspaceExamples[0] != "ws-reviewer-scarcity-health-cap-1" || snapshot.SaturatedWorkspaceExamples[2] != "ws-reviewer-scarcity-health-cap-3" {
		t.Fatalf("expected capped saturated workspace examples to stay ordered, got %+v", snapshot)
	}
}
