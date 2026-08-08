package sqlite_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceReplicaInstallBeginAndComplete(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-install")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9300-1")

	beginRecord, err := store.BeginWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallBeginInput{
		WorkspaceID:            workspaceID,
		Scope:                  "workspace",
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    holder.CommitWatermark,
		InstallReason:          "bootstrap base-state fetch started",
	})
	if err != nil {
		t.Fatalf("begin replica install: %v", err)
	}
	if beginRecord.InstallStatus != sqlite.WorkspaceReplicaInstallPending {
		t.Fatalf("expected pending install record, got %+v", beginRecord)
	}
	if beginRecord.InstallCompletedAt != "" {
		t.Fatalf("pending install should not set install_completed_at, got %+v", beginRecord)
	}

	installRecord, replicaState, err := store.CompleteWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallCompleteInput{
		WorkspaceID:            workspaceID,
		Scope:                  "workspace",
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    holder.CommitWatermark,
		InstallReason:          "base-state copied to local replica store",
	})
	if err != nil {
		t.Fatalf("complete replica install: %v", err)
	}
	if installRecord.InstallStatus != sqlite.WorkspaceReplicaInstallInstalled {
		t.Fatalf("expected installed replica record, got %+v", installRecord)
	}
	if installRecord.InstallCompletedAt == "" {
		t.Fatalf("installed replica record should set install_completed_at, got %+v", installRecord)
	}
	if replicaState.MembershipState != sqlite.WorkspaceReplicaMembershipCatchingUp {
		t.Fatalf("expected catching-up replica state after install, got %+v", replicaState)
	}
	if replicaState.CommitWatermark != holder.CommitWatermark || replicaState.AppliedWatermark != holder.CommitWatermark {
		t.Fatalf("expected base watermark to seed replica state, got %+v", replicaState)
	}
	if !strings.Contains(replicaState.MembershipReason, "incremental apply pending") {
		t.Fatalf("expected install completion to preserve non-complete membership semantics, got %+v", replicaState)
	}

	gotInstall, err := store.GetWorkspaceReplicaInstallState(ctx, workspaceID, "workspace", followerNodeID)
	if err != nil {
		t.Fatalf("get replica install state: %v", err)
	}
	if gotInstall.InstallStatus != sqlite.WorkspaceReplicaInstallInstalled {
		t.Fatalf("unexpected stored install state %+v", gotInstall)
	}

	items, err := store.ListWorkspaceReplicaInstallStates(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("list replica install states: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 replica install row, got %+v", items)
	}
}

func TestWorkspaceReplicaInstallRejectsStaleTermAndWatermarkLies(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-install-rejects")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9300-2")

	if _, err := store.BeginWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallBeginInput{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term - 1,
		BaseCommitWatermark:    holder.CommitWatermark,
	}); err == nil {
		t.Fatal("expected stale authority term to fail closed")
	}
	if _, err := store.BeginWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallBeginInput{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    holder.CommitWatermark + 1,
	}); err == nil {
		t.Fatal("expected base watermark beyond leader commit watermark to fail")
	}
	if _, err := store.GetWorkspaceReplicaInstallState(ctx, workspaceID, "workspace", followerNodeID); err == nil {
		t.Fatal("expected no install row after rejected bootstrap attempts")
	}
}

func TestWorkspaceReplicaInstallRequiresPendingBeginAndRejectsReplacement(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-install-progress")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9300-3")

	if _, _, err := store.CompleteWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallCompleteInput{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    holder.CommitWatermark,
	}); err == nil {
		t.Fatal("expected install completion without begin to fail")
	}

	if _, err := store.BeginWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallBeginInput{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    holder.CommitWatermark,
	}); err != nil {
		t.Fatalf("begin replica install: %v", err)
	}
	if _, _, err := store.CompleteWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallCompleteInput{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    holder.CommitWatermark,
	}); err != nil {
		t.Fatalf("complete replica install: %v", err)
	}
	if _, err := store.BeginWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallBeginInput{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    holder.CommitWatermark,
	}); err == nil {
		t.Fatal("expected begin after installed base-state to reject instead of regressing to pending")
	}
}
