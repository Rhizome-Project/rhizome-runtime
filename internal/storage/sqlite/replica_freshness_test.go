package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceReplicaFreshnessExposesTransportAndApplyLag(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-freshness")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9600-1")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 3)
	if _, _, err := store.ApplyWorkspaceReplicaBatch(ctx, sqlite.WorkspaceReplicaApplyBatchInput{
		WorkspaceID:                 workspaceID,
		Scope:                       "workspace",
		ReplicaAuthorityNodeID:      followerNodeID,
		LeaderAuthorityNodeID:       holder.HolderAuthorityNodeID,
		AuthorityTerm:               holder.Term,
		ExportedHeadCommitWatermark: events[2].IngestSeq,
		EventIDs:                    []string{events[0].EventID, events[1].EventID},
		AppliedAt:                   "2026-04-11T05:00:00Z",
		ApplyReason:                 "freshness view batch",
	}); err != nil {
		t.Fatalf("apply workspace replica batch: %v", err)
	}

	freshness, err := store.GetWorkspaceReplicaFreshness(ctx, workspaceID, "workspace", followerNodeID)
	if err != nil {
		t.Fatalf("get workspace replica freshness: %v", err)
	}
	if freshness.LeaderJournalHeadCommitWatermark != events[2].IngestSeq {
		t.Fatalf("expected leader head 3, got %+v", freshness)
	}
	if freshness.ExportedHeadCommitWatermark != events[2].IngestSeq || freshness.FetchedThroughCommitWatermark != events[1].IngestSeq || freshness.AcknowledgedCommitWatermark != events[1].IngestSeq {
		t.Fatalf("expected transport watermarks 3/2/2, got %+v", freshness)
	}
	if freshness.CommitWatermark != events[2].IngestSeq || freshness.AppliedWatermark != events[1].IngestSeq {
		t.Fatalf("expected replica commit/applied 3/2, got %+v", freshness)
	}
	if freshness.ExportLag != 0 || freshness.FetchLag != 1 || freshness.AckLag != 1 || freshness.ApplyLag != 1 {
		t.Fatalf("expected export/fetch/ack/apply lag 0/1/1/1, got %+v", freshness)
	}
	if freshness.ApplyStatus != sqlite.WorkspaceReplicaApplyStatusIdle || freshness.FailureCount != 0 {
		t.Fatalf("expected no apply failure state after success, got %+v", freshness)
	}
}

func TestWorkspaceReplicaFreshnessIncludesRetryPendingFailureState(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-freshness-retry")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9600-2")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 3)
	if _, err := store.RecordWorkspaceReplicaApplyFailure(ctx, sqlite.WorkspaceReplicaApplyFailureInput{
		WorkspaceID:                     workspaceID,
		Scope:                           "workspace",
		ReplicaAuthorityNodeID:          followerNodeID,
		LeaderAuthorityNodeID:           holder.HolderAuthorityNodeID,
		AuthorityTerm:                   holder.Term,
		ExportedHeadCommitWatermark:     events[2].IngestSeq,
		AttemptedThroughCommitWatermark: events[1].IngestSeq,
		FailureAt:                       "2026-04-11T06:00:00Z",
		FailureReason:                   "retry pending freshness view",
		Retryable:                       true,
	}); err != nil {
		t.Fatalf("record workspace replica apply failure: %v", err)
	}

	freshness, err := store.GetWorkspaceReplicaFreshness(ctx, workspaceID, "workspace", followerNodeID)
	if err != nil {
		t.Fatalf("get workspace replica freshness: %v", err)
	}
	if freshness.ApplyStatus != sqlite.WorkspaceReplicaApplyStatusRetryPending || freshness.FailureCount != 1 || freshness.NextRetryAt == "" {
		t.Fatalf("expected retry-pending freshness state, got %+v", freshness)
	}
	if freshness.AttemptedThroughCommitWatermark != events[1].IngestSeq {
		t.Fatalf("expected attempted through watermark 2, got %+v", freshness)
	}
	if freshness.ApplyLag != events[2].IngestSeq {
		t.Fatalf("expected apply lag to reflect unapplied exported head, got %+v", freshness)
	}
}

func TestListWorkspaceReplicaFreshnessIncludesFollowerRows(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-freshness-list")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9600-3")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 1)

	items, err := store.ListWorkspaceReplicaFreshness(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("list workspace replica freshness: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one follower freshness row, got %+v", items)
	}
	if items[0].ReplicaAuthorityNodeID != followerNodeID || items[0].MembershipState != sqlite.WorkspaceReplicaMembershipCatchingUp {
		t.Fatalf("unexpected follower freshness row %+v", items[0])
	}
}
