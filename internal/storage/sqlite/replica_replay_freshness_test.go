package sqlite_test

import (
	"context"
	"slices"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestReplayRuntimeJournalIncludesReplicaCoverageAndFreshness(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replay-replica-freshness")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9700-1")

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
		AppliedAt:                   "2026-04-11T07:00:00Z",
		ApplyReason:                 "replay freshness coverage",
	}); err != nil {
		t.Fatalf("apply replica batch: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if len(report.ReplicaFreshness) != 1 {
		t.Fatalf("expected one replica freshness row, got %+v", report.ReplicaFreshness)
	}
	if report.ReplicaCoverage.ReplicaCount != 1 || report.ReplicaCoverage.LaggingReplicaCount != 1 {
		t.Fatalf("expected one lagging replica in coverage summary, got %+v", report.ReplicaCoverage)
	}
	if report.ReplicaCoverage.MaxExportLag != 0 || report.ReplicaCoverage.MaxFetchLag != 1 || report.ReplicaCoverage.MaxAckLag != 1 || report.ReplicaCoverage.MaxApplyLag != 1 {
		t.Fatalf("expected replica coverage lag 0/1/1/1, got %+v", report.ReplicaCoverage)
	}
	if report.ReplicaFreshness[0].ReplicaAuthorityNodeID != followerNodeID || report.ReplicaFreshness[0].ApplyStatus != sqlite.WorkspaceReplicaApplyStatusIdle {
		t.Fatalf("unexpected replica freshness row %+v", report.ReplicaFreshness[0])
	}
}

func TestReplayRuntimeJournalIncludesReplicaRetryPendingCoverage(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replay-replica-retry")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9700-2")

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
		FailureAt:                       "2026-04-11T08:00:00Z",
		FailureReason:                   "retry pending replay coverage",
		Retryable:                       true,
	}); err != nil {
		t.Fatalf("record workspace replica apply failure: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.ReplicaCoverage.RetryPendingCount != 1 || report.ReplicaCoverage.MaxApplyLag != events[2].IngestSeq {
		t.Fatalf("expected retry-pending replica coverage with apply lag 3, got %+v", report.ReplicaCoverage)
	}
	if len(report.ReplicaFreshness) != 1 || report.ReplicaFreshness[0].ApplyStatus != sqlite.WorkspaceReplicaApplyStatusRetryPending {
		t.Fatalf("expected retry-pending replica freshness row, got %+v", report.ReplicaFreshness)
	}
}

func TestReplayRuntimeJournalMarksLocalFollowerLagAsPartial(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replay-local-follower-lag")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	localNode, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 3)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, holder, "authnode-9700-3")
	currentAuthority, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get transferred workspace authority: %v", err)
	}
	mustInstallReplicaBaseState(t, ctx, store, workspaceID, currentAuthority, localNode.AuthorityNodeID, 0)

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.LocalReplicaFreshness == nil {
		t.Fatalf("expected local replica freshness, got nil with rows=%+v", report.ReplicaFreshness)
	}
	if report.LocalReplicaFreshness.ReplicaAuthorityNodeID != localNode.AuthorityNodeID || report.LocalReplicaFreshness.ReplicaRole != sqlite.WorkspaceReplicaRoleFollower {
		t.Fatalf("expected local follower freshness for %s, got %+v", localNode.AuthorityNodeID, report.LocalReplicaFreshness)
	}
	if report.Scope.Authoritative || report.Scope.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected local follower replay to stay partial, got %+v", report.Scope)
	}
	if !slices.Contains(report.Scope.Reasons, "local_store_not_current_authority_holder") ||
		!slices.Contains(report.Scope.Reasons, "local_replica_membership_catching_up") ||
		!slices.Contains(report.Scope.Reasons, "local_replica_export_lag") {
		t.Fatalf("expected local follower lag reasons, got %+v", report.Scope.Reasons)
	}
	if !hasReplayFindingCode(report, "replay_scope_partial") {
		t.Fatalf("expected replay_scope_partial finding, got %+v", report.Evaluation.Findings)
	}
}

func TestReplayRuntimeJournalMarksLocalFollowerRetryPendingAsPartial(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replay-local-follower-retry")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	localNode, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 3)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, holder, "authnode-9700-4")
	currentAuthority, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get transferred workspace authority: %v", err)
	}
	mustInstallReplicaBaseState(t, ctx, store, workspaceID, currentAuthority, localNode.AuthorityNodeID, 0)
	if _, err := store.RecordWorkspaceReplicaApplyFailure(ctx, sqlite.WorkspaceReplicaApplyFailureInput{
		WorkspaceID:                     workspaceID,
		Scope:                           "workspace",
		ReplicaAuthorityNodeID:          localNode.AuthorityNodeID,
		LeaderAuthorityNodeID:           currentAuthority.HolderAuthorityNodeID,
		AuthorityTerm:                   currentAuthority.Term,
		ExportedHeadCommitWatermark:     currentAuthority.CommitWatermark,
		AttemptedThroughCommitWatermark: events[1].IngestSeq,
		FailureAt:                       "2026-04-11T10:00:00Z",
		FailureReason:                   "local follower retry pending",
		Retryable:                       true,
	}); err != nil {
		t.Fatalf("record local follower apply failure: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.LocalReplicaFreshness == nil || report.LocalReplicaFreshness.ApplyStatus != sqlite.WorkspaceReplicaApplyStatusRetryPending {
		t.Fatalf("expected retry-pending local replica freshness, got %+v", report.LocalReplicaFreshness)
	}
	if report.Scope.Authoritative {
		t.Fatalf("expected retry-pending follower replay to stay partial, got %+v", report.Scope)
	}
	if !slices.Contains(report.Scope.Reasons, "local_replica_apply_retry_pending") || !slices.Contains(report.Scope.Reasons, "local_replica_apply_lag") {
		t.Fatalf("expected retry-pending local replica reasons, got %+v", report.Scope.Reasons)
	}
}

func TestReplayRuntimeJournalMarksTransferWithoutInstallAsPartial(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replay-transfer-without-install")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 2)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, holder, "authnode-9700-6")

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.LocalReplicaFreshness != nil {
		t.Fatalf("expected no local replica freshness before install, got %+v", report.LocalReplicaFreshness)
	}
	if report.Scope.Authoritative || report.Scope.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected transferred-but-uninstalled local replay to stay partial, got %+v", report.Scope)
	}
	if !slices.Contains(report.Scope.Reasons, "local_store_not_current_authority_holder") || !slices.Contains(report.Scope.Reasons, "local_replica_state_missing") {
		t.Fatalf("expected missing local replica state reasons after transfer, got %+v", report.Scope.Reasons)
	}
	if !hasReplayFindingCode(report, "replay_scope_partial") {
		t.Fatalf("expected replay_scope_partial finding after transfer without install, got %+v", report.Evaluation.Findings)
	}
}

func TestReplayRuntimeJournalLeaderStaysCompleteDespiteLaggingFollower(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replay-leader-complete")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9700-5")

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
		AppliedAt:                   "2026-04-11T11:00:00Z",
		ApplyReason:                 "leader replay should remain complete",
	}); err != nil {
		t.Fatalf("apply lagging follower batch: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.ReplicaCoverage.LaggingReplicaCount != 1 {
		t.Fatalf("expected lagging follower coverage, got %+v", report.ReplicaCoverage)
	}
	if report.LocalReplicaFreshness != nil {
		t.Fatalf("expected no local replica freshness on leader replay, got %+v", report.LocalReplicaFreshness)
	}
	if !report.Scope.Authoritative || report.Scope.IntegrityBand != "COMPLETE" || len(report.Scope.Reasons) != 0 {
		t.Fatalf("expected leader replay to remain complete, got %+v", report.Scope)
	}
	if hasReplayFindingCode(report, "replay_scope_partial") {
		t.Fatalf("expected no replay_scope_partial finding for leader replay, got %+v", report.Evaluation.Findings)
	}
}
