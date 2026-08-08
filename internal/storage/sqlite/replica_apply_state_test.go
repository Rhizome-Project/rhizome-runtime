package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceReplicaApplyFailureMarksRetryPendingAndBlocksEarlyRetry(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-apply-retry-gate")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9500-1")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 3)
	failureAt := "2026-04-11T01:00:00Z"

	applyState, err := store.RecordWorkspaceReplicaApplyFailure(ctx, sqlite.WorkspaceReplicaApplyFailureInput{
		WorkspaceID:                     workspaceID,
		Scope:                           "workspace",
		ReplicaAuthorityNodeID:          followerNodeID,
		LeaderAuthorityNodeID:           holder.HolderAuthorityNodeID,
		AuthorityTerm:                   holder.Term,
		ExportedHeadCommitWatermark:     events[2].IngestSeq,
		AttemptedThroughCommitWatermark: events[1].IngestSeq,
		FailureAt:                       failureAt,
		FailureReason:                   "simulated follower materialization failure",
		Retryable:                       true,
	})
	if err != nil {
		t.Fatalf("record replica apply failure: %v", err)
	}
	if applyState.ApplyStatus != sqlite.WorkspaceReplicaApplyStatusRetryPending || applyState.FailureCount != 1 || applyState.NextRetryAt == "" {
		t.Fatalf("expected retry-pending apply state after first failure, got %+v", applyState)
	}

	if _, _, err := store.ApplyWorkspaceReplicaBatch(ctx, sqlite.WorkspaceReplicaApplyBatchInput{
		WorkspaceID:                 workspaceID,
		Scope:                       "workspace",
		ReplicaAuthorityNodeID:      followerNodeID,
		LeaderAuthorityNodeID:       holder.HolderAuthorityNodeID,
		AuthorityTerm:               holder.Term,
		ExportedHeadCommitWatermark: events[2].IngestSeq,
		EventIDs:                    []string{events[0].EventID, events[1].EventID},
		AppliedAt:                   failureAt,
	}); err == nil {
		t.Fatal("expected retry gate to block apply before next_retry_at")
	}

	replicaState, err := store.GetWorkspaceReplicaState(ctx, workspaceID, "workspace", followerNodeID)
	if err != nil {
		t.Fatalf("get replica state after retry gate reject: %v", err)
	}
	if replicaState.AppliedWatermark != 0 {
		t.Fatalf("expected retry gate reject not to advance apply watermark, got %+v", replicaState)
	}
}

func TestWorkspaceReplicaApplyFailureClearsOnSuccessAfterRetryWindow(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-apply-retry-clear")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9500-2")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 2)

	failedState, err := store.RecordWorkspaceReplicaApplyFailure(ctx, sqlite.WorkspaceReplicaApplyFailureInput{
		WorkspaceID:                     workspaceID,
		Scope:                           "workspace",
		ReplicaAuthorityNodeID:          followerNodeID,
		LeaderAuthorityNodeID:           holder.HolderAuthorityNodeID,
		AuthorityTerm:                   holder.Term,
		ExportedHeadCommitWatermark:     events[1].IngestSeq,
		AttemptedThroughCommitWatermark: events[1].IngestSeq,
		FailureAt:                       "2026-04-11T02:00:00Z",
		FailureReason:                   "transient replica apply failure",
		Retryable:                       true,
	})
	if err != nil {
		t.Fatalf("record retryable apply failure: %v", err)
	}

	transportState, replicaState, err := store.ApplyWorkspaceReplicaBatch(ctx, sqlite.WorkspaceReplicaApplyBatchInput{
		WorkspaceID:                 workspaceID,
		Scope:                       "workspace",
		ReplicaAuthorityNodeID:      followerNodeID,
		LeaderAuthorityNodeID:       holder.HolderAuthorityNodeID,
		AuthorityTerm:               holder.Term,
		ExportedHeadCommitWatermark: events[1].IngestSeq,
		EventIDs:                    []string{events[0].EventID, events[1].EventID},
		AppliedAt:                   failedState.NextRetryAt,
		ApplyReason:                 "retry window reopened",
	})
	if err != nil {
		t.Fatalf("apply batch after retry window: %v", err)
	}
	if transportState.FetchedThroughCommitWatermark != events[1].IngestSeq || replicaState.AppliedWatermark != events[1].IngestSeq {
		t.Fatalf("expected apply after retry window to advance state, got transport=%+v replica=%+v", transportState, replicaState)
	}

	clearedState, err := store.GetWorkspaceReplicaApplyState(ctx, workspaceID, "workspace", followerNodeID)
	if err != nil {
		t.Fatalf("get cleared apply state: %v", err)
	}
	if clearedState.ApplyStatus != sqlite.WorkspaceReplicaApplyStatusIdle || clearedState.FailureCount != 0 || clearedState.NextRetryAt != "" || clearedState.DeadLetteredAt != "" {
		t.Fatalf("expected apply success to clear retry state, got %+v", clearedState)
	}
}

func TestWorkspaceReplicaApplyFailureDeadLettersAfterBoundedRetries(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-apply-dead-letter")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9500-3")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 2)

	var state sqlite.WorkspaceReplicaApplyStateRecord
	var err error
	for i := 0; i < 3; i++ {
		state, err = store.RecordWorkspaceReplicaApplyFailure(ctx, sqlite.WorkspaceReplicaApplyFailureInput{
			WorkspaceID:                     workspaceID,
			Scope:                           "workspace",
			ReplicaAuthorityNodeID:          followerNodeID,
			LeaderAuthorityNodeID:           holder.HolderAuthorityNodeID,
			AuthorityTerm:                   holder.Term,
			ExportedHeadCommitWatermark:     events[1].IngestSeq,
			AttemptedThroughCommitWatermark: events[1].IngestSeq,
			FailureAt:                       time.Date(2026, 4, 11, 3, 0, i, 0, time.UTC).Format(time.RFC3339Nano),
			FailureReason:                   "bounded retry exhaustion",
			Retryable:                       true,
		})
		if err != nil {
			t.Fatalf("record retryable apply failure %d: %v", i+1, err)
		}
	}
	if state.ApplyStatus != sqlite.WorkspaceReplicaApplyStatusDeadLetter || state.FailureCount != 3 || state.DeadLetteredAt == "" {
		t.Fatalf("expected bounded retries to dead-letter the apply line, got %+v", state)
	}

	if _, _, err := store.ApplyWorkspaceReplicaBatch(ctx, sqlite.WorkspaceReplicaApplyBatchInput{
		WorkspaceID:                 workspaceID,
		Scope:                       "workspace",
		ReplicaAuthorityNodeID:      followerNodeID,
		LeaderAuthorityNodeID:       holder.HolderAuthorityNodeID,
		AuthorityTerm:               holder.Term,
		ExportedHeadCommitWatermark: events[1].IngestSeq,
		EventIDs:                    []string{events[0].EventID, events[1].EventID},
		AppliedAt:                   "2026-04-11T03:10:00Z",
	}); err == nil {
		t.Fatal("expected dead-lettered apply line to block further apply")
	}
}

func TestWorkspaceReplicaApplyFailureRejectsStaleLeaderTermWithoutPoisoningCurrentLine(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-apply-stale-failure")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9500-4")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 2)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, holder, "authnode-9500-5")

	if _, err := store.RecordWorkspaceReplicaApplyFailure(ctx, sqlite.WorkspaceReplicaApplyFailureInput{
		WorkspaceID:                     workspaceID,
		Scope:                           "workspace",
		ReplicaAuthorityNodeID:          followerNodeID,
		LeaderAuthorityNodeID:           holder.HolderAuthorityNodeID,
		AuthorityTerm:                   holder.Term,
		ExportedHeadCommitWatermark:     events[1].IngestSeq,
		AttemptedThroughCommitWatermark: events[1].IngestSeq,
		FailureAt:                       "2026-04-11T04:00:00Z",
		FailureReason:                   "stale leader failure should not poison current line",
		Retryable:                       true,
	}); err == nil {
		t.Fatal("expected stale leader failure record to fail closed")
	}

	if _, err := store.GetWorkspaceReplicaApplyState(ctx, workspaceID, "workspace", followerNodeID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no apply-state row after stale leader failure reject, got %v", err)
	}
}
