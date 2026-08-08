package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceReplicaApplyBatchAdvancesTransportAndReplicaState(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-apply-advance")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9400-1")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 3)

	transportState, replicaState, err := store.ApplyWorkspaceReplicaBatch(ctx, sqlite.WorkspaceReplicaApplyBatchInput{
		WorkspaceID:                 workspaceID,
		Scope:                       "workspace",
		ReplicaAuthorityNodeID:      followerNodeID,
		LeaderAuthorityNodeID:       holder.HolderAuthorityNodeID,
		AuthorityTerm:               holder.Term,
		ExportedHeadCommitWatermark: events[2].IngestSeq,
		EventIDs:                    []string{events[0].EventID, events[1].EventID},
		FetchedAt:                   time.Now().UTC().Format(time.RFC3339Nano),
		AcknowledgedAt:              time.Now().UTC().Format(time.RFC3339Nano),
		AppliedAt:                   time.Now().UTC().Format(time.RFC3339Nano),
		ApplyReason:                 "first contiguous follower batch",
	})
	if err != nil {
		t.Fatalf("apply workspace replica batch: %v", err)
	}
	if transportState.ExportedHeadCommitWatermark != events[2].IngestSeq {
		t.Fatalf("expected exported head %d, got %+v", events[2].IngestSeq, transportState)
	}
	if transportState.FetchedThroughCommitWatermark != events[1].IngestSeq || transportState.AcknowledgedCommitWatermark != events[1].IngestSeq {
		t.Fatalf("expected fetched/acknowledged through %d, got %+v", events[1].IngestSeq, transportState)
	}
	if replicaState.CommitWatermark != events[2].IngestSeq || replicaState.AppliedWatermark != events[1].IngestSeq {
		t.Fatalf("expected replica commit/applied to advance to 3/2, got %+v", replicaState)
	}
	if replicaState.MembershipState != sqlite.WorkspaceReplicaMembershipCatchingUp {
		t.Fatalf("expected follower to remain catching_up, got %+v", replicaState)
	}

	storedTransport, err := store.GetWorkspaceReplicaTransportState(ctx, workspaceID, "workspace", followerNodeID)
	if err != nil {
		t.Fatalf("get workspace replica transport state: %v", err)
	}
	if storedTransport != transportState {
		t.Fatalf("expected stored transport state to match returned state:\nreturned=%+v\nstored=%+v", transportState, storedTransport)
	}
}

func TestWorkspaceReplicaApplyBatchIsIdempotentForSameBoundary(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-apply-idempotent")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9400-2")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 2)
	input := sqlite.WorkspaceReplicaApplyBatchInput{
		WorkspaceID:                 workspaceID,
		Scope:                       "workspace",
		ReplicaAuthorityNodeID:      followerNodeID,
		LeaderAuthorityNodeID:       holder.HolderAuthorityNodeID,
		AuthorityTerm:               holder.Term,
		ExportedHeadCommitWatermark: events[1].IngestSeq,
		EventIDs:                    []string{events[0].EventID, events[1].EventID},
		FetchedAt:                   "2026-04-11T00:00:01Z",
		AcknowledgedAt:              "2026-04-11T00:00:02Z",
		AppliedAt:                   "2026-04-11T00:00:03Z",
		ApplyReason:                 "idempotent follower batch",
	}

	firstTransport, firstReplica, err := store.ApplyWorkspaceReplicaBatch(ctx, input)
	if err != nil {
		t.Fatalf("first apply workspace replica batch: %v", err)
	}
	secondTransport, secondReplica, err := store.ApplyWorkspaceReplicaBatch(ctx, input)
	if err != nil {
		t.Fatalf("second apply workspace replica batch: %v", err)
	}
	if firstTransport != secondTransport {
		t.Fatalf("expected identical transport state on reapply:\nfirst=%+v\nsecond=%+v", firstTransport, secondTransport)
	}
	if firstReplica != secondReplica {
		t.Fatalf("expected identical replica state on reapply:\nfirst=%+v\nsecond=%+v", firstReplica, secondReplica)
	}
}

func TestWorkspaceReplicaApplyBatchRejectsMissingTailGap(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-apply-gap")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9400-3")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 3)

	if _, _, err := store.ApplyWorkspaceReplicaBatch(ctx, sqlite.WorkspaceReplicaApplyBatchInput{
		WorkspaceID:                 workspaceID,
		Scope:                       "workspace",
		ReplicaAuthorityNodeID:      followerNodeID,
		LeaderAuthorityNodeID:       holder.HolderAuthorityNodeID,
		AuthorityTerm:               holder.Term,
		ExportedHeadCommitWatermark: events[2].IngestSeq,
		EventIDs:                    []string{events[0].EventID, events[2].EventID},
		ApplyReason:                 "gap should fail closed",
	}); err == nil {
		t.Fatal("expected missing-tail gap to fail closed")
	}

	if _, err := store.GetWorkspaceReplicaTransportState(ctx, workspaceID, "workspace", followerNodeID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no transport row after rejected apply gap, got %v", err)
	}
	replicaState, err := store.GetWorkspaceReplicaState(ctx, workspaceID, "workspace", followerNodeID)
	if err != nil {
		t.Fatalf("get follower replica state after rejected gap: %v", err)
	}
	if replicaState.CommitWatermark != 0 || replicaState.AppliedWatermark != 0 {
		t.Fatalf("expected rejected apply not to advance replica state, got %+v", replicaState)
	}
}

func TestWorkspaceReplicaApplyBatchRejectsStaleLeaderAfterTransfer(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-apply-stale")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9400-4")

	mustInstallReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedReplicaApplyRuntimeEvents(t, ctx, store, workspaceID, 2)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, holder, "authnode-9400-5")

	if _, _, err := store.ApplyWorkspaceReplicaBatch(ctx, sqlite.WorkspaceReplicaApplyBatchInput{
		WorkspaceID:                 workspaceID,
		Scope:                       "workspace",
		ReplicaAuthorityNodeID:      followerNodeID,
		LeaderAuthorityNodeID:       holder.HolderAuthorityNodeID,
		AuthorityTerm:               holder.Term,
		ExportedHeadCommitWatermark: events[1].IngestSeq,
		EventIDs:                    []string{events[0].EventID, events[1].EventID},
		ApplyReason:                 "stale leader should fail closed",
	}); err == nil {
		t.Fatal("expected stale leader apply to fail closed")
	}

	if _, err := store.GetWorkspaceReplicaTransportState(ctx, workspaceID, "workspace", followerNodeID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no transport row after stale leader reject, got %v", err)
	}
}

func mustInstallReplicaBaseState(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, holder sqlite.WorkspaceAuthorityRecord, followerNodeID string, baseCommitWatermark int64) {
	t.Helper()

	if _, err := store.BeginWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallBeginInput{
		WorkspaceID:            workspaceID,
		Scope:                  "workspace",
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    baseCommitWatermark,
		InstallReason:          "replica install begin",
	}); err != nil {
		t.Fatalf("begin workspace replica install: %v", err)
	}
	if _, _, err := store.CompleteWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallCompleteInput{
		WorkspaceID:            workspaceID,
		Scope:                  "workspace",
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    baseCommitWatermark,
		InstallReason:          "replica install complete",
	}); err != nil {
		t.Fatalf("complete workspace replica install: %v", err)
	}
}

func seedReplicaApplyRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, count int) []sqlite.RuntimeEventRecord {
	t.Helper()

	records := make([]sqlite.RuntimeEventRecord, 0, count)
	baseTime := time.Now().UTC()
	for i := 0; i < count; i++ {
		record, err := store.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, sqlite.RuntimeEventInput{
			EventID:     fmt.Sprintf("rtev-replica-apply-%s-%02d", workspaceID, i+1),
			WorkspaceID: workspaceID,
			EventType:   "replica.apply.seed",
			EntityType:  "replica_apply_test",
			EntityID:    fmt.Sprintf("seed-%02d", i+1),
			ActorType:   "system",
			ActorID:     "replica-apply-tests",
			PayloadJSON: fmt.Sprintf(`{"seed_index":%d}`, i+1),
			CreatedAt:   baseTime.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
		})
		if err != nil {
			t.Fatalf("seed runtime event %d: %v", i+1, err)
		}
		records = append(records, record)
	}
	return records
}
