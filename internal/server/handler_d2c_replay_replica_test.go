package server

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceEventsEvaluateIncludesReplicaCoverageAndFreshness(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-replay-replica"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Replay Replica",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	holder := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedServerTestReplicaRuntimeNode(t, ctx, store, "authnode-9800-1")

	mustInstallServerReplicaBaseState(t, ctx, store, workspaceID, holder, followerNodeID, 0)
	events := seedServerReplicaRuntimeEvents(t, ctx, store, workspaceID, 3)
	if _, err := store.RecordWorkspaceReplicaApplyFailure(ctx, sqlite.WorkspaceReplicaApplyFailureInput{
		WorkspaceID:                     workspaceID,
		Scope:                           "workspace",
		ReplicaAuthorityNodeID:          followerNodeID,
		LeaderAuthorityNodeID:           holder.HolderAuthorityNodeID,
		AuthorityTerm:                   holder.Term,
		ExportedHeadCommitWatermark:     events[2].IngestSeq,
		AttemptedThroughCommitWatermark: events[1].IngestSeq,
		FailureAt:                       "2026-04-11T09:00:00Z",
		FailureReason:                   "handler replay replica coverage",
		Retryable:                       true,
	}); err != nil {
		t.Fatalf("record workspace replica apply failure: %v", err)
	}

	raw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	respAny, rpcErr := h.workspaceEventsEvaluate(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate rpc error: %+v", rpcErr)
	}
	respJSON, err := json.Marshal(respAny)
	if err != nil {
		t.Fatalf("marshal replay evaluate response: %v", err)
	}
	var resp struct {
		Scope                 sqlite.RuntimeReplayScopeAssessment      `json:"scope"`
		ReplicaCoverage       sqlite.RuntimeReplayReplicaCoverage      `json:"replica_coverage"`
		ReplicaFreshness      []sqlite.WorkspaceReplicaFreshnessRecord `json:"replica_freshness"`
		LocalReplicaFreshness *sqlite.WorkspaceReplicaFreshnessRecord  `json:"local_replica_freshness"`
		Counts                map[string]int                           `json:"counts"`
	}
	if err := json.Unmarshal(respJSON, &resp); err != nil {
		t.Fatalf("decode replay evaluate response: %v", err)
	}
	if resp.Counts["replicas"] != 1 || resp.ReplicaCoverage.RetryPendingCount != 1 {
		t.Fatalf("expected replay evaluate response to expose retry-pending replica coverage, got counts=%+v coverage=%+v", resp.Counts, resp.ReplicaCoverage)
	}
	if len(resp.ReplicaFreshness) != 1 || resp.ReplicaFreshness[0].ApplyStatus != sqlite.WorkspaceReplicaApplyStatusRetryPending {
		t.Fatalf("expected replay evaluate response to expose replica freshness rows, got %+v", resp.ReplicaFreshness)
	}
	if resp.LocalReplicaFreshness != nil {
		t.Fatalf("expected no local replica freshness for leader evaluate path, got %+v", resp.LocalReplicaFreshness)
	}
	if !resp.Scope.Authoritative || resp.Scope.IntegrityBand != "COMPLETE" || len(resp.Scope.Reasons) != 0 {
		t.Fatalf("expected leader evaluate scope to stay complete, got %+v", resp.Scope)
	}
}

func TestWorkspaceEventsEvaluateMarksLocalFollowerAsPartial(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-replay-local-follower"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Replay Local Follower",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	holder := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	localNode, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	seedServerReplicaRuntimeEvents(t, ctx, store, workspaceID, 3)
	transferServerWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, holder, "authnode-9800-2")
	currentAuthority, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get transferred workspace authority: %v", err)
	}
	mustInstallServerReplicaBaseState(t, ctx, store, workspaceID, currentAuthority, localNode.AuthorityNodeID, 0)

	raw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	respAny, rpcErr := h.workspaceEventsEvaluate(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate rpc error: %+v", rpcErr)
	}
	respJSON, err := json.Marshal(respAny)
	if err != nil {
		t.Fatalf("marshal replay evaluate response: %v", err)
	}
	var resp struct {
		Scope                 sqlite.RuntimeReplayScopeAssessment     `json:"scope"`
		LocalReplicaFreshness *sqlite.WorkspaceReplicaFreshnessRecord `json:"local_replica_freshness"`
	}
	if err := json.Unmarshal(respJSON, &resp); err != nil {
		t.Fatalf("decode replay evaluate response: %v", err)
	}
	if resp.LocalReplicaFreshness == nil || resp.LocalReplicaFreshness.ReplicaAuthorityNodeID != localNode.AuthorityNodeID {
		t.Fatalf("expected local replica freshness for local follower, got %+v", resp.LocalReplicaFreshness)
	}
	if resp.Scope.Authoritative || resp.Scope.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected local follower evaluate scope to stay partial, got %+v", resp.Scope)
	}
	if !slices.Contains(resp.Scope.Reasons, "local_store_not_current_authority_holder") || !slices.Contains(resp.Scope.Reasons, "local_replica_membership_catching_up") {
		t.Fatalf("expected local follower reasons in evaluate scope, got %+v", resp.Scope.Reasons)
	}
}

func seedServerTestReplicaRuntimeNode(t *testing.T, ctx context.Context, store *sqlite.Store, authorityNodeID string) string {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(authority_node_id) DO UPDATE SET
	node_kind = excluded.node_kind,
	host_label = excluded.host_label,
	boot_instance_id = excluded.boot_instance_id,
	last_seen_at = excluded.last_seen_at,
	status = excluded.status`,
		authorityNodeID,
		"sqlite_follower",
		"server-replica-test-host",
		"boot-"+authorityNodeID,
		now,
		now,
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("seed server replica runtime node %s: %v", authorityNodeID, err)
	}
	return authorityNodeID
}

func mustInstallServerReplicaBaseState(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, holder sqlite.WorkspaceAuthorityRecord, followerNodeID string, baseCommitWatermark int64) {
	t.Helper()

	if _, err := store.BeginWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallBeginInput{
		WorkspaceID:            workspaceID,
		Scope:                  "workspace",
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    baseCommitWatermark,
		InstallReason:          "server replica install begin",
	}); err != nil {
		t.Fatalf("begin server replica install: %v", err)
	}
	if _, _, err := store.CompleteWorkspaceReplicaInstall(ctx, sqlite.WorkspaceReplicaInstallCompleteInput{
		WorkspaceID:            workspaceID,
		Scope:                  "workspace",
		ReplicaAuthorityNodeID: followerNodeID,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		BaseCommitWatermark:    baseCommitWatermark,
		InstallReason:          "server replica install complete",
	}); err != nil {
		t.Fatalf("complete server replica install: %v", err)
	}
}

func seedServerReplicaRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, count int) []sqlite.RuntimeEventRecord {
	t.Helper()

	records := make([]sqlite.RuntimeEventRecord, 0, count)
	baseTime := time.Now().UTC()
	for i := 0; i < count; i++ {
		record, err := store.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, sqlite.RuntimeEventInput{
			EventID:     fmt.Sprintf("rtev-server-replay-replica-%s-%02d", workspaceID, i+1),
			WorkspaceID: workspaceID,
			EventType:   "replica.replay.seed",
			EntityType:  "replica_replay_test",
			EntityID:    "seed",
			ActorType:   "system",
			ActorID:     "server-replay-tests",
			PayloadJSON: `{"kind":"server_replica_replay_seed"}`,
			CreatedAt:   baseTime.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
		})
		if err != nil {
			t.Fatalf("seed server replica runtime event %d: %v", i+1, err)
		}
		records = append(records, record)
	}
	return records
}

func transferServerWorkspaceAuthorityToExternalPeer(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, current sqlite.WorkspaceAuthorityRecord, peerNodeID string) {
	t.Helper()

	referenceAt := time.Now().UTC().Round(0)
	var journalHead int64
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&journalHead); err != nil {
		t.Fatalf("query runtime journal head before transfer: %v", err)
	}
	commitWatermark := current.CommitWatermark + 1
	if journalHead > commitWatermark {
		commitWatermark = journalHead
	}
	appliedWatermark := current.AppliedWatermark + 1
	if appliedWatermark > commitWatermark {
		appliedWatermark = commitWatermark
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		peerNodeID,
		"sqlite_peer_store",
		"peer-host",
		"boot-"+peerNodeID,
		referenceAt.Format(time.RFC3339Nano),
		referenceAt.Format(time.RFC3339Nano),
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		Scope:                        "workspace",
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-" + peerNodeID,
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    "system",
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority to peer: %v", err)
	}
}
