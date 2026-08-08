package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceReplicaStateUpsertAndList(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-state-upsert")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9200-1")

	holderRecord, err := store.UpsertWorkspaceReplicaState(ctx, sqlite.WorkspaceReplicaStateRecord{
		WorkspaceID:            workspaceID,
		Scope:                  "workspace",
		ReplicaAuthorityNodeID: holder.HolderAuthorityNodeID,
		ReplicaRole:            sqlite.WorkspaceReplicaRoleHolder,
		MembershipState:        sqlite.WorkspaceReplicaMembershipActive,
		AuthorityTerm:          holder.Term,
		CommitWatermark:        holder.CommitWatermark,
		AppliedWatermark:       holder.AppliedWatermark,
		MembershipReason:       "leader authority is active",
	})
	if err != nil {
		t.Fatalf("upsert holder replica state: %v", err)
	}
	if holderRecord.LeaderAuthorityNodeID != holder.HolderAuthorityNodeID {
		t.Fatalf("expected holder leader_authority_node_id to default to itself, got %+v", holderRecord)
	}

	followerRecord, err := store.UpsertWorkspaceReplicaState(ctx, sqlite.WorkspaceReplicaStateRecord{
		WorkspaceID:            workspaceID,
		Scope:                  "workspace",
		ReplicaAuthorityNodeID: followerNodeID,
		ReplicaRole:            sqlite.WorkspaceReplicaRoleFollower,
		MembershipState:        sqlite.WorkspaceReplicaMembershipCatchingUp,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		CommitWatermark:        11,
		AppliedWatermark:       7,
		LastFetchAt:            time.Now().UTC().Format(time.RFC3339Nano),
		LastApplyAt:            time.Now().UTC().Format(time.RFC3339Nano),
		MembershipReason:       "follower has not applied through leader commit watermark",
	})
	if err != nil {
		t.Fatalf("upsert follower replica state: %v", err)
	}
	if followerRecord.ReplicaRole != sqlite.WorkspaceReplicaRoleFollower || followerRecord.LeaderAuthorityNodeID != holder.HolderAuthorityNodeID {
		t.Fatalf("unexpected follower replica state %+v", followerRecord)
	}

	gotFollower, err := store.GetWorkspaceReplicaState(ctx, workspaceID, "workspace", followerNodeID)
	if err != nil {
		t.Fatalf("get follower replica state: %v", err)
	}
	if gotFollower.CommitWatermark != 11 || gotFollower.AppliedWatermark != 7 {
		t.Fatalf("unexpected fetched follower replica state %+v", gotFollower)
	}

	items, err := store.ListWorkspaceReplicaStates(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("list workspace replica states: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 replica state rows, got %+v", items)
	}
	if items[0].ReplicaRole != sqlite.WorkspaceReplicaRoleHolder {
		t.Fatalf("expected holder row to sort first, got %+v", items)
	}
}

func TestWorkspaceReplicaStateRejectsInvalidWatermarkOrRoleContracts(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-state-invalid")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9200-2")

	cases := []struct {
		name   string
		record sqlite.WorkspaceReplicaStateRecord
	}{
		{
			name: "applied above commit",
			record: sqlite.WorkspaceReplicaStateRecord{
				WorkspaceID:            workspaceID,
				ReplicaAuthorityNodeID: followerNodeID,
				ReplicaRole:            sqlite.WorkspaceReplicaRoleFollower,
				MembershipState:        sqlite.WorkspaceReplicaMembershipCatchingUp,
				LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
				AuthorityTerm:          holder.Term,
				CommitWatermark:        5,
				AppliedWatermark:       6,
			},
		},
		{
			name: "follower without leader",
			record: sqlite.WorkspaceReplicaStateRecord{
				WorkspaceID:            workspaceID,
				ReplicaAuthorityNodeID: followerNodeID,
				ReplicaRole:            sqlite.WorkspaceReplicaRoleFollower,
				MembershipState:        sqlite.WorkspaceReplicaMembershipActive,
				AuthorityTerm:          holder.Term,
				CommitWatermark:        5,
				AppliedWatermark:       4,
			},
		},
		{
			name: "holder leader mismatch",
			record: sqlite.WorkspaceReplicaStateRecord{
				WorkspaceID:            workspaceID,
				ReplicaAuthorityNodeID: holder.HolderAuthorityNodeID,
				ReplicaRole:            sqlite.WorkspaceReplicaRoleHolder,
				MembershipState:        sqlite.WorkspaceReplicaMembershipActive,
				LeaderAuthorityNodeID:  followerNodeID,
				AuthorityTerm:          holder.Term,
				CommitWatermark:        5,
				AppliedWatermark:       5,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.UpsertWorkspaceReplicaState(ctx, tc.record); err == nil {
				t.Fatalf("expected %s to fail validation", tc.name)
			}
		})
	}
}

func TestWorkspaceReplicaStateRejectsUnknownNodesAndWatermarkRegression(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := seedWorkspaceReplicaStateScenario(t, ctx, store, "ws-replica-state-regression")
	holder := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	followerNodeID := seedAdditionalReplicaRuntimeNode(t, ctx, store, "authnode-9200-3")

	if _, err := store.UpsertWorkspaceReplicaState(ctx, sqlite.WorkspaceReplicaStateRecord{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: followerNodeID,
		ReplicaRole:            sqlite.WorkspaceReplicaRoleFollower,
		MembershipState:        sqlite.WorkspaceReplicaMembershipCatchingUp,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		CommitWatermark:        14,
		AppliedWatermark:       9,
		MembershipReason:       "initial follower watermark state",
	}); err != nil {
		t.Fatalf("seed follower replica state: %v", err)
	}

	if _, err := store.UpsertWorkspaceReplicaState(ctx, sqlite.WorkspaceReplicaStateRecord{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: followerNodeID,
		ReplicaRole:            sqlite.WorkspaceReplicaRoleFollower,
		MembershipState:        sqlite.WorkspaceReplicaMembershipCatchingUp,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		CommitWatermark:        13,
		AppliedWatermark:       9,
	}); err == nil {
		t.Fatal("expected commit watermark regression to fail")
	}
	if _, err := store.UpsertWorkspaceReplicaState(ctx, sqlite.WorkspaceReplicaStateRecord{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: followerNodeID,
		ReplicaRole:            sqlite.WorkspaceReplicaRoleFollower,
		MembershipState:        sqlite.WorkspaceReplicaMembershipCatchingUp,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		CommitWatermark:        14,
		AppliedWatermark:       8,
	}); err == nil {
		t.Fatal("expected applied watermark regression to fail")
	}
	if _, err := store.UpsertWorkspaceReplicaState(ctx, sqlite.WorkspaceReplicaStateRecord{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: "authnode-9200-999",
		ReplicaRole:            sqlite.WorkspaceReplicaRoleFollower,
		MembershipState:        sqlite.WorkspaceReplicaMembershipActive,
		LeaderAuthorityNodeID:  holder.HolderAuthorityNodeID,
		AuthorityTerm:          holder.Term,
		CommitWatermark:        20,
		AppliedWatermark:       20,
	}); err == nil {
		t.Fatal("expected unknown replica runtime node to fail")
	}
	if _, err := store.UpsertWorkspaceReplicaState(ctx, sqlite.WorkspaceReplicaStateRecord{
		WorkspaceID:            workspaceID,
		ReplicaAuthorityNodeID: followerNodeID,
		ReplicaRole:            sqlite.WorkspaceReplicaRoleFollower,
		MembershipState:        sqlite.WorkspaceReplicaMembershipActive,
		LeaderAuthorityNodeID:  "authnode-9200-998",
		AuthorityTerm:          holder.Term,
		CommitWatermark:        20,
		AppliedWatermark:       19,
	}); err == nil {
		t.Fatal("expected unknown leader runtime node to fail")
	}
}

func seedWorkspaceReplicaStateScenario(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) string {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Replica State",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	return workspaceID
}

func seedAdditionalReplicaRuntimeNode(t *testing.T, ctx context.Context, store *sqlite.Store, authorityNodeID string) string {
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
		"replica-test-host",
		"boot-"+authorityNodeID,
		now,
		now,
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("seed runtime follower node %s: %v", authorityNodeID, err)
	}
	return authorityNodeID
}
