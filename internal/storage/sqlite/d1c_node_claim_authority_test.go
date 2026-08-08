package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestClaimNodeRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-node-claim-missing-authority"
		taskID      = "task-d1c-node-claim-missing-authority"
		nodeID      = "node-1"
		agentID     = "agent-d1c-node-claim-missing-authority"
	)

	seedD1CNodeClaimFixture(t, ctx, store, workspaceID, taskID, agentID, dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}},
	}, false)

	err := store.ClaimNode(ctx, sqlite.NodeClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "should fail closed",
	})
	if err == nil {
		t.Fatal("expected missing authority reject on ClaimNode")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	assertNodeClaimAbsent(t, ctx, store, workspaceID, taskID, nodeID)
	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusPending, 0)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusPending)
}

func TestReleaseNodeClaimRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-node-release-stale-authority"
		taskID      = "task-d1c-node-release-stale-authority"
		nodeID      = "node-1"
		agentID     = "agent-d1c-node-release-stale-authority"
	)

	seedD1CNodeClaimFixture(t, ctx, store, workspaceID, taskID, agentID, dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}},
	}, true)
	if err := store.ClaimNode(ctx, sqlite.NodeClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "seed claimed node",
	}); err != nil {
		t.Fatalf("seed claim node: %v", err)
	}
	transferD1CNodeWorkspaceAuthorityToPeer(t, ctx, store, workspaceID)

	err := store.ReleaseNodeClaim(ctx, sqlite.NodeReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Reason:      "should fail closed",
	})
	if err == nil {
		t.Fatal("expected stale authority reject on ReleaseNodeClaim")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject code, got %+v", reject)
	}

	assertNodeClaimStatus(t, ctx, store, workspaceID, taskID, nodeID, model.TaskClaimStatusClaimed)
	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusRunning, 1)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)
}

func TestCompleteNodeClaimRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-node-complete-stale-authority"
		taskID      = "task-d1c-node-complete-stale-authority"
		nodeID      = "node-1"
		depNodeID   = "node-2"
		agentID     = "agent-d1c-node-complete-stale-authority"
	)

	seedD1CNodeClaimFixture(t, ctx, store, workspaceID, taskID, agentID, dag.Graph{
		Nodes: []dag.NodeSpec{
			{NodeID: nodeID, Type: "generic"},
			{NodeID: depNodeID, Type: "generic", DependsOn: []string{nodeID}},
		},
	}, true)
	if err := store.ClaimNode(ctx, sqlite.NodeClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "seed claimed node",
	}); err != nil {
		t.Fatalf("seed claim node: %v", err)
	}
	transferD1CNodeWorkspaceAuthorityToPeer(t, ctx, store, workspaceID)

	err := store.CompleteNodeClaim(ctx, sqlite.NodeCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "should fail closed",
	})
	if err == nil {
		t.Fatal("expected stale authority reject on CompleteNodeClaim")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject code, got %+v", reject)
	}

	assertNodeClaimStatus(t, ctx, store, workspaceID, taskID, nodeID, model.TaskClaimStatusClaimed)
	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusRunning, 1)
	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, depNodeID, model.NodeStatusBlocked, 0)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)
	assertTaskClaimStatusOptional(t, ctx, store, taskID, "")
}

func seedD1CNodeClaimFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID string, graph dag.Graph, claimAuthority bool) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Node Claim Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if claimAuthority {
		claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "D1C Node Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	graph = dag.NormalizeGraph(graph)
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "tests",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task with graph: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "tests",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
}

func transferD1CNodeWorkspaceAuthorityToPeer(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) sqlite.WorkspaceAuthorityRecord {
	t.Helper()

	current, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("load current workspace authority: %v", err)
	}
	now := time.Now().UTC()
	referenceAt := now.Format(time.RFC3339Nano)
	peerNodeID := "authnode-peer-" + workspaceID
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(authority_node_id) DO UPDATE SET
	node_kind = excluded.node_kind,
	host_label = excluded.host_label,
	boot_instance_id = excluded.boot_instance_id,
	last_seen_at = excluded.last_seen_at,
	status = excluded.status
`, peerNodeID, "runtime_peer", "peer-"+workspaceID, "boot-peer-"+workspaceID, referenceAt, referenceAt, string(sqlite.RuntimeNodeStatusOnline)); err != nil {
		t.Fatalf("seed peer runtime node: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE workspace_authority
SET holder_authority_node_id = ?,
    lease_token = ?,
    term = ?,
    lease_expires_at = ?,
    status = ?,
    updated_at = ?
WHERE workspace_id = ? AND scope = ?
`, peerNodeID, "lease-peer-"+workspaceID, current.Term+1, now.Add(time.Hour).Format(time.RFC3339Nano), string(sqlite.WorkspaceAuthorityStatusActive), referenceAt, workspaceID, "workspace"); err != nil {
		t.Fatalf("transfer workspace authority to peer: %v", err)
	}
	record, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("reload workspace authority: %v", err)
	}
	return record
}

func assertNodeClaimAbsent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, nodeID string) {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(1) FROM node_claims WHERE workspace_id = ? AND task_id = ? AND node_id = ?`, workspaceID, taskID, nodeID).Scan(&count); err != nil {
		t.Fatalf("count node claims: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no node claim rows for %s/%s, got %d", taskID, nodeID, count)
	}
}

func assertNodeClaimStatus(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, nodeID, want string) {
	t.Helper()

	var status sql.NullString
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM node_claims WHERE workspace_id = ? AND task_id = ? AND node_id = ?`, workspaceID, taskID, nodeID).Scan(&status); err != nil {
		t.Fatalf("load node claim status: %v", err)
	}
	got := ""
	if status.Valid {
		got = strings.TrimSpace(status.String)
	}
	if got != want {
		t.Fatalf("expected node claim status %q, got %q", want, got)
	}
}

func assertDagNodeStatusAndAttempt(t *testing.T, ctx context.Context, store *sqlite.Store, taskID, nodeID, wantStatus string, wantAttempt int) {
	t.Helper()

	var gotStatus string
	var gotAttempt int
	if err := store.DB().QueryRowContext(ctx, `SELECT status, attempt_count FROM dag_nodes WHERE task_id = ? AND node_id = ?`, taskID, nodeID).Scan(&gotStatus, &gotAttempt); err != nil {
		t.Fatalf("load dag node status: %v", err)
	}
	if gotStatus != wantStatus || gotAttempt != wantAttempt {
		t.Fatalf("expected dag node %s/%s to be (%s,%d), got (%s,%d)", taskID, nodeID, wantStatus, wantAttempt, gotStatus, gotAttempt)
	}
}

func assertTaskStatus(t *testing.T, ctx context.Context, store *sqlite.Store, taskID, want string) {
	t.Helper()

	status, err := store.GetTaskStatus(ctx, "", taskID)
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.Status != want {
		t.Fatalf("expected task %s status %q, got %q", taskID, want, status.Status)
	}
}

func assertTaskClaimStatusOptional(t *testing.T, ctx context.Context, store *sqlite.Store, taskID, want string) {
	t.Helper()

	var status sql.NullString
	err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE task_id = ? LIMIT 1`, taskID).Scan(&status)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("load task claim status: %v", err)
	}
	got := ""
	if status.Valid {
		got = strings.TrimSpace(status.String)
	}
	if got != want {
		t.Fatalf("expected task claim status %q, got %q", want, got)
	}
}
