package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestNodeClaimRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-d1c-node-claim-missing-authority"
		taskID      = "task-d1c-node-claim-missing-authority"
		nodeID      = "node-1"
		agentID     = "agent-d1c-node-claim-missing-authority"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	seedD1CServerNodeFixture(t, ctx, store, workspaceID, taskID, agentID, dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}},
	}, false)

	raw, err := json.Marshal(agentNodeClaimParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "should fail closed",
	})
	if err != nil {
		t.Fatalf("marshal node claim params: %v", err)
	}

	result, rpcErr := h.agentNodeClaim(ctx, raw)
	assertD1CNodeAuthorityReject(t, result, rpcErr, workspaceID, "agent.node.claim", string(sqlite.AuthorityRejectMissing))
	assertD1CNoNodeRuntimeEvent(t, ctx, store, workspaceID, taskID, nodeID, "node.claimed")
}

func TestNodeReleaseRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-d1c-node-release-stale-authority"
		taskID      = "task-d1c-node-release-stale-authority"
		nodeID      = "node-1"
		agentID     = "agent-d1c-node-release-stale-authority"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	seedD1CServerNodeFixture(t, ctx, store, workspaceID, taskID, agentID, dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}},
	}, true)
	if err := store.ClaimNode(ctx, sqlite.NodeClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "seed claim",
	}); err != nil {
		t.Fatalf("seed claim node: %v", err)
	}
	transferD1CServerNodeWorkspaceAuthorityToPeer(t, ctx, store, workspaceID)

	raw, err := json.Marshal(agentNodeReleaseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Reason:      "should fail closed",
	})
	if err != nil {
		t.Fatalf("marshal node release params: %v", err)
	}

	result, rpcErr := h.agentNodeRelease(ctx, raw)
	assertD1CNodeAuthorityReject(t, result, rpcErr, workspaceID, "agent.node.release", string(sqlite.AuthorityRejectStale))
	assertD1CNoNodeRuntimeEvent(t, ctx, store, workspaceID, taskID, nodeID, "node.released")

	status, err := store.GetTaskStatus(ctx, "", taskID)
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if len(status.Nodes) != 1 || status.Nodes[0].Status != model.NodeStatusRunning {
		t.Fatalf("expected node to remain running after stale authority reject, got %+v", status.Nodes)
	}
}

func TestNodeCompleteRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-d1c-node-complete-stale-authority"
		taskID      = "task-d1c-node-complete-stale-authority"
		nodeID      = "node-1"
		agentID     = "agent-d1c-node-complete-stale-authority"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	seedD1CServerNodeFixture(t, ctx, store, workspaceID, taskID, agentID, dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}},
	}, true)
	if err := store.ClaimNode(ctx, sqlite.NodeClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "seed claim",
	}); err != nil {
		t.Fatalf("seed claim node: %v", err)
	}
	transferD1CServerNodeWorkspaceAuthorityToPeer(t, ctx, store, workspaceID)

	raw, err := json.Marshal(agentNodeCompleteParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "should fail closed",
	})
	if err != nil {
		t.Fatalf("marshal node complete params: %v", err)
	}

	result, rpcErr := h.agentNodeComplete(ctx, raw)
	assertD1CNodeAuthorityReject(t, result, rpcErr, workspaceID, "agent.node.complete", string(sqlite.AuthorityRejectStale))
	assertD1CNoNodeRuntimeEvent(t, ctx, store, workspaceID, taskID, nodeID, "node.completed")

	status, err := store.GetTaskStatus(ctx, "", taskID)
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if len(status.Nodes) != 1 || status.Nodes[0].Status != model.NodeStatusRunning {
		t.Fatalf("expected node to remain running after stale authority reject, got %+v", status.Nodes)
	}
}

func seedD1CServerNodeFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID string, graph dag.Graph, claimAuthority bool) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Node Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if claimAuthority {
		claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
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

func transferD1CServerNodeWorkspaceAuthorityToPeer(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
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
}

func assertD1CNodeAuthorityReject(t *testing.T, result any, rpcErr *RPCError, workspaceID, surface, rejectCode string) {
	t.Helper()

	if rpcErr == nil {
		t.Fatal("expected typed authority reject")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("unexpected RPC authority reject: %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["error_kind"] != "authority_reject" || details["reject_code"] != rejectCode || details["workspace_id"] != workspaceID || details["surface"] != surface {
		t.Fatalf("unexpected authority reject details: %+v", details)
	}
}

func assertD1CNoNodeRuntimeEvent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, nodeID, eventType string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "dag_node",
		EntityID:    nodeID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events for %s: %v", eventType, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events on authority reject, got %+v", eventType, events)
	}
}
