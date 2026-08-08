package sqlite_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestMemoryNodeWriteWritesThroughCanonicalWorkspaceMemory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-node-write"
		agentID     = "agent-memory-node"
		taskID      = "task-memory-node"
		sessionID   = "sess-memory-node"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Write",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Node Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-memory-node-write")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	result, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Adopt canonical memory writes",
		Body:        "All node writes should flow through canonical workspace memory.",
		Summary:     "Write-through only.",
		AgentID:     agentID,
		SessionID:   sessionID,
		TaskID:      taskID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "write"},
		Importance:  0.84,
		Confidence:  0.91,
	})
	if err != nil {
		t.Fatalf("write memory node: %v", err)
	}

	if result.Status != "RECORDED" || result.OriginKind != "workspace_memory" {
		t.Fatalf("unexpected write result metadata: %+v", result)
	}
	if result.MemoryID == "" || result.NodeID == "" {
		t.Fatalf("expected both memory and node ids, got %+v", result)
	}
	if result.Node.MemoryID != result.NodeID || result.Node.OriginID != result.MemoryID {
		t.Fatalf("expected derived node to point at backing memory id, got %+v", result.Node)
	}

	record, err := store.GetWorkspaceMemory(ctx, workspaceID, result.MemoryID)
	if err != nil {
		t.Fatalf("get workspace memory after node write: %v", err)
	}
	if record.Body != "All node writes should flow through canonical workspace memory." || record.TaskID != taskID {
		t.Fatalf("unexpected workspace memory backing record: %+v", record)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, result.NodeID)
	if err != nil {
		t.Fatalf("get derived memory graph node: %v", err)
	}
	if detail.Node.OriginKind != "workspace_memory" || detail.Node.OriginID != result.MemoryID {
		t.Fatalf("expected workspace_memory origin, got %+v", detail.Node)
	}
	if detail.Node.MemoryType != "DECISION_RECORD" || detail.Node.CompatType != "DECISION" || detail.Node.SessionID != sessionID {
		t.Fatalf("unexpected derived node projection: %+v", detail.Node)
	}
}

func TestMemoryNodeWritePreservesAntiProcedureAndPromotedClaimEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-anti-procedure"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Anti Procedure",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	result, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		NodeID:      "memnode:workspace_memory:memory-node-anti-procedure",
		MemoryType:  "anti_procedure",
		Title:       "Never bypass rollback gates",
		Body:        "Anti-procedure memory-node writes should preserve their canonical type and promoted claim effects.",
		Summary:     "Anti-procedure node write parity.",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
	})
	if err != nil {
		t.Fatalf("write anti procedure memory node: %v", err)
	}

	if result.Record.MemoryType != "ANTI_PROCEDURE" || result.Node.MemoryType != "ANTI_PROCEDURE" {
		t.Fatalf("expected anti-procedure memory node write to preserve canonical type, got %+v", result)
	}
	if result.PromotedClaimEffects == nil || result.PromotedClaimEffects.Claim == nil || result.PromotedClaimEffects.ClaimEvent == nil {
		t.Fatalf("expected anti-procedure memory node write to surface promoted claim effects, got %+v", result.PromotedClaimEffects)
	}
	if result.PromotedClaimEffects.Claim.ClaimType != "ANTI_PROCEDURE" || result.PromotedClaimEffects.Claim.MemoryID != result.MemoryID {
		t.Fatalf("expected anti-procedure promoted claim effect, got %+v", result.PromotedClaimEffects.Claim)
	}
	if result.PromotedClaimEffects.ClaimEvent.EventType != "knowledge_claim.written" {
		t.Fatalf("expected anti-procedure promoted claim event, got %+v", result.PromotedClaimEffects.ClaimEvent)
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    result.MemoryID,
		ClaimType:   "ANTI_PROCEDURE",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list anti-procedure promoted claims: %v", err)
	}
	if len(claims) != 1 || claims[0].ClaimType != "ANTI_PROCEDURE" || claims[0].MemoryID != result.MemoryID {
		t.Fatalf("expected one anti-procedure promoted claim, got %+v", claims)
	}
}

func TestMemoryNodeWriteSupportsPolicyTraceWithoutPromotedClaimEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-policy-trace"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Policy Trace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	result, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		NodeID:      "memnode:workspace_memory:memory-node-policy-trace",
		MemoryType:  "policy_trace",
		Title:       "Safety policy trace",
		Body:        "Direct node writes should preserve already-supported policy-trace identity memory types.",
		Summary:     "Policy-trace node write parity.",
		SourceKind:  "manual",
		SourceID:    "agent-memory",
	})
	if err != nil {
		t.Fatalf("write policy trace memory node: %v", err)
	}

	if result.Record.MemoryType != "POLICY_TRACE" || result.Node.MemoryType != "POLICY_TRACE" {
		t.Fatalf("expected policy-trace memory node write to preserve canonical type, got %+v", result)
	}
	if result.Node.MemoryLayer != "IDENTITY" {
		t.Fatalf("expected policy-trace node to project to IDENTITY layer, got %+v", result.Node)
	}
	if result.PromotedClaimEffects != nil {
		t.Fatalf("did not expect policy-trace node write to synthesize promoted claim effects, got %+v", result.PromotedClaimEffects)
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    result.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list policy-trace claims after node write: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("did not expect policy-trace node write to materialize claims yet, got %+v", claims)
	}
}

func TestMemoryNodeWriteSupportsGoalCommitmentWithoutPromotedClaimEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-goal-commitment"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Goal Commitment",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	result, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		NodeID:      "memnode:workspace_memory:memory-node-goal-commitment",
		MemoryType:  "goal_commitment",
		Title:       "Guard the control corridor",
		Body:        "Direct node writes should preserve already-supported goal-commitment identity memory types.",
		Summary:     "Goal-commitment node write parity.",
		SourceKind:  "manual",
		SourceID:    "agent-memory",
	})
	if err != nil {
		t.Fatalf("write goal commitment memory node: %v", err)
	}

	if result.Record.MemoryType != "GOAL_COMMITMENT" || result.Node.MemoryType != "GOAL_COMMITMENT" {
		t.Fatalf("expected goal-commitment memory node write to preserve canonical type, got %+v", result)
	}
	if result.Node.MemoryLayer != "IDENTITY" {
		t.Fatalf("expected goal-commitment node to project to IDENTITY layer, got %+v", result.Node)
	}
	if result.PromotedClaimEffects != nil {
		t.Fatalf("did not expect goal-commitment node write to synthesize promoted claim effects, got %+v", result.PromotedClaimEffects)
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    result.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list goal-commitment claims after node write: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("did not expect goal-commitment node write to materialize claims yet, got %+v", claims)
	}
}

func TestMemoryNodeWriteAcceptsAliasesAndReusesBackingMemory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-alias"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Alias",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	first, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		NodeID:      "memnode:workspace_memory:memory-node-alias",
		MemoryType:  "procedure",
		Title:       "Doctor gate",
		Body:        "Run doctor after the rollout and before cutover.",
		Summary:     "First version.",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
		Importance:  0.4,
	})
	if err != nil {
		t.Fatalf("write first memory node: %v", err)
	}

	second, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		MemoryID:    "memory-node-alias",
		MemoryType:  "procedure",
		Title:       "Doctor gate",
		Body:        "Run doctor after the rollout and before cutover.",
		Summary:     "Updated version.",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
		Importance:  0.9,
	})
	if err != nil {
		t.Fatalf("write second memory node: %v", err)
	}

	if first.MemoryID != second.MemoryID || first.NodeID != second.NodeID {
		t.Fatalf("expected alias writes to reuse backing identity, got %+v and %+v", first, second)
	}
	if second.Record.Summary != "Updated version." || second.Record.Importance != 0.9 {
		t.Fatalf("expected second write to update the canonical record, got %+v", second.Record)
	}
}

func TestMemoryNodeWriteRejectsForeignOriginAndMismatchedAliases(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-node-invalid",
		Title:       "Memory Node Invalid",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: "ws-memory-node-invalid",
		NodeID:      "memnode:knowledge_claim:claim-1",
		Body:        "This should fail.",
	}); err == nil || !strings.Contains(err.Error(), "workspace_memory origin") {
		t.Fatalf("expected workspace_memory origin rejection, got %v", err)
	}

	if _, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: "ws-memory-node-invalid",
		NodeID:      "memnode:workspace_memory:memory-a",
		MemoryID:    "memory-b",
		Body:        "This should also fail.",
	}); err == nil || !strings.Contains(err.Error(), "same workspace_memory origin") {
		t.Fatalf("expected alias mismatch rejection, got %v", err)
	}
}

func TestMemoryNodeWriteRejectsCrossWorkspaceReuseAndAnchorMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceA = "ws-memory-node-a"
		workspaceB = "ws-memory-node-b"
		agentA     = "agent-memory-node-a"
		agentB     = "agent-memory-node-b"
		taskA      = "task-memory-node-a"
		taskB      = "task-memory-node-b"
		sessionA   = "sess-memory-node-a"
	)

	for _, workspaceID := range []string{workspaceA, workspaceB} {
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       workspaceID,
			CreatedBy:   "developer",
		}); err != nil {
			t.Fatalf("create workspace %s: %v", workspaceID, err)
		}
	}
	for _, item := range []struct {
		workspaceID string
		agentID     string
	}{
		{workspaceA, agentA},
		{workspaceA, agentB},
		{workspaceB, agentB},
	} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: item.workspaceID,
			AgentID:     item.agentID,
			OwnerUserID: "developer",
			DisplayName: item.agentID,
		}); err != nil {
			t.Fatalf("register agent %+v: %v", item, err)
		}
	}
	createSingleNodeTask(t, ctx, store, taskA, "node-memory-node-a")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceA,
		TaskID:      taskA,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task A: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskB, "node-memory-node-b")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceA,
		TaskID:      taskB,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task B: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionA,
		AgentID:     agentA,
		WorkspaceID: workspaceA,
		TaskID:      taskA,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session A: %v", err)
	}

	first, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceA,
		MemoryID:    "shared-memory-id",
		MemoryType:  "lesson",
		Body:        "Canonical backing row.",
	})
	if err != nil {
		t.Fatalf("seed cross-workspace owned memory id: %v", err)
	}
	if _, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceB,
		MemoryID:    first.MemoryID,
		MemoryType:  "lesson",
		Body:        "Should not be allowed to re-home.",
	}); err == nil || !strings.Contains(err.Error(), "already belongs to workspace") {
		t.Fatalf("expected cross-workspace ownership rejection, got %v", err)
	}
	if _, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceA,
		SessionID:   sessionA,
		AgentID:     agentB,
		Body:        "Wrong agent for session.",
	}); err == nil || !strings.Contains(err.Error(), "does not belong to agent_id") {
		t.Fatalf("expected session/agent mismatch rejection, got %v", err)
	}
	if _, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceA,
		SessionID:   sessionA,
		TaskID:      taskB,
		Body:        "Wrong task for session.",
	}); err == nil || !strings.Contains(err.Error(), "does not belong to task_id") {
		t.Fatalf("expected session/task mismatch rejection, got %v", err)
	}
	if _, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceA,
		MemoryType:  "fact",
		Body:        "Derived graph type should not be accepted as direct input.",
	}); err == nil || !strings.Contains(err.Error(), "memory_type must be one of") {
		t.Fatalf("expected memory_type allowlist rejection, got %v", err)
	}
}

func TestMemoryNodeWriteDoesNotBackfillUnrelatedGraphRows(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-no-backfill"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node No Backfill",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	first, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Older memory",
		Body:        "This row already exists before the node write seam is used.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record first memory: %v", err)
	}
	firstNodeID := "memnode:workspace_memory:" + first.MemoryID
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_edges WHERE workspace_id = ? AND (from_memory_id = ? OR to_memory_id = ?)`, workspaceID, firstNodeID, firstNodeID); err != nil {
		t.Fatalf("delete first node edges: %v", err)
	}
	for _, stmt := range []string{
		`DELETE FROM memory_node_metrics WHERE workspace_id = ? AND memory_id = ?`,
		`DELETE FROM memory_node_versions WHERE workspace_id = ? AND memory_id = ?`,
		`DELETE FROM memory_node_refs WHERE workspace_id = ? AND memory_id = ?`,
		`DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`,
	} {
		if _, err := store.DB().ExecContext(ctx, stmt, workspaceID, firstNodeID); err != nil {
			t.Fatalf("delete first node projection with %q: %v", stmt, err)
		}
	}

	second, err := store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Body:        "Only the new memory should be projected.",
	})
	if err != nil {
		t.Fatalf("write second memory node: %v", err)
	}
	if second.NodeID == "" {
		t.Fatalf("expected derived node id for second write, got %+v", second)
	}

	if _, err := store.GetMemoryGraphNode(ctx, workspaceID, firstNodeID); err == nil {
		t.Fatalf("expected old unrelated graph row to remain missing until explicit graph sync")
	}
	if _, err := store.GetMemoryGraphNode(ctx, workspaceID, second.NodeID); err != nil {
		t.Fatalf("expected new write to project its own graph row: %v", err)
	}
}
