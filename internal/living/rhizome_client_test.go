package living_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// newTestStore creates a temporary SQLite store with all migrations applied.
func newTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "rhizome-test.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	authorityNodeIDLiteral := strings.ReplaceAll(node.AuthorityNodeID, `'`, `''`)
	triggerSQL := fmt.Sprintf(`
CREATE TRIGGER IF NOT EXISTS test_seed_workspace_authority_after_insert
AFTER INSERT ON workspaces
BEGIN
	INSERT INTO workspace_authority(
		workspace_id,
		scope,
		holder_authority_node_id,
		lease_token,
		term,
		lease_expires_at,
		commit_watermark,
		applied_watermark,
		status,
		updated_at
	) VALUES (
		NEW.workspace_id,
		'workspace',
		'%s',
		'lease-living-test-auto-' || NEW.workspace_id,
		1,
		strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ','now','+1 hour'),
		1,
		1,
		'ACTIVE',
		strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ','now')
	)
	ON CONFLICT(workspace_id, scope) DO NOTHING;
END
`, authorityNodeIDLiteral)
	if _, err := store.DB().ExecContext(ctx, triggerSQL); err != nil {
		t.Fatalf("install workspace authority seed trigger: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// seedWorkspaceAndTask creates a workspace, a task attached to it, and
// registers the given agent. Returns the task ID.
func seedWorkspaceAndTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, _ string) string {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Test workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimLivingTestWorkspaceAuthority(t, ctx, store, workspaceID)

	taskID := "task-test-001"
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "n1", Type: "generic"}},
	})
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "test-user",
		Priority:    "normal",
		Title:       "Test Task",
		Description: "A test task",
		TaskKind:    "EXECUTION",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}

	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "test-user",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Test Agent",
		Role:        "worker",
		Status:      "REGISTERED",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	return taskID
}

// claimAndRunLivingTestTask claims a seeded task for the given agent and drives it
// to RUNNING via the canonical store path. Recording a task-bound session.start
// event requires a live same-owner claim (see
// requireLiveSameOwnerClaimForSessionStartTx), so any test that records a
// task-bound session start must establish the claim first.
func claimAndRunLivingTestTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID string) {
	t.Helper()
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claim before task-bound session start",
	}); err != nil {
		t.Fatalf("claim task before session start: %v", err)
	}
}

type blockedControlClusterScenario struct {
	workspaceID    string
	taskID         string
	protoClusterID string
	docKey         string
	artifactRef    string
	sessionID      string
}

func seedBlockedControlClusterScenario(t *testing.T, ctx context.Context, store *sqlite.Store, suffix string) blockedControlClusterScenario {
	t.Helper()

	scenario := blockedControlClusterScenario{
		workspaceID: "ws-control-cluster-" + suffix,
		taskID:      "task-control-cluster-" + suffix,
		docKey:      "doc-control-cluster-" + suffix,
		artifactRef: "artifact://control-cluster-" + suffix,
		sessionID:   "sess-control-cluster-" + suffix,
	}
	scenario.protoClusterID = "task:" + scenario.workspaceID + "/" + scenario.taskID

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: scenario.workspaceID,
		Title:       "Control Cluster Workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimLivingTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: scenario.workspaceID,
			AgentID:     agentID,
			OwnerUserID: "test-user",
			DisplayName: agentID,
			Role:        "worker",
			Status:      "REGISTERED",
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-control-cluster-" + suffix, Type: "generic"}},
	})
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      scenario.taskID,
		OwnerUserID: "test-user",
		Priority:    "normal",
		Title:       "Blocked Control Cluster Task",
		Description: "Task fixture for control cluster detail direct client coverage.",
		TaskKind:    "EXECUTION",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		LinkedBy:    "test-user",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.docKey,
		Title:       "Control Cluster Runbook",
		Content:     "blocked control cluster doc",
		UpdatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Title:       "Control Cluster Artifact",
		ArtifactRef: scenario.artifactRef,
		Kind:        "log",
		ContentType: "text/plain",
		CreatedBy:   "agent-a",
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: scenario.workspaceID,
		AgentID:     "agent-b",
		UpdateType:  "status",
		Summary:     "control cluster update",
		PayloadJSON: `{"task_ids":["` + scenario.taskID + `"],"doc_keys":["` + scenario.docKey + `"],"artifacts":[{"ref":"` + scenario.artifactRef + `"}]}`,
	}); err != nil {
		t.Fatalf("record agent update: %v", err)
	}
	// agent-a is the session owner below; the task-bound session start requires a
	// live same-owner claim before the start event is recorded.
	claimAndRunLivingTestTask(t, ctx, store, scenario.workspaceID, scenario.taskID, "agent-a")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "start control cluster session",
		OwnerScope:  "task/session",
		RelatedDocKeys: []string{
			scenario.docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: scenario.artifactRef},
		},
	}); err != nil {
		t.Fatalf("record start session: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "blocked control cluster session",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "approve control cluster action"},
		},
		RelatedDocKeys:      []string{scenario.docKey},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{{Ref: scenario.artifactRef}},
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue from session state: %v", err)
	}

	return scenario
}

func TestDirectRhizomeClient_FetchTasks(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-fetch-tasks"
	agentID := "agent-1"
	seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	client := living.NewDirectRhizomeClient(store, workspaceID)

	// Fetch all pending tasks (no filter)
	tasks, err := client.FetchTasks(ctx, nil)
	if err != nil {
		t.Fatalf("FetchTasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].TaskID != "task-test-001" {
		t.Fatalf("expected task-test-001, got %s", tasks[0].TaskID)
	}

	// Fetch with matching type
	tasks, err = client.FetchTasks(ctx, []string{"EXECUTION"})
	if err != nil {
		t.Fatalf("FetchTasks with type: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task with type filter, got %d", len(tasks))
	}

	// Fetch with non-matching type
	tasks, err = client.FetchTasks(ctx, []string{"deploy"})
	if err != nil {
		t.Fatalf("FetchTasks with wrong type: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected 0 tasks with wrong type filter, got %d", len(tasks))
	}
}

func TestDirectRhizomeClient_BuildMemoryShellPacket(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-memory-shell-packet-client"
	agentID := "agent-memory-shell-packet-client"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-direct-shell-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Direct self model",
		Body:        "Direct client shell packet should surface task-scoped identity memories.",
		Summary:     "Direct shell self model.",
		SourceKind:  "manual",
		SourceID:    agentID,
		TaskID:      taskID,
		AgentID:     agentID,
		Importance:  0.9,
	}); err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	packet, err := client.BuildMemoryShellPacket(ctx, living.WorkspaceMemoryPacketFilter{
		TaskID: taskID,
	})
	if err != nil {
		t.Fatalf("BuildMemoryShellPacket: %v", err)
	}
	if packet.Meta.PacketKind != "SHELL" || packet.Meta.WorkspaceID != workspaceID || packet.Meta.TaskID != taskID || packet.Meta.AgentID != agentID {
		t.Fatalf("unexpected packet meta: %+v", packet.Meta)
	}
	if packet.KernelRef.PacketKey == "" || packet.KernelRef.BasisDigest == "" {
		t.Fatalf("expected shell packet to preserve kernel_ref linkage, got %+v", packet.KernelRef)
	}
	if packet.BoundarySummary == nil || packet.BasisSummary == nil {
		t.Fatalf("expected shell packet summaries from direct client, got %+v / %+v", packet.BoundarySummary, packet.BasisSummary)
	}
	if packet.BasisSummary.TotalRefCount != len(packet.BasisRefs) {
		t.Fatalf("expected basis summary to match shell packet basis refs, got %+v vs %+v", packet.BasisSummary, packet.BasisRefs)
	}
	if len(packet.IdentityMemories) == 0 || packet.IdentityMemories[0].MemoryType != "SELF_MODEL" {
		t.Fatalf("expected identity memory in shell packet, got %+v", packet.IdentityMemories)
	}
}

func TestDirectRhizomeClient_BuildMemoryKernelPacket(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-memory-kernel-packet-client"
	agentID := "agent-memory-kernel-packet-client"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "coordination")

	client := living.NewDirectRhizomeClient(store, workspaceID)
	packet, err := client.BuildMemoryKernelPacket(ctx, living.WorkspaceMemoryPacketFilter{
		TaskID: taskID,
	})
	if err != nil {
		t.Fatalf("BuildMemoryKernelPacket: %v", err)
	}
	if packet.Meta.PacketKind != "KERNEL" || packet.Meta.WorkspaceID != workspaceID || packet.Meta.TaskID != taskID {
		t.Fatalf("unexpected packet meta: %+v", packet.Meta)
	}
	if packet.BoundarySummary == nil || packet.BasisSummary == nil {
		t.Fatalf("expected kernel packet summaries from direct client, got %+v / %+v", packet.BoundarySummary, packet.BasisSummary)
	}
	if packet.BasisSummary.TotalRefCount != len(packet.BasisRefs) {
		t.Fatalf("expected basis summary to match kernel packet basis refs, got %+v vs %+v", packet.BasisSummary, packet.BasisRefs)
	}
}

func TestDirectRhizomeClient_ListAndGetMemoryPromotions(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-memory-promotion-client"
	agentID := "agent-memory-promotion-client"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	record, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "lesson",
			Title:      "Promotion candidate",
			Body:       "Living client should expose existing advisory promotion candidates without mutation semantics.",
			TaskID:     taskID,
			AgentID:    agentID,
			SourceKind: "memory_packet_shell",
			SourceID:   "shell-packet-client-promotion",
			Importance: 0.8,
			Confidence: 0.9,
		},
		BasisDigest: "basis-digest-client-promotion",
		BasisRefs:   []string{"packet:shell-packet-client-promotion"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("EnqueueMemoryPromotion: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	items, err := client.ListMemoryPromotions(ctx, living.WorkspaceMemoryPromotionFilter{
		State: "PENDING",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListMemoryPromotions: %v", err)
	}
	if len(items) != 1 || items[0].PromotionID != record.PromotionID {
		t.Fatalf("expected pending promotion list to return the enqueued record, got %+v", items)
	}
	if items[0].CandidateType != "LESSON" || items[0].Candidate.SourceKind != "memory_packet_shell" {
		t.Fatalf("expected advisory promotion candidate shape to be preserved, got %+v", items[0])
	}

	got, err := client.GetMemoryPromotion(ctx, living.WorkspaceMemoryPromotionFilter{
		PromotionID: record.PromotionID,
	})
	if err != nil {
		t.Fatalf("GetMemoryPromotion: %v", err)
	}
	if got.PromotionID != record.PromotionID || got.State != "PENDING" {
		t.Fatalf("expected get promotion to hydrate the same pending record, got %+v", got)
	}
	if got.CoherenceGate != nil && got.CoherenceGate.AdvisoryAction == "" {
		t.Fatalf("expected coherence gate shape to stay hydrated when present, got %+v", got.CoherenceGate)
	}
}

func TestDirectRhizomeClient_GetMemoryCoherenceScope(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-coherence-client"
		agentID     = "agent-memory-coherence-client"
		sessionID   = "sess-memory-coherence-client"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("CreateAgentSession: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:      "memres-client-coherence",
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		SessionID:     sessionID,
		ReportScope:   "SESSION",
		StaleReadRate: 0.25,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:     "P2",
				ReplicaKind:       "memory_node",
				CoherenceClass:    "A",
				State:             "INVALIDATED",
				CanonicalMemoryID: "memory:client-coherence",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "doc-client-coherence", VersionToken: "doc-v1", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("ReportMemoryResidency: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	scope, err := client.GetMemoryCoherenceScope(ctx, living.WorkspaceMemoryCoherenceFilter{
		AgentID:     agentID,
		SessionID:   sessionID,
		ReportScope: "SESSION",
	})
	if err != nil {
		t.Fatalf("GetMemoryCoherenceScope: %v", err)
	}
	if scope.AgentID != agentID || scope.SessionID != sessionID || scope.ReportScope != "SESSION" {
		t.Fatalf("expected scoped coherence payload, got %+v", scope)
	}
	if scope.CoherenceBandHint != "DEGRADED" || !scope.NeedsAttention || scope.ReadyInvalidationCount != 1 {
		t.Fatalf("expected degraded coherence scope from existing read-side, got %+v", scope)
	}
	if scope.TimeAuthority.WorkspaceID != workspaceID {
		t.Fatalf("expected coherence scope to preserve time authority, got %+v", scope.TimeAuthority)
	}
}

func TestDirectRhizomeClient_MemoryInvalidationReadFacades(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-client"
		agentID     = "agent-memory-invalidation-client"
		sessionID   = "sess-memory-invalidation-client"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("CreateAgentSession: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:      "memres-client-invalidation",
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		SessionID:     sessionID,
		ReportScope:   "SESSION",
		StaleReadRate: 0.30,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:     "P2",
				ReplicaKind:       "memory_node",
				CoherenceClass:    "A",
				State:             "INVALIDATED",
				CanonicalMemoryID: "memory:client-invalidation",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "doc-client-invalidation", VersionToken: "doc-v1", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("ReportMemoryResidency: %v", err)
	}
	polled, _, err := store.PollMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		SessionID:     sessionID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("PollMemoryInvalidationsWithEvents: %v", err)
	}
	if len(polled) != 1 {
		t.Fatalf("expected one invalidation after residency report, got %+v", polled)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	listResult, err := client.ListMemoryInvalidations(ctx, living.WorkspaceMemoryInvalidationListFilter{
		SessionID: sessionID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListMemoryInvalidations: %v", err)
	}
	if listResult.WorkspaceID != workspaceID || listResult.AgentID != agentID {
		t.Fatalf("expected invalidation list to stay workspace/agent scoped, got %+v", listResult)
	}
	if listResult.TimeAuthority.WorkspaceID != workspaceID || listResult.Count != 1 || len(listResult.Items) != 1 {
		t.Fatalf("expected one invalidation with time authority, got %+v", listResult)
	}
	if listResult.Items[0].InvalidationID != polled[0].InvalidationID || listResult.Items[0].DeliveredAt == "" {
		t.Fatalf("expected canonical invalidation row with delivered marker, got %+v", listResult.Items[0])
	}

	record, err := client.GetMemoryInvalidation(ctx, living.WorkspaceMemoryInvalidationGetFilter{
		InvalidationID: polled[0].InvalidationID,
	})
	if err != nil {
		t.Fatalf("GetMemoryInvalidation: %v", err)
	}
	if record.InvalidationID != polled[0].InvalidationID || record.Reason != polled[0].Reason {
		t.Fatalf("expected fetched invalidation to preserve canonical record shape, got %+v", record)
	}
	if record.TimeAuthority.WorkspaceID != workspaceID {
		t.Fatalf("expected fetched invalidation to preserve time authority, got %+v", record.TimeAuthority)
	}

	cursor, err := client.GetMemoryInvalidationCursor(ctx, living.WorkspaceMemoryInvalidationCursorFilter{
		SessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("GetMemoryInvalidationCursor: %v", err)
	}
	if cursor.WorkspaceID != workspaceID || cursor.AgentID != agentID || cursor.SessionID != sessionID {
		t.Fatalf("expected invalidation cursor to stay scoped, got %+v", cursor)
	}
	if cursor.LastDeliveredInvalidationID != polled[0].InvalidationID || cursor.LastPollCount != 1 {
		t.Fatalf("expected invalidation cursor to preserve delivered/poll markers, got %+v", cursor)
	}
}

func TestDirectRhizomeClient_MemoryGraphReadFacades(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-memory-graph-client"
	agentID := "agent-memory-graph-client"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Memory graph client lesson",
		Body:        "Native-agent graph facades should stay read-only.",
		Summary:     "bounded graph client detail",
		AgentID:     agentID,
		TaskID:      taskID,
		SourceKind:  "workspace_memory_write",
		SourceID:    "client-test",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.SyncMemoryGraphWorkspace(ctx, workspaceID); err != nil {
		t.Fatalf("sync memory graph workspace: %v", err)
	}
	nodeID := "memnode:workspace_memory:" + record.MemoryID

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	listResult, err := client.ListMemoryGraphNodes(ctx, living.WorkspaceMemoryGraphListFilter{
		TaskID:     taskID,
		OriginKind: "workspace_memory",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListMemoryGraphNodes: %v", err)
	}
	if listResult.WorkspaceID != workspaceID {
		t.Fatalf("expected graph list to stay scoped to workspace, got %+v", listResult)
	}
	if listResult.TimeAuthority.WorkspaceID != workspaceID || listResult.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected graph list to preserve time authority, got %+v", listResult.TimeAuthority)
	}
	if listResult.Count != 1 || len(listResult.Items) != 1 {
		t.Fatalf("expected one bounded graph node, got %+v", listResult)
	}
	if listResult.Items[0].MemoryID != nodeID || listResult.Items[0].TaskID != taskID {
		t.Fatalf("expected graph list to preserve canonical node payload, got %+v", listResult.Items[0])
	}

	detail, err := client.GetMemoryGraphNode(ctx, living.WorkspaceMemoryGraphGetFilter{
		MemoryID: nodeID,
	})
	if err != nil {
		t.Fatalf("GetMemoryGraphNode: %v", err)
	}
	if detail.TimeAuthority.WorkspaceID != workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected graph detail to preserve time authority, got %+v", detail.TimeAuthority)
	}
	if detail.Node.MemoryID != nodeID || detail.Node.SemanticLineageID != "workspace_memory:"+record.MemoryID {
		t.Fatalf("expected graph detail to preserve canonical node shape, got %+v", detail.Node)
	}
}

func TestDirectRhizomeClient_SearchMemoryNodes(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-memory-node-search-client"
	agentID := "agent-memory-node-search-client"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Memory node search client lesson",
		Body:        "Canonical node search should stay read-only on the living boundary.",
		Summary:     "bounded node search detail",
		AgentID:     agentID,
		TaskID:      taskID,
		SourceKind:  "workspace_memory_write",
		SourceID:    "client-search-test",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.SyncMemoryGraphWorkspace(ctx, workspaceID); err != nil {
		t.Fatalf("sync memory graph workspace: %v", err)
	}
	nodeID := "memnode:workspace_memory:" + record.MemoryID

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	result, err := client.SearchMemoryNodes(ctx, living.WorkspaceMemoryNodeSearchFilter{
		Query:      "read-only on the living boundary",
		OriginKind: "workspace_memory",
		TaskID:     taskID,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("SearchMemoryNodes: %v", err)
	}
	if result.WorkspaceID != workspaceID || result.Query != "read-only on the living boundary" {
		t.Fatalf("expected bounded node search workspace/query mirrors, got %+v", result)
	}
	if result.TimeAuthority.WorkspaceID != workspaceID || result.GeneratedAt == "" {
		t.Fatalf("expected node search to preserve time authority/generated_at, got %+v", result)
	}
	if result.Count != 1 || len(result.Hits) != 1 {
		t.Fatalf("expected one bounded node-search hit, got %+v", result)
	}
	if result.Hits[0].MemoryID != nodeID || result.Hits[0].Snippet == "" {
		t.Fatalf("expected canonical node-search hit, got %+v", result.Hits[0])
	}
}

func TestDirectRhizomeClient_TensionReadFacades(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	scenario := seedBlockedControlClusterScenario(t, ctx, store, "living-tension-client")
	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("RefreshTensions: %v", err)
	}
	if refresh.CreatedCount+refresh.UpdatedCount+refresh.RecoveredCount == 0 {
		t.Fatalf("expected refresh to touch canonical tensions, got %+v", refresh)
	}

	client := living.NewDirectRhizomeClient(store, scenario.workspaceID)
	client.SetAgentID("agent-a")

	listResult, err := client.ListTensions(ctx, living.WorkspaceTensionFilter{
		TaskID: scenario.taskID,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListTensions: %v", err)
	}
	if listResult.WorkspaceID != scenario.workspaceID || listResult.TimeAuthority.WorkspaceID != scenario.workspaceID {
		t.Fatalf("expected bounded tension list workspace/time authority, got %+v", listResult)
	}
	if listResult.Count == 0 || len(listResult.Items) == 0 {
		t.Fatalf("expected non-empty canonical tension list, got %+v", listResult)
	}
	if listResult.Items[0].TensionID == "" || listResult.Items[0].ProtoClusterID != scenario.protoClusterID {
		t.Fatalf("expected canonical tension list item, got %+v", listResult.Items[0])
	}
	primary := listResult.Items[0]

	detail, err := client.GetTension(ctx, living.WorkspaceTensionGetFilter{
		TensionID: primary.TensionID,
	})
	if err != nil {
		t.Fatalf("GetTension: %v", err)
	}
	if detail.TimeAuthority.WorkspaceID != scenario.workspaceID || detail.Tension.TensionID != primary.TensionID {
		t.Fatalf("expected canonical tension detail/time authority, got %+v", detail)
	}
	if detail.Tension.ProtoClusterID != scenario.protoClusterID || strings.TrimSpace(detail.Tension.Title) == "" {
		t.Fatalf("expected hydrated tension detail, got %+v", detail.Tension)
	}

	frontierResult, err := client.ListTensionFrontier(ctx, living.WorkspaceTensionFilter{
		TaskID: scenario.taskID,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListTensionFrontier: %v", err)
	}
	if frontierResult.WorkspaceID != scenario.workspaceID || frontierResult.TimeAuthority.WorkspaceID != scenario.workspaceID {
		t.Fatalf("expected bounded frontier workspace/time authority, got %+v", frontierResult)
	}
	if frontierResult.Count == 0 || len(frontierResult.Items) == 0 {
		t.Fatalf("expected non-empty canonical tension frontier, got %+v", frontierResult)
	}
	if frontierResult.Items[0].TensionID == "" || frontierResult.Items[0].ProtoClusterID != scenario.protoClusterID {
		t.Fatalf("expected canonical frontier item, got %+v", frontierResult.Items[0])
	}
}

func TestDirectRhizomeClient_TensionAttachableFacade(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	scenario := seedBlockedControlClusterScenario(t, ctx, store, "living-tension-attachable-client")
	if _, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
		Limit:       50,
	}); err != nil {
		t.Fatalf("RefreshTensions: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, scenario.workspaceID)
	client.SetAgentID("agent-a")

	result, err := client.ListAttachableTensions(ctx, living.WorkspaceTensionAttachableFilter{})
	if err != nil {
		t.Fatalf("ListAttachableTensions: %v", err)
	}
	if result.WorkspaceID != scenario.workspaceID || result.AgentID != "agent-a" {
		t.Fatalf("expected bounded attachable workspace/agent mirrors, got %+v", result)
	}
	if result.Count == 0 || len(result.Items) == 0 {
		t.Fatalf("expected non-empty attachable shortlist, got %+v", result)
	}
	if result.Items[0].TensionID == "" || result.Items[0].AttachProb <= 0 || result.Items[0].AttachScore == 0 {
		t.Fatalf("expected canonical scored tension payload, got %+v", result.Items[0])
	}
	if result.Items[0].AttachFactors.Fit <= 0 || result.Items[0].AttachFactors.CrowdingRatio < 0 {
		t.Fatalf("expected surfaced attachment factors, got %+v", result.Items[0].AttachFactors)
	}
}

func TestDirectRhizomeClient_GetRSPStateReport(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-state-client"
		agentID     = "agent-rsp-state-client"
		sessionID   = "sess-rsp-state-client"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("CreateAgentSession: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	report, err := client.GetRSPStateReport(ctx, living.WorkspaceRSPStateFilter{
		TaskID:    taskID,
		SessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("GetRSPStateReport: %v", err)
	}
	if report.WorkspaceID != workspaceID || report.AgentID != agentID || report.TaskID != taskID || report.SessionID != sessionID {
		t.Fatalf("expected direct client to preserve state scope, got %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp state report to preserve time authority, got %+v", report.TimeAuthority)
	}
	if strings.TrimSpace(report.SignalType) == "" || strings.TrimSpace(report.Summary) == "" {
		t.Fatalf("expected direct client rsp state report to stay hydrated, got %+v", report)
	}
}

func TestDirectRhizomeClient_GetRSPForecastReport(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-forecast-client"
		agentID     = "agent-rsp-forecast-client"
		sessionID   = "sess-rsp-forecast-client"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("CreateAgentSession: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	report, err := client.GetRSPForecastReport(ctx, living.WorkspaceRSPForecastFilter{
		TaskID:    taskID,
		SessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("GetRSPForecastReport: %v", err)
	}
	if report.WorkspaceID != workspaceID || report.AgentID != agentID || report.TaskID != taskID || report.SessionID != sessionID {
		t.Fatalf("expected direct client to preserve forecast scope, got %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp forecast report to preserve time authority, got %+v", report.TimeAuthority)
	}
	if strings.TrimSpace(report.SignalType) == "" || strings.TrimSpace(report.Summary) == "" {
		t.Fatalf("expected direct client rsp forecast report to stay hydrated, got %+v", report)
	}
}

func TestDirectRhizomeClient_GetRSPBeliefReport(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-belief-client"
		agentID     = "agent-rsp-belief-client"
		sessionID   = "sess-rsp-belief-client"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("CreateAgentSession: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	report, err := client.GetRSPBeliefReport(ctx, living.WorkspaceRSPBeliefFilter{
		TaskID:    taskID,
		SessionID: sessionID,
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("GetRSPBeliefReport: %v", err)
	}
	if report.WorkspaceID != workspaceID || report.AgentID != agentID || report.TaskID != taskID || report.SessionID != sessionID {
		t.Fatalf("expected direct client to preserve belief scope, got %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp belief report to preserve time authority, got %+v", report.TimeAuthority)
	}
	if strings.TrimSpace(report.SignalType) == "" || strings.TrimSpace(report.Summary) == "" {
		t.Fatalf("expected direct client rsp belief report to stay hydrated, got %+v", report)
	}
}

func TestDirectRhizomeClient_GetRSPBeliefClaim(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-rsp-belief-claim-client"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Belief Claim Client Workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rsp-belief-client",
		ClaimType:   "FACT",
		Subject:     "belief-claim-client",
		Body:        "Belief claim reader should stay on the canonical read-side.",
		Summary:     "bounded belief claim direct client",
		Status:      "OPEN",
	})
	if err != nil {
		t.Fatalf("RecordKnowledgeClaim: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	item, err := client.GetRSPBeliefClaim(ctx, living.WorkspaceRSPBeliefClaimFilter{
		ClaimID: claim.ClaimID,
	})
	if err != nil {
		t.Fatalf("GetRSPBeliefClaim: %v", err)
	}
	if item.WorkspaceID != workspaceID || item.ClaimID != claim.ClaimID {
		t.Fatalf("expected direct client to preserve belief-claim scope, got %+v", item)
	}
	if item.TimeAuthority.WorkspaceID != workspaceID || item.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp belief claim to preserve time authority, got %+v", item.TimeAuthority)
	}
	if strings.TrimSpace(item.SignalType) == "" || strings.TrimSpace(item.Summary) == "" {
		t.Fatalf("expected direct client rsp belief claim to stay hydrated, got %+v", item)
	}
}

func TestDirectRhizomeClient_GetRSPTelemetryDump(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-rsp-telemetry-client"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Telemetry Client Workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	dump, err := client.GetRSPTelemetryDump(ctx, living.WorkspaceRSPTelemetryFilter{Limit: 10})
	if err != nil {
		t.Fatalf("GetRSPTelemetryDump: %v", err)
	}
	if dump.Summary.BeliefLogCount != 0 || dump.Summary.AnomalyAlertCount != 0 || dump.Summary.StateLogCount != 0 {
		t.Fatalf("expected empty telemetry dump counters on fresh workspace, got %+v", dump.Summary)
	}
}

func TestDirectRhizomeClient_GetUnifiedControlReport(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-unified-control-client"
		agentID     = "agent-unified-control-client"
		sessionID   = "sess-unified-control-client"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("CreateAgentSession: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	report, err := client.GetUnifiedControlReport(ctx, living.WorkspaceUnifiedControlFilter{
		TaskID:    taskID,
		SessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("GetUnifiedControlReport: %v", err)
	}
	if report.WorkspaceID != workspaceID {
		t.Fatalf("expected unified control report to stay scoped to workspace, got %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected unified control report to preserve time authority, got %+v", report.TimeAuthority)
	}
	if !report.AdvisoryOnly || strings.TrimSpace(report.Summary) == "" {
		t.Fatalf("expected unified control report summary to stay hydrated, got %+v", report)
	}
}

func TestDirectRhizomeClient_GetControlReport(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-control-report-client"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Report Client Workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	report, err := client.GetControlReport(ctx, living.WorkspaceControlReportFilter{})
	if err != nil {
		t.Fatalf("GetControlReport: %v", err)
	}
	if report.WorkspaceID != workspaceID {
		t.Fatalf("expected control report to stay scoped to workspace, got %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected control report to preserve time authority, got %+v", report.TimeAuthority)
	}
	if report.Workspace.TotalClusters != 0 {
		t.Fatalf("expected empty control report workspace metrics on fresh workspace, got %+v", report.Workspace)
	}
}

func TestDirectRhizomeClient_GetControlClusterDetail(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	scenario := seedBlockedControlClusterScenario(t, ctx, store, "client")

	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("RefreshTensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected tension refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("ListTensions: %v", err)
	}
	if len(items) == 0 {
		t.Fatalf("expected tensions after refresh")
	}
	primary := items[0]
	if _, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for control cluster detail",
	}); err != nil {
		t.Fatalf("ConfirmTension: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, scenario.workspaceID)
	detail, err := client.GetControlClusterDetail(ctx, living.WorkspaceControlClusterFilter{
		ProtoClusterID: primary.ProtoClusterID,
	})
	if err != nil {
		t.Fatalf("GetControlClusterDetail: %v", err)
	}
	if detail.TimeAuthority.WorkspaceID != scenario.workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected control cluster detail to preserve time authority, got %+v", detail.TimeAuthority)
	}
	if detail.Cluster.ProtoClusterID != primary.ProtoClusterID {
		t.Fatalf("expected cluster detail to stay scoped to proto cluster, got %+v", detail.Cluster)
	}
	if len(detail.Tensions) == 0 {
		t.Fatalf("expected cluster detail tensions, got %+v", detail)
	}
}

func TestDirectRhizomeClient_GetControlStateReport(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-control-state-client"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control State Client Workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	report, err := client.GetControlStateReport(ctx, living.WorkspaceControlStateFilter{})
	if err != nil {
		t.Fatalf("GetControlStateReport: %v", err)
	}
	if report.WorkspaceID != workspaceID {
		t.Fatalf("expected control state report to stay scoped to workspace, got %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected control state report to preserve time authority, got %+v", report.TimeAuthority)
	}
	if report.Workspace.TotalClusters != 0 {
		t.Fatalf("expected empty control state workspace metrics on fresh workspace, got %+v", report.Workspace)
	}
}

func TestDirectRhizomeClient_GetControlStateClusterDetail(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	scenario := seedBlockedControlClusterScenario(t, ctx, store, "state-client")

	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("RefreshTensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected tension refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("ListTensions: %v", err)
	}
	if len(items) == 0 {
		t.Fatalf("expected tensions after refresh")
	}
	primary := items[0]
	if _, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for control state cluster detail",
	}); err != nil {
		t.Fatalf("ConfirmTension: %v", err)
	}
	tick, err := store.TickClusterControlState(ctx, sqlite.ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.protoClusterID,
		ActorID:        "operator",
	})
	if err != nil {
		t.Fatalf("TickClusterControlState: %v", err)
	}
	if tick.UpdatedCount == 0 {
		t.Fatalf("expected control state tick to update at least one cluster, got %+v", tick)
	}

	client := living.NewDirectRhizomeClient(store, scenario.workspaceID)
	detail, err := client.GetControlStateClusterDetail(ctx, living.WorkspaceControlStateClusterFilter{
		ProtoClusterID: scenario.protoClusterID,
	})
	if err != nil {
		t.Fatalf("GetControlStateClusterDetail: %v", err)
	}
	if detail.TimeAuthority.WorkspaceID != scenario.workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected control state detail to preserve time authority, got %+v", detail.TimeAuthority)
	}
	if detail.State.ProtoClusterID != scenario.protoClusterID || detail.Cluster.ProtoClusterID != scenario.protoClusterID {
		t.Fatalf("expected control state detail to stay scoped to proto cluster, got %+v", detail)
	}
	if len(detail.Tensions) == 0 || len(detail.Events) == 0 {
		t.Fatalf("expected control state detail tensions and events, got %+v", detail)
	}
}

func TestDirectRhizomeClient_ListSessionCompactionCandidates(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-direct-compaction-candidates"
		agentID     = "agent-direct-compaction-candidates"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Direct Compaction Candidates Workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Direct Compaction Candidates Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-direct-compaction-1",
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      "task-direct-compaction-1",
		StartedAt:   "2026-03-30T00:00:00Z",
	}); err != nil {
		t.Fatalf("create candidate session: %v", err)
	}
	if err := store.AppendAgentSessionMessage(ctx, sqlite.AgentSessionMessageInput{
		SessionID:   "sess-direct-compaction-1",
		Sequence:    1,
		Role:        "assistant",
		ContentJSON: `{"type":"message","content":"candidate session"}`,
		TokenCount:  13000,
	}); err != nil {
		t.Fatalf("append candidate session message: %v", err)
	}
	if err := store.UpdateAgentSession(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:         "sess-direct-compaction-1",
		Status:            "RUNNING",
		TotalInputTokens:  7000,
		TotalOutputTokens: 6000,
	}); err != nil {
		t.Fatalf("update candidate session totals: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-direct-compaction-2",
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      "task-direct-compaction-2",
		StartedAt:   "2026-03-30T00:10:00Z",
	}); err != nil {
		t.Fatalf("create non-candidate session: %v", err)
	}
	if err := store.AppendAgentSessionMessage(ctx, sqlite.AgentSessionMessageInput{
		SessionID:   "sess-direct-compaction-2",
		Sequence:    1,
		Role:        "assistant",
		ContentJSON: `{"type":"message","content":"non candidate session"}`,
		TokenCount:  10,
	}); err != nil {
		t.Fatalf("append non-candidate session message: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	items, err := client.ListSessionCompactionCandidates(ctx, "", 12, 12000)
	if err != nil {
		t.Fatalf("ListSessionCompactionCandidates: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 compaction candidate, got %+v", items)
	}
	if items[0].SessionID != "sess-direct-compaction-1" || items[0].MessageTokens != 13000 || items[0].TotalTokens != 13000 {
		t.Fatalf("expected bounded direct compaction candidate, got %+v", items[0])
	}
}

func TestDirectRhizomeClient_ListSessionCompactionSnapshots(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-direct-compaction-snapshots"
		agentID     = "agent-direct-compaction-snapshots"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Direct Compaction Snapshots Workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Direct Compaction Snapshots Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-direct-compaction-snapshot-1",
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      "task-direct-compaction-snapshot-1",
		StartedAt:   "2026-03-30T00:00:00Z",
	}); err != nil {
		t.Fatalf("create snapshot session: %v", err)
	}
	snapshot, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
		SnapshotID:          "compaction-direct-snapshot-1",
		SessionID:           "sess-direct-compaction-snapshot-1",
		WorkspaceID:         workspaceID,
		AgentID:             agentID,
		TriggerKind:         "token_budget_exceeded",
		TokenBudget:         12000,
		MessageCountBefore:  18,
		MessageCountAfter:   4,
		MessageTokensBefore: 13000,
		MessageTokensAfter:  2000,
		TotalInputTokens:    7000,
		TotalOutputTokens:   6000,
		SummaryText:         "bounded direct compaction snapshot",
	})
	if err != nil {
		t.Fatalf("record compaction snapshot: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	items, err := client.ListSessionCompactionSnapshots(ctx, living.WorkspaceCompactionSnapshotFilter{})
	if err != nil {
		t.Fatalf("ListSessionCompactionSnapshots: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 compaction snapshot, got %+v", items)
	}
	if items[0].SnapshotID != snapshot.SnapshotID || items[0].CanonicalMemoryID == "" || items[0].TotalTokens != 13000 {
		t.Fatalf("expected bounded direct compaction snapshot, got %+v", items[0])
	}
}

func TestDirectRhizomeClient_ReplayWorkspaceEvents(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-direct-replay-events"
	agentID := "agent-direct-replay-events"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")
	claimAndRunLivingTestTask(t, ctx, store, workspaceID, taskID, agentID)

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:  model.SessionEventStart,
		SessionID:  "sess-direct-replay-1",
		AgentID:    agentID,
		TaskID:     taskID,
		Summary:    "bounded replay start",
		Status:     model.SessionStatusActive,
		OwnerScope: "task/session",
	}); err != nil {
		t.Fatalf("RecordSessionEvent start: %v", err)
	}
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType: model.SessionEventEnd,
		SessionID: "sess-direct-replay-1",
		AgentID:   agentID,
		TaskID:    taskID,
		Summary:   "bounded replay end",
		Status:    model.SessionStatusEnded,
	}); err != nil {
		t.Fatalf("RecordSessionEvent end: %v", err)
	}

	report, err := client.ReplayWorkspaceEvents(ctx, living.WorkspaceEventsReplayFilter{
		SessionID:     "sess-direct-replay-1",
		AgentID:       agentID,
		Limit:         20,
		IncludeEvents: true,
	})
	if err != nil {
		t.Fatalf("ReplayWorkspaceEvents: %v", err)
	}
	if report.WorkspaceID != workspaceID {
		t.Fatalf("expected replay report to stay scoped to workspace, got %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected replay report to preserve time authority, got %+v", report.TimeAuthority)
	}
	if report.Filter.SessionID != "sess-direct-replay-1" || report.Filter.AgentID != agentID {
		t.Fatalf("expected replay filter to stay scoped, got %+v", report.Filter)
	}
	if len(report.Sessions) != 1 || len(report.Events) == 0 || report.Metrics.TotalEvents == 0 {
		t.Fatalf("expected hydrated replay report, got %+v", report)
	}
	if strings.TrimSpace(report.Evaluation.Verdict) == "" {
		t.Fatalf("expected replay evaluation verdict, got %+v", report.Evaluation)
	}
}

func TestDirectRhizomeClient_ListWorkspaceEvents(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-direct-events-list"
	agentID := "agent-direct-events-list"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")
	claimAndRunLivingTestTask(t, ctx, store, workspaceID, taskID, agentID)

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:  model.SessionEventStart,
		SessionID:  "sess-direct-events-list-1",
		AgentID:    agentID,
		TaskID:     taskID,
		Summary:    "bounded events list start",
		Status:     model.SessionStatusActive,
		OwnerScope: "task/session",
	}); err != nil {
		t.Fatalf("RecordSessionEvent start: %v", err)
	}

	result, err := client.ListWorkspaceEvents(ctx, living.WorkspaceEventsListFilter{
		EventType: model.SessionEventStart,
		SessionID: "sess-direct-events-list-1",
		AgentID:   agentID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListWorkspaceEvents: %v", err)
	}
	if result.WorkspaceID != workspaceID {
		t.Fatalf("expected events list to stay scoped to workspace, got %+v", result)
	}
	if result.TimeAuthority.WorkspaceID != workspaceID || result.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected events list to preserve time authority, got %+v", result.TimeAuthority)
	}
	if result.Count != 1 || len(result.Items) != 1 {
		t.Fatalf("expected one bounded runtime event, got %+v", result)
	}
	if result.Items[0].SessionID != "sess-direct-events-list-1" || result.Items[0].EventType != model.SessionEventStart {
		t.Fatalf("expected canonical runtime event row, got %+v", result.Items[0])
	}
}

func TestDirectRhizomeClient_GetRSPCapabilityFlags(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-rsp-capability-client"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Capability Client Workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	flags, err := client.GetRSPCapabilityFlags(ctx, living.WorkspaceRSPCapabilityFilter{})
	if err != nil {
		t.Fatalf("GetRSPCapabilityFlags: %v", err)
	}
	if !flags.AnomalyShadow || !flags.StateShadow || flags.BeliefLive {
		t.Fatalf("expected default bounded capability flags, got %+v", flags)
	}
}

func TestDirectRhizomeClient_ClaimAndRelease(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-claim-release"
	agentID := "agent-1"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")

	client := living.NewDirectRhizomeClient(store, workspaceID)

	// Claim
	if err := client.ClaimTask(ctx, taskID, agentID); err != nil {
		t.Fatalf("ClaimTask: %v", err)
	}

	// After claiming, the task should no longer appear as PENDING in FetchTasks
	// (its claim_status is CLAIMED, but the task status itself is still PENDING
	// at the workspace level -- this depends on the store's behavior).

	// Release
	if err := client.ReleaseTask(ctx, taskID, agentID, "no longer needed"); err != nil {
		t.Fatalf("ReleaseTask: %v", err)
	}
}

func TestDirectRhizomeClient_CompleteTask(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-complete"
	agentID := "agent-1"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	// Must claim first
	if err := client.ClaimTask(ctx, taskID, agentID); err != nil {
		t.Fatalf("ClaimTask: %v", err)
	}

	if err := client.CompleteTask(ctx, taskID, "done successfully"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
}

func TestDirectRhizomeClient_FailTask(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-fail"
	agentID := "agent-1"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	if err := client.ClaimTask(ctx, taskID, agentID); err != nil {
		t.Fatalf("ClaimTask: %v", err)
	}

	if err := client.FailTask(ctx, taskID, "compilation error"); err != nil {
		t.Fatalf("FailTask: %v", err)
	}
}

func TestDirectRhizomeClient_SendAndFetchMessages(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-messages"
	agentID := "agent-1"
	seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")

	// Register a second agent for messaging
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-2",
		OwnerUserID: "test-user",
		DisplayName: "Agent Two",
		Role:        "worker",
		Status:      "REGISTERED",
	}); err != nil {
		t.Fatalf("register agent-2: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)

	before := time.Now().Add(-1 * time.Second)

	// Send
	if err := client.SendMessage(ctx, "agent-1", "agent-2", "hello from agent 1", "task-test-001"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Fetch
	msgs, err := client.FetchMessages(ctx, "agent-2", before)
	if err != nil {
		t.Fatalf("FetchMessages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Content != "hello from agent 1" {
		t.Fatalf("unexpected content: %s", msgs[0].Content)
	}
	if msgs[0].FromAgentID != "agent-1" {
		t.Fatalf("unexpected from: %s", msgs[0].FromAgentID)
	}
}

func TestDirectRhizomeClient_FetchMessagesSameTimestampCursorReproducer(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-messages-same-ts"
	agentID := "agent-1"
	seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-2",
		OwnerUserID: "test-user",
		DisplayName: "Agent Two",
		Role:        "worker",
		Status:      "REGISTERED",
	}); err != nil {
		t.Fatalf("register agent-2: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	if err := client.SendMessage(ctx, "agent-1", "agent-2", "first", ""); err != nil {
		t.Fatalf("SendMessage first: %v", err)
	}

	rows, err := store.ListWorkspaceMessages(ctx, workspaceID, "default", 10)
	if err != nil {
		t.Fatalf("ListWorkspaceMessages: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 stored message, got %d", len(rows))
	}
	firstID := rows[0].MessageID
	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id = ?`,
		sameTimestamp, firstID,
	); err != nil {
		t.Fatalf("force first created_at: %v", err)
	}

	initialMsgs, err := client.FetchMessages(ctx, "agent-2", mustTime(t, "2026-03-20T13:24:59.000000000Z"))
	if err != nil {
		t.Fatalf("FetchMessages initial: %v", err)
	}
	if len(initialMsgs) != 1 || initialMsgs[0].MessageID != firstID {
		t.Fatalf("expected first delivery to seed composite cursor, got %+v", initialMsgs)
	}

	if err := client.SendMessage(ctx, "agent-1", "agent-2", "second", ""); err != nil {
		t.Fatalf("SendMessage second: %v", err)
	}

	rows, err = store.ListWorkspaceMessages(ctx, workspaceID, "default", 10)
	if err != nil {
		t.Fatalf("ListWorkspaceMessages after second send: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("expected 2 stored messages after second send, got %d", len(rows))
	}
	secondID := rows[0].MessageID
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id = ?`,
		sameTimestamp, secondID,
	); err != nil {
		t.Fatalf("force second created_at: %v", err)
	}

	followerMsgs, err := client.FetchMessages(ctx, "agent-2", mustTime(t, sameTimestamp))
	if err != nil {
		t.Fatalf("FetchMessages follower: %v", err)
	}
	if len(followerMsgs) != 1 || followerMsgs[0].MessageID != secondID {
		t.Fatalf("expected composite-cursor follower only, got %+v", followerMsgs)
	}

	repeatMsgs, err := client.FetchMessages(ctx, "agent-2", mustTime(t, sameTimestamp))
	if err != nil {
		t.Fatalf("FetchMessages repeat: %v", err)
	}
	if len(repeatMsgs) != 0 {
		t.Fatalf("expected repeated same-timestamp poll to preserve composite cursor without redelivery, got %+v", repeatMsgs)
	}
}

func TestDirectRhizomeClient_Heartbeat(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-heartbeat"
	agentID := "agent-1"
	seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")

	client := living.NewDirectRhizomeClient(store, workspaceID)

	if err := client.Heartbeat(ctx, agentID, "REGISTERED", "all good"); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
}

func TestDirectRhizomeClient_SendUpdateAndGetTaskUpdates(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-updates"
	agentID := "agent-1"
	seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")

	client := living.NewDirectRhizomeClient(store, workspaceID)

	before := time.Now().Add(-1 * time.Second)

	if err := client.SendUpdate(ctx, agentID, workspaceID, "progress", "step 1 done", `{"step":1}`); err != nil {
		t.Fatalf("SendUpdate: %v", err)
	}

	updates, err := client.GetTaskUpdates(ctx, "task-test-001", before)
	if err != nil {
		t.Fatalf("GetTaskUpdates: %v", err)
	}
	if len(updates) < 1 {
		t.Fatalf("expected at least 1 update, got %d", len(updates))
	}
	found := false
	for _, u := range updates {
		if u.Summary == "step 1 done" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected to find update with summary 'step 1 done'")
	}
}

func mustTime(t *testing.T, ts string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatalf("parse time %q: %v", ts, err)
	}
	return parsed
}

func TestDirectRhizomeClient_EscalateTask(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-escalate"
	agentID := "agent-1"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	if err := client.EscalateTask(ctx, taskID, "need human approval"); err != nil {
		t.Fatalf("EscalateTask: %v", err)
	}

	// Verify the escalation update was recorded
	updates, err := client.GetTaskUpdates(ctx, taskID, time.Time{})
	if err != nil {
		t.Fatalf("GetTaskUpdates after escalation: %v", err)
	}
	found := false
	for _, u := range updates {
		if u.UpdateType == "escalation" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected to find escalation update")
	}
}

func TestDirectRhizomeClient_RecordSessionEvent(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-session-event"
	agentID := "agent-1"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")
	claimAndRunLivingTestTask(t, ctx, store, workspaceID, taskID, agentID)

	client := living.NewDirectRhizomeClient(store, workspaceID)

	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:  model.SessionEventStart,
		SessionID:  "sess-direct-1",
		AgentID:    agentID,
		TaskID:     taskID,
		Summary:    "Executing task-test-001",
		OwnerScope: "task/session",
	}); err != nil {
		t.Fatalf("RecordSessionEvent start: %v", err)
	}
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType: model.SessionEventEnd,
		SessionID: "sess-direct-1",
		AgentID:   agentID,
		TaskID:    taskID,
		Summary:   "Finished task-test-001",
	}); err != nil {
		t.Fatalf("RecordSessionEvent end: %v", err)
	}

	sessions, err := store.ListWorkspaceSessionStates(ctx, workspaceID, false, 10)
	if err != nil {
		t.Fatalf("ListWorkspaceSessionStates: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session state, got %+v", sessions)
	}
	if sessions[0].SessionID != "sess-direct-1" || sessions[0].Status != model.SessionStatusEnded {
		t.Fatalf("unexpected session state: %+v", sessions[0])
	}

	memoryItems, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		SessionID:   "sess-direct-1",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListWorkspaceMemory: %v", err)
	}
	if len(memoryItems) != 1 {
		t.Fatalf("expected 1 derived session memory record, got %+v", memoryItems)
	}
	if memoryItems[0].MemoryType != "UPDATE_DIGEST" || memoryItems[0].SourceKind != "session_event" {
		t.Fatalf("expected session-derived update digest, got %+v", memoryItems[0])
	}
	if !strings.Contains(memoryItems[0].Body, "Finished task-test-001") {
		t.Fatalf("expected final session summary in memory body, got %+v", memoryItems[0])
	}
}

func TestDirectRhizomeClient_RecordSessionEvent_ArchivesResolvedWaitStateMemory(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-session-memory-archive"
	agentID := "agent-1"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")
	claimAndRunLivingTestTask(t, ctx, store, workspaceID, taskID, agentID)

	client := living.NewDirectRhizomeClient(store, workspaceID)

	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType: model.SessionEventStart,
		SessionID: "sess-direct-2",
		AgentID:   agentID,
		TaskID:    taskID,
		Summary:   "Started work",
	}); err != nil {
		t.Fatalf("RecordSessionEvent start: %v", err)
	}

	keepFalse := false
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:         model.SessionEventBlocked,
		SessionID:         "sess-direct-2",
		AgentID:           agentID,
		TaskID:            taskID,
		Summary:           "Blocked waiting on bridge credential",
		KeepSessionActive: &keepFalse,
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "auth", Detail: "Need bridge credential"},
		},
	}); err != nil {
		t.Fatalf("RecordSessionEvent blocked: %v", err)
	}

	activeItems, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		SessionID:   "sess-direct-2",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListWorkspaceMemory after blocked: %v", err)
	}
	if len(activeItems) != 1 {
		t.Fatalf("expected 1 active wait-state memory after blocked event, got %+v", activeItems)
	}

	keepTrue := true
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:         model.SessionEventKeepalive,
		SessionID:         "sess-direct-2",
		AgentID:           agentID,
		TaskID:            taskID,
		Summary:           "Resumed after operator reply",
		KeepSessionActive: &keepTrue,
	}); err != nil {
		t.Fatalf("RecordSessionEvent keepalive: %v", err)
	}

	activeItems, err = store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		SessionID:   "sess-direct-2",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListWorkspaceMemory after keepalive: %v", err)
	}
	if len(activeItems) != 0 {
		t.Fatalf("expected wait-state memory to be archived after keepalive, got %+v", activeItems)
	}

	archivedItems, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID:     workspaceID,
		SessionID:       "sess-direct-2",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("ListWorkspaceMemory include archived: %v", err)
	}
	if len(archivedItems) != 1 || archivedItems[0].ArchivedAt == nil {
		t.Fatalf("expected archived wait-state memory record, got %+v", archivedItems)
	}
}

func TestDirectRhizomeClient_RecordSessionEvent_ProjectsOperatorQueueAndExecutionRun(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-session-projections"
	agentID := "agent-1"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")
	claimAndRunLivingTestTask(t, ctx, store, workspaceID, taskID, agentID)

	client := living.NewDirectRhizomeClient(store, workspaceID)

	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType: model.SessionEventStart,
		SessionID: "sess-direct-projection",
		AgentID:   agentID,
		TaskID:    taskID,
		Summary:   "Started projected work",
	}); err != nil {
		t.Fatalf("RecordSessionEvent start: %v", err)
	}

	keepFalse := false
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:         model.SessionEventBlocked,
		SessionID:         "sess-direct-projection",
		AgentID:           agentID,
		TaskID:            taskID,
		Summary:           "Blocked on operator approval",
		KeepSessionActive: &keepFalse,
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "approve deploy gate"},
		},
	}); err != nil {
		t.Fatalf("RecordSessionEvent blocked: %v", err)
	}

	queueItems, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		SessionID:   "sess-direct-projection",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListOperatorQueueItems: %v", err)
	}
	if len(queueItems) != 1 || queueItems[0].Status != "OPEN" {
		t.Fatalf("expected one open operator queue item, got %+v", queueItems)
	}

	detail, err := store.GetExecutionRun(ctx, workspaceID, "session:sess-direct-projection")
	if err != nil {
		t.Fatalf("GetExecutionRun: %v", err)
	}
	if detail.Run.Status != "BLOCKED" {
		t.Fatalf("expected blocked execution run, got %+v", detail.Run)
	}
	if len(detail.Steps) < 2 {
		t.Fatalf("expected execution steps projected from direct session events, got %+v", detail.Steps)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "session.blocked",
		EntityType:  "agent_session",
		EntityID:    "sess-direct-projection",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListRuntimeEvents: %v", err)
	}
	if len(events) != 1 || events[0].AgentID != agentID {
		t.Fatalf("expected blocked runtime event for direct path, got %+v", events)
	}
}

func TestDirectRhizomeClient_RecordSessionEvent_TracksHandoffPendingMemory(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx := context.Background()

	workspaceID := "ws-session-handoff"
	agentID := "agent-1"
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")
	claimAndRunLivingTestTask(t, ctx, store, workspaceID, taskID, agentID)

	client := living.NewDirectRhizomeClient(store, workspaceID)

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-2",
		OwnerUserID: "test-user",
		DisplayName: "Agent Two",
		Role:        "worker",
		Status:      "REGISTERED",
	}); err != nil {
		t.Fatalf("register agent-2: %v", err)
	}

	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType: model.SessionEventStart,
		SessionID: "sess-direct-3",
		AgentID:   agentID,
		TaskID:    taskID,
		Summary:   "Started handoff flow",
	}); err != nil {
		t.Fatalf("RecordSessionEvent start: %v", err)
	}

	keepTrue := true
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:         model.SessionEventStatus,
		SessionID:         "sess-direct-3",
		AgentID:           agentID,
		TaskID:            taskID,
		Summary:           "Passing transport triage to agent-2",
		Status:            model.SessionStatusHandoffPending,
		KeepSessionActive: &keepTrue,
		HandoffTo:         "agent-2",
	}); err != nil {
		t.Fatalf("RecordSessionEvent handoff pending: %v", err)
	}

	activeItems, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		SessionID:   "sess-direct-3",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListWorkspaceMemory after handoff pending: %v", err)
	}
	if len(activeItems) != 1 {
		t.Fatalf("expected 1 active handoff memory record, got %+v", activeItems)
	}
	if !strings.Contains(activeItems[0].Title, "Handoff pending") {
		t.Fatalf("expected handoff title, got %+v", activeItems[0])
	}
	if !strings.Contains(activeItems[0].Body, "Handoff to: agent-2") {
		t.Fatalf("expected handoff target in body, got %+v", activeItems[0])
	}

	keepFalse := false
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:         model.SessionEventStatus,
		SessionID:         "sess-direct-3",
		AgentID:           agentID,
		TaskID:            taskID,
		Summary:           "Handoff resolved, owner resumed directly",
		Status:            model.SessionStatusActive,
		KeepSessionActive: &keepFalse,
		HandoffTo:         "",
	}); err != nil {
		t.Fatalf("RecordSessionEvent clear handoff: %v", err)
	}

	activeItems, err = store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		SessionID:   "sess-direct-3",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListWorkspaceMemory after handoff cleared: %v", err)
	}
	if len(activeItems) != 0 {
		t.Fatalf("expected handoff memory to archive after clearing handoff, got %+v", activeItems)
	}

	archivedItems, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID:     workspaceID,
		SessionID:       "sess-direct-3",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("ListWorkspaceMemory include archived after handoff: %v", err)
	}
	if len(archivedItems) != 1 || archivedItems[0].ArchivedAt == nil {
		t.Fatalf("expected archived handoff memory record, got %+v", archivedItems)
	}
}

func TestDirectRhizomeClient_InterfaceCompliance(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	client := living.NewDirectRhizomeClient(store, "ws-interface")

	// Verify the client satisfies the interface
	var _ living.RhizomeClient = client
	var _ living.SessionAwareRhizomeClient = client
	var _ living.WorkspaceMemoryEffectsAwareRhizomeClient = client
}

func TestDirectRhizomeClient_RecordWorkspaceMemoryWithEffects(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-direct-memory-effects"
		agentID     = "agent-direct-memory-effects"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Direct Client Memory Effects",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Direct Memory Effects Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	result, err := client.RecordWorkspaceMemoryWithEffects(ctx, living.WorkspaceMemoryInput{
		MemoryType: "decision",
		Title:      "Direct client memory effects",
		Body:       "Promotable direct client writes should surface derived claim effects.",
		Summary:    "Direct client promoted claim.",
		AgentID:    agentID,
		SourceKind: "manual",
		SourceID:   "test-user",
	})
	if err != nil {
		t.Fatalf("RecordWorkspaceMemoryWithEffects: %v", err)
	}
	if result.Memory.MemoryID == "" {
		t.Fatalf("expected memory result, got %+v", result)
	}
	if result.PromotedClaimEffects == nil || result.PromotedClaimEffects.Claim == nil || result.PromotedClaimEffects.ClaimEvent == nil {
		t.Fatalf("expected promoted claim effects, got %+v", result)
	}
	if result.PromotedClaimEffects.Claim.ClaimID != "claim:memory:"+result.Memory.MemoryID {
		t.Fatalf("unexpected promoted claim %+v", result.PromotedClaimEffects.Claim)
	}
	if result.PromotedClaimEffects.ClaimEvent.EventType != "knowledge_claim.written" {
		t.Fatalf("unexpected promoted claim event %+v", result.PromotedClaimEffects.ClaimEvent)
	}
	if result.PromotedClaimEffects.ClaimEvent.EntityID != result.PromotedClaimEffects.Claim.ClaimID {
		t.Fatalf("expected claim event entity to match surfaced promoted claim, got %+v", result.PromotedClaimEffects)
	}
	if result.PromotedClaimEffects.Queue != nil || result.PromotedClaimEffects.QueueEvent != nil || len(result.PromotedClaimEffects.InvalidationEvents) != 0 {
		t.Fatalf("expected direct promotable write to surface only additive claim effects, got %+v", result.PromotedClaimEffects)
	}
}
