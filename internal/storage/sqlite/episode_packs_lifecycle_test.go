package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestRecordAgentSessionCoordinationCreatesLifecycleEpisodePacks(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-episode-pack-lifecycle"
		agentID     = "agent-episode-pack-lifecycle"
		sessionID   = "sess-episode-pack-lifecycle"
		taskID      = "task-episode-pack-lifecycle"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Episode Pack Lifecycle",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, aID := range []string{agentID, "agent-specialist"} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     aID,
			OwnerUserID: "developer",
			DisplayName: "Episode Lifecycle Agent",
		}); err != nil {
			t.Fatalf("register agent: %v", err)
		}
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claim before lifecycle session",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	for _, msg := range []AgentSessionMessageInput{
		{SessionID: sessionID, Sequence: 0, Role: "user", ContentJSON: `{"text":"Need rollout approval."}`, TokenCount: 12},
		{SessionID: sessionID, Sequence: 1, Role: "assistant", ContentJSON: `{"text":"Preparing rollout path."}`, TokenCount: 16},
	} {
		if err := store.AppendAgentSessionMessage(ctx, msg); err != nil {
			t.Fatalf("append session message: %v", err)
		}
	}

	blocked, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:           model.SessionEventBlocked,
		WorkspaceID:         workspaceID,
		SessionID:           sessionID,
		AgentID:             agentID,
		TaskID:              taskID,
		Summary:             "Blocked pending production approval",
		BlockedOn:           []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve rollout"}},
		RelatedDocKeys:      []string{"deploy/checklist"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{{Ref: "artifact://runbook", Kind: "runbook"}},
	})
	if err != nil {
		t.Fatalf("record blocked session coordination: %v", err)
	}
	if blocked.Status != model.SessionStatusBlocked {
		t.Fatalf("expected blocked status, got %+v", blocked)
	}

	blockedPacks, err := store.ListEpisodePacks(ctx, EpisodePackFilter{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		PackType:    episodePackTypeSessionBlocked,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list blocked episode packs: %v", err)
	}
	if len(blockedPacks) != 1 {
		t.Fatalf("expected one blocked episode pack, got %+v", blockedPacks)
	}
	blockedPack := blockedPacks[0]
	if blockedPack.LifecycleEventID == "" || blockedPack.CompactionSnapshotID != "" {
		t.Fatalf("expected lifecycle-origin blocked pack, got %+v", blockedPack)
	}
	if blockedPack.MessageCountBefore != 2 || blockedPack.MessageCountAfter != 2 || blockedPack.SourceWindowDigest == "" {
		t.Fatalf("expected source window metrics on blocked pack, got %+v", blockedPack)
	}
	if len(blockedPack.BlockerLedger) != 1 || blockedPack.BlockerLedger[0] != "human_input:approve rollout" {
		t.Fatalf("unexpected blocked ledger %+v", blockedPack.BlockerLedger)
	}

	blockedDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, blockedPack.CanonicalMemoryID)
	if err != nil {
		t.Fatalf("get blocked pack graph node: %v", err)
	}
	assertMemoryGraphRefPresent(t, blockedDetail.Refs, "runtime_event", blockedPack.LifecycleEventID)
	assertMemoryGraphRefPresent(t, blockedDetail.Refs, "workspace_doc", "deploy/checklist")
	assertMemoryGraphRefPresent(t, blockedDetail.Refs, "artifact_ref", "artifact://runbook")

	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:          model.SessionEventDecisionNeeded,
		WorkspaceID:        workspaceID,
		SessionID:          sessionID,
		AgentID:            agentID,
		TaskID:             taskID,
		Summary:            "Need operator go/no-go",
		DecisionNeededFrom: "developer",
		DecisionType:       "rollout_approval",
	}); err != nil {
		t.Fatalf("record decision-needed session coordination: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Pending specialist handoff",
		Status:      model.SessionStatusHandoffPending,
		HandoffTo:   "agent-specialist",
	}); err != nil {
		t.Fatalf("record handoff session coordination: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Session finished after operator decision",
	}); err != nil {
		t.Fatalf("record end session coordination: %v", err)
	}

	allPacks, err := store.ListEpisodePacks(ctx, EpisodePackFilter{WorkspaceID: workspaceID, SessionID: sessionID, Limit: 10})
	if err != nil {
		t.Fatalf("list lifecycle episode packs: %v", err)
	}
	if len(allPacks) != 4 {
		t.Fatalf("expected 4 lifecycle episode packs, got %+v", allPacks)
	}
}

func TestTakeOverAgentSessionCreatesTakeoverEpisodePack(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-episode-pack-takeover"
		sourceAgent = "agent-source"
		targetAgent = "agent-target"
		sourceSID   = "sess-source"
		taskID      = "task-episode-pack-takeover"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Episode Pack Takeover",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{sourceAgent, targetAgent} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sourceSID,
		AgentID:     sourceAgent,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create source session: %v", err)
	}
	for _, msg := range []AgentSessionMessageInput{
		{SessionID: sourceSID, Sequence: 0, Role: "user", ContentJSON: `{"text":"Investigate production drift."}`, TokenCount: 11},
		{SessionID: sourceSID, Sequence: 1, Role: "assistant", ContentJSON: `{"text":"Found likely release mismatch."}`, TokenCount: 15},
	} {
		if err := store.AppendAgentSessionMessage(ctx, msg); err != nil {
			t.Fatalf("append source session message: %v", err)
		}
	}
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, sourceAgent)
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sourceSID,
		AgentID:     sourceAgent,
		TaskID:      taskID,
		Summary:     "Initial diagnostic session",
		HandoffTo:   targetAgent,
	}); err != nil {
		t.Fatalf("record source session start: %v", err)
	}

	takeover, err := store.TakeOverAgentSession(ctx, AgentSessionTakeoverInput{
		WorkspaceID:     workspaceID,
		SessionID:       sourceSID,
		TakeoverAgentID: targetAgent,
		Summary:         "Escalate to deployment specialist",
	})
	if err != nil {
		t.Fatalf("take over agent session: %v", err)
	}

	packs, err := store.ListEpisodePacks(ctx, EpisodePackFilter{
		WorkspaceID: workspaceID,
		PackType:    episodePackTypeSessionTakeover,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list takeover episode packs: %v", err)
	}
	if len(packs) != 1 {
		t.Fatalf("expected one takeover episode pack, got %+v", packs)
	}
	pack := packs[0]
	if pack.SessionID != sourceSID || pack.LineageSessionID != takeover.SuccessorState.SessionID {
		t.Fatalf("unexpected takeover lineage %+v", pack)
	}
	if pack.AgentID != targetAgent || pack.LifecycleEventID == "" || pack.SourceWindowDigest == "" {
		t.Fatalf("unexpected takeover pack payload %+v", pack)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, pack.CanonicalMemoryID)
	if err != nil {
		t.Fatalf("get takeover pack graph node: %v", err)
	}
	assertMemoryGraphRefPresent(t, detail.Refs, "runtime_event", pack.LifecycleEventID)
	assertMemoryGraphRefPresent(t, detail.Refs, "session", takeover.SuccessorState.SessionID)
}

func assertMemoryGraphRefPresent(t *testing.T, refs []MemoryGraphNodeRefRecord, kind, id string) {
	t.Helper()
	for _, ref := range refs {
		if ref.RefKind == kind && ref.RefID == id {
			return
		}
	}
	t.Fatalf("expected ref %s:%s in %+v", kind, id, refs)
}
