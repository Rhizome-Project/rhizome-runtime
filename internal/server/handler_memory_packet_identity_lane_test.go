package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryPacketShellRPCSurfacesIdentityMemoriesInDedicatedLaneWhenAvailable(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-identity-lane"
		taskID      = "task-memory-packet-identity-lane"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	selfModel, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Self model",
		Body:        "Identity memories should surface in an additive shell bucket.",
		Summary:     "RPC identity self model.",
		Importance:  0.92,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record self-model workspace memory: %v", err)
	}
	goalCommitment, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-goal",
		MemoryType:  "GOAL_COMMITMENT",
		Title:       "Goal commitment",
		Body:        "Identity lane should stay bounded under explicit identity budget.",
		Summary:     "RPC identity goal commitment.",
		Importance:  0.75,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record goal-commitment workspace memory: %v", err)
	}
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-identity-dissent",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Dissent marker",
		Body:        "Identity claims should not displace the shell differential lane.",
		Summary:     "RPC contrastive control.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Budget: &sqlite.MemoryPacketBudget{
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal shell params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPacketShell(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryShellPacket)

	if len(packet.IdentityMemories) != 1 {
		t.Fatalf("expected bounded identity lane of 1 memory, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	identity := packet.IdentityMemories[0]
	if identity.MemoryID != selfModel.MemoryID && identity.MemoryID != goalCommitment.MemoryID {
		t.Fatalf("expected identity_memories to contain one seeded identity memory, got %+v", packet.IdentityMemories)
	}
	if identity.MemoryType != "SELF_MODEL" && identity.MemoryType != "GOAL_COMMITMENT" {
		t.Fatalf("expected canonical identity memory type, got %+v", identity)
	}
	if !hasServerMemoryPacketClaim(packet.DifferentialClaims, "claim-rpc-identity-dissent") {
		t.Fatalf("expected contrastive control claim to remain in differential lane, got %+v", packet.DifferentialClaims)
	}
}

func TestWorkspaceMemoryPacketShellRPCFallsBackToWorkspaceAgentIdentityMemoriesWhenTaskScopeIsEmpty(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-identity-fallback"
		taskID      = "task-memory-packet-identity-fallback"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	fallbackSelf, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-fallback-self",
		MemoryType:  "SELF_MODEL",
		Title:       "RPC fallback self model",
		Body:        "Identity lane should fall back to workspace+agent scoped self-model memories.",
		Summary:     "RPC fallback self model.",
		Importance:  0.94,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record workspace-scoped self-model memory: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-fallback-other-agent",
		MemoryType:  "GOAL_COMMITMENT",
		Title:       "Other agent identity",
		Body:        "Identity fallback should stay agent-scoped on RPC surface.",
		Summary:     "Other agent goal commitment.",
		Importance:  0.97,
		SourceKind:  "manual",
		SourceID:    "agent-b",
		AgentID:     "agent-b",
	}); err != nil {
		t.Fatalf("record other-agent identity memory: %v", err)
	}
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-identity-fallback-dissent",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Fallback dissent",
		Body:        "Identity fallback should not displace the shell differential lane.",
		Summary:     "RPC fallback contrastive control.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Budget: &sqlite.MemoryPacketBudget{
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal shell params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPacketShell(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryShellPacket)

	if len(packet.IdentityMemories) != 1 {
		t.Fatalf("expected bounded fallback identity lane of 1 memory, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	identity := packet.IdentityMemories[0]
	if identity.MemoryID != fallbackSelf.MemoryID {
		t.Fatalf("expected workspace+agent fallback identity memory %q, got %+v", fallbackSelf.MemoryID, identity)
	}
	if identity.TaskID != "" || identity.SessionID != "" {
		t.Fatalf("expected workspace-scoped fallback identity memory, got %+v", identity)
	}
	if identity.AgentID != "agent-a" {
		t.Fatalf("expected fallback identity memory to remain agent-scoped, got %+v", identity)
	}
	if !hasServerMemoryPacketClaim(packet.DifferentialClaims, "claim-rpc-identity-fallback-dissent") {
		t.Fatalf("expected fallback contrastive control claim to remain in differential lane, got %+v", packet.DifferentialClaims)
	}
}

func TestWorkspaceMemoryPacketShellRPCFallsBackToWorkspaceScopedIdentityMemories(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-identity-fallback"
		taskID      = "task-memory-packet-identity-fallback"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	globalIdentity, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		MemoryID:    "mem-rpc-identity-global",
		WorkspaceID: workspaceID,
		MemoryType:  "SELF_MODEL",
		Title:       "Global self model",
		Body:        "RPC shell packet should fall back to workspace-scoped identity memory for the requesting agent.",
		Summary:     "RPC global identity fallback.",
		Importance:  0.88,
		AgentID:     "agent-a",
		SourceKind:  "manual",
		SourceID:    "agent-a",
	})
	if err != nil {
		t.Fatalf("record global identity workspace memory: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		MemoryID:    "mem-rpc-identity-other-agent",
		WorkspaceID: workspaceID,
		MemoryType:  "SELF_MODEL",
		Title:       "Other agent self model",
		Body:        "RPC identity fallback must not leak another agent's memory.",
		Summary:     "RPC other agent identity.",
		Importance:  0.99,
		AgentID:     "agent-b",
		SourceKind:  "manual",
		SourceID:    "agent-b",
	}); err != nil {
		t.Fatalf("record other-agent identity workspace memory: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Budget: &sqlite.MemoryPacketBudget{
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal shell params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPacketShell(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryShellPacket)

	if len(packet.IdentityMemories) != 1 {
		t.Fatalf("expected bounded identity fallback lane of 1 memory, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	if packet.IdentityMemories[0].MemoryID != globalIdentity.MemoryID || packet.IdentityMemories[0].AgentID != "agent-a" {
		t.Fatalf("expected identity fallback to stay on the requesting agent's workspace memory, got %+v", packet.IdentityMemories)
	}
}

func TestWorkspaceMemoryPacketShellRPCDedupesIdentityFallbackBySemanticLineage(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-identity-lineage-dedupe"
		taskID      = "task-memory-packet-identity-lineage-dedupe"
		sessionID   = "sess-memory-packet-identity-lineage-dedupe"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	workspaceSelf, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-lineage-workspace-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Workspace self model",
		Body:        "Workspace-scoped identity should not duplicate the same lineage when a fresher scoped variant exists.",
		Summary:     "Workspace identity lineage.",
		Importance:  0.7,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record workspace-scoped identity memory: %v", err)
	}
	goalCommitment, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-lineage-goal",
		MemoryType:  "GOAL_COMMITMENT",
		Title:       "Goal commitment",
		Body:        "Distinct identity lineage should keep its own slot.",
		Summary:     "Goal identity lineage.",
		Importance:  0.83,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record distinct goal identity memory: %v", err)
	}
	sessionSelf, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-lineage-session-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Session self model",
		Body:        "Session-scoped identity should not consume a second slot when the same semantic lineage already exists.",
		Summary:     "Session identity lineage.",
		Importance:  0.86,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		SessionID:   sessionID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record session-scoped identity memory: %v", err)
	}
	taskSelf, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-lineage-task-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Scoped self model",
		Body:        "Scoped identity should win inside the duplicated lineage bucket after the normal sort.",
		Summary:     "Scoped identity lineage.",
		Importance:  0.91,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record scoped identity memory: %v", err)
	}
	const lineageID = "identity_lineage:agent-a:self"
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE memory_nodes
		   SET semantic_lineage_id = ?
		 WHERE workspace_id = ? AND origin_kind = 'workspace_memory' AND origin_id IN (?, ?, ?)
	`, lineageID, workspaceID, workspaceSelf.MemoryID, sessionSelf.MemoryID, taskSelf.MemoryID); err != nil {
		t.Fatalf("seed shared identity lineage: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		Budget: &sqlite.MemoryPacketBudget{
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneIdentity:      {ItemLimit: 3},
				sqlite.MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal shell params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPacketShell(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryShellPacket)

	if len(packet.IdentityMemories) != 2 {
		t.Fatalf("expected lineage-aware identity dedupe to keep 2 distinct identity memories, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	if !hasServerWorkspaceMemory(packet.IdentityMemories, taskSelf.MemoryID) {
		t.Fatalf("expected scoped identity lineage winner %q, got %+v", taskSelf.MemoryID, packet.IdentityMemories)
	}
	if hasServerWorkspaceMemory(packet.IdentityMemories, workspaceSelf.MemoryID) {
		t.Fatalf("did not expect workspace fallback duplicate lineage %q, got %+v", workspaceSelf.MemoryID, packet.IdentityMemories)
	}
	if hasServerWorkspaceMemory(packet.IdentityMemories, sessionSelf.MemoryID) {
		t.Fatalf("did not expect session fallback duplicate lineage %q, got %+v", sessionSelf.MemoryID, packet.IdentityMemories)
	}
	if !hasServerWorkspaceMemory(packet.IdentityMemories, goalCommitment.MemoryID) {
		t.Fatalf("expected distinct identity lineage %q to remain visible, got %+v", goalCommitment.MemoryID, packet.IdentityMemories)
	}
}

func TestWorkspaceMemoryPacketShellRPCPrefersTaskScopedIdentityOverSessionScopedFreshness(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-identity-task-session-precedence"
		taskID      = "task-memory-packet-identity-task-session-precedence"
		sessionID   = "sess-memory-packet-identity-task-session-precedence"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	taskSelf, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-task-preferred",
		MemoryType:  "SELF_MODEL",
		Title:       "Task self model",
		Body:        "Task-scoped identity should win before session-scoped identity even when the session memory is fresher.",
		Summary:     "Task identity precedence.",
		Importance:  0.71,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record task-scoped identity memory: %v", err)
	}
	sessionSelf, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-session-fresher",
		MemoryType:  "SELF_MODEL",
		Title:       "Session self model",
		Body:        "Session-scoped identity is fresher but should lose to task-scoped precedence.",
		Summary:     "Session identity precedence.",
		Importance:  0.99,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		SessionID:   sessionID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record session-scoped identity memory: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		Budget: &sqlite.MemoryPacketBudget{
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal shell params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPacketShell(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryShellPacket)

	if len(packet.IdentityMemories) != 1 {
		t.Fatalf("expected bounded identity lane of 1 memory, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	if packet.IdentityMemories[0].MemoryID != taskSelf.MemoryID {
		t.Fatalf("expected task-scoped identity %q to win over fresher session identity %q, got %+v", taskSelf.MemoryID, sessionSelf.MemoryID, packet.IdentityMemories)
	}
}

func TestWorkspaceMemoryPacketShellRPCIdentityBasisRefsEncodeTypeAndScope(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-identity-basis-roles"
		taskID      = "task-memory-packet-identity-basis-roles"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	taskSelf, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-basis-task-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Task self model",
		Body:        "Task-scoped identity basis refs should retain the memory type and scope.",
		Summary:     "Task identity basis.",
		Importance:  0.91,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record task-scoped self-model memory: %v", err)
	}
	policyTrace, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-basis-policy",
		MemoryType:  "POLICY_TRACE",
		Title:       "Policy trace",
		Body:        "Workspace-agent policy traces should retain their own basis-ref role.",
		Summary:     "Policy trace basis.",
		Importance:  0.88,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record policy-trace memory: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Budget: &sqlite.MemoryPacketBudget{
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneIdentity:      {ItemLimit: 2},
				sqlite.MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal shell params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPacketShell(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryShellPacket)

	if !hasServerMemoryPacketBasisRef(packet.BasisRefs, "workspace_memory", taskSelf.MemoryID, "identity_self_model_task") {
		t.Fatalf("expected task self-model basis ref role, got %+v", packet.BasisRefs)
	}
	if !hasServerMemoryPacketBasisRef(packet.BasisRefs, "workspace_memory", policyTrace.MemoryID, "identity_policy_trace_workspace_agent") {
		t.Fatalf("expected workspace-agent policy-trace basis ref role, got %+v", packet.BasisRefs)
	}
}

func TestWorkspaceMemoryPacketShellRPCIdentityLaneIncludesPolicyTraceWhenPresent(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-identity-policy-trace"
		taskID      = "task-memory-packet-identity-policy-trace"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-policy-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Self model",
		Body:        "Identity lane should still support self-model memories.",
		Summary:     "RPC identity self model.",
		Importance:  0.60,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	}); err != nil {
		t.Fatalf("record self-model workspace memory: %v", err)
	}
	policyTrace, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-policy-trace",
		MemoryType:  "POLICY_TRACE",
		Title:       "Policy trace",
		Body:        "Identity lane should surface policy traces additively on RPC shell packets.",
		Summary:     "RPC policy trace identity lane.",
		Importance:  0.95,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record policy-trace workspace memory: %v", err)
	}
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-identity-policy-dissent",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Policy dissent marker",
		Body:        "Policy traces should not displace contrastive claims from the shell differential lane.",
		Summary:     "RPC policy contrastive control.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Budget: &sqlite.MemoryPacketBudget{
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal shell params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPacketShell(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryShellPacket)

	if len(packet.IdentityMemories) != 1 {
		t.Fatalf("expected bounded identity lane of 1 memory, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	identity := packet.IdentityMemories[0]
	if identity.MemoryID != policyTrace.MemoryID {
		t.Fatalf("expected higher-priority policy trace to occupy identity lane, got %+v", packet.IdentityMemories)
	}
	if identity.MemoryType != "POLICY_TRACE" {
		t.Fatalf("expected policy trace canonical identity memory type, got %+v", identity)
	}
	if !hasServerMemoryPacketClaim(packet.DifferentialClaims, "claim-rpc-identity-policy-dissent") {
		t.Fatalf("expected policy contrastive control claim to remain in differential lane, got %+v", packet.DifferentialClaims)
	}
}

func TestWorkspaceMemoryPacketShellRPCSurfacesPolicyTraceInIdentityLaneWhenAvailable(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-identity-policy-trace"
		taskID      = "task-memory-packet-identity-policy-trace"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	policyTrace, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-rpc-identity-policy-trace",
		MemoryType:  "POLICY_TRACE",
		Title:       "Safety policy trace",
		Body:        "RPC shell packet should surface policy traces in the identity lane.",
		Summary:     "RPC policy trace identity.",
		Importance:  0.86,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record policy-trace workspace memory: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Budget: &sqlite.MemoryPacketBudget{
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal shell params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPacketShell(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryShellPacket)

	if len(packet.IdentityMemories) != 1 {
		t.Fatalf("expected bounded identity lane of 1 memory, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	if packet.IdentityMemories[0].MemoryID != policyTrace.MemoryID || packet.IdentityMemories[0].MemoryType != "POLICY_TRACE" {
		t.Fatalf("expected policy trace to surface in identity lane, got %+v", packet.IdentityMemories)
	}
}

func hasServerWorkspaceMemory(records []sqlite.WorkspaceMemoryRecord, memoryID string) bool {
	for _, record := range records {
		if record.MemoryID == memoryID {
			return true
		}
	}
	return false
}

func hasServerMemoryPacketBasisRef(refs []sqlite.MemoryPacketBasisRef, refKind, refID, role string) bool {
	for _, ref := range refs {
		if ref.RefKind == refKind && ref.RefID == refID && ref.Role == role {
			return true
		}
	}
	return false
}
