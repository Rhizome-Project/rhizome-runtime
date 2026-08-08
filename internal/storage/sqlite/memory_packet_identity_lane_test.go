package sqlite

import (
	"context"
	"testing"
)

func TestMemoryPacketIdentityLaneBudgetRemainsDisjointFromCoordinationFloor(t *testing.T) {
	t.Parallel()

	filter := normalizeMemoryPacketFilter(MemoryPacketFilter{
		WorkspaceID: "ws-identity-lane-budget",
		TaskID:      "task-identity-lane-budget",
		Budget: &MemoryPacketBudget{
			CoordinationFloor: 3,
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic: {ItemLimit: 1},
				MemoryRetrievalLaneIdentity: {ItemLimit: 1},
				MemoryRetrievalLaneBridge:   {ItemLimit: 1},
			},
		},
	})

	if got := memoryPacketLaneLimit(filter.Budget, MemoryRetrievalLaneIdentity); got != 1 {
		t.Fatalf("memoryPacketLaneLimit(identity) = %d, want explicit identity budget 1", got)
	}
	if got := memoryPacketLaneLimit(filter.Budget, MemoryRetrievalLaneSemantic); got != 1 {
		t.Fatalf("memoryPacketLaneLimit(semantic) = %d, want explicit semantic budget 1", got)
	}
	if got := memoryPacketLaneLimit(filter.Budget, MemoryRetrievalLaneBridge); got != 3 {
		t.Fatalf("memoryPacketLaneLimit(bridge) = %d, want coordination floor 3", got)
	}
}

func TestBuildMemoryShellPacketSurfacesIdentityMemoriesInDedicatedLaneWhenAvailable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-shell-identity-lane")

	selfModel, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Self model",
		Body:        "Identity lane should surface self-model memories additively.",
		Summary:     "Identity self model.",
		Importance:  0.92,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record self-model workspace memory: %v", err)
	}
	goalCommitment, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-goal",
		MemoryType:  "GOAL_COMMITMENT",
		Title:       "Goal commitment",
		Body:        "Identity lane should stay bounded under an explicit identity budget.",
		Summary:     "Identity goal commitment.",
		Importance:  0.75,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record goal-commitment workspace memory: %v", err)
	}
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-identity-dissent",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Dissent marker",
		Body:        "Identity claims should not steal the shell differential lane.",
		Summary:     "Contrastive control.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}

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
	for _, claim := range packet.DifferentialClaims {
		if claim.SourceID == selfModel.MemoryID || claim.SourceID == goalCommitment.MemoryID {
			t.Fatalf("expected identity memories to stay out of differential lane, got %+v", packet.DifferentialClaims)
		}
	}
	if !hasKnowledgeClaim(packet.DifferentialClaims, "claim-shell-identity-dissent") {
		t.Fatalf("expected contrastive control claim to remain in differential lane, got %+v", packet.DifferentialClaims)
	}
}

func TestBuildMemoryShellPacketFallsBackToWorkspaceAgentIdentityMemoriesWhenTaskScopeIsEmpty(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-shell-identity-fallback")

	fallbackSelf, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-fallback-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Agent fallback self model",
		Body:        "Identity lane should fall back to workspace+agent scoped self-model memories.",
		Summary:     "Fallback self model.",
		Importance:  0.94,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record workspace-scoped self-model memory: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-fallback-other-agent",
		MemoryType:  "GOAL_COMMITMENT",
		Title:       "Other agent identity",
		Body:        "Identity fallback should stay agent-scoped and not leak another agent's memories.",
		Summary:     "Other agent goal commitment.",
		Importance:  0.97,
		SourceKind:  "manual",
		SourceID:    "agent-b",
		AgentID:     "agent-b",
	}); err != nil {
		t.Fatalf("record other-agent identity memory: %v", err)
	}
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-identity-fallback-dissent",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Fallback dissent",
		Body:        "Identity fallback should not disturb the shell differential lane.",
		Summary:     "Fallback contrastive control.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build fallback memory shell packet: %v", err)
	}

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
	if !hasKnowledgeClaim(packet.DifferentialClaims, "claim-shell-identity-fallback-dissent") {
		t.Fatalf("expected fallback contrastive control claim to remain in differential lane, got %+v", packet.DifferentialClaims)
	}
}

func TestBuildMemoryShellPacketFallsBackToWorkspaceScopedIdentityMemories(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-shell-identity-fallback")

	globalIdentity, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		MemoryID:    "mem-shell-identity-global",
		WorkspaceID: scenario.workspaceID,
		MemoryType:  "SELF_MODEL",
		Title:       "Global self model",
		Body:        "Identity lane should fall back to agent-scoped workspace identity memories.",
		Summary:     "Global identity fallback.",
		Importance:  0.88,
		AgentID:     "agent-a",
		SourceKind:  "manual",
		SourceID:    "agent-a",
	})
	if err != nil {
		t.Fatalf("record global identity workspace memory: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		MemoryID:    "mem-shell-identity-other-agent",
		WorkspaceID: scenario.workspaceID,
		MemoryType:  "SELF_MODEL",
		Title:       "Other agent self model",
		Body:        "Identity fallback must stay scoped to the requesting agent.",
		Summary:     "Other agent identity.",
		Importance:  0.99,
		AgentID:     "agent-b",
		SourceKind:  "manual",
		SourceID:    "agent-b",
	}); err != nil {
		t.Fatalf("record other-agent identity workspace memory: %v", err)
	}

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}

	if len(packet.IdentityMemories) != 1 {
		t.Fatalf("expected bounded identity fallback lane of 1 memory, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	if packet.IdentityMemories[0].MemoryID != globalIdentity.MemoryID || packet.IdentityMemories[0].AgentID != "agent-a" {
		t.Fatalf("expected identity fallback to stay on the requesting agent's workspace memory, got %+v", packet.IdentityMemories)
	}
}

func TestBuildMemoryShellPacketDedupesIdentityFallbackBySemanticLineage(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-shell-identity-lineage-dedupe")

	workspaceSelf, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-lineage-workspace-self",
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
	goalCommitment, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-lineage-goal",
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
	sessionSelf, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-lineage-session-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Session self model",
		Body:        "Session-scoped identity should not consume a second slot when the same semantic lineage already exists.",
		Summary:     "Session identity lineage.",
		Importance:  0.86,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record session-scoped identity memory: %v", err)
	}
	taskSelf, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-lineage-task-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Scoped self model",
		Body:        "Scoped identity should win inside the duplicated lineage bucket after the normal sort.",
		Summary:     "Scoped identity lineage.",
		Importance:  0.91,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
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
	`, lineageID, scenario.workspaceID, workspaceSelf.MemoryID, sessionSelf.MemoryID, taskSelf.MemoryID); err != nil {
		t.Fatalf("seed shared identity lineage: %v", err)
	}

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneIdentity:      {ItemLimit: 3},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}

	if len(packet.IdentityMemories) != 2 {
		t.Fatalf("expected lineage-aware identity dedupe to keep 2 distinct identity memories, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	if !hasWorkspaceMemory(packet.IdentityMemories, taskSelf.MemoryID) {
		t.Fatalf("expected scoped identity lineage winner %q, got %+v", taskSelf.MemoryID, packet.IdentityMemories)
	}
	if hasWorkspaceMemory(packet.IdentityMemories, workspaceSelf.MemoryID) {
		t.Fatalf("did not expect workspace fallback duplicate lineage %q, got %+v", workspaceSelf.MemoryID, packet.IdentityMemories)
	}
	if hasWorkspaceMemory(packet.IdentityMemories, sessionSelf.MemoryID) {
		t.Fatalf("did not expect session fallback duplicate lineage %q, got %+v", sessionSelf.MemoryID, packet.IdentityMemories)
	}
	if !hasWorkspaceMemory(packet.IdentityMemories, goalCommitment.MemoryID) {
		t.Fatalf("expected distinct identity lineage %q to remain visible, got %+v", goalCommitment.MemoryID, packet.IdentityMemories)
	}
}

func TestBuildMemoryShellPacketPrefersTaskScopedIdentityOverSessionScopedFreshness(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-shell-identity-task-session-precedence")

	taskSelf, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-task-preferred",
		MemoryType:  "SELF_MODEL",
		Title:       "Task self model",
		Body:        "Task-scoped identity should win before session-scoped identity even when the session memory is fresher.",
		Summary:     "Task identity precedence.",
		Importance:  0.71,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record task-scoped identity memory: %v", err)
	}
	sessionSelf, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-session-fresher",
		MemoryType:  "SELF_MODEL",
		Title:       "Session self model",
		Body:        "Session-scoped identity is fresher but should lose to task-scoped precedence.",
		Summary:     "Session identity precedence.",
		Importance:  0.99,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record session-scoped identity memory: %v", err)
	}

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}

	if len(packet.IdentityMemories) != 1 {
		t.Fatalf("expected bounded identity lane of 1 memory, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	if packet.IdentityMemories[0].MemoryID != taskSelf.MemoryID {
		t.Fatalf("expected task-scoped identity %q to win over fresher session identity %q, got %+v", taskSelf.MemoryID, sessionSelf.MemoryID, packet.IdentityMemories)
	}
}

func TestBuildMemoryShellPacketIdentityBasisRefsEncodeTypeAndScope(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-shell-identity-basis-roles")

	taskSelf, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-basis-task-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Task self model",
		Body:        "Task-scoped identity basis refs should retain the memory type and scope.",
		Summary:     "Task identity basis.",
		Importance:  0.91,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record task-scoped self-model memory: %v", err)
	}
	policyTrace, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-basis-policy",
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

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneIdentity:      {ItemLimit: 2},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}

	if !hasMemoryPacketBasisRef(packet.BasisRefs, "workspace_memory", taskSelf.MemoryID, "identity_self_model_task") {
		t.Fatalf("expected task self-model basis ref role, got %+v", packet.BasisRefs)
	}
	if !hasMemoryPacketBasisRef(packet.BasisRefs, "workspace_memory", policyTrace.MemoryID, "identity_policy_trace_workspace_agent") {
		t.Fatalf("expected workspace-agent policy-trace basis ref role, got %+v", packet.BasisRefs)
	}
}

func TestBuildMemoryShellPacketIdentityLaneIncludesPolicyTraceWhenPresent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-shell-identity-policy-trace")

	if _, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-policy-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Self model",
		Body:        "Identity lane should still support self-model memories.",
		Summary:     "Identity self model.",
		Importance:  0.60,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	}); err != nil {
		t.Fatalf("record self-model workspace memory: %v", err)
	}
	policyTrace, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-policy-trace",
		MemoryType:  "POLICY_TRACE",
		Title:       "Policy trace",
		Body:        "Identity lane should surface policy traces additively without broad retrieval refactor.",
		Summary:     "Policy trace identity lane.",
		Importance:  0.95,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record policy-trace workspace memory: %v", err)
	}
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-identity-policy-dissent",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Policy dissent marker",
		Body:        "Policy traces should not displace contrastive claims from the differential lane.",
		Summary:     "Policy contrastive control.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 1},
				MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}

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
	if !hasKnowledgeClaim(packet.DifferentialClaims, "claim-shell-identity-policy-dissent") {
		t.Fatalf("expected policy contrastive control claim to remain in differential lane, got %+v", packet.DifferentialClaims)
	}
}

func hasWorkspaceMemory(records []WorkspaceMemoryRecord, memoryID string) bool {
	for _, record := range records {
		if record.MemoryID == memoryID {
			return true
		}
	}
	return false
}

func hasMemoryPacketBasisRef(refs []MemoryPacketBasisRef, refKind, refID, role string) bool {
	for _, ref := range refs {
		if ref.RefKind == refKind && ref.RefID == refID && ref.Role == role {
			return true
		}
	}
	return false
}

func TestBuildMemoryShellPacketSurfacesPolicyTraceInIdentityLaneWhenAvailable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-shell-identity-policy-trace")

	policyTrace, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: scenario.workspaceID,
		MemoryID:    "mem-shell-identity-policy-trace",
		MemoryType:  "POLICY_TRACE",
		Title:       "Safety policy trace",
		Body:        "Identity lane should surface bounded policy traces additively.",
		Summary:     "Policy trace identity.",
		Importance:  0.86,
		SourceKind:  "manual",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("record policy-trace workspace memory: %v", err)
	}

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneIdentity:      {ItemLimit: 1},
				MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}

	if len(packet.IdentityMemories) != 1 {
		t.Fatalf("expected bounded identity lane of 1 memory, got %d (%+v)", len(packet.IdentityMemories), packet.IdentityMemories)
	}
	if packet.IdentityMemories[0].MemoryID != policyTrace.MemoryID || packet.IdentityMemories[0].MemoryType != "POLICY_TRACE" {
		t.Fatalf("expected policy trace to surface in identity lane, got %+v", packet.IdentityMemories)
	}
}
