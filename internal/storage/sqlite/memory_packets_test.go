package sqlite

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestBuildMemoryKernelPacketCollectsMandatoryCoordinationSet(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-kernel")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-kernel-constraint",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Deploy window",
		Body:        "Do not deploy outside the maintenance window.",
		Summary:     "Stay inside the maintenance window.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-kernel-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Rollout strategy",
		Body:        "Use guarded canary rollout.",
		Summary:     "Canary rollout accepted.",
		Confidence:  0.95,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-kernel-blocker",
		ClaimType:   "BLOCKER",
		Status:      "ACTIVE",
		Subject:     "Approval missing",
		Body:        "Need operator approval before rollout.",
		Summary:     "Operator approval still missing.",
		Confidence:  0.8,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "Pending explicit handoff to agent-b",
		Status:      model.SessionStatusHandoffPending,
		HandoffTo:   "agent-b",
	}); err != nil {
		t.Fatalf("record handoff coordination: %v", err)
	}
	if _, err := store.TakeOverAgentSession(ctx, AgentSessionTakeoverInput{
		WorkspaceID:        scenario.workspaceID,
		SessionID:          scenario.sessionID,
		SuccessorSessionID: scenario.sessionID + "-successor",
		TakeoverAgentID:    "agent-b",
		Summary:            "Take over blocked rollout",
		SuccessorSummary:   "Continue rollout handoff",
	}); err != nil {
		t.Fatalf("take over agent session: %v", err)
	}
	// CA-17: the takeover now resolves the SOURCE session's operator queue (its
	// session is ended), so the live open queue must come from the successor. The
	// successor owner is itself blocked on the same approval; record that and
	// project its BLOCKER so the kernel packet has a legitimately-open queue.
	successorBlocked, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID + "-successor",
		AgentID:     "agent-b",
		TaskID:      scenario.taskID,
		Summary:     "Still blocked on operator approval after handoff",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "operator approval before rollout"},
		},
	})
	if err != nil {
		t.Fatalf("record successor blocked coordination: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, successorBlocked); err != nil {
		t.Fatalf("sync successor operator queue: %v", err)
	}
	if _, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}

	packet, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		DocKeys:      []string{scenario.docKey},
		ArtifactRefs: []string{scenario.artifactRef},
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 20},
				MemoryRetrievalLaneBridge:        {ItemLimit: 10},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 10},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory kernel packet: %v", err)
	}
	if packet.Meta.PacketKind != memoryPacketKindKernel {
		t.Fatalf("expected kernel packet, got %+v", packet.Meta)
	}
	if packet.Meta.TaskID != scenario.taskID || packet.Meta.WorkspaceID != scenario.workspaceID {
		t.Fatalf("unexpected packet scope: %+v", packet.Meta)
	}
	if packet.Task.TaskID != scenario.taskID || packet.TaskCharter.Priority == "" || packet.TaskCharter.TaskKind == "" {
		t.Fatalf("expected task charter in packet, got %+v / %+v", packet.Task, packet.TaskCharter)
	}
	if len(packet.Docs) != 1 || packet.Docs[0].DocKey != scenario.docKey {
		t.Fatalf("expected scoped doc in kernel packet, got %+v", packet.Docs)
	}
	if len(packet.Artifacts) == 0 || packet.Artifacts[0].ArtifactRef != scenario.artifactRef {
		t.Fatalf("expected task artifacts in kernel packet, got %+v", packet.Artifacts)
	}
	if !hasKnowledgeClaim(packet.Coordination.HardConstraints, "claim-kernel-constraint") {
		t.Fatalf("expected hard constraint in kernel packet, got %+v", packet.Coordination.HardConstraints)
	}
	if !hasKnowledgeClaim(packet.Coordination.AcceptedDecisions, "claim-kernel-decision") {
		t.Fatalf("expected decision in kernel packet, got %+v", packet.Coordination.AcceptedDecisions)
	}
	if !hasKnowledgeClaim(packet.Coordination.ActiveBlockers, "claim-kernel-blocker") {
		t.Fatalf("expected blocker in kernel packet, got %+v", packet.Coordination.ActiveBlockers)
	}
	if packet.BoundarySummary == nil {
		t.Fatalf("expected kernel boundary summary, got %+v", packet)
	}
	if packet.BoundarySummary.HardConstraintCount != len(packet.Coordination.HardConstraints) ||
		packet.BoundarySummary.AcceptedDecisionCount != len(packet.Coordination.AcceptedDecisions) ||
		packet.BoundarySummary.DecisionRecordCount != len(packet.Coordination.DecisionRecords) ||
		packet.BoundarySummary.ActiveBlockerCount != len(packet.Coordination.ActiveBlockers) ||
		packet.BoundarySummary.BlockerHypothesisCount != len(packet.Coordination.BlockerHypotheses) {
		t.Fatalf("expected kernel boundary summary to match coordination packet content, got %+v vs %+v", packet.BoundarySummary, packet.Coordination)
	}
	if len(packet.Coordination.OpenQueues) == 0 {
		t.Fatalf("expected open queue projection in kernel packet, got %+v", packet.Coordination)
	}
	if packet.Coordination.LastVerifiedHandoff == nil || packet.Coordination.LastVerifiedHandoff.PackID == "" {
		t.Fatalf("expected verified handoff in kernel packet, got %+v", packet.Coordination.LastVerifiedHandoff)
	}
	if !packet.Cluster.Resolved || packet.Cluster.ProtoClusterID == "" {
		t.Fatalf("expected resolved cluster in kernel packet, got %+v", packet.Cluster)
	}
	if len(packet.BasisRefs) == 0 || packet.Meta.BasisDigest == "" {
		t.Fatalf("expected basis refs and digest, got %+v", packet)
	}
	if packet.BasisSummary == nil {
		t.Fatalf("expected kernel basis summary, got %+v", packet)
	}
	if packet.BasisSummary.TotalRefCount != len(packet.BasisRefs) ||
		packet.BasisSummary.RuntimeEventRefCount != countMemoryPacketBasisRefsByKind(packet.BasisRefs, "runtime_event") ||
		packet.BasisSummary.EpisodePackRefCount != countMemoryPacketBasisRefsByKind(packet.BasisRefs, "episode_pack") ||
		packet.BasisSummary.KnowledgeClaimRefCount != countMemoryPacketBasisRefsByKind(packet.BasisRefs, "knowledge_claim") ||
		packet.BasisSummary.WorkspaceMemoryRefCount != countMemoryPacketBasisRefsByKind(packet.BasisRefs, "workspace_memory") ||
		packet.BasisSummary.CoordinationBasisCount != countMemoryPacketBasisRefsByRole(packet.BasisRefs, "hard_constraint", "accepted_decision", "active_blocker", "blocker_hypothesis") ||
		packet.BasisSummary.RecentTraceBasisCount != countMemoryPacketBasisRefsByRole(packet.BasisRefs, "recent_episode_pack", "recent_runtime_event") {
		t.Fatalf("expected kernel basis summary to match basis refs, got %+v vs %+v", packet.BasisSummary, packet.BasisRefs)
	}

	rebuilt, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		DocKeys:      []string{scenario.docKey},
		ArtifactRefs: []string{scenario.artifactRef},
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 20},
				MemoryRetrievalLaneBridge:        {ItemLimit: 10},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 10},
			},
		},
	})
	if err != nil {
		t.Fatalf("rebuild memory kernel packet: %v", err)
	}
	if rebuilt.Meta.BasisDigest != packet.Meta.BasisDigest {
		t.Fatalf("expected stable kernel basis digest, first=%s second=%s", packet.Meta.BasisDigest, rebuilt.Meta.BasisDigest)
	}
}

func TestBuildMemoryShellPacketScopesDifferentialContextToAgent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-shell")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-dissent",
		ClaimType:   "DISSENT",
		Status:      "ACTIVE",
		Subject:     "Rollback first",
		Body:        "One agent prefers rollback-first mitigation.",
		Summary:     "Rollback-first dissent.",
		Confidence:  0.7,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-dissent-marker",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Rollback disagreement exists",
		Body:        "The shell packet should surface the dissent marker as a first-class contrastive item.",
		Summary:     "Rollback dissent marker.",
		Confidence:  0.72,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-dissent-content",
		ClaimType:   "DISSENT_CONTENT",
		Status:      "ACTIVE",
		Subject:     "Rollback counter-argument",
		Body:        "The shell packet should surface the dissent content alongside its marker.",
		Summary:     "Rollback dissent content.",
		Confidence:  0.69,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-procedure",
		ClaimType:   "PROCEDURE",
		Status:      "ACTIVE",
		Subject:     "Rollback checklist",
		Body:        "If rollout is blocked, check rollback checklist first.",
		Summary:     "Rollback checklist.",
		Confidence:  0.75,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-anti-procedure",
		ClaimType:   "ANTI_PROCEDURE",
		Status:      "ACTIVE",
		Subject:     "Do not shortcut rollback",
		Body:        "Anti-procedural guidance should share the dedicated procedural bucket instead of disappearing from shell recall.",
		Summary:     "Avoid rollback shortcut.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-branch-active",
		ClaimType:   "ALTERNATIVE_BRANCH",
		Status:      "ACTIVE",
		Subject:     "Delayed rollback branch",
		Body:        "Keep a delayed rollback branch available for contrastive recall.",
		Summary:     "Delayed rollback alternative branch.",
		Confidence:  0.76,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-dissent-archived",
		ClaimType:   "DISSENT",
		Status:      "ACTIVE",
		Subject:     "Rollback dissent archive",
		Body:        "Preserve an older dissent path for recovery-sensitive contrastive recall.",
		Summary:     "Archived dissent path.",
		Confidence:  0.65,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-dissent-content-archived",
		ClaimType:   "DISSENT_CONTENT",
		Status:      "ACTIVE",
		Subject:     "Rollback critique archive",
		Body:        "Preserve archived dissent content for bounded recovery-sensitive contrastive recall.",
		Summary:     "Archived dissent critique.",
		Confidence:  0.64,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if _, err := store.ArchiveKnowledgeClaim(ctx, KnowledgeClaimArchiveInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-dissent-archived",
		ArchivedBy:  "rmp_pruner",
		Reason:      rmpArchivedReasonExpired,
	}); err != nil {
		t.Fatalf("archive recoverable dissent claim: %v", err)
	}
	if _, err := store.ArchiveKnowledgeClaim(ctx, KnowledgeClaimArchiveInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-dissent-content-archived",
		ArchivedBy:  "rmp_pruner",
		Reason:      rmpArchivedReasonExpired,
	}); err != nil {
		t.Fatalf("archive recoverable dissent content claim: %v", err)
	}
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-branch-archived",
		ClaimType:   "ALTERNATIVE_BRANCH",
		Status:      "ACTIVE",
		Subject:     "Warm-standby branch",
		Body:        "Preserve a warm-standby branch for recovery lane recall.",
		Summary:     "Archived alternative branch.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if _, err := store.ArchiveKnowledgeClaim(ctx, KnowledgeClaimArchiveInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-branch-archived",
		ArchivedBy:  "rmp_pruner",
		Reason:      rmpArchivedReasonExpired,
	}); err != nil {
		t.Fatalf("archive recoverable alternative branch claim: %v", err)
	}
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-constraint",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Noise gate",
		Body:        "Constraint should stay in kernel only.",
		Summary:     "Kernel-only constraint.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	if _, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		SessionID:    scenario.sessionID,
		AgentID:      "agent-a",
		DocKeys:      []string{scenario.docKey},
		ArtifactRefs: []string{scenario.artifactRef},
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 20},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 9},
				MemoryRetrievalLaneBridge:        {ItemLimit: 10},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 10},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}
	if packet.Meta.PacketKind != memoryPacketKindShell {
		t.Fatalf("expected shell packet, got %+v", packet.Meta)
	}
	if packet.Meta.AgentID != "agent-a" || packet.Meta.TaskID != scenario.taskID || packet.Meta.SessionID != scenario.sessionID {
		t.Fatalf("unexpected shell scope: %+v", packet.Meta)
	}
	if packet.KernelRef.PacketKey == "" || packet.KernelRef.BasisDigest == "" {
		t.Fatalf("expected kernel ref in shell packet, got %+v", packet.KernelRef)
	}
	if packet.Session == nil || packet.Session.SessionID != scenario.sessionID {
		t.Fatalf("expected session in shell packet, got %+v", packet.Session)
	}
	if packet.SessionState == nil || packet.SessionState.SessionID != scenario.sessionID {
		t.Fatalf("expected session state in shell packet, got %+v", packet.SessionState)
	}
	if packet.FocusSummary == "" {
		t.Fatalf("expected shell focus summary, got %+v", packet)
	}
	if len(packet.RecentUpdates) == 0 || len(packet.RecentEpisodePacks) == 0 || len(packet.RecentRuntimeEvents) == 0 {
		t.Fatalf("expected recent delta surfaces in shell packet, got %+v", packet)
	}
	if packet.BoundarySummary == nil {
		t.Fatalf("expected shell boundary summary, got %+v", packet)
	}
	if !hasKnowledgeClaim(packet.DifferentialClaims, "claim-shell-dissent") ||
		!hasKnowledgeClaim(packet.DifferentialClaims, "claim-shell-dissent-marker") ||
		!hasKnowledgeClaim(packet.DifferentialClaims, "claim-shell-dissent-content") ||
		!hasKnowledgeClaim(packet.DifferentialClaims, "claim-shell-branch-active") {
		t.Fatalf("expected differential claims in shell packet, got %+v", packet.DifferentialClaims)
	}
	if !hasKnowledgeClaim(packet.ProceduralClaims, "claim-shell-procedure") {
		t.Fatalf("expected procedure in dedicated procedural lane, got %+v", packet.ProceduralClaims)
	}
	if !hasKnowledgeClaim(packet.ProceduralClaims, "claim-shell-anti-procedure") {
		t.Fatalf("expected anti procedure in dedicated procedural lane, got %+v", packet.ProceduralClaims)
	}
	if hasKnowledgeClaim(packet.DifferentialClaims, "claim-shell-procedure") {
		t.Fatalf("did not expect procedure to remain in differential claims, got %+v", packet.DifferentialClaims)
	}
	if hasKnowledgeClaim(packet.DifferentialClaims, "claim-shell-anti-procedure") {
		t.Fatalf("did not expect anti procedure to remain in differential claims, got %+v", packet.DifferentialClaims)
	}
	recoverableArchivedCount := 0
	for _, claim := range packet.DifferentialClaims {
		if isMemoryPacketRecoverableArchivedContrastiveClaim(claim) {
			recoverableArchivedCount++
		}
	}
	if recoverableArchivedCount == 0 {
		t.Fatalf("expected bounded recoverable archived contrastive tail in shell packet, got %+v", packet.DifferentialClaims)
	}
	if packet.BoundarySummary.DissentClaimCount < 3 {
		t.Fatalf("expected shell boundary summary to count dissent claims, got %+v", packet.BoundarySummary)
	}
	if packet.BoundarySummary.ArchivedDissentClaimCount < 1 {
		t.Fatalf("expected shell boundary summary to count archived dissent claims, got %+v", packet.BoundarySummary)
	}
	if packet.BoundarySummary.AlternativeBranchCount < 1 || packet.BoundarySummary.ArchivedAlternativeBranchCount < 1 {
		t.Fatalf("expected shell boundary summary to count active and archived alternative branches, got %+v", packet.BoundarySummary)
	}
	if packet.BoundarySummary.ProceduralClaimCount != len(packet.ProceduralClaims) {
		t.Fatalf("expected shell boundary summary to match procedural lane, got %+v vs %+v", packet.BoundarySummary, packet.ProceduralClaims)
	}
	if packet.BoundarySummary.TraceContextCount != len(packet.RecentEpisodePacks)+len(packet.RecentRuntimeEvents) {
		t.Fatalf("expected shell boundary summary to match trace context surfaces, got %+v", packet.BoundarySummary)
	}
	if hasKnowledgeClaim(packet.DifferentialClaims, "claim-shell-constraint") {
		t.Fatalf("did not expect kernel constraint in shell differential claims, got %+v", packet.DifferentialClaims)
	}
	if len(packet.BasisRefs) == 0 || packet.Meta.BasisDigest == "" {
		t.Fatalf("expected shell basis refs and digest, got %+v", packet)
	}
	if packet.BasisSummary == nil {
		t.Fatalf("expected shell basis summary, got %+v", packet)
	}
	if packet.BasisSummary.TotalRefCount != len(packet.BasisRefs) ||
		packet.BasisSummary.RuntimeEventRefCount != countMemoryPacketBasisRefsByKind(packet.BasisRefs, "runtime_event") ||
		packet.BasisSummary.EpisodePackRefCount != countMemoryPacketBasisRefsByKind(packet.BasisRefs, "episode_pack") ||
		packet.BasisSummary.KnowledgeClaimRefCount != countMemoryPacketBasisRefsByKind(packet.BasisRefs, "knowledge_claim") ||
		packet.BasisSummary.WorkspaceMemoryRefCount != countMemoryPacketBasisRefsByKind(packet.BasisRefs, "workspace_memory") ||
		packet.BasisSummary.DifferentialBasisCount != countMemoryPacketBasisRefsByRole(packet.BasisRefs, "differential_claim") ||
		packet.BasisSummary.ProceduralBasisCount != countMemoryPacketBasisRefsByRole(packet.BasisRefs, "procedural_claim") ||
		packet.BasisSummary.IdentityBasisCount != countMemoryPacketBasisRefsByRole(packet.BasisRefs, "identity_memory_task", "identity_memory_session", "identity_memory_workspace") ||
		packet.BasisSummary.RecentTraceBasisCount != countMemoryPacketBasisRefsByRole(packet.BasisRefs, "recent_episode_pack", "recent_runtime_event") {
		t.Fatalf("expected shell basis summary to match basis refs, got %+v vs %+v", packet.BasisSummary, packet.BasisRefs)
	}
}

func TestBuildMemoryPacketsDoNotMutatePersistentState(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-readonly")

	before := map[string]int{
		"runtime_events":            countMemoryPacketRows(t, ctx, store, "runtime_events"),
		"workspace_memory":          countMemoryPacketRows(t, ctx, store, "workspace_memory"),
		"knowledge_claims":          countMemoryPacketRows(t, ctx, store, "knowledge_claims"),
		"episode_packs":             countMemoryPacketRows(t, ctx, store, "episode_packs"),
		"memory_residency_reports":  countMemoryPacketRows(t, ctx, store, "memory_residency_reports"),
		"memory_invalidation_queue": countMemoryPacketRows(t, ctx, store, "memory_invalidation_queue"),
	}

	if _, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		SessionID:    scenario.sessionID,
		AgentID:      "agent-a",
		DocKeys:      []string{scenario.docKey},
		ArtifactRefs: []string{scenario.artifactRef},
	}); err != nil {
		t.Fatalf("build memory kernel packet: %v", err)
	}
	if _, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		SessionID:    scenario.sessionID,
		AgentID:      "agent-a",
		DocKeys:      []string{scenario.docKey},
		ArtifactRefs: []string{scenario.artifactRef},
	}); err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}

	for table, beforeCount := range before {
		if afterCount := countMemoryPacketRows(t, ctx, store, table); afterCount != beforeCount {
			t.Fatalf("expected read-only packet builders to leave %s unchanged, before=%d after=%d", table, beforeCount, afterCount)
		}
	}
}

func TestBuildMemoryShellPacketHonorsBoundedDissentQuota(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-dissent-quota")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-quota-dissent-marker",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Rollback disagreement exists",
		Body:        "Keep the dissent marker even when the contrastive lane is tight.",
		Summary:     "Dissent marker.",
		Confidence:  0.72,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-quota-dissent-content",
		ClaimType:   "DISSENT_CONTENT",
		Status:      "ACTIVE",
		Subject:     "Rollback counter-argument",
		Body:        "Keep the dissent content even when the contrastive lane is tight.",
		Summary:     "Dissent content.",
		Confidence:  0.69,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-quota-branch",
		ClaimType:   "ALTERNATIVE_BRANCH",
		Status:      "ACTIVE",
		Subject:     "Delayed rollback branch",
		Body:        "Alternative branch competes for the same contrastive lane.",
		Summary:     "Rollback branch.",
		Confidence:  0.7,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-quota-procedure",
		ClaimType:   "PROCEDURE",
		Status:      "ACTIVE",
		Subject:     "Rollback checklist",
		Body:        "Procedure competes for the same contrastive lane.",
		Summary:     "Rollback checklist.",
		Confidence:  0.71,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-quota-fact-noise-a",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Noise fact A",
		Body:        "Semantic saturation should not crowd out active dissent coverage.",
		Summary:     "Noise fact A.",
		Confidence:  0.95,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-quota-fact-noise-b",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Noise fact B",
		Body:        "A second semantic fact keeps the mixed active lane saturated.",
		Summary:     "Noise fact B.",
		Confidence:  0.94,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			CoordinationFloor: 1,
			DissentQuota:      2,
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 2},
				MemoryRetrievalLaneBridge:        {ItemLimit: 1},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}
	if len(packet.DifferentialClaims) != 2 {
		t.Fatalf("expected exactly two differential claims under tight contrastive budget, got %+v", packet.DifferentialClaims)
	}
	if !hasKnowledgeClaim(packet.DifferentialClaims, "claim-quota-dissent-marker") || !hasKnowledgeClaim(packet.DifferentialClaims, "claim-quota-dissent-content") {
		t.Fatalf("expected bounded dissent quota to keep marker and content, got %+v", packet.DifferentialClaims)
	}
	if !hasKnowledgeClaim(packet.ProceduralClaims, "claim-quota-procedure") {
		t.Fatalf("expected procedure to survive in procedural lane despite tight contrastive budget, got %+v", packet.ProceduralClaims)
	}
}

func TestBuildMemoryShellPacketAppliesCoordinationFloorToBridgeLane(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-coordination-floor")

	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "Pending specialist handoff",
		Status:      model.SessionStatusHandoffPending,
		HandoffTo:   "agent-b",
	}); err != nil {
		t.Fatalf("record handoff coordination: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "Blocked after handoff planning",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve escalation"}},
	}); err != nil {
		t.Fatalf("record later blocked coordination: %v", err)
	}

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			CoordinationFloor: 2,
			DissentQuota:      1,
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 12},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 2},
				MemoryRetrievalLaneBridge:        {ItemLimit: 1},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet with coordination floor: %v", err)
	}
	if len(packet.RecentEpisodePacks) < 2 {
		t.Fatalf("expected coordination floor to keep at least two episode packs despite bridge lane item_limit=1, got %+v", packet.RecentEpisodePacks)
	}
}

func recordMemoryPacketClaim(t *testing.T, ctx context.Context, store *Store, input KnowledgeClaimInput) {
	t.Helper()
	if _, err := store.RecordKnowledgeClaim(ctx, input); err != nil {
		t.Fatalf("record knowledge claim %s: %v", input.ClaimID, err)
	}
}

func hasKnowledgeClaim(records []KnowledgeClaimRecord, claimID string) bool {
	for _, record := range records {
		if record.ClaimID == claimID {
			return true
		}
	}
	return false
}

func countMemoryPacketBasisRefsByKind(refs []MemoryPacketBasisRef, kinds ...string) int {
	want := make(map[string]struct{}, len(kinds))
	for _, kind := range kinds {
		want[kind] = struct{}{}
	}
	count := 0
	for _, ref := range refs {
		if _, ok := want[ref.RefKind]; ok {
			count++
		}
	}
	return count
}

func countMemoryPacketBasisRefsByRole(refs []MemoryPacketBasisRef, roles ...string) int {
	want := make(map[string]struct{}, len(roles))
	for _, role := range roles {
		want[role] = struct{}{}
	}
	count := 0
	for _, ref := range refs {
		if _, ok := want[ref.Role]; ok {
			count++
		}
	}
	return count
}

func countMemoryPacketRows(t *testing.T, ctx context.Context, store *Store, table string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(ctx, "SELECT COUNT(1) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count rows in %s: %v", table, err)
	}
	return count
}
