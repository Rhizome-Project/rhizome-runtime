package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryPacketRPCSurface(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet"
		taskID      = "task-memory-packet"
		sessionID   = "sess-memory-packet"
		docKey      = "task-memory-packet-doc"
		artifactRef = "artifact://memory-packet"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Memory Packet Doc",
		Content:     "Kernel packet should stay read-only and task scoped.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Title:       "Memory Packet Artifact",
		ArtifactRef: artifactRef,
		Kind:        "workspace_doc",
		ContentType: "text/markdown",
		CreatedBy:   "agent-a",
	}); err != nil {
		t.Fatalf("create artifact: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Summary:     "claim memory packet task",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "blocked on approval",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "approve packet rollout"},
		},
		RelatedDocKeys:      []string{docKey},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{{Ref: artifactRef}},
	}); err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-memory-packet-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Packet read path",
		Body:        "Use memory packets for read-only context assembly.",
		Summary:     "Memory packets are read-only.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	}); err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-memory-packet-branch",
		ClaimType:   "ALTERNATIVE_BRANCH",
		Status:      "ACTIVE",
		Subject:     "Delayed rollout alternative",
		Body:        "Preserve a delayed rollout alternative branch for contrastive recall.",
		Summary:     "Delayed rollout branch.",
		Confidence:  0.71,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
	}); err != nil {
		t.Fatalf("record alternative branch claim: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-memory-packet-dissent-marker",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Rollback disagreement exists",
		Body:        "Preserve a dissent marker in the shell contrastive lane.",
		Summary:     "Dissent marker.",
		Confidence:  0.68,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
	}); err != nil {
		t.Fatalf("record dissent marker claim: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-memory-packet-procedure",
		ClaimType:   "PROCEDURE",
		Status:      "ACTIVE",
		Subject:     "Rollback checklist",
		Body:        "Preserve procedural guidance in its own shell lane.",
		Summary:     "Rollback checklist.",
		Confidence:  0.75,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	}); err != nil {
		t.Fatalf("record procedural claim: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-memory-packet-anti-procedure",
		ClaimType:   "ANTI_PROCEDURE",
		Status:      "ACTIVE",
		Subject:     "Do not shortcut rollback",
		Body:        "Anti-procedural guidance should share the dedicated shell procedural lane.",
		Summary:     "Avoid rollback shortcut.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	}); err != nil {
		t.Fatalf("record anti procedural claim: %v", err)
	}
	if _, err := store.ArchiveKnowledgeClaim(ctx, sqlite.KnowledgeClaimArchiveInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-memory-packet-branch",
		ArchivedBy:  "rmp_pruner",
		Reason:      "rmp_gc_expired",
	}); err != nil {
		t.Fatalf("archive alternative branch claim: %v", err)
	}

	rawKernel, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID:  workspaceID,
		SessionID:    sessionID,
		DocKeys:      []string{docKey},
		ArtifactRefs: []string{artifactRef},
	})
	if err != nil {
		t.Fatalf("marshal kernel params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryPacketKernel(ctx, rawKernel)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketKernel rpc error: %+v", rpcErr)
	}
	kernelPayload := result.(map[string]any)
	kernel, ok := kernelPayload["packet"].(sqlite.MemoryKernelPacket)
	if !ok {
		t.Fatalf("unexpected kernel packet type %T", kernelPayload["packet"])
	}
	if kernel.Meta.PacketKind != "KERNEL" || kernel.Meta.TaskID != taskID || kernel.Meta.SessionID != sessionID {
		t.Fatalf("unexpected kernel packet payload: %+v", kernel)
	}
	if len(kernel.Docs) != 1 || kernel.Docs[0].DocKey != docKey {
		t.Fatalf("expected scoped docs in kernel packet, got %+v", kernel.Docs)
	}
	if kernel.BoundarySummary == nil {
		t.Fatalf("expected kernel packet to surface boundary summary, got %+v", kernel)
	}
	if kernel.BasisSummary == nil {
		t.Fatalf("expected kernel packet to surface basis summary, got %+v", kernel)
	}
	if kernel.BoundarySummary.HardConstraintCount != len(kernel.Coordination.HardConstraints) ||
		kernel.BoundarySummary.AcceptedDecisionCount != len(kernel.Coordination.AcceptedDecisions) ||
		kernel.BoundarySummary.DecisionRecordCount != len(kernel.Coordination.DecisionRecords) ||
		kernel.BoundarySummary.ActiveBlockerCount != len(kernel.Coordination.ActiveBlockers) ||
		kernel.BoundarySummary.BlockerHypothesisCount != len(kernel.Coordination.BlockerHypotheses) {
		t.Fatalf("expected kernel boundary summary to match coordination payload, got %+v vs %+v", kernel.BoundarySummary, kernel.Coordination)
	}
	if kernel.BasisSummary.TotalRefCount != len(kernel.BasisRefs) ||
		kernel.BasisSummary.RuntimeEventRefCount != countServerMemoryPacketBasisRefsByKind(kernel.BasisRefs, "runtime_event") ||
		kernel.BasisSummary.EpisodePackRefCount != countServerMemoryPacketBasisRefsByKind(kernel.BasisRefs, "episode_pack") ||
		kernel.BasisSummary.KnowledgeClaimRefCount != countServerMemoryPacketBasisRefsByKind(kernel.BasisRefs, "knowledge_claim") ||
		kernel.BasisSummary.WorkspaceMemoryRefCount != countServerMemoryPacketBasisRefsByKind(kernel.BasisRefs, "workspace_memory") ||
		kernel.BasisSummary.CoordinationBasisCount != countServerMemoryPacketBasisRefsByRole(kernel.BasisRefs, "hard_constraint", "accepted_decision", "active_blocker", "blocker_hypothesis") ||
		kernel.BasisSummary.RecentTraceBasisCount != countServerMemoryPacketBasisRefsByRole(kernel.BasisRefs, "recent_episode_pack", "recent_runtime_event") {
		t.Fatalf("expected kernel basis summary to match basis refs, got %+v vs %+v", kernel.BasisSummary, kernel.BasisRefs)
	}

	rawShell, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID:  workspaceID,
		AgentID:      "agent-a",
		SessionID:    sessionID,
		DocKeys:      []string{docKey},
		ArtifactRefs: []string{artifactRef},
	})
	if err != nil {
		t.Fatalf("marshal shell params: %v", err)
	}
	result, rpcErr = h.workspaceMemoryPacketShell(ctx, rawShell)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
	}
	shellPayload := result.(map[string]any)
	shell, ok := shellPayload["packet"].(sqlite.MemoryShellPacket)
	if !ok {
		t.Fatalf("unexpected shell packet type %T", shellPayload["packet"])
	}
	if shell.Meta.PacketKind != "SHELL" || shell.Meta.AgentID != "agent-a" || shell.Meta.TaskID != taskID {
		t.Fatalf("unexpected shell packet payload: %+v", shell)
	}
	if shell.KernelRef.PacketKey == "" || shell.KernelRef.BasisDigest == "" {
		t.Fatalf("expected shell packet to reference kernel, got %+v", shell.KernelRef)
	}
	if shell.BoundarySummary == nil {
		t.Fatalf("expected shell packet to surface boundary summary, got %+v", shell)
	}
	if shell.BasisSummary == nil {
		t.Fatalf("expected shell packet to surface basis summary, got %+v", shell)
	}
	foundArchivedBranch := false
	foundDissentMarker := false
	for _, claim := range shell.DifferentialClaims {
		switch claim.ClaimID {
		case "claim-memory-packet-branch":
			if claim.ArchivedAt == nil || claim.LifecycleReason != "rmp_gc_expired" {
				t.Fatalf("expected recoverable archived branch metadata on shell differential claim, got %+v", claim)
			}
			foundArchivedBranch = true
		case "claim-memory-packet-dissent-marker":
			if claim.ClaimType != "DISSENT_MARKER" {
				t.Fatalf("expected dissent marker type on shell differential claim, got %+v", claim)
			}
			foundDissentMarker = true
		}
	}
	if !foundArchivedBranch {
		t.Fatalf("expected shell packet to surface recoverable archived alternative branch, got %+v", shell.DifferentialClaims)
	}
	if !foundDissentMarker {
		t.Fatalf("expected shell packet to surface dissent marker claim, got %+v", shell.DifferentialClaims)
	}
	if shell.BoundarySummary.DissentClaimCount < 1 {
		t.Fatalf("expected shell packet boundary summary to count dissent claims, got %+v", shell.BoundarySummary)
	}
	if shell.BoundarySummary.AlternativeBranchCount < 1 || shell.BoundarySummary.ArchivedAlternativeBranchCount < 1 {
		t.Fatalf("expected shell packet boundary summary to count active and archived alternative branches, got %+v", shell.BoundarySummary)
	}
	if !hasServerMemoryPacketClaim(shell.ProceduralClaims, "claim-memory-packet-procedure") {
		t.Fatalf("expected shell packet to surface dedicated procedural claim, got %+v", shell.ProceduralClaims)
	}
	if !hasServerMemoryPacketClaim(shell.ProceduralClaims, "claim-memory-packet-anti-procedure") {
		t.Fatalf("expected shell packet to surface dedicated anti procedural claim, got %+v", shell.ProceduralClaims)
	}
	if hasServerMemoryPacketClaim(shell.DifferentialClaims, "claim-memory-packet-procedure") {
		t.Fatalf("did not expect procedural claim to stay in differential claims, got %+v", shell.DifferentialClaims)
	}
	if hasServerMemoryPacketClaim(shell.DifferentialClaims, "claim-memory-packet-anti-procedure") {
		t.Fatalf("did not expect anti procedural claim to stay in differential claims, got %+v", shell.DifferentialClaims)
	}
	if shell.BoundarySummary.ProceduralClaimCount != len(shell.ProceduralClaims) {
		t.Fatalf("expected shell packet boundary summary to match procedural lane, got %+v vs %+v", shell.BoundarySummary, shell.ProceduralClaims)
	}
	if shell.BasisSummary.TotalRefCount != len(shell.BasisRefs) ||
		shell.BasisSummary.RuntimeEventRefCount != countServerMemoryPacketBasisRefsByKind(shell.BasisRefs, "runtime_event") ||
		shell.BasisSummary.EpisodePackRefCount != countServerMemoryPacketBasisRefsByKind(shell.BasisRefs, "episode_pack") ||
		shell.BasisSummary.KnowledgeClaimRefCount != countServerMemoryPacketBasisRefsByKind(shell.BasisRefs, "knowledge_claim") ||
		shell.BasisSummary.WorkspaceMemoryRefCount != countServerMemoryPacketBasisRefsByKind(shell.BasisRefs, "workspace_memory") ||
		shell.BasisSummary.DifferentialBasisCount != countServerMemoryPacketBasisRefsByRole(shell.BasisRefs, "differential_claim") ||
		shell.BasisSummary.ProceduralBasisCount != countServerMemoryPacketBasisRefsByRole(shell.BasisRefs, "procedural_claim") ||
		shell.BasisSummary.IdentityBasisCount != countServerMemoryPacketBasisRefsByRole(shell.BasisRefs, "identity_memory_task", "identity_memory_session", "identity_memory_workspace") ||
		shell.BasisSummary.RecentTraceBasisCount != countServerMemoryPacketBasisRefsByRole(shell.BasisRefs, "recent_episode_pack", "recent_runtime_event") {
		t.Fatalf("expected shell basis summary to match basis refs, got %+v vs %+v", shell.BasisSummary, shell.BasisRefs)
	}
}

func countServerMemoryPacketBasisRefsByKind(refs []sqlite.MemoryPacketBasisRef, kinds ...string) int {
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

func countServerMemoryPacketBasisRefsByRole(refs []sqlite.MemoryPacketBasisRef, roles ...string) int {
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

func TestWorkspaceMemoryPacketRPCRejectsInvalidParams(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	seedHandlerAgentWorkWorkspace(t, ctx, store, "ws-handler-memory-packet-invalid", []string{"agent-a", "agent-b"})
	createHandlerAgentWorkTask(t, ctx, store, "ws-handler-memory-packet-invalid", "task-a", "normal")
	createHandlerAgentWorkTask(t, ctx, store, "ws-handler-memory-packet-invalid", "task-b", "normal")
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-b",
		AgentID:     "agent-b",
		WorkspaceID: "ws-handler-memory-packet-invalid",
		TaskID:      "task-b",
		StartedAt:   "2026-03-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	cases := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params workspaceMemoryPacketParams
	}{
		{name: "kernel missing workspace", call: h.workspaceMemoryPacketKernel, params: workspaceMemoryPacketParams{TaskID: "task-a"}},
		{name: "kernel missing anchor", call: h.workspaceMemoryPacketKernel, params: workspaceMemoryPacketParams{WorkspaceID: "ws-handler-memory-packet-invalid"}},
		{name: "shell missing agent", call: h.workspaceMemoryPacketShell, params: workspaceMemoryPacketParams{WorkspaceID: "ws-handler-memory-packet-invalid", TaskID: "task-a"}},
		{name: "kernel mismatched task session", call: h.workspaceMemoryPacketKernel, params: workspaceMemoryPacketParams{WorkspaceID: "ws-handler-memory-packet-invalid", TaskID: "task-a", SessionID: "sess-b"}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			if _, rpcErr := tc.call(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params rpc error, got %+v", rpcErr)
			}
		})
	}
}
