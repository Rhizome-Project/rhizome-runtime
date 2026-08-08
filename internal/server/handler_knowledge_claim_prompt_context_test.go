package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceClaimRPCAddsPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-claim-rpc-prompt-context"
		agentID     = "agent-claim-rpc-prompt-context"
		claimID     = "claim-rpc-prompt-context"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim RPC Prompt Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	writeRaw, err := json.Marshal(workspaceClaimWriteParams{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Claim RPC prompt context",
		Body:        "Direct claim writes should carry durable prompt context.",
		Summary:     "Claim prompt context.",
		Confidence:  0.77,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("marshal claim write params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimWrite(testAuthContext(workspaceID, "human", "developer"), writeRaw); rpcErr != nil {
		t.Fatalf("workspaceClaimWrite rpc error: %+v", rpcErr)
	}
	writeLive := nextEventOfType(t, ch, "workspace.claim.written")
	writePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	assertLiveEventMirrorsRuntimeEvent(t, writeLive, writePersisted, "workspace.claim.written")
	assertServerKnowledgeClaimRuntimePromptContext(t, writePersisted, "workspace.claim.write", workspaceID, claimID, "human", "developer", map[string]string{
		"claim_type":  "FACT",
		"status":      "ACTIVE",
		"source_kind": "manual",
		"source_id":   "developer",
		"agent_id":    agentID,
		"actor_type":  "agent",
		"actor_id":    agentID,
	})

	reviewRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ActorID:     "developer",
		Reason:      "needs operator confirmation",
		DueAt:       "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-rpc",
	})
	if err != nil {
		t.Fatalf("marshal review params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimReview(testAuthContext(workspaceID, "human", "developer"), reviewRaw); rpcErr != nil {
		t.Fatalf("workspaceClaimReview rpc error: %+v", rpcErr)
	}
	reviewPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_requested",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	assertServerKnowledgeClaimRuntimePromptContext(t, reviewPersisted, "workspace.claim.review", workspaceID, claimID, "human", "developer", map[string]string{
		"status":     "REVIEW",
		"actor_type": "operator",
		"actor_id":   "developer",
	})

	confirmRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ActorID:     "developer",
		Reason:      "confirmed from prompt context test",
	})
	if err != nil {
		t.Fatalf("marshal confirm params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimConfirm(testAuthContext(workspaceID, "human", "developer"), confirmRaw); rpcErr != nil {
		t.Fatalf("workspaceClaimConfirm rpc error: %+v", rpcErr)
	}
	confirmPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.confirmed",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	assertServerKnowledgeClaimRuntimePromptContext(t, confirmPersisted, "workspace.claim.confirm", workspaceID, claimID, "human", "developer", map[string]string{
		"status":      "CONFIRMED",
		"actor_type":  "operator",
		"actor_id":    "developer",
		"reviewed_by": "developer",
	})
}

func TestWorkspaceClaimArchiveRPCAddsPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-claim-archive-rpc-prompt-context"
		agentID     = "agent-claim-archive-rpc-prompt-context"
		claimID     = "claim-archive-rpc-prompt-context"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Archive RPC Prompt Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Archive prompt context",
		Body:        "Archive RPC should carry durable prompt context.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("seed claim: %v", err)
	}

	raw, err := json.Marshal(workspaceClaimArchiveParams{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ArchivedBy:  "developer",
		Reason:      "obsolete claim",
	})
	if err != nil {
		t.Fatalf("marshal archive params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimArchive(testAuthContext(workspaceID, "human", "developer"), raw); rpcErr != nil {
		t.Fatalf("workspaceClaimArchive rpc error: %+v", rpcErr)
	}
	archivePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	assertServerKnowledgeClaimRuntimePromptContext(t, archivePersisted, "workspace.claim.archive", workspaceID, claimID, "human", "developer", map[string]string{
		"status":      "ARCHIVED",
		"actor_type":  "operator",
		"actor_id":    "developer",
		"archived_by": "developer",
	})
}

func TestWorkspaceClaimEscalateRPCAddsPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-claim-escalate-rpc-prompt-context"
		agentID     = "agent-claim-escalate-rpc-prompt-context"
		claimID     = "claim-escalate-rpc-prompt-context"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Escalate RPC Prompt Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Escalate prompt context",
		Body:        "Escalate RPC should carry durable prompt context.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	if _, _, _, _, _, err := store.RequestKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ActorID:     "developer",
		Reason:      "seed review before escalation",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-one",
	}); err != nil {
		t.Fatalf("seed review before escalation: %v", err)
	}

	raw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ActorID:     "developer",
		Reason:      "needs reviewer mesh attention",
		DueAt:       "2099-01-02T00:00:00Z",
		AssignedTo:  "reviewer-two",
		Urgency:     "HIGH",
	})
	if err != nil {
		t.Fatalf("marshal escalate params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimEscalate(testAuthContext(workspaceID, "human", "developer"), raw); rpcErr != nil {
		t.Fatalf("workspaceClaimEscalate rpc error: %+v", rpcErr)
	}
	escalatePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_escalated",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	assertServerKnowledgeClaimRuntimePromptContext(t, escalatePersisted, "workspace.claim.escalate", workspaceID, claimID, "human", "developer", map[string]string{
		"status":     "REVIEW",
		"actor_type": "operator",
		"actor_id":   "developer",
	})
}

func assertServerKnowledgeClaimRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantClaimID, wantPrincipalType, wantPrincipalID string, extra map[string]string) {
	t.Helper()
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected knowledge claim prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_knowledge_claim_write",
		"surface":                            wantSurface,
		"origin":                             "server_rpc",
		"workspace_id":                       wantWorkspaceID,
		"claim_id":                           wantClaimID,
		"principal_type":                     wantPrincipalType,
		"principal_id":                       wantPrincipalID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	for key, value := range extra {
		expected[key] = value
	}
	for key, want := range expected {
		got, ok := envelope[key].(string)
		if !ok {
			t.Fatalf("knowledge claim prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
		}
		if got != want {
			t.Fatalf("knowledge claim prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
		}
	}
}
