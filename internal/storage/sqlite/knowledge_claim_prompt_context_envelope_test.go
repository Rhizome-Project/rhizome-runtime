package sqlite_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestKnowledgeClaimPromptContextEnvelopeCarriesDirectClaimEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-claim-prompt-context-lifecycle"
		agentID     = "agent-claim-prompt-context-lifecycle"
	)
	createKnowledgeClaimPromptContextFixture(t, ctx, store, workspaceID, agentID)

	claim, writeEvent, invalidationEvents, err := store.RecordKnowledgeClaimWithAuthorityEffects(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:               "claim-prompt-context-write",
		WorkspaceID:           workspaceID,
		ClaimType:             "FACT",
		Status:                "ACTIVE",
		Subject:               "Claim prompt context write",
		Body:                  "Direct workspace claim writes should carry operation-bound prompt context.",
		Summary:               "Claim prompt context write.",
		Confidence:            0.82,
		SourceKind:            "manual",
		SourceID:              "developer",
		AgentID:               agentID,
		PromptContextEnvelope: sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.write", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("record knowledge claim with prompt context: %v", err)
	}
	assertKnowledgeClaimRuntimePromptContext(t, writeEvent, "workspace.claim.write", workspaceID, claim.ClaimID, "human", "developer", map[string]string{
		"claim_type":  "FACT",
		"status":      "ACTIVE",
		"source_kind": "manual",
		"source_id":   "developer",
		"agent_id":    agentID,
		"actor_type":  "agent",
		"actor_id":    agentID,
	})
	for _, event := range invalidationEvents {
		assertNoRuntimePromptContextEnvelope(t, event.PayloadJSON)
	}

	reviewed, queueRecord, reviewEvent, queueEvent, reviewInvalidations, err := store.RequestKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:           workspaceID,
		ClaimID:               claim.ClaimID,
		ActorID:               "developer",
		Reason:                "needs operator confirmation",
		ReviewDueAt:           "2099-01-01T00:00:00Z",
		AssignedTo:            "reviewer-one",
		PromptContextEnvelope: sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.review", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("request knowledge claim review with prompt context: %v", err)
	}
	if reviewed.Status != "REVIEW" || queueRecord.QueueID == "" || queueEvent.EventID == "" {
		t.Fatalf("expected review status and queue side effect, claim=%+v queue=%+v queueEvent=%+v", reviewed, queueRecord, queueEvent)
	}
	assertKnowledgeClaimRuntimePromptContext(t, reviewEvent, "workspace.claim.review", workspaceID, claim.ClaimID, "human", "developer", map[string]string{
		"status":     "REVIEW",
		"actor_type": "operator",
		"actor_id":   "developer",
	})
	assertNoRuntimePromptContextEnvelope(t, queueEvent.PayloadJSON)
	for _, event := range reviewInvalidations {
		assertNoRuntimePromptContextEnvelope(t, event.PayloadJSON)
	}

	confirmed, _, confirmEvent, confirmQueueEvent, confirmInvalidations, err := store.ConfirmKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:           workspaceID,
		ClaimID:               claim.ClaimID,
		ActorID:               "developer",
		Reason:                "confirmed by operator",
		PromptContextEnvelope: sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.confirm", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("confirm knowledge claim with prompt context: %v", err)
	}
	assertKnowledgeClaimRuntimePromptContext(t, confirmEvent, "workspace.claim.confirm", workspaceID, confirmed.ClaimID, "human", "developer", map[string]string{
		"status":      "CONFIRMED",
		"actor_type":  "operator",
		"actor_id":    "developer",
		"reviewed_by": "developer",
	})
	if confirmQueueEvent.EventID != "" {
		assertNoRuntimePromptContextEnvelope(t, confirmQueueEvent.PayloadJSON)
	}
	for _, event := range confirmInvalidations {
		assertNoRuntimePromptContextEnvelope(t, event.PayloadJSON)
	}
}

func TestKnowledgeClaimPromptContextEnvelopeCarriesArchiveAndEscalateEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-claim-prompt-context-archive-escalate"
		agentID     = "agent-claim-prompt-context-archive-escalate"
	)
	createKnowledgeClaimPromptContextFixture(t, ctx, store, workspaceID, agentID)

	archiveClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-prompt-context-archive",
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Claim prompt context archive",
		Body:        "Archive should carry claim prompt context.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed archive claim: %v", err)
	}
	archived, _, archiveEvent, archiveQueueEvent, archiveInvalidations, err := store.ArchiveKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimArchiveInput{
		WorkspaceID:           workspaceID,
		ClaimID:               archiveClaim.ClaimID,
		ArchivedBy:            "developer",
		Reason:                "not useful for current run",
		PromptContextEnvelope: sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.archive", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("archive knowledge claim with prompt context: %v", err)
	}
	if archived.Status != "ARCHIVED" {
		t.Fatalf("expected archived claim, got %+v", archived)
	}
	assertKnowledgeClaimRuntimePromptContext(t, archiveEvent, "workspace.claim.archive", workspaceID, archiveClaim.ClaimID, "human", "developer", map[string]string{
		"status":      "ARCHIVED",
		"actor_type":  "operator",
		"actor_id":    "developer",
		"archived_by": "developer",
	})
	if archiveQueueEvent.EventID != "" {
		assertNoRuntimePromptContextEnvelope(t, archiveQueueEvent.PayloadJSON)
	}
	for _, event := range archiveInvalidations {
		assertNoRuntimePromptContextEnvelope(t, event.PayloadJSON)
	}

	escalateClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-prompt-context-escalate",
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Claim prompt context escalate",
		Body:        "Escalation should carry claim prompt context.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed escalate claim: %v", err)
	}
	if _, _, _, _, _, err := store.RequestKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     escalateClaim.ClaimID,
		ActorID:     "developer",
		Reason:      "seed review before escalation",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-one",
	}); err != nil {
		t.Fatalf("seed review before escalation: %v", err)
	}
	escalated, escalateEvent, escalateQueueEvent, escalateInvalidations, err := store.EscalateKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:           workspaceID,
		ClaimID:               escalateClaim.ClaimID,
		ActorID:               "developer",
		Reason:                "needs reviewer mesh attention",
		ReviewDueAt:           "2099-01-02T00:00:00Z",
		AssignedTo:            "reviewer-two",
		Urgency:               "HIGH",
		PromptContextEnvelope: sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.escalate", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("escalate knowledge claim review with prompt context: %v", err)
	}
	if escalated.Queue.QueueID == "" || escalateQueueEvent.EventID == "" {
		t.Fatalf("expected escalation queue side effect, got %+v / %+v", escalated, escalateQueueEvent)
	}
	assertKnowledgeClaimRuntimePromptContext(t, escalateEvent, "workspace.claim.escalate", workspaceID, escalateClaim.ClaimID, "human", "developer", map[string]string{
		"status":     "REVIEW",
		"actor_type": "operator",
		"actor_id":   "developer",
	})
	assertNoRuntimePromptContextEnvelope(t, escalateQueueEvent.PayloadJSON)
	for _, event := range escalateInvalidations {
		assertNoRuntimePromptContextEnvelope(t, event.PayloadJSON)
	}
}

func TestKnowledgeClaimPromptContextEnvelopeCarriesRemainingLifecycleEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-claim-prompt-context-remaining-lifecycle"
		agentID     = "agent-claim-prompt-context-remaining-lifecycle"
	)
	createKnowledgeClaimPromptContextFixture(t, ctx, store, workspaceID, agentID)

	conflictClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-prompt-context-conflict-target",
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Conflict target",
		Body:        "This claim is used as a conflict target.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed conflict target: %v", err)
	}
	disputeClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-prompt-context-dispute",
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Dispute target",
		Body:        "This claim will be disputed.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed dispute claim: %v", err)
	}
	disputed, _, disputeEvent, disputeQueueEvent, _, err := store.DisputeKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:           workspaceID,
		ClaimID:               disputeClaim.ClaimID,
		ActorID:               "developer",
		Reason:                "conflicts with current evidence",
		ReviewDueAt:           "2099-01-01T00:00:00Z",
		ConflictsClaimID:      conflictClaim.ClaimID,
		PromptContextEnvelope: sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.dispute", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("dispute knowledge claim with prompt context: %v", err)
	}
	assertKnowledgeClaimRuntimePromptContext(t, disputeEvent, "workspace.claim.dispute", workspaceID, disputed.ClaimID, "human", "developer", map[string]string{
		"status":             "DISPUTED",
		"actor_type":         "operator",
		"actor_id":           "developer",
		"reviewed_by":        "developer",
		"conflicts_claim_id": conflictClaim.ClaimID,
	})
	if disputeQueueEvent.EventID != "" {
		assertNoRuntimePromptContextEnvelope(t, disputeQueueEvent.PayloadJSON)
	}

	staleClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-prompt-context-stale",
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Stale target",
		Body:        "This claim will become stale.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed stale claim: %v", err)
	}
	stale, _, staleEvent, staleQueueEvent, _, err := store.MarkKnowledgeClaimStaleWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:           workspaceID,
		ClaimID:               staleClaim.ClaimID,
		ActorID:               "developer",
		Reason:                "needs fresh evidence",
		ReviewDueAt:           "2099-01-03T00:00:00Z",
		PromptContextEnvelope: sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.stale", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("mark knowledge claim stale with prompt context: %v", err)
	}
	assertKnowledgeClaimRuntimePromptContext(t, staleEvent, "workspace.claim.stale", workspaceID, stale.ClaimID, "human", "developer", map[string]string{
		"status":      "STALE",
		"actor_type":  "operator",
		"actor_id":    "developer",
		"reviewed_by": "developer",
	})
	if staleQueueEvent.EventID != "" {
		assertNoRuntimePromptContextEnvelope(t, staleQueueEvent.PayloadJSON)
	}

	supersedingClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-prompt-context-superseding",
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Superseding claim",
		Body:        "This claim supersedes another claim.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed superseding claim: %v", err)
	}
	supersededClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-prompt-context-superseded",
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Superseded target",
		Body:        "This claim will be superseded.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed superseded claim: %v", err)
	}
	superseded, _, supersedeEvent, supersedeQueueEvent, _, err := store.SupersedeKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:           workspaceID,
		ClaimID:               supersededClaim.ClaimID,
		ActorID:               "developer",
		Reason:                "superseded by stronger claim",
		SupersedingClaimID:    supersedingClaim.ClaimID,
		PromptContextEnvelope: sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.supersede", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("supersede knowledge claim with prompt context: %v", err)
	}
	assertKnowledgeClaimRuntimePromptContext(t, supersedeEvent, "workspace.claim.supersede", workspaceID, superseded.ClaimID, "human", "developer", map[string]string{
		"status":                 "SUPERSEDED",
		"actor_type":             "operator",
		"actor_id":               "developer",
		"reviewed_by":            "developer",
		"superseded_by_claim_id": supersedingClaim.ClaimID,
	})
	if supersedeQueueEvent.EventID != "" {
		assertNoRuntimePromptContextEnvelope(t, supersedeQueueEvent.PayloadJSON)
	}
}

func TestKnowledgeClaimPromptContextEnvelopeRejectsForgedBindings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		envelope map[string]any
	}{
		{
			name:     "wrong surface",
			envelope: sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.archive", "server_rpc", "ws-claim-prompt-context-forged", "human", "developer"),
		},
		{
			name:     "wrong workspace",
			envelope: sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.write", "server_rpc", "ws-claim-prompt-context-other", "human", "developer"),
		},
		{
			name: "wrong claim id",
			envelope: func() map[string]any {
				envelope := sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.write", "server_rpc", "ws-claim-prompt-context-forged", "human", "developer")
				envelope["claim_id"] = "claim-forged-other"
				return envelope
			}(),
		},
		{
			name: "nested wrong workspace",
			envelope: func() map[string]any {
				envelope := sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.write", "server_rpc", "ws-claim-prompt-context-forged", "human", "developer")
				envelope["nested"] = map[string]any{
					"prompt_context_envelope": sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.write", "server_rpc", "ws-claim-prompt-context-other", "human", "developer"),
				}
				return envelope
			}(),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			const workspaceID = "ws-claim-prompt-context-forged"
			createKnowledgeClaimPromptContextFixture(t, ctx, store, workspaceID, "agent-claim-prompt-context-forged")

			_, _, _, err := store.RecordKnowledgeClaimWithAuthorityEffects(ctx, sqlite.KnowledgeClaimInput{
				ClaimID:               "claim-forged",
				WorkspaceID:           workspaceID,
				ClaimType:             "FACT",
				Status:                "ACTIVE",
				Subject:               "Forged claim prompt context",
				Body:                  "Forged knowledge-claim prompt context should fail closed.",
				SourceKind:            "manual",
				SourceID:              "developer",
				PromptContextEnvelope: tt.envelope,
			})
			if err == nil {
				t.Fatal("expected forged knowledge claim prompt context to fail")
			}
			if got := countKnowledgeClaimRowsByID(t, ctx, store, workspaceID, "claim-forged"); got != 0 {
				t.Fatalf("expected no knowledge_claim row after forged prompt context reject, got %d", got)
			}
			if got := countKnowledgeClaimRuntimeEventsByID(t, ctx, store, workspaceID, "claim-forged", "knowledge_claim.written"); got != 0 {
				t.Fatalf("expected no knowledge_claim.written event after forged prompt context reject, got %d", got)
			}
		})
	}
}

func TestKnowledgeClaimLifecyclePromptContextRejectsForgedBindings(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-prompt-context-lifecycle-forged"
		claimID     = "claim-lifecycle-forged"
	)
	createKnowledgeClaimPromptContextFixture(t, ctx, store, workspaceID, "agent-claim-lifecycle-forged")
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Lifecycle forged prompt context",
		Body:        "Lifecycle prompt-context mismatches should roll back.",
		SourceKind:  "manual",
		SourceID:    "developer",
	}); err != nil {
		t.Fatalf("seed lifecycle forged claim: %v", err)
	}
	envelope := sqlite.BuildKnowledgeClaimPromptContextEnvelope("workspace.claim.confirm", "server_rpc", workspaceID, "human", "developer")
	envelope["actor_id"] = "other-actor"
	if _, _, _, _, _, err := store.ConfirmKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:           workspaceID,
		ClaimID:               claimID,
		ActorID:               "developer",
		Reason:                "should fail before state change",
		PromptContextEnvelope: envelope,
	}); err == nil {
		t.Fatal("expected forged lifecycle prompt context to fail")
	}
	claim, err := store.GetKnowledgeClaim(ctx, workspaceID, claimID)
	if err != nil {
		t.Fatalf("get claim after forged lifecycle reject: %v", err)
	}
	if claim.Status != "ACTIVE" {
		t.Fatalf("expected claim status rollback to ACTIVE after forged prompt context reject, got %+v", claim)
	}
	if got := countKnowledgeClaimRuntimeEventsByID(t, ctx, store, workspaceID, claimID, "knowledge_claim.confirmed"); got != 0 {
		t.Fatalf("expected no confirmed event after forged lifecycle prompt context reject, got %d", got)
	}
}

func createKnowledgeClaimPromptContextFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Knowledge Claim Prompt Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if strings.TrimSpace(agentID) != "" {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: "Knowledge Claim Prompt Context Agent",
		}); err != nil {
			t.Fatalf("register agent: %v", err)
		}
	}
}

func assertKnowledgeClaimRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, surface, workspaceID, claimID, principalType, principalID string, extra map[string]string) {
	t.Helper()
	if event.EventID == "" {
		t.Fatal("expected runtime event")
	}
	payload := decodeWorkspaceMemoryPromptPayload(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in payload %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_knowledge_claim_write",
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"claim_id":                           claimID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	for key, value := range extra {
		expected[key] = value
	}
	for key, want := range expected {
		if got, _ := envelope[key].(string); got != want {
			t.Fatalf("expected envelope %s=%q, got %q in %+v", key, want, got, envelope)
		}
	}
	if got, _ := payload["workspace_id"].(string); got != workspaceID {
		t.Fatalf("expected payload workspace_id=%q, got %+v", workspaceID, payload)
	}
	if got, _ := payload["claim_id"].(string); got != claimID {
		t.Fatalf("expected payload claim_id=%q, got %+v", claimID, payload)
	}
}

func countKnowledgeClaimRowsByID(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, claimID string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM knowledge_claims WHERE workspace_id = ? AND claim_id = ?`, workspaceID, claimID).Scan(&count); err != nil {
		t.Fatalf("count knowledge_claim rows: %v", err)
	}
	return count
}

func countKnowledgeClaimRuntimeEventsByID(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, claimID, eventType string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND entity_id = ? AND event_type = ?`, workspaceID, claimID, eventType).Scan(&count); err != nil {
		t.Fatalf("count knowledge_claim runtime events: %v", err)
	}
	return count
}
