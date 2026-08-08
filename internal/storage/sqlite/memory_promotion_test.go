package sqlite_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestMemoryPromotionEnqueueDedupesQueueKeyAndHydratesCandidate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-promo-enqueue"
		agentID     = "agent-memory-promo-enqueue"
		sessionID   = "sess-memory-promo-enqueue"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Promotion Enqueue",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Promotion Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	first, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "lesson",
			Body:       "Always capture basis digests before promoting local candidates.",
			SessionID:  sessionID,
			SourceKind: "episode_pack",
			SourceID:   "pack-123",
			Tags:       []string{"memory", "promotion"},
		},
		BasisDigest: "basis-digest-1",
		BasisRefs:   []string{"episode_pack:pack-123", "runtime_event:rtev-1"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}
	if first.State != "PENDING" || first.Candidate.AgentID != agentID || first.Candidate.MemoryType != "LESSON" {
		t.Fatalf("unexpected first promotion record %+v", first)
	}

	second, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "lesson",
			Body:       "Always capture basis digests before promoting local candidates.",
			SessionID:  sessionID,
			SourceKind: "episode_pack",
			SourceID:   "pack-123",
			Tags:       []string{"memory", "promotion"},
		},
		BasisDigest: "basis-digest-1",
		BasisRefs:   []string{"runtime_event:rtev-1", "episode_pack:pack-123"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("reenqueue memory promotion: %v", err)
	}
	if second.PromotionID != first.PromotionID || second.QueueKey != first.QueueKey {
		t.Fatalf("expected queue-key dedupe, got first=%+v second=%+v", first, second)
	}

	items, err := store.ListMemoryPromotions(ctx, sqlite.MemoryPromotionFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory promotions: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one promotion row after dedupe, got %+v", items)
	}
	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "OPEN",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list mirrored operator queues: %v", err)
	}
	if len(queues) != 1 || queues[0].SourceKind != "memory_promotion" || queues[0].SourceID != first.PromotionID {
		t.Fatalf("expected mirrored operator queue for memory promotion, got %+v", queues)
	}
}

func TestMemoryPromotionResolveAcceptedWritesThroughWorkspaceMemory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-promo-accept"
		agentID     = "agent-memory-promo-accept"
		sessionID   = "sess-memory-promo-accept"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Promotion Accept",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Promotion Accept Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	record, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "decision",
			Title:      "Prefer deterministic promotion gates",
			Body:       "Promotion acceptance must still flow through canonical workspace memory writes.",
			SessionID:  sessionID,
			SourceKind: "memory_packet_shell",
			SourceID:   "shell-packet-1",
			Tags:       []string{"rrp", "memory"},
			Importance: 0.8,
			Confidence: 0.9,
		},
		BasisDigest: "basis-digest-accept",
		BasisRefs:   []string{"packet:shell-packet-1"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}

	resolved, err := store.ResolveMemoryPromotion(ctx, sqlite.MemoryPromotionResolveInput{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "ACCEPTED",
		ResolvedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("resolve memory promotion: %v", err)
	}
	if resolved.Promotion.State != "ACCEPTED" || resolved.AppliedMemory == nil {
		t.Fatalf("expected accepted promotion with applied memory, got %+v", resolved)
	}
	if resolved.AppliedMemory.MemoryID != record.TargetMemoryID || resolved.Promotion.AppliedID != record.TargetMemoryID {
		t.Fatalf("expected deterministic target memory id application, got %+v", resolved)
	}
	if resolved.AppliedMemory.Record.SourceKind != "memory_packet_shell" || resolved.AppliedMemory.Record.AgentID != agentID {
		t.Fatalf("unexpected applied memory payload %+v", resolved.AppliedMemory.Record)
	}
	if resolved.AppliedMemory.PromotedClaimEffects == nil ||
		resolved.AppliedMemory.PromotedClaimEffects.Claim == nil ||
		resolved.AppliedMemory.PromotedClaimEffects.ClaimEvent == nil {
		t.Fatalf("expected applied memory to carry promoted claim side effects, got %+v", resolved.AppliedMemory)
	}
	if resolved.AppliedMemory.PromotedClaimEffects.Claim.ClaimID != "claim:memory:"+record.TargetMemoryID ||
		resolved.AppliedMemory.PromotedClaimEffects.ClaimEvent.EventType != "knowledge_claim.written" {
		t.Fatalf("unexpected promoted claim effect %+v", resolved.AppliedMemory.PromotedClaimEffects)
	}
	if _, err := store.GetWorkspaceMemory(ctx, workspaceID, record.TargetMemoryID); err != nil {
		t.Fatalf("expected canonical workspace memory after promotion acceptance: %v", err)
	}
	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "RESOLVED",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list resolved operator queues: %v", err)
	}
	if len(queues) != 1 || queues[0].SourceID != record.PromotionID {
		t.Fatalf("expected resolved mirrored operator queue, got %+v", queues)
	}
}

func TestMemoryPromotionResolveAcceptedRevalidatesCrossEpisodeEvidence(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		memoryType string
		sessionIDs []string
	}{
		{
			name:       "procedural",
			memoryType: "procedure",
			sessionIDs: []string{"sess-memory-promo-stale-proc-1", "sess-memory-promo-stale-proc-2"},
		},
		{
			name:       "identity",
			memoryType: "self_model",
			sessionIDs: []string{"sess-memory-promo-stale-ident-1", "sess-memory-promo-stale-ident-2", "sess-memory-promo-stale-ident-3"},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			workspaceID := "ws-memory-promo-stale-" + tc.name
			agentID := "agent-memory-promo-stale-" + tc.name

			if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
				WorkspaceID: workspaceID,
				Title:       "Memory Promotion Stale Evidence " + tc.name,
				CreatedBy:   "developer",
			}); err != nil {
				t.Fatalf("create workspace: %v", err)
			}
			claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
			if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
				WorkspaceID: workspaceID,
				AgentID:     agentID,
				OwnerUserID: "developer",
				DisplayName: "Memory Promotion Stale Evidence Agent " + tc.name,
			}); err != nil {
				t.Fatalf("register agent: %v", err)
			}
			for _, sessionID := range tc.sessionIDs {
				if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
					SessionID:   sessionID,
					AgentID:     agentID,
					WorkspaceID: workspaceID,
					StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
				}); err != nil {
					t.Fatalf("create session %s: %v", sessionID, err)
				}
			}

			basisRefs := make([]string, 0, len(tc.sessionIDs))
			for _, sessionID := range tc.sessionIDs {
				basisRefs = append(basisRefs, "session:"+sessionID)
			}
			record, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
				WorkspaceID: workspaceID,
				Candidate: sqlite.MemoryPromotionCandidate{
					MemoryType: tc.memoryType,
					Title:      "Revalidate " + tc.name + " evidence",
					Body:       "Accepted promotions must re-check basis refs before applying canonical workspace memory writes.",
					SessionID:  tc.sessionIDs[0],
					SourceKind: "episode_pack",
					SourceID:   "pack-stale-" + tc.name,
				},
				BasisDigest: "basis-digest-stale-" + tc.name,
				BasisRefs:   basisRefs,
				ProposedBy:  agentID,
			})
			if err != nil {
				t.Fatalf("enqueue memory promotion: %v", err)
			}

			staleSessionID := tc.sessionIDs[len(tc.sessionIDs)-1]
			result, err := store.DB().ExecContext(ctx, `DELETE FROM agent_sessions WHERE workspace_id = ? AND session_id = ?`, workspaceID, staleSessionID)
			if err != nil {
				t.Fatalf("delete stale evidence session %s: %v", staleSessionID, err)
			}
			if affected, _ := result.RowsAffected(); affected != 1 {
				t.Fatalf("expected one deleted stale evidence session, got %d", affected)
			}

			_, err = store.ResolveMemoryPromotion(ctx, sqlite.MemoryPromotionResolveInput{
				WorkspaceID: workspaceID,
				PromotionID: record.PromotionID,
				Resolution:  "ACCEPTED",
				ResolvedBy:  "developer",
			})
			if err == nil {
				t.Fatalf("expected stale evidence accept to fail")
			}
			if !strings.Contains(err.Error(), "memory promotion evidence is stale") || !strings.Contains(err.Error(), staleSessionID) {
				t.Fatalf("expected stale evidence error mentioning removed session %q, got %v", staleSessionID, err)
			}

			persisted, err := store.GetMemoryPromotion(ctx, workspaceID, record.PromotionID)
			if err != nil {
				t.Fatalf("get memory promotion after stale accept: %v", err)
			}
			if persisted.State != "PENDING" || persisted.AppliedID != "" {
				t.Fatalf("expected promotion to remain pending after stale evidence reject, got %+v", persisted)
			}
			if _, err := store.GetWorkspaceMemory(ctx, workspaceID, record.TargetMemoryID); err == nil {
				t.Fatalf("expected no applied workspace memory for stale evidence promotion %s", record.TargetMemoryID)
			}
		})
	}
}

func TestMemoryPromotionRejectsPromotionIDReplayMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-promo-replay"
		agentID     = "agent-memory-promo-replay"
		sessionID   = "sess-memory-promo-replay"
		promotionID = "promotion-replay-1"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Promotion Replay",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Promotion Replay Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	if _, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		PromotionID: promotionID,
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			Body:       "Original candidate body.",
			SessionID:  sessionID,
			SourceKind: "episode_pack",
			SourceID:   "pack-original",
		},
		BasisDigest: "basis-digest-replay",
		ProposedBy:  agentID,
	}); err != nil {
		t.Fatalf("enqueue original memory promotion: %v", err)
	}

	if _, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		PromotionID: promotionID,
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			Body:       "Different candidate body.",
			SessionID:  sessionID,
			SourceKind: "episode_pack",
			SourceID:   "pack-different",
		},
		BasisDigest: "basis-digest-replay-different",
		ProposedBy:  agentID,
	}); err == nil || !strings.Contains(err.Error(), "promotion_id replay payload does not match existing candidate") {
		t.Fatalf("expected promotion_id replay mismatch rejection, got %v", err)
	}
}

func TestMemoryPromotionRejectsAlreadyResolvedTransitions(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-promo-resolved"
		agentID     = "agent-memory-promo-resolved"
		sessionID   = "sess-memory-promo-resolved"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Promotion Resolved",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Promotion Resolved Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	record, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			Body:       "Reject this candidate.",
			SessionID:  sessionID,
			SourceKind: "episode_pack",
			SourceID:   "pack-reject",
		},
		BasisDigest: "basis-digest-reject",
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}

	if _, err := store.ResolveMemoryPromotion(ctx, sqlite.MemoryPromotionResolveInput{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "REJECTED",
		ResolvedBy:  "developer",
	}); err != nil {
		t.Fatalf("reject memory promotion: %v", err)
	}
	if _, err := store.ResolveMemoryPromotion(ctx, sqlite.MemoryPromotionResolveInput{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "ACCEPTED",
		ResolvedBy:  "developer",
	}); err == nil || !strings.Contains(err.Error(), "already resolved") {
		t.Fatalf("expected already resolved rejection, got %v", err)
	}
}

func TestMemoryPromotionEnqueueSurfacesCoherenceGateAndElevatesReviewUrgency(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-promo-coherence-gate"
		agentID     = "agent-memory-promo-coherence-gate"
		sessionID   = "sess-memory-promo-coherence-gate"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Promotion Coherence Gate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Promotion Coherence Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:      "memres-promo-coherence-gate",
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
				CanonicalMemoryID: "memory:promo-candidate",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "doc-missing", VersionToken: "doc-v1", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}

	record, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "lesson",
			Body:       "Do not promote memory candidates while coherence is degraded without explicit review.",
			SessionID:  sessionID,
			SourceKind: "episode_pack",
			SourceID:   "pack-promo-coherence",
		},
		BasisDigest: "basis-digest-promo-coherence",
		BasisRefs:   []string{"episode_pack:pack-promo-coherence"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}
	if record.CoherenceGate == nil {
		t.Fatalf("expected coherence gate to be surfaced on promotion enqueue, got %+v", record)
	}
	if record.CoherenceGate.ReportScope != "SESSION" || record.CoherenceGate.AdvisoryAction != "DEFER_ACCEPT" || record.CoherenceGate.CoherenceBand != "DEGRADED" {
		t.Fatalf("unexpected coherence gate %+v", record.CoherenceGate)
	}
	if record.CoherenceGate.ReadyInvalidationCount != 1 || record.CoherenceGate.InvalidatedReplicaCount != 1 {
		t.Fatalf("expected coherence gate to reflect ready invalidations and invalidated replicas, got %+v", record.CoherenceGate)
	}
	reasonBlob := strings.Join(record.CoherenceGate.AttentionReasons, ",")
	if !strings.Contains(reasonBlob, "READY_INVALIDATIONS") || !strings.Contains(reasonBlob, "INVALIDATED_REPLICAS") {
		t.Fatalf("expected coherence attention reasons to include invalidation signals, got %+v", record.CoherenceGate.AttentionReasons)
	}

	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "OPEN",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list mirrored operator queues: %v", err)
	}
	if len(queues) != 1 || queues[0].SourceKind != "memory_promotion" || queues[0].SourceID != record.PromotionID {
		t.Fatalf("expected one mirrored operator queue for the promotion, got %+v", queues)
	}
	if queues[0].Urgency != "HIGH" {
		t.Fatalf("expected degraded coherence gate to elevate review urgency, got %+v", queues[0])
	}
	if !strings.Contains(queues[0].Details, "Coherence gate: DEFER_ACCEPT (DEGRADED)") || !strings.Contains(queues[0].PayloadJSON, "\"coherence_gate\"") {
		t.Fatalf("expected operator queue payload/details to surface coherence gate, got %+v", queues[0])
	}
}

func TestMemoryPromotionResolveAcceptedRejectsDeferredCoherenceGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-promo-coherence-resolve"
		agentID     = "agent-memory-promo-coherence-resolve"
		sessionID   = "sess-memory-promo-coherence-resolve"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Promotion Resolve Coherence Gate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Promotion Resolve Coherence Gate Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	record, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "lesson",
			Body:       "Acceptance must stop when a deferred coherence gate appears after enqueue.",
			SessionID:  sessionID,
			SourceKind: "episode_pack",
			SourceID:   "pack-promo-coherence-resolve",
		},
		BasisDigest: "basis-digest-promo-coherence-resolve",
		BasisRefs:   []string{"episode_pack:pack-promo-coherence-resolve"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}
	if record.CoherenceGate != nil {
		t.Fatalf("expected enqueue without coherence gate before degradation, got %+v", record.CoherenceGate)
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   sessionID,
		ReportScope: "SESSION",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:     "P2",
				ReplicaKind:       "memory_node",
				CoherenceClass:    "A",
				State:             "INVALIDATED",
				CanonicalMemoryID: "memory:promo-coherence-resolve",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "doc-missing-coherence-resolve", VersionToken: "doc-v1", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}

	_, err = store.ResolveMemoryPromotion(ctx, sqlite.MemoryPromotionResolveInput{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "ACCEPTED",
		ResolvedBy:  "developer",
	})
	if err == nil {
		t.Fatalf("expected deferred coherence gate to block accepted resolve")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "coherence gate") || !strings.Contains(strings.ToLower(err.Error()), "deferred accept") {
		t.Fatalf("expected deferred coherence gate error, got %v", err)
	}

	persisted, err := store.GetMemoryPromotion(ctx, workspaceID, record.PromotionID)
	if err != nil {
		t.Fatalf("get promotion after deferred gate reject: %v", err)
	}
	if persisted.State != "PENDING" {
		t.Fatalf("expected promotion to remain pending after deferred gate reject, got %+v", persisted)
	}
	if persisted.CoherenceGate == nil || persisted.CoherenceGate.AdvisoryAction != "DEFER_ACCEPT" || persisted.CoherenceGate.ReadyInvalidationCount != 1 {
		t.Fatalf("expected refreshed deferred coherence gate on pending promotion, got %+v", persisted.CoherenceGate)
	}
	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "OPEN",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list mirrored operator queues after deferred gate reject: %v", err)
	}
	if len(queues) != 1 || queues[0].SourceKind != "memory_promotion" || queues[0].SourceID != record.PromotionID {
		t.Fatalf("expected mirrored operator queue to remain open after deferred gate reject, got %+v", queues)
	}
	queue := queues[0]
	if queues[0].Urgency != "HIGH" {
		t.Fatalf("expected deferred coherence gate to keep mirrored queue urgency elevated, got %+v", queues[0])
	}
	if !strings.Contains(queues[0].Details, "Coherence gate: DEFER_ACCEPT (DEGRADED)") ||
		!strings.Contains(queues[0].Details, "READY_INVALIDATIONS") ||
		!strings.Contains(queues[0].PayloadJSON, "\"coherence_gate\"") ||
		!strings.Contains(queues[0].PayloadJSON, "\"advisory_action\":\"DEFER_ACCEPT\"") ||
		!strings.Contains(queues[0].PayloadJSON, "\"ready_invalidation_count\":1") {
		t.Fatalf("expected mirrored operator queue payload/details to refresh deferred coherence gate, got %+v", queues[0])
	}
	if _, err := store.GetWorkspaceMemory(ctx, workspaceID, record.TargetMemoryID); err == nil {
		t.Fatalf("expected no workspace memory write after deferred gate reject")
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.promotion_resolved",
		EntityType:  "memory_promotion",
		EntityID:    record.PromotionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events after deferred gate reject: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no promotion resolved runtime event after deferred gate reject, got %+v", events)
	}
	queueEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list operator queue refresh events after deferred gate reject: %v", err)
	}
	if len(queueEvents) != 1 {
		t.Fatalf("expected one operator queue refresh event after deferred gate reject, got %+v", queueEvents)
	}
	assertRuntimeEventAuthorityMetadata(t, queueEvents[0], authority)
	if !strings.Contains(queueEvents[0].PayloadJSON, "\"urgency\":\"HIGH\"") ||
		!strings.Contains(queueEvents[0].PayloadJSON, "\\\"coherence_gate\\\"") ||
		!strings.Contains(queueEvents[0].PayloadJSON, "\\\"advisory_action\\\":\\\"DEFER_ACCEPT\\\"") {
		t.Fatalf("expected operator queue refresh event to mirror deferred gate snapshot, got %+v", queueEvents[0])
	}
}

func TestMemoryPromotionResolveAcceptedConcurrentDoesNotAppendExtraMemoryWrites(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-promo-concurrent-accept"
		agentID     = "agent-memory-promo-concurrent-accept"
		sessionID   = "sess-memory-promo-concurrent-accept"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Promotion Concurrent Accept",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Promotion Concurrent Accept Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	record, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "decision",
			Body:       "Concurrent acceptance should still materialize one canonical memory write.",
			SessionID:  sessionID,
			SourceKind: "memory_packet_shell",
			SourceID:   "shell-packet-concurrent-accept",
		},
		BasisDigest: "basis-digest-concurrent-accept",
		BasisRefs:   []string{"packet:shell-packet-concurrent-accept"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for idx := 0; idx < 8; idx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.ResolveMemoryPromotion(ctx, sqlite.MemoryPromotionResolveInput{
				WorkspaceID: workspaceID,
				PromotionID: record.PromotionID,
				Resolution:  "ACCEPTED",
				ResolvedBy:  "developer",
			})
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent accept returned error: %v", err)
	}

	persisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    record.TargetMemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(persisted) != 1 {
		t.Fatalf("expected exactly one canonical memory write after concurrent accepts, got %+v", persisted)
	}
	assertRuntimeEventAuthorityMetadata(t, persisted[0], authority)

	resolved, err := store.GetMemoryPromotion(ctx, workspaceID, record.PromotionID)
	if err != nil {
		t.Fatalf("get resolved promotion: %v", err)
	}
	if resolved.State != "ACCEPTED" || resolved.AppliedID != record.TargetMemoryID {
		t.Fatalf("expected accepted promotion with deterministic applied memory id, got %+v", resolved)
	}
}

func TestMemoryPromotionResolveAcceptedRejectsStaleEvidenceAtResolveTime(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		memoryType    string
		basisRefs     []string
		removeRefKind string
		removeRefID   string
	}{
		{
			name:          "procedural task evidence removed",
			memoryType:    "procedure",
			basisRefs:     []string{"session:sess-memory-promo-stale-a", "task:task-memory-promo-stale-a", "task:task-memory-promo-stale-b"},
			removeRefKind: "task",
			removeRefID:   "task-memory-promo-stale-a",
		},
		{
			name:          "identity session evidence removed",
			memoryType:    "self_model",
			basisRefs:     []string{"session:sess-memory-promo-stale-a", "session:sess-memory-promo-stale-b", "session:sess-memory-promo-stale-c", "task:task-memory-promo-stale-b"},
			removeRefKind: "session",
			removeRefID:   "sess-memory-promo-stale-b",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			store := sqlite.NewTestStore(t)
			ctx := context.Background()

			const (
				workspaceID = "ws-memory-promo-stale-evidence"
				agentID     = "agent-memory-promo-stale-evidence"
			)

			if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
				WorkspaceID: workspaceID,
				Title:       "Memory Promotion Stale Evidence",
				CreatedBy:   "developer",
			}); err != nil {
				t.Fatalf("create workspace: %v", err)
			}
			claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
			if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
				WorkspaceID: workspaceID,
				AgentID:     agentID,
				OwnerUserID: "developer",
				DisplayName: "Memory Promotion Stale Evidence Agent",
			}); err != nil {
				t.Fatalf("register agent: %v", err)
			}
			for _, sessionID := range []string{"sess-memory-promo-stale-a", "sess-memory-promo-stale-b", "sess-memory-promo-stale-c"} {
				if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
					SessionID:   sessionID,
					AgentID:     agentID,
					WorkspaceID: workspaceID,
					StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
				}); err != nil {
					t.Fatalf("create session %s: %v", sessionID, err)
				}
			}
			for _, taskID := range []string{"task-memory-promo-stale-a", "task-memory-promo-stale-b"} {
				createWorkspaceTask(t, ctx, store, workspaceID, taskID)
			}

			record, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
				WorkspaceID: workspaceID,
				Candidate: sqlite.MemoryPromotionCandidate{
					MemoryType: tc.memoryType,
					Body:       "Resolve should recheck basis evidence freshness before accepting promotion.",
					SessionID:  "sess-memory-promo-stale-a",
					SourceKind: "memory_packet_shell",
					SourceID:   "shell-packet-stale-evidence-" + strings.ReplaceAll(tc.name, " ", "-"),
				},
				BasisDigest: "basis-digest-" + strings.ReplaceAll(tc.name, " ", "-"),
				BasisRefs:   tc.basisRefs,
				ProposedBy:  agentID,
			})
			if err != nil {
				t.Fatalf("enqueue memory promotion: %v", err)
			}

			switch tc.removeRefKind {
			case "session":
				if _, err := store.DB().ExecContext(ctx, `DELETE FROM agent_sessions WHERE workspace_id = ? AND session_id = ?`, workspaceID, tc.removeRefID); err != nil {
					t.Fatalf("delete stale evidence session %s: %v", tc.removeRefID, err)
				}
			case "task":
				if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_tasks WHERE workspace_id = ? AND task_id = ?`, workspaceID, tc.removeRefID); err != nil {
					t.Fatalf("delete stale evidence task %s: %v", tc.removeRefID, err)
				}
			default:
				t.Fatalf("unsupported removeRefKind %q", tc.removeRefKind)
			}

			resolved, err := store.ResolveMemoryPromotion(ctx, sqlite.MemoryPromotionResolveInput{
				WorkspaceID: workspaceID,
				PromotionID: record.PromotionID,
				Resolution:  "ACCEPTED",
				ResolvedBy:  "developer",
			})
			if err == nil {
				t.Fatalf("expected stale evidence acceptance to fail, got %+v", resolved)
			}
			if !strings.Contains(strings.ToLower(err.Error()), "invalid evidence") && !strings.Contains(strings.ToLower(err.Error()), "not found in workspace") {
				t.Fatalf("expected stale evidence rejection, got %v", err)
			}

			persisted, err := store.GetMemoryPromotion(ctx, workspaceID, record.PromotionID)
			if err != nil {
				t.Fatalf("get memory promotion after stale resolve: %v", err)
			}
			if persisted.State != "PENDING" || persisted.AppliedID != "" {
				t.Fatalf("expected promotion to remain pending after stale evidence rejection, got %+v", persisted)
			}
			if _, err := store.GetWorkspaceMemory(ctx, workspaceID, record.TargetMemoryID); err == nil {
				t.Fatalf("expected stale evidence acceptance not to materialize workspace memory %s", record.TargetMemoryID)
			}
			events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "memory.promotion_resolved",
				EntityType:  "memory_promotion",
				EntityID:    record.PromotionID,
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list stale resolve runtime events: %v", err)
			}
			if len(events) != 0 {
				t.Fatalf("expected no resolve runtime event after stale evidence rejection, got %+v", events)
			}
		})
	}
}
