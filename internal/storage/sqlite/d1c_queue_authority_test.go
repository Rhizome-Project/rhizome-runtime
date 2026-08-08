package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestUpsertOperatorQueueItemWithRuntimeEventRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-queue-runtime-missing-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Queue Runtime Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	_, err := store.UpsertOperatorQueueItemWithRuntimeEvent(ctx, OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:d1c-runtime-missing-authority",
		QueueType:   "FOLLOW_UP",
		Title:       "Runtime queue without authority",
		Summary:     "should fail closed",
		SourceKind:  "manual",
		SourceID:    "tests",
	}, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    "action-d1c-runtime-missing-authority",
		ActorType:   "operator",
		ActorID:     "tests",
		PayloadJSON: `{"status":"STARTED"}`,
	})
	if err == nil {
		t.Fatal("expected missing authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	if items, err := store.ListOperatorQueueItems(ctx, OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list queue items after authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no queue rows after authority reject, got %+v", items)
	}
	if events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list runtime events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no runtime events after authority reject, got %+v", events)
	}
}

func TestEscalateOperatorQueueItemRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-queue-escalate-missing-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Queue Escalate Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:d1c-escalate-missing-authority",
		QueueType:   "FOLLOW_UP",
		Title:       "Escalate under missing authority",
		Summary:     "seed queue",
		AssignedTo:  "reviewer-a",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	_, err = store.EscalateOperatorQueueItem(ctx, OperatorQueueEscalateInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		EscalatedBy: "tests",
		Reason:      "should fail closed",
		AssignedTo:  "reviewer-b",
	})
	if err == nil {
		t.Fatal("expected missing authority reject on escalate")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	reloaded, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after authority reject: %v", err)
	}
	if reloaded.AssignedTo != queue.AssignedTo || reloaded.EscalationCount != queue.EscalationCount || (reloaded.LastEscalatedBy != nil && strings.TrimSpace(*reloaded.LastEscalatedBy) != "") {
		t.Fatalf("expected escalate reject not to mutate queue, got %+v", reloaded)
	}
	if events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list escalated runtime events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no operator_queue.escalated event after authority reject, got %+v", events)
	}
}

func TestRequestKnowledgeClaimReviewWithEffectsRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-claim-review-missing-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Claim Review Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Claim review missing authority",
		Body:        "review should fail closed without authority",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	_, _, _, _, _, err = store.RequestKnowledgeClaimReviewWithEffects(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "tests",
		Reason:      "should fail closed",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-a",
	})
	if err == nil {
		t.Fatal("expected missing authority reject on claim review")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	reloaded, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("reload claim after authority reject: %v", err)
	}
	if reloaded.Status != claim.Status || reloaded.ReviewDueAt != nil {
		t.Fatalf("expected claim review reject not to mutate lifecycle, got %+v", reloaded)
	}
	if items, err := store.ListOperatorQueueItems(ctx, OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list operator queue items after claim review reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no queue rows after claim review reject, got %+v", items)
	}
	if events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_requested",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list claim review runtime events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no knowledge_claim.review_requested event after authority reject, got %+v", events)
	}
}

func TestEscalateKnowledgeClaimReviewWithEffectsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-claim-escalate-stale-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Claim Escalate Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Claim review escalation stale authority",
		Body:        "escalation should fail closed under stale authority",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	if _, _, _, _, _, err := store.RequestKnowledgeClaimReviewWithEffects(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "tests",
		Reason:      "seed review workflow",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-a",
	}); err != nil {
		t.Fatalf("seed claim review: %v", err)
	}
	queueItems, err := store.ListOperatorQueueItems(ctx, OperatorQueueFilter{WorkspaceID: workspaceID, QueueType: "FOLLOW_UP", Limit: 10})
	if err != nil || len(queueItems) != 1 {
		t.Fatalf("expected seeded follow-up queue, got %+v err=%v", queueItems, err)
	}
	seedQueue := queueItems[0]

	transferQueueAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-707")

	_, _, _, _, err = store.EscalateKnowledgeClaimReviewWithEffects(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "tests",
		Reason:      "should fail closed",
		AssignedTo:  "reviewer-b",
		Urgency:     "CRITICAL",
		ReviewDueAt: "2099-01-02T00:00:00Z",
	})
	if err == nil {
		t.Fatal("expected stale authority reject on claim review escalation")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject code, got %+v", reject)
	}

	reloadedClaim, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("reload claim after escalation authority reject: %v", err)
	}
	if reloadedClaim.Status != "REVIEW" || derefString(reloadedClaim.ReviewDueAt) != "2099-01-01T00:00:00Z" {
		t.Fatalf("expected escalation reject not to mutate review claim, got %+v", reloadedClaim)
	}
	reloadedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, seedQueue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after escalation authority reject: %v", err)
	}
	if reloadedQueue.AssignedTo != seedQueue.AssignedTo || reloadedQueue.EscalationCount != seedQueue.EscalationCount {
		t.Fatalf("expected escalation reject not to mutate queue, got %+v", reloadedQueue)
	}
	if events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_escalated",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list claim escalation runtime events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no knowledge_claim.review_escalated event after authority reject, got %+v", events)
	}
}

func TestArchiveKnowledgeClaimWithEffectsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-claim-archive-stale-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Claim Archive Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Claim archive stale authority",
		Body:        "archive should fail closed under stale authority",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	if _, _, _, _, _, err := store.RequestKnowledgeClaimReviewWithEffects(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "tests",
		Reason:      "seed review workflow",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-a",
	}); err != nil {
		t.Fatalf("seed claim review: %v", err)
	}
	queueItems, err := store.ListOperatorQueueItems(ctx, OperatorQueueFilter{WorkspaceID: workspaceID, QueueType: "FOLLOW_UP", Limit: 10})
	if err != nil || len(queueItems) != 1 {
		t.Fatalf("expected seeded follow-up queue, got %+v err=%v", queueItems, err)
	}
	seedQueue := queueItems[0]

	transferQueueAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-808")

	_, _, _, _, _, err = store.ArchiveKnowledgeClaimWithEffects(ctx, KnowledgeClaimArchiveInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ArchivedBy:  "tests",
		Reason:      "should fail closed",
	})
	if err == nil {
		t.Fatal("expected stale authority reject on claim archive")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject code, got %+v", reject)
	}

	reloadedClaim, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("reload claim after archive authority reject: %v", err)
	}
	if reloadedClaim.Status != "REVIEW" || reloadedClaim.ArchivedAt != nil {
		t.Fatalf("expected archive reject not to archive claim, got %+v", reloadedClaim)
	}
	reloadedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, seedQueue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after archive authority reject: %v", err)
	}
	if reloadedQueue.Status != "OPEN" {
		t.Fatalf("expected archive reject not to resolve queue, got %+v", reloadedQueue)
	}
	if events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list claim archive runtime events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no knowledge_claim.archived event after authority reject, got %+v", events)
	}
}

func TestEnqueueMemoryPromotionWithEventRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-memory-promotion-enqueue-missing-authority"
		agentID     = "agent-d1c-memory-promotion-enqueue-missing-authority"
		sessionID   = "sess-d1c-memory-promotion-enqueue-missing-authority"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Memory Promotion Enqueue Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "D1C Memory Promotion Missing Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	_, _, err := store.EnqueueMemoryPromotionWithEvent(ctx, MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: MemoryPromotionCandidate{
			MemoryType: "lesson",
			Body:       "enqueue should fail closed without workspace authority",
			SessionID:  sessionID,
			SourceKind: "episode_pack",
			SourceID:   "pack-d1c-memory-promotion-enqueue",
		},
		BasisDigest: "basis-d1c-memory-promotion-enqueue",
		BasisRefs:   []string{"session:" + sessionID},
		ProposedBy:  agentID,
	})
	if err == nil {
		t.Fatal("expected missing authority reject on memory promotion enqueue")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	if promotions, err := store.ListMemoryPromotions(ctx, MemoryPromotionFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list memory promotions after authority reject: %v", err)
	} else if len(promotions) != 0 {
		t.Fatalf("expected no memory promotion rows after authority reject, got %+v", promotions)
	}
	if items, err := store.ListOperatorQueueItems(ctx, OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list queue items after authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no operator queue rows after authority reject, got %+v", items)
	}
	if events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list runtime events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no runtime events after authority reject, got %+v", events)
	}
}

func TestResolveMemoryPromotionRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-memory-promotion-resolve-stale-authority"
		agentID     = "agent-d1c-memory-promotion-resolve-stale-authority"
		sessionID   = "sess-d1c-memory-promotion-resolve-stale-authority"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Memory Promotion Resolve Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "D1C Memory Promotion Stale Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	record, _, err := store.EnqueueMemoryPromotionWithEvent(ctx, MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: MemoryPromotionCandidate{
			MemoryType: "lesson",
			Body:       "resolve should fail closed under stale workspace authority",
			SessionID:  sessionID,
			SourceKind: "episode_pack",
			SourceID:   "pack-d1c-memory-promotion-resolve",
		},
		BasisDigest: "basis-d1c-memory-promotion-resolve",
		BasisRefs:   []string{"session:" + sessionID},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}
	queueItems, err := store.ListOperatorQueueItems(ctx, OperatorQueueFilter{WorkspaceID: workspaceID, QueueType: "FOLLOW_UP", Limit: 10})
	if err != nil || len(queueItems) != 1 {
		t.Fatalf("expected seeded memory promotion queue, got %+v err=%v", queueItems, err)
	}
	seedQueue := queueItems[0]

	transferQueueAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-909")

	_, err = store.ResolveMemoryPromotion(ctx, MemoryPromotionResolveInput{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "ACCEPTED",
		ResolvedBy:  "tests",
	})
	if err == nil {
		t.Fatal("expected stale authority reject on memory promotion resolve")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject code, got %+v", reject)
	}

	persisted, err := store.GetMemoryPromotion(ctx, workspaceID, record.PromotionID)
	if err != nil {
		t.Fatalf("reload memory promotion after authority reject: %v", err)
	}
	if persisted.State != memoryPromotionStatePending || persisted.AppliedID != "" {
		t.Fatalf("expected stale authority reject not to resolve promotion, got %+v", persisted)
	}
	reloadedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, seedQueue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after authority reject: %v", err)
	}
	if reloadedQueue.Status != "OPEN" || reloadedQueue.Resolution != "" {
		t.Fatalf("expected stale authority reject not to resolve queue, got %+v", reloadedQueue)
	}
	if _, err := store.GetWorkspaceMemory(ctx, workspaceID, record.TargetMemoryID); err == nil {
		t.Fatalf("expected no workspace memory write after stale authority reject for promotion %s", record.TargetMemoryID)
	}
	if events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    record.TargetMemoryID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list workspace memory recorded events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no workspace_memory.recorded event after authority reject, got %+v", events)
	}
	if events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.promotion_resolved",
		EntityType:  "memory_promotion",
		EntityID:    record.PromotionID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list memory promotion resolved events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no memory.promotion_resolved event after authority reject, got %+v", events)
	}
}

func TestEvaluateCoalitionBundleRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-bundle-missing-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Bundle Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	ensureTensionOverlayTables(t, ctx, store)

	_, err := store.EvaluateCoalitionBundle(ctx, workspaceID, "coalition-d1c-bundle-missing-authority", d1cBundleUtilityRebaseDecisionParams(), "patch-ref-d1c-bundle-missing-authority")
	if err == nil {
		t.Fatal("expected missing authority reject on EvaluateCoalitionBundle")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	var tensionCount int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type IN ('fork_candidate', 'repair')`,
		workspaceID,
	).Scan(&tensionCount); err != nil {
		t.Fatalf("count bundle tensions after authority reject: %v", err)
	}
	if tensionCount != 0 {
		t.Fatalf("expected no bundle tensions after authority reject, got %d", tensionCount)
	}

	var dependencyCount int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM workspace_tension_dependencies
		 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&dependencyCount); err != nil {
		t.Fatalf("count bundle dependencies after authority reject: %v", err)
	}
	if dependencyCount != 0 {
		t.Fatalf("expected no bundle dependencies after authority reject, got %d", dependencyCount)
	}

	if items, err := store.ListOperatorQueueItems(ctx, OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list operator queue after bundle authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no operator queue rows after bundle authority reject, got %+v", items)
	}

	if events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	}); err != nil {
		t.Fatalf("list runtime events after bundle authority reject: %v", err)
	} else {
		for _, event := range events {
			if event.EventType == "coalition_fork_generated" || event.EventType == "tension.emerged" || event.EventType == "operator_queue.rebase_followup_created" {
				t.Fatalf("expected no bundle runtime events after authority reject, got %+v", events)
			}
		}
	}
}

func TestEvaluateCoalitionBundleRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d1c-bundle-stale-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Bundle Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	ensureTensionOverlayTables(t, ctx, store)
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	transferQueueAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1001")

	_, err := store.EvaluateCoalitionBundle(ctx, workspaceID, "coalition-d1c-bundle-stale-authority", d1cBundleUtilityRebaseDecisionParams(), "patch-ref-d1c-bundle-stale-authority")
	if err == nil {
		t.Fatal("expected stale authority reject on EvaluateCoalitionBundle")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject code, got %+v", reject)
	}

	var tensionCount int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type IN ('fork_candidate', 'repair')`,
		workspaceID,
	).Scan(&tensionCount); err != nil {
		t.Fatalf("count bundle tensions after stale authority reject: %v", err)
	}
	if tensionCount != 0 {
		t.Fatalf("expected no bundle tensions after stale authority reject, got %d", tensionCount)
	}

	var dependencyCount int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM workspace_tension_dependencies
		 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&dependencyCount); err != nil {
		t.Fatalf("count bundle dependencies after stale authority reject: %v", err)
	}
	if dependencyCount != 0 {
		t.Fatalf("expected no bundle dependencies after stale authority reject, got %d", dependencyCount)
	}

	if items, err := store.ListOperatorQueueItems(ctx, OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list operator queue after stale bundle authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no operator queue rows after stale bundle authority reject, got %+v", items)
	}

	if events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	}); err != nil {
		t.Fatalf("list runtime events after stale bundle authority reject: %v", err)
	} else {
		for _, event := range events {
			if event.EventType == "coalition_fork_generated" || event.EventType == "tension.emerged" || event.EventType == "operator_queue.rebase_followup_created" {
				t.Fatalf("expected no bundle runtime events after stale authority reject, got %+v", events)
			}
		}
	}
}

func transferQueueAuthorityToPeer(t *testing.T, ctx context.Context, store *Store, workspaceID string, current WorkspaceAuthorityRecord, peerNodeID string) {
	t.Helper()

	referenceAt := time.Now().UTC().Round(0)
	var journalHead int64
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&journalHead); err != nil {
		t.Fatalf("query runtime journal head before transfer: %v", err)
	}
	commitWatermark := current.CommitWatermark + 1
	if journalHead > commitWatermark {
		commitWatermark = journalHead
	}
	appliedWatermark := current.AppliedWatermark + 1
	if appliedWatermark > commitWatermark {
		appliedWatermark = commitWatermark
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		peerNodeID,
		"sqlite_peer_store",
		"peer-host",
		"boot-"+peerNodeID,
		referenceAt.Format(time.RFC3339Nano),
		referenceAt.Format(time.RFC3339Nano),
		string(RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		Scope:                        "workspace",
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-" + peerNodeID,
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    "system",
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority: %v", err)
	}
}

func d1cBundleUtilityRebaseDecisionParams() BundleUtilityParams {
	return BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		RedundancyScore:                0.9,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaRedundancy:                0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.95,
	}
}
