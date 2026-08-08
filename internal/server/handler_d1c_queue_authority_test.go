package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceOpsUpsertRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-queue-upsert-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Queue Upsert Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:d1c-upsert-missing-authority",
		QueueType:   "FOLLOW_UP",
		Title:       "Missing authority queue",
		Summary:     "should fail before queue write",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("marshal queue upsert params: %v", err)
	}

	result, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectMissing) || details["surface"] != "workspace.ops.upsert" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list operator queue items after missing-authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no operator queue rows on missing-authority reject, got %+v", items)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EntityType: "operator_queue", Limit: 10}); err != nil {
		t.Fatalf("list queue runtime events after missing-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no operator_queue runtime events on missing-authority reject, got %+v", events)
	}
}

func TestWorkspaceOpsResolveRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-queue-resolve-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Queue Resolve Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:d1c-resolve-stale-authority",
		QueueType:   "FOLLOW_UP",
		Title:       "Resolve stale authority queue",
		Summary:     "seed queue",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-202")

	raw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "dashboard",
		Resolution:  "should fail closed on stale authority",
	})
	if err != nil {
		t.Fatalf("marshal queue resolve params: %v", err)
	}

	result, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "workspace.ops.resolve" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	reloaded, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after stale-authority reject: %v", err)
	}
	if reloaded.Status != "OPEN" || reloaded.Resolution != "" {
		t.Fatalf("expected stale authority reject not to resolve queue, got %+v", reloaded)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list resolved queue runtime events after stale-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no operator_queue.resolved event on stale-authority reject, got %+v", events)
	}
}

func TestWorkspaceOpsEscalateRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-queue-escalate-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Queue Escalate Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:d1c-escalate-stale-authority",
		QueueType:   "FOLLOW_UP",
		Title:       "Escalate stale authority queue",
		Summary:     "seed queue",
		AssignedTo:  "reviewer-a",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-303")

	raw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		EscalatedBy: "dashboard",
		Reason:      "should fail closed on stale authority",
		AssignedTo:  "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal queue escalate params: %v", err)
	}

	result, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "workspace.ops.escalate" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	reloaded, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after stale-authority reject: %v", err)
	}
	if reloaded.AssignedTo != queue.AssignedTo || reloaded.EscalationCount != queue.EscalationCount {
		t.Fatalf("expected stale authority reject not to mutate queue, got %+v", reloaded)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list escalated runtime events after stale-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no operator_queue.escalated event on stale-authority reject, got %+v", events)
	}
}

func TestActionStartRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-action-start-stale-authority"
		taskID      = "task-d1c-action-start-stale-authority"
		agentID     = "agent-d1c-action-start-stale-authority"
		queueKey    = "tension_rebase_followup:tens-d1c-action-start-stale-authority"
	)

	_, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "d1c-action-start-stale-authority")
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-404")

	raw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "should fail closed on stale authority",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}

	result, rpcErr := h.actionStart(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "action.start" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("reload action after stale-authority reject: %v", err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("expected stale authority reject not to start action, got %+v", action)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list action.started runtime events after stale-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no action.started event on stale-authority reject, got %+v", events)
	}
}

func TestWorkspaceClaimReviewRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-claim-review-missing-authority-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Claim Review Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Review under missing authority RPC",
		Body:        "review should fail closed",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	raw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "tests",
		Reason:      "should fail closed",
		DueAt:       "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal claim review params: %v", err)
	}
	result, rpcErr := h.workspaceClaimReview(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on missing authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectMissing) || details["surface"] != "workspace.claim.review" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	reloaded, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("reload claim after missing-authority reject: %v", err)
	}
	if reloaded.Status != claim.Status {
		t.Fatalf("expected missing-authority reject not to mutate claim, got %+v", reloaded)
	}
}

func TestWorkspaceClaimEscalateRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-claim-escalate-stale-authority-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Claim Escalate Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Escalate under stale authority RPC",
		Body:        "claim escalate should fail closed",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	if _, _, _, _, _, err := store.RequestKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "tests",
		Reason:      "seed review workflow",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-a",
	}); err != nil {
		t.Fatalf("seed review workflow: %v", err)
	}
	queueItems, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{WorkspaceID: workspaceID, QueueType: "FOLLOW_UP", Limit: 10})
	if err != nil || len(queueItems) != 1 {
		t.Fatalf("expected seeded follow-up queue, got %+v err=%v", queueItems, err)
	}
	seedQueue := queueItems[0]
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-505")

	raw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "tests",
		Reason:      "should fail closed",
		DueAt:       "2099-01-02T00:00:00Z",
		AssignedTo:  "reviewer-b",
		Urgency:     "CRITICAL",
	})
	if err != nil {
		t.Fatalf("marshal claim escalate params: %v", err)
	}
	result, rpcErr := h.workspaceClaimEscalate(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "workspace.claim.escalate" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	reloadedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, seedQueue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after stale-authority reject: %v", err)
	}
	if reloadedQueue.AssignedTo != seedQueue.AssignedTo || reloadedQueue.EscalationCount != seedQueue.EscalationCount {
		t.Fatalf("expected stale-authority reject not to mutate review queue, got %+v", reloadedQueue)
	}
}

func TestWorkspaceClaimArchiveRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-claim-archive-stale-authority-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Claim Archive Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Archive under stale authority RPC",
		Body:        "claim archive should fail closed",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	if _, _, _, _, _, err := store.RequestKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "tests",
		Reason:      "seed review workflow",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-a",
	}); err != nil {
		t.Fatalf("seed review workflow: %v", err)
	}
	queueItems, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{WorkspaceID: workspaceID, QueueType: "FOLLOW_UP", Limit: 10})
	if err != nil || len(queueItems) != 1 {
		t.Fatalf("expected seeded follow-up queue, got %+v err=%v", queueItems, err)
	}
	seedQueue := queueItems[0]
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-606")

	raw, err := json.Marshal(workspaceClaimArchiveParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ArchivedBy:  "tests",
		Reason:      "should fail closed",
	})
	if err != nil {
		t.Fatalf("marshal claim archive params: %v", err)
	}
	result, rpcErr := h.workspaceClaimArchive(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "workspace.claim.archive" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	reloadedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, seedQueue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after stale-authority reject: %v", err)
	}
	if reloadedQueue.Status != "OPEN" {
		t.Fatalf("expected stale-authority reject not to resolve review queue, got %+v", reloadedQueue)
	}
}

func TestWorkspaceMemoryPromotionEnqueueRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-memory-promotion-enqueue-missing-authority-rpc"
		agentID     = "agent-d1c-memory-promotion-enqueue-missing-authority-rpc"
		sessionID   = "sess-d1c-memory-promotion-enqueue-missing-authority-rpc"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Memory Promotion Enqueue Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "D1C Memory Promotion Missing Authority RPC Agent",
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

	raw, err := json.Marshal(workspaceMemoryPromotionEnqueueParams{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Body:        "enqueue should fail closed without workspace authority",
		SessionID:   sessionID,
		SourceKind:  "episode_pack",
		SourceID:    "pack-d1c-memory-promotion-enqueue-rpc",
		BasisDigest: "basis-d1c-memory-promotion-enqueue-rpc",
		BasisRefs:   []string{"session:" + sessionID},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("marshal enqueue params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPromotionEnqueue(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on missing-authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectMissing) || details["surface"] != "workspace.memory.promotion.enqueue" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if promotions, err := store.ListMemoryPromotions(ctx, sqlite.MemoryPromotionFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list memory promotions after authority reject: %v", err)
	} else if len(promotions) != 0 {
		t.Fatalf("expected no promotion rows after authority reject, got %+v", promotions)
	}
}

func TestWorkspaceMemoryPromotionResolveRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-memory-promotion-resolve-stale-authority-rpc"
		agentID     = "agent-d1c-memory-promotion-resolve-stale-authority-rpc"
		sessionID   = "sess-d1c-memory-promotion-resolve-stale-authority-rpc"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Memory Promotion Resolve Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "D1C Memory Promotion Resolve Stale Authority RPC Agent",
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
	record, _, err := store.EnqueueMemoryPromotionWithEvent(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "lesson",
			Body:       "resolve should fail closed under stale workspace authority",
			SessionID:  sessionID,
			SourceKind: "episode_pack",
			SourceID:   "pack-d1c-memory-promotion-resolve-rpc",
		},
		BasisDigest: "basis-d1c-memory-promotion-resolve-rpc",
		BasisRefs:   []string{"session:" + sessionID},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}
	queueItems, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{WorkspaceID: workspaceID, QueueType: "FOLLOW_UP", Limit: 10})
	if err != nil || len(queueItems) != 1 {
		t.Fatalf("expected seeded memory promotion queue, got %+v err=%v", queueItems, err)
	}
	seedQueue := queueItems[0]
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-707")

	raw, err := json.Marshal(workspaceMemoryPromotionResolveParams{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "ACCEPTED",
		ResolvedBy:  "tests",
	})
	if err != nil {
		t.Fatalf("marshal resolve params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPromotionResolve(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale-authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "workspace.memory.promotion.resolve" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	persisted, err := store.GetMemoryPromotion(ctx, workspaceID, record.PromotionID)
	if err != nil {
		t.Fatalf("reload memory promotion after stale-authority reject: %v", err)
	}
	if persisted.State != "PENDING" || persisted.AppliedID != "" {
		t.Fatalf("expected stale-authority reject not to resolve promotion, got %+v", persisted)
	}
	reloadedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, seedQueue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after stale-authority reject: %v", err)
	}
	if reloadedQueue.Status != "OPEN" || reloadedQueue.Resolution != "" {
		t.Fatalf("expected stale-authority reject not to mutate queue, got %+v", reloadedQueue)
	}
}

func transferServerTestWorkspaceAuthorityToPeer(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, current sqlite.WorkspaceAuthorityRecord, peerNodeID string) {
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
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
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
		t.Fatalf("transfer workspace authority to peer: %v", err)
	}
}
