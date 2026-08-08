package server

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceOpsUpsertRejectsAgentScopeSpoof(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-p0-spoof-ops-upsert", "agent", "agent-a")

	const workspaceID = "ws-p0-spoof-ops-upsert"
	createP0SpoofWorkspace(t, ctx, store, workspaceID)
	registerP0SpoofAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")

	result, rpcErr := h.workspaceOpsUpsert(ctx, mustJSONRaw(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:p0-spoof-upsert",
		QueueType:   "FOLLOW_UP",
		Title:       "Spoofed agent scope",
		SourceKind:  "manual",
		SourceID:    "agent-b",
		AgentID:     "agent-b",
	}))
	if rpcErr == nil {
		t.Fatal("expected spoofed agent scope to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "agent_id") {
		t.Fatalf("unexpected spoof reject %+v", rpcErr)
	}
	if got := countP0SpoofQueues(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no operator queue rows after spoof reject, got %d", got)
	}

	result, rpcErr = h.workspaceOpsUpsert(ctx, mustJSONRaw(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:p0-spoof-upsert-source",
		QueueType:   "FOLLOW_UP",
		Title:       "Spoofed agent source scope",
		SourceKind:  "agent",
		SourceID:    "agent-b",
		AgentID:     "agent-a",
	}))
	if rpcErr == nil {
		t.Fatal("expected spoofed agent source scope to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on source spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "source_id") {
		t.Fatalf("unexpected source spoof reject %+v", rpcErr)
	}
	if got := countP0SpoofQueues(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no operator queue rows after source spoof reject, got %d", got)
	}

	result, rpcErr = h.workspaceOpsRequest(ctx, mustJSONRaw(workspaceOpsRequestParams{
		WorkspaceID: workspaceID,
		RequestKey:  "request:p0-spoof-source",
		GateType:    "operator_review",
		Title:       "Spoofed request source scope",
		SourceKind:  "agent",
		SourceID:    "agent-b",
		AgentID:     "agent-a",
	}))
	if rpcErr == nil {
		t.Fatal("expected spoofed request source scope to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on request source spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "source_id") {
		t.Fatalf("unexpected request source spoof reject %+v", rpcErr)
	}
	if got := countP0SpoofQueues(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no operator queue rows after request source spoof reject, got %d", got)
	}
}

func TestWorkspaceOpsResolveRejectsWorkspaceAndResolvedBySpoof(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceA = "ws-p0-spoof-ops-resolve-a"
		workspaceB = "ws-p0-spoof-ops-resolve-b"
	)
	createP0SpoofWorkspace(t, ctx, store, workspaceA)
	createP0SpoofWorkspace(t, ctx, store, workspaceB)
	queueB := createP0SpoofQueue(t, ctx, store, workspaceB, "queue:p0-spoof-resolve-b")

	result, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceA, "human", "operator-a"), mustJSONRaw(workspaceOpsResolveParams{
		WorkspaceID: workspaceB,
		QueueID:     queueB.QueueID,
		ResolvedBy:  "operator-a",
		Resolution:  "cross-workspace payload must fail",
	}))
	if rpcErr == nil {
		t.Fatal("expected cross-workspace resolve spoof to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on cross-workspace spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected cross-workspace spoof reject %+v", rpcErr)
	}
	assertP0SpoofQueueOpen(t, ctx, store, workspaceB, queueB.QueueID)

	result, rpcErr = h.workspaceOpsResolve(testAuthContext(workspaceB, "human", "operator-a"), mustJSONRaw(workspaceOpsResolveParams{
		WorkspaceID: workspaceB,
		QueueID:     queueB.QueueID,
		ResolvedBy:  "operator-b",
		Resolution:  "operator spoof must fail",
	}))
	if rpcErr == nil {
		t.Fatal("expected resolved_by spoof to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on resolved_by spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "resolved_by") {
		t.Fatalf("unexpected resolved_by spoof reject %+v", rpcErr)
	}
	assertP0SpoofQueueOpen(t, ctx, store, workspaceB, queueB.QueueID)
}

func TestWorkspaceOpsEscalateRejectsEscalatedBySpoof(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-p0-spoof-ops-escalate"
	createP0SpoofWorkspace(t, ctx, store, workspaceID)
	queue := createP0SpoofQueue(t, ctx, store, workspaceID, "queue:p0-spoof-escalate")

	result, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "human", "operator-a"), mustJSONRaw(workspaceOpsEscalateParams{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		EscalatedBy: "operator-b",
		Reason:      "operator spoof must fail",
		AssignedTo:  "reviewer-b",
	}))
	if rpcErr == nil {
		t.Fatal("expected escalated_by spoof to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on escalated_by spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "escalated_by") {
		t.Fatalf("unexpected escalated_by spoof reject %+v", rpcErr)
	}
	reloaded, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after spoof reject: %v", err)
	}
	if reloaded.AssignedTo != queue.AssignedTo || reloaded.EscalationCount != queue.EscalationCount {
		t.Fatalf("expected queue to remain unchanged after spoof reject, got %+v", reloaded)
	}
}

func TestWorkspaceClaimWriteRejectsAgentScopeSpoof(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-p0-spoof-claim-write", "agent", "agent-a")

	const workspaceID = "ws-p0-spoof-claim-write"
	createP0SpoofWorkspace(t, ctx, store, workspaceID)
	registerP0SpoofAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")

	tests := []struct {
		name   string
		params workspaceClaimWriteParams
		needle string
	}{
		{
			name: "agent_id mismatch",
			params: workspaceClaimWriteParams{
				WorkspaceID: workspaceID,
				ClaimID:     "claim-p0-spoof-agent-id",
				ClaimType:   "FACT",
				Subject:     "Spoofed agent id",
				Body:        "agent-a must not write as agent-b",
				SourceKind:  "manual",
				SourceID:    "operator",
				AgentID:     "agent-b",
			},
			needle: "agent_id",
		},
		{
			name: "agent source mismatch",
			params: workspaceClaimWriteParams{
				WorkspaceID: workspaceID,
				ClaimID:     "claim-p0-spoof-source-id",
				ClaimType:   "FACT",
				Subject:     "Spoofed source id",
				Body:        "agent-a must not claim agent-b source authorship",
				SourceKind:  "agent",
				SourceID:    "agent-b",
				AgentID:     "agent-a",
			},
			needle: "source_id",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, rpcErr := h.workspaceClaimWrite(ctx, mustJSONRaw(tc.params))
			if rpcErr == nil {
				t.Fatal("expected claim identity spoof to fail closed")
			}
			if result != nil {
				t.Fatalf("expected no result on claim identity spoof reject, got %+v", result)
			}
			if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, tc.needle) {
				t.Fatalf("unexpected claim identity spoof reject %+v", rpcErr)
			}
			if got := countKnowledgeClaimRows(t, ctx, store, workspaceID, tc.params.ClaimID); got != 0 {
				t.Fatalf("expected no claim row after spoof reject, got %d", got)
			}
		})
	}
}

func TestWorkspaceClaimLifecycleRejectsActorSpoof(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-p0-spoof-claim-lifecycle"
	createP0SpoofWorkspace(t, ctx, store, workspaceID)
	claim := createP0SpoofClaim(t, ctx, store, workspaceID, "claim-p0-spoof-lifecycle")

	result, rpcErr := h.workspaceClaimReview(testAuthContext(workspaceID, "human", "operator-a"), mustJSONRaw(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "operator-b",
		Reason:      "actor spoof must fail",
		DueAt:       "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-a",
	}))
	if rpcErr == nil {
		t.Fatal("expected claim actor spoof to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on actor spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "actor_id") {
		t.Fatalf("unexpected actor spoof reject %+v", rpcErr)
	}
	reloaded, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("reload claim after actor spoof reject: %v", err)
	}
	if reloaded.Status != claim.Status {
		t.Fatalf("expected claim status to remain %q, got %+v", claim.Status, reloaded)
	}
	if got := countP0SpoofQueues(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no review queue after actor spoof reject, got %d", got)
	}
}

func TestWorkspaceClaimArchiveRejectsArchivedBySpoof(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-p0-spoof-claim-archive"
	createP0SpoofWorkspace(t, ctx, store, workspaceID)
	claim := createP0SpoofClaim(t, ctx, store, workspaceID, "claim-p0-spoof-archive")

	result, rpcErr := h.workspaceClaimArchive(testAuthContext(workspaceID, "human", "operator-a"), mustJSONRaw(workspaceClaimArchiveParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ArchivedBy:  "operator-b",
		Reason:      "archived_by spoof must fail",
	}))
	if rpcErr == nil {
		t.Fatal("expected archived_by spoof to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on archived_by spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "archived_by") {
		t.Fatalf("unexpected archived_by spoof reject %+v", rpcErr)
	}
	reloaded, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("reload claim after archived_by spoof reject: %v", err)
	}
	if reloaded.Status == "ARCHIVED" || reloaded.ArchivedAt != nil {
		t.Fatalf("expected claim to remain unarchived after spoof reject, got %+v", reloaded)
	}
}

func TestWorkspacePolicyRejectsCreatedByAndSubjectSpoof(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-p0-spoof-policy"
	createP0SpoofWorkspace(t, ctx, store, workspaceID)

	result, rpcErr := h.workspacePolicyPut(testAuthContext(workspaceID, "human", "operator-a"), mustJSONRaw(workspacePolicyPutParams{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "deploy-tool",
		Effect:      "DENY",
		CreatedBy:   "operator-b",
	}))
	if rpcErr == nil {
		t.Fatal("expected created_by spoof to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on created_by spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "created_by") {
		t.Fatalf("unexpected created_by spoof reject %+v", rpcErr)
	}

	result, rpcErr = h.workspacePolicyPut(testAuthContext(workspaceID, "agent", "agent-a"), mustJSONRaw(workspacePolicyPutParams{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-b",
		Capability:  "tool.call",
		ToolID:      "deploy-tool",
		Effect:      "DENY",
		CreatedBy:   "agent-a",
	}))
	if rpcErr == nil {
		t.Fatal("expected policy subject spoof to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on subject spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "subject_id") {
		t.Fatalf("unexpected subject spoof reject %+v", rpcErr)
	}

	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-b",
		Capability:  "tool.call",
		ToolID:      "deploy-tool",
		Effect:      "DENY",
		CreatedBy:   "operator-a",
	}); err != nil {
		t.Fatalf("seed policy for check spoof: %v", err)
	}
	result, rpcErr = h.workspacePolicyCheck(testAuthContext(workspaceID, "agent", "agent-a"), mustJSONRaw(workspacePolicyCheckParams{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-b",
		Capability:  "tool.call",
		ToolID:      "deploy-tool",
	}))
	if rpcErr == nil {
		t.Fatal("expected policy check subject spoof to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on policy check subject spoof reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "subject_id") {
		t.Fatalf("unexpected policy check subject spoof reject %+v", rpcErr)
	}

	result, rpcErr = h.workspacePolicyList(testAuthContext(workspaceID, "agent", "agent-a"), mustJSONRaw(workspacePolicyListParams{
		WorkspaceID: workspaceID,
	}))
	if rpcErr == nil {
		t.Fatal("expected broad policy list by agent principal to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on broad policy list reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "subject_id") {
		t.Fatalf("unexpected broad policy list reject %+v", rpcErr)
	}
}

func TestP0AuthorityHandlersRejectNoPrincipalWithoutSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-p0-no-principal"
	createP0SpoofWorkspace(t, ctx, store, workspaceID)

	t.Run("workspace ops upsert", func(t *testing.T) {
		result, rpcErr := h.workspaceOpsUpsert(context.Background(), mustJSONRaw(workspaceOpsUpsertParams{
			WorkspaceID: workspaceID,
			QueueKey:    "queue:no-principal-upsert",
			QueueType:   "FOLLOW_UP",
			Title:       "no principal upsert",
		}))
		assertP0SpoofUnauthorizedNoResult(t, result, rpcErr)
		if got := countP0SpoofQueues(t, ctx, store, workspaceID); got != 0 {
			t.Fatalf("expected no operator queue rows after no-principal upsert reject, got %d", got)
		}
	})

	queue := createP0SpoofQueue(t, ctx, store, workspaceID, "queue:no-principal-resolve")
	t.Run("workspace ops resolve", func(t *testing.T) {
		result, rpcErr := h.workspaceOpsResolve(context.Background(), mustJSONRaw(workspaceOpsResolveParams{
			WorkspaceID: workspaceID,
			QueueID:     queue.QueueID,
			Resolution:  "DONE",
			ResolvedBy:  "operator-a",
		}))
		assertP0SpoofUnauthorizedNoResult(t, result, rpcErr)
		assertP0SpoofQueueOpen(t, ctx, store, workspaceID, queue.QueueID)
	})

	t.Run("workspace claim write", func(t *testing.T) {
		result, rpcErr := h.workspaceClaimWrite(context.Background(), mustJSONRaw(workspaceClaimWriteParams{
			WorkspaceID: workspaceID,
			ClaimID:     "claim-no-principal-write",
			ClaimType:   "FACT",
			Subject:     "no principal claim",
			Body:        "must not persist",
			SourceKind:  "manual",
			SourceID:    "operator-a",
		}))
		assertP0SpoofUnauthorizedNoResult(t, result, rpcErr)
		if got := countP0SpoofClaims(t, ctx, store, workspaceID); got != 0 {
			t.Fatalf("expected no claim rows after no-principal write reject, got %d", got)
		}
	})

	claim := createP0SpoofClaim(t, ctx, store, workspaceID, "claim-no-principal-archive")
	t.Run("workspace claim archive", func(t *testing.T) {
		result, rpcErr := h.workspaceClaimArchive(context.Background(), mustJSONRaw(workspaceClaimArchiveParams{
			WorkspaceID: workspaceID,
			ClaimID:     claim.ClaimID,
			ArchivedBy:  "operator-a",
			Reason:      "no principal archive",
		}))
		assertP0SpoofUnauthorizedNoResult(t, result, rpcErr)
		items, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{WorkspaceID: workspaceID, Limit: 10})
		if err != nil {
			t.Fatalf("list claims after archive reject: %v", err)
		}
		if len(items) != 1 || items[0].Status != "ACTIVE" {
			t.Fatalf("expected seeded claim to remain active after archive reject, got %+v", items)
		}
	})

	t.Run("workspace policy put", func(t *testing.T) {
		result, rpcErr := h.workspacePolicyPut(context.Background(), mustJSONRaw(workspacePolicyPutParams{
			WorkspaceID: workspaceID,
			SubjectType: "agent",
			SubjectID:   "agent-no-principal",
			Capability:  "tool.call",
			ToolID:      "deploy-tool",
			Effect:      "DENY",
			CreatedBy:   "operator-a",
		}))
		assertP0SpoofUnauthorizedNoResult(t, result, rpcErr)
		if got := countP0SpoofPolicies(t, ctx, store, workspaceID); got != 0 {
			t.Fatalf("expected no policy rows after no-principal put reject, got %d", got)
		}
	})

	t.Run("workspace policy list", func(t *testing.T) {
		if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
			WorkspaceID: workspaceID,
			SubjectType: "agent",
			SubjectID:   "agent-a",
			Capability:  "tool.call",
			ToolID:      "deploy-tool",
			Effect:      "DENY",
			CreatedBy:   "operator-a",
		}); err != nil {
			t.Fatalf("seed policy for no-principal list: %v", err)
		}
		result, rpcErr := h.workspacePolicyList(context.Background(), mustJSONRaw(workspacePolicyListParams{
			WorkspaceID: workspaceID,
		}))
		assertP0SpoofUnauthorizedNoResult(t, result, rpcErr)
	})
}

func createP0SpoofWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
}

func registerP0SpoofAgents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, agentIDs ...string) {
	t.Helper()

	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "tests",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s/%s: %v", workspaceID, agentID, err)
		}
	}
}

func createP0SpoofQueue(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueKey string) sqlite.OperatorQueueRecord {
	t.Helper()

	queue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    queueKey,
		QueueType:   "FOLLOW_UP",
		Title:       queueKey,
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("create queue %s/%s: %v", workspaceID, queueKey, err)
	}
	return queue
}

func createP0SpoofClaim(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, claimID string) sqlite.KnowledgeClaimRecord {
	t.Helper()

	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ClaimType:   "FACT",
		Subject:     claimID,
		Body:        "spoof negative fixture",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("create claim %s/%s: %v", workspaceID, claimID, err)
	}
	return claim
}

func countP0SpoofQueues(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_queue_items WHERE workspace_id = ?`, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count operator queue rows: %v", err)
	}
	return count
}

func countP0SpoofClaims(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) int {
	t.Helper()

	items, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{WorkspaceID: workspaceID, Limit: 100})
	if err != nil {
		t.Fatalf("list knowledge claims: %v", err)
	}
	return len(items)
}

func countP0SpoofPolicies(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) int {
	t.Helper()

	items, err := store.ListCapabilityPolicies(ctx, sqlite.CapabilityPolicyFilter{WorkspaceID: workspaceID, Limit: 100})
	if err != nil {
		t.Fatalf("list capability policies: %v", err)
	}
	return len(items)
}

func assertP0SpoofUnauthorizedNoResult(t *testing.T, result any, rpcErr *RPCError) {
	t.Helper()

	if rpcErr == nil {
		t.Fatal("expected no-principal call to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on no-principal reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "unauthorized") {
		t.Fatalf("unexpected no-principal reject %+v", rpcErr)
	}
}

func assertP0SpoofQueueOpen(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueID string) {
	t.Helper()

	reloaded, err := store.GetOperatorQueueItem(ctx, workspaceID, queueID, "")
	if err != nil {
		t.Fatalf("reload queue %s/%s: %v", workspaceID, queueID, err)
	}
	if reloaded.Status != "OPEN" || reloaded.Resolution != "" {
		t.Fatalf("expected queue to remain open after spoof reject, got %+v", reloaded)
	}
}
