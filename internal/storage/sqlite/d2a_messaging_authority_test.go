package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestSendMessageWithAuthorityEventRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-message-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Message Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	_, _, err := store.SendMessageWithAuthorityEvent(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "should fail closed without workspace authority",
	})
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
	}

	if got := countMessagingRows(t, ctx, store, workspaceID, "agent_messages"); got != 0 {
		t.Fatalf("expected no message rows after missing-authority reject, got %d", got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no agent_message.sent events after authority reject, got %d", got)
	}
}

func TestSendMessageRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-message-helper-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Message Helper Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	_, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "generic helper should fail closed without workspace authority",
	})
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
	}

	if got := countMessagingRows(t, ctx, store, workspaceID, "agent_messages"); got != 0 {
		t.Fatalf("expected no message rows after missing-authority reject, got %d", got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no agent_message.sent events after authority reject, got %d", got)
	}
}

func TestCreateAgentRequestWithAuthorityEventRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-request-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Request Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	_, _, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"task":"authority"}`,
	})
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
	}

	if got := countMessagingRows(t, ctx, store, workspaceID, "agent_requests"); got != 0 {
		t.Fatalf("expected no agent_requests rows after missing-authority reject, got %d", got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_request.sent",
		EntityType:  "agent_request",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no agent_request.sent events after authority reject, got %d", got)
	}
}

func TestCreateAgentRequestRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-request-helper-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Request Helper Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	_, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"task":"helper-authority"}`,
	})
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
	}

	if got := countMessagingRows(t, ctx, store, workspaceID, "agent_requests"); got != 0 {
		t.Fatalf("expected no agent_requests rows after missing-authority reject, got %d", got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_request.sent",
		EntityType:  "agent_request",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no agent_request.sent events after authority reject, got %d", got)
	}
}

func TestRespondAgentRequestWithAuthorityEventRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-respond-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Respond Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	requestID, _, err := store.CreateAgentRequestWithEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"task":"stale"}`,
	})
	if err != nil {
		t.Fatalf("seed agent request: %v", err)
	}
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-2701")

	_, err = store.RespondAgentRequestWithAuthorityEvent(ctx, requestID, `{"status":"ok"}`)
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}

	requestRecord, err := store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("reload request after stale-authority reject: %v", err)
	}
	if requestRecord.Status != "PENDING" || requestRecord.Response != "" || requestRecord.RespondedAt != "" {
		t.Fatalf("expected request to remain pending after stale-authority reject, got %+v", requestRecord)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_response.recorded",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no agent_response.recorded events after stale-authority reject, got %d", got)
	}
	assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectStale)
}

func TestRespondAgentRequestRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-respond-helper-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Respond Helper Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	requestID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"task":"helper-stale"}`,
	})
	if err != nil {
		t.Fatalf("seed agent request: %v", err)
	}
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-2702")

	err = store.RespondAgentRequest(ctx, requestID, `{"status":"ok"}`)
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}

	requestRecord, err := store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("reload request after stale-authority reject: %v", err)
	}
	if requestRecord.Status != "PENDING" || requestRecord.Response != "" || requestRecord.RespondedAt != "" {
		t.Fatalf("expected request to remain pending after stale-authority reject, got %+v", requestRecord)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_response.recorded",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no agent_response.recorded events after stale-authority reject, got %d", got)
	}
	assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectStale)
}

func registerMessagingAuthorityTestAgents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, agentIDs ...string) {
	t.Helper()

	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "tests",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s in %s: %v", agentID, workspaceID, err)
		}
	}
}

func countMessagingRows(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, table string) int {
	t.Helper()

	var count int
	query := "SELECT COUNT(*) FROM " + table + " WHERE workspace_id = ?"
	if err := store.DB().QueryRowContext(ctx, query, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count %s rows for %s: %v", table, workspaceID, err)
	}
	return count
}
