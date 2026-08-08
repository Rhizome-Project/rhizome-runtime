package sqlite

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestAppendRuntimeEventWithOptionalAuthorityTxAllowsTrulyAbsentAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-optional-authority-absent"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Optional Authority Absent",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	event, err := store.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, WorkspaceAuthorityRecord{}, RuntimeEventInput{
		EventID:     "rtev-d2a-optional-authority-absent",
		WorkspaceID: workspaceID,
		EventType:   "d2a.optional_authority_absent_probe",
		EntityType:  "d2a_probe",
		EntityID:    "probe-absent",
		ActorType:   "tester",
		ActorID:     "tester-a",
	})
	if err != nil {
		t.Fatalf("append runtime event without authority envelope: %v", err)
	}
	if event.AuthorityHolderNodeID != "" || event.AuthorityTerm != 0 || event.AuthorityLeaseTokenFingerprint != "" {
		t.Fatalf("expected raw runtime event without authority metadata, got %+v", event)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
}

func TestAppendAuthorityBackedRuntimeEventTxRejectsIncompleteAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-authority-backed-append-invalid"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Invalid Authority Append",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = store.appendAuthorityBackedRuntimeEventTx(ctx, tx, WorkspaceAuthorityRecord{
		WorkspaceID: workspaceID,
		Scope:       authorityScopeWorkspace,
		Status:      WorkspaceAuthorityStatusActive,
	}, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "d2a.invalid_authority_probe",
		EntityType:  "d2a_probe",
		EntityID:    "probe-1",
		ActorType:   "tester",
		ActorID:     "tester-a",
	})
	if err == nil {
		t.Fatal("expected authority-backed append to reject incomplete authority")
	}
	if !strings.Contains(err.Error(), "requires") {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestAppendRuntimeEventWithOptionalAuthorityTxRejectsPartialAuthorityEnvelope(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-optional-authority-partial"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Optional Authority Partial",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = store.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, WorkspaceAuthorityRecord{
		Scope:                 authorityScopeWorkspace,
		HolderAuthorityNodeID: "authnode-partial-probe",
		LeaseToken:            "lease-partial-probe",
		Term:                  1,
		Status:                WorkspaceAuthorityStatusActive,
	}, RuntimeEventInput{
		EventID:     "rtev-d2a-optional-authority-partial",
		WorkspaceID: workspaceID,
		EventType:   "d2a.optional_authority_partial_probe",
		EntityType:  "d2a_probe",
		EntityID:    "probe-partial",
		ActorType:   "tester",
		ActorID:     "tester-b",
	})
	if err == nil {
		t.Fatal("expected partial authority envelope to fail closed")
	}

	var runtimeEventCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND event_type = ?`, workspaceID, "d2a.optional_authority_partial_probe").Scan(&runtimeEventCount); err != nil {
		t.Fatalf("count partial-authority runtime events: %v", err)
	}
	if runtimeEventCount != 0 {
		t.Fatalf("expected no runtime event rows after partial authority reject, got %d", runtimeEventCount)
	}
}

func TestSendMessageWithOptionalAuthorityTxRejectsPartialAuthorityWithoutPersistedRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-message-partial-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Message Partial Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{"agent-sender", "agent-receiver"} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "tests",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	_, _, err = store.sendMessageWithOptionalAuthorityTx(ctx, tx, WorkspaceAuthorityRecord{
		Scope:                 authorityScopeWorkspace,
		HolderAuthorityNodeID: "authnode-partial-message",
		LeaseToken:            "lease-partial-message",
		Term:                  2,
		Status:                WorkspaceAuthorityStatusActive,
	}, MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-sender",
		ToAgentID:   "agent-receiver",
		Content:     "partial authority should fail closed",
	})
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("expected partial authority message send to fail closed")
	}
	if rollbackErr := tx.Rollback(); rollbackErr != nil {
		t.Fatalf("rollback tx: %v", rollbackErr)
	}

	var messageCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_messages WHERE workspace_id = ?`, workspaceID).Scan(&messageCount); err != nil {
		t.Fatalf("count agent_messages: %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("expected no persisted agent_messages after rollback, got %d", messageCount)
	}

	var runtimeEventCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND event_type = 'agent_message.sent'`, workspaceID).Scan(&runtimeEventCount); err != nil {
		t.Fatalf("count runtime_events: %v", err)
	}
	if runtimeEventCount != 0 {
		t.Fatalf("expected no persisted agent_message.sent runtime events after rollback, got %d", runtimeEventCount)
	}
}

func TestRecordAgentSessionCoordinationWithAuthorityTxRejectsPartialAuthorityWithoutPersistedRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-session-partial-authority"
		agentID     = "agent-d2a-session-partial-authority"
		sessionID   = "sess-d2a-session-partial-authority"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Session Partial Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	_, _, err = store.recordAgentSessionCoordinationWithAuthorityTx(ctx, tx, WorkspaceAuthorityRecord{
		Scope:                 authorityScopeWorkspace,
		HolderAuthorityNodeID: "authnode-partial-session",
		LeaseToken:            "lease-partial-session",
		Term:                  3,
		Status:                WorkspaceAuthorityStatusActive,
	}, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "partial authority should fail closed",
	})
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("expected partial authority session coordination to fail closed")
	}
	if rollbackErr := tx.Rollback(); rollbackErr != nil {
		t.Fatalf("rollback tx: %v", rollbackErr)
	}

	var sessionCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE workspace_id = ? AND session_id = ?`, workspaceID, sessionID).Scan(&sessionCount); err != nil {
		t.Fatalf("count agent_sessions: %v", err)
	}
	if sessionCount != 0 {
		t.Fatalf("expected no persisted agent_sessions after rollback, got %d", sessionCount)
	}

	var runtimeEventCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND event_type = 'session.start' AND entity_id = ?`, workspaceID, sessionID).Scan(&runtimeEventCount); err != nil {
		t.Fatalf("count session.start runtime events: %v", err)
	}
	if runtimeEventCount != 0 {
		t.Fatalf("expected no persisted session.start runtime events after rollback, got %d", runtimeEventCount)
	}
}
