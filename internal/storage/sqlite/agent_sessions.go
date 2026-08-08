package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

var ErrSessionNotFound = errors.New("session not found")
var ErrSessionNotActive = errors.New("session is not active")
var ErrSessionOwnershipMismatch = errors.New("session ownership mismatch")
var ErrSessionTakeoverNotAuthorized = errors.New("session takeover not authorized")
var ErrSessionTakeoverAgentSame = errors.New("takeover_agent_id must differ from the current session owner")
var ErrSessionTakeoverSuccessorSame = errors.New("successor_session_id must differ from session_id")
var ErrSessionTakeoverSuccessorExists = errors.New("successor session already exists")

// AgentSessionCreateInput is the input for creating an agent session.
type AgentSessionCreateInput struct {
	SessionID   string
	AgentID     string
	WorkspaceID string
	TaskID      string
	StartedAt   string
}

// AgentSessionUpdateInput is the input for updating an agent session.
type AgentSessionUpdateInput struct {
	SessionID         string
	Status            string
	Iterations        int
	TotalInputTokens  int
	TotalOutputTokens int
	ToolCalls         int
	StopReason        string
	ErrorMessage      string
	CompletedAt       string
}

// AgentSessionMessageInput is the input for appending a session message.
type AgentSessionMessageInput struct {
	SessionID   string
	Sequence    int
	Role        string
	ContentJSON string
	TokenCount  int
}

// AgentSessionRecord represents a stored agent session.
type AgentSessionRecord struct {
	SessionID         string `json:"session_id"`
	AgentID           string `json:"agent_id"`
	WorkspaceID       string `json:"workspace_id"`
	TaskID            string `json:"task_id,omitempty"`
	Status            string `json:"status"`
	Iterations        int    `json:"iterations"`
	TotalInputTokens  int    `json:"total_input_tokens"`
	TotalOutputTokens int    `json:"total_output_tokens"`
	ToolCalls         int    `json:"tool_calls"`
	StopReason        string `json:"stop_reason,omitempty"`
	ErrorMessage      string `json:"error_message,omitempty"`
	StartedAt         string `json:"started_at"`
	CompletedAt       string `json:"completed_at,omitempty"`
	CreatedAt         string `json:"created_at"`
}

// AgentSessionMessageRecord represents a stored session message.
type AgentSessionMessageRecord struct {
	ID          int    `json:"id"`
	SessionID   string `json:"session_id"`
	Sequence    int    `json:"sequence"`
	Role        string `json:"role"`
	ContentJSON string `json:"content_json"`
	TokenCount  int    `json:"token_count"`
	CreatedAt   string `json:"created_at"`
}

// CreateAgentSession inserts a new agent session.
//
// This is a low-level storage helper for tests and legacy fixtures. Runtime
// execution paths should use CreateAgentSessionWithRuntimeEvent so visible
// RUNNING state is created with a matching durable receipt.
func (s *Store) CreateAgentSession(ctx context.Context, input AgentSessionCreateInput) error {
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO agent_sessions (session_id, agent_id, workspace_id, task_id, status, started_at)
		 VALUES (?, ?, ?, ?, 'RUNNING', ?)`,
		input.SessionID, input.AgentID, input.WorkspaceID, input.TaskID, input.StartedAt,
	)
	if err != nil {
		return fmt.Errorf("create agent session: %w", err)
	}
	return nil
}

// CreateAgentSessionWithRuntimeEvent inserts a RUNNING session and its authority-backed receipt atomically.
func (s *Store) CreateAgentSessionWithRuntimeEvent(ctx context.Context, input AgentSessionCreateInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	sessionID := strings.TrimSpace(input.SessionID)
	agentID := strings.TrimSpace(input.AgentID)
	if workspaceID == "" || sessionID == "" || agentID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id, session_id, and agent_id are required")
	}
	startedAt := strings.TrimSpace(input.StartedAt)
	if startedAt == "" {
		startedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, startedAt)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin agent session start tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		taskID := strings.TrimSpace(input.TaskID)
		if taskID != "" {
			if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
				return err
			}
			if err := s.requireLiveSameOwnerClaimForSessionStartTx(ctx, tx, workspaceID, taskID, agentID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO agent_sessions (session_id, agent_id, workspace_id, task_id, status, started_at)
			 VALUES (?, ?, ?, ?, 'RUNNING', ?)`,
			sessionID, agentID, workspaceID, taskID, startedAt,
		); err != nil {
			return fmt.Errorf("create agent session: %w", err)
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "agent.session.started",
			EntityType:  "agent_session",
			EntityID:    sessionID,
			ActorType:   "agent",
			ActorID:     agentID,
			AgentID:     agentID,
			SessionID:   sessionID,
			TaskID:      taskID,
			PayloadJSON: mustJSON(map[string]any{
				"session_id": sessionID,
				"agent_id":   agentID,
				"task_id":    taskID,
				"status":     "RUNNING",
				"started_at": startedAt,
			}),
			CreatedAt: startedAt,
		})
		return appendErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit agent session start tx: %w", err)
	}
	return event, nil
}

// UpdateAgentSession updates an existing agent session.
//
// This is a low-level storage helper for tests and metric-only updates. Runtime
// execution paths that change lifecycle state should use
// UpdateAgentSessionWithRuntimeEvent.
func (s *Store) UpdateAgentSession(ctx context.Context, input AgentSessionUpdateInput) error {
	var currentStatus, currentCompletedAt string
	if err := s.writeDB.QueryRowContext(ctx,
		`SELECT status, COALESCE(completed_at, '') FROM agent_sessions WHERE session_id = ?`,
		input.SessionID,
	).Scan(&currentStatus, &currentCompletedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSessionNotFound
		}
		return fmt.Errorf("query agent session for metrics update: %w", err)
	}
	status := strings.TrimSpace(input.Status)
	completedAt := strings.TrimSpace(input.CompletedAt)
	if !model.IsSessionStatusActive(currentStatus) {
		status = currentStatus
		completedAt = currentCompletedAt
	} else {
		if status == "" {
			status = currentStatus
		}
	}
	_, err := s.writeDB.ExecContext(ctx,
		`UPDATE agent_sessions
		 SET status = ?, iterations = ?, total_input_tokens = ?, total_output_tokens = ?,
		     tool_calls = ?, stop_reason = ?, error_message = ?, completed_at = ?
		 WHERE session_id = ?`,
		status, input.Iterations, input.TotalInputTokens, input.TotalOutputTokens,
		input.ToolCalls, input.StopReason, input.ErrorMessage, completedAt,
		input.SessionID,
	)
	if err != nil {
		return fmt.Errorf("update agent session: %w", err)
	}
	return nil
}

// UpdateAgentSessionWithRuntimeEvent updates a session lifecycle state and records the receipt atomically.
func (s *Store) UpdateAgentSessionWithRuntimeEvent(ctx context.Context, input AgentSessionUpdateInput) (RuntimeEventRecord, error) {
	sessionID := strings.TrimSpace(input.SessionID)
	if sessionID == "" {
		return RuntimeEventRecord{}, errors.New("session_id is required")
	}
	status := strings.TrimSpace(input.Status)
	completedAt := strings.TrimSpace(input.CompletedAt)
	createdAt := completedAt
	if createdAt == "" {
		createdAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	var workspaceID string
	if err := s.writeDB.QueryRowContext(ctx, `SELECT workspace_id FROM agent_sessions WHERE session_id = ?`, sessionID).Scan(&workspaceID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RuntimeEventRecord{}, ErrSessionNotFound
		}
		return RuntimeEventRecord{}, fmt.Errorf("query agent session workspace: %w", err)
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, createdAt)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin agent session update tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var current AgentSessionRecord
		if err := tx.QueryRowContext(ctx, `
SELECT session_id, agent_id, workspace_id, COALESCE(task_id,''), status, iterations,
       total_input_tokens, total_output_tokens, tool_calls, COALESCE(stop_reason,''),
       COALESCE(error_message,''), started_at, COALESCE(completed_at,''), created_at
  FROM agent_sessions
 WHERE session_id = ? AND workspace_id = ?`,
			sessionID, workspaceID,
		).Scan(
			&current.SessionID,
			&current.AgentID,
			&current.WorkspaceID,
			&current.TaskID,
			&current.Status,
			&current.Iterations,
			&current.TotalInputTokens,
			&current.TotalOutputTokens,
			&current.ToolCalls,
			&current.StopReason,
			&current.ErrorMessage,
			&current.StartedAt,
			&current.CompletedAt,
			&current.CreatedAt,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrSessionNotFound
			}
			return fmt.Errorf("query agent session for update: %w", err)
		}
		if !model.IsSessionStatusActive(current.Status) {
			return sessionNotActiveError(sessionID, current.Status)
		}
		if status == "" {
			status = current.Status
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE agent_sessions
			 SET status = ?, iterations = ?, total_input_tokens = ?, total_output_tokens = ?,
			     tool_calls = ?, stop_reason = ?, error_message = ?, completed_at = ?
			 WHERE session_id = ? AND workspace_id = ? AND agent_id = ? AND status = ?`,
			status, input.Iterations, input.TotalInputTokens, input.TotalOutputTokens,
			input.ToolCalls, strings.TrimSpace(input.StopReason), strings.TrimSpace(input.ErrorMessage), completedAt,
			sessionID, current.WorkspaceID, current.AgentID, current.Status,
		)
		if err != nil {
			return fmt.Errorf("update agent session: %w", err)
		}
		if affected, _ := res.RowsAffected(); affected != 1 {
			return sessionOwnershipMismatchError(sessionID)
		}
		eventType := "agent.session.updated"
		if status == "COMPLETED" {
			eventType = "agent.session.completed"
		} else if status == "FAILED" || status == "ERROR" {
			eventType = "agent.session.failed"
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: current.WorkspaceID,
			EventType:   eventType,
			EntityType:  "agent_session",
			EntityID:    sessionID,
			ActorType:   "agent",
			ActorID:     current.AgentID,
			AgentID:     current.AgentID,
			SessionID:   sessionID,
			TaskID:      current.TaskID,
			PayloadJSON: mustJSON(map[string]any{
				"session_id":          sessionID,
				"agent_id":            current.AgentID,
				"task_id":             current.TaskID,
				"status":              status,
				"iterations":          input.Iterations,
				"total_input_tokens":  input.TotalInputTokens,
				"total_output_tokens": input.TotalOutputTokens,
				"tool_calls":          input.ToolCalls,
				"stop_reason":         strings.TrimSpace(input.StopReason),
				"error_message":       strings.TrimSpace(input.ErrorMessage),
				"completed_at":        completedAt,
			}),
			CreatedAt: createdAt,
		})
		return appendErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit agent session update tx: %w", err)
	}
	return event, nil
}

// AppendAgentSessionMessage appends a message to a session.
func (s *Store) AppendAgentSessionMessage(ctx context.Context, input AgentSessionMessageInput) error {
	if len(input.ContentJSON) > 1024*1024 {
		return fmt.Errorf("append session message: payload exceeds 1MB hard limit")
	}
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO agent_session_messages (session_id, sequence, role, content_json, token_count)
		 VALUES (?, ?, ?, ?, ?)`,
		input.SessionID, input.Sequence, input.Role, input.ContentJSON, input.TokenCount,
	)
	if err != nil {
		return fmt.Errorf("append session message: %w", err)
	}
	return nil
}

// ReplaceAgentSessionMessages replaces the canonical runtime prompt state for a session.
func (s *Store) ReplaceAgentSessionMessages(ctx context.Context, sessionID string, messages []AgentSessionMessageInput) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return errors.New("session_id is required")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin replace session messages tx: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM agent_session_messages WHERE session_id = ?`, sessionID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("clear session messages: %w", err)
	}
	for i, msg := range messages {
		if len(msg.ContentJSON) > 1024*1024 {
			_ = tx.Rollback()
			return fmt.Errorf("replace session message %d: payload exceeds 1MB hard limit", i)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO agent_session_messages (session_id, sequence, role, content_json, token_count)
			 VALUES (?, ?, ?, ?, ?)`,
			sessionID, msg.Sequence, msg.Role, msg.ContentJSON, msg.TokenCount,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("replace session message %d: %w", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit replace session messages: %w", err)
	}
	return nil
}

// GetAgentSession retrieves a session by ID.
func (s *Store) GetAgentSession(ctx context.Context, sessionID string) (*AgentSessionRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT session_id, agent_id, workspace_id, COALESCE(task_id,''), status,
		        iterations, total_input_tokens, total_output_tokens, tool_calls,
		        COALESCE(stop_reason,''), COALESCE(error_message,''),
		        started_at, COALESCE(completed_at,''), created_at
		 FROM agent_sessions WHERE session_id = ?`, sessionID,
	)

	var r AgentSessionRecord
	err := row.Scan(
		&r.SessionID, &r.AgentID, &r.WorkspaceID, &r.TaskID, &r.Status,
		&r.Iterations, &r.TotalInputTokens, &r.TotalOutputTokens, &r.ToolCalls,
		&r.StopReason, &r.ErrorMessage,
		&r.StartedAt, &r.CompletedAt, &r.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrSessionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get agent session: %w", err)
	}
	return &r, nil
}

// ListAgentSessionMessages returns all messages for a session ordered by sequence.
func (s *Store) ListAgentSessionMessages(ctx context.Context, sessionID string) ([]AgentSessionMessageRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, session_id, sequence, role, content_json, token_count, created_at
		 FROM agent_session_messages WHERE session_id = ? ORDER BY sequence ASC`, sessionID,
	)
	if err != nil {
		return nil, fmt.Errorf("list session messages: %w", err)
	}
	defer rows.Close()

	var messages []AgentSessionMessageRecord
	for rows.Next() {
		var m AgentSessionMessageRecord
		if err := rows.Scan(&m.ID, &m.SessionID, &m.Sequence, &m.Role, &m.ContentJSON, &m.TokenCount, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan session message: %w", err)
		}
		messages = append(messages, m)
	}
	if messages == nil {
		messages = []AgentSessionMessageRecord{}
	}
	return messages, rows.Err()
}

// ListAgentSessions returns the most recent sessions for an agent within a workspace.
func (s *Store) ListAgentSessions(ctx context.Context, workspaceID, agentID string, limit int) ([]AgentSessionRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, errors.New("agent_id is required")
	}
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT session_id, agent_id, workspace_id, COALESCE(task_id,''), status,
		        iterations, total_input_tokens, total_output_tokens, tool_calls,
		        COALESCE(stop_reason,''), COALESCE(error_message,''),
		        started_at, COALESCE(completed_at,''), created_at
		 FROM agent_sessions WHERE workspace_id = ? AND agent_id = ? ORDER BY started_at DESC LIMIT ?`,
		workspaceID, agentID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list agent sessions: %w", err)
	}
	defer rows.Close()

	var sessions []AgentSessionRecord
	for rows.Next() {
		var r AgentSessionRecord
		if err := rows.Scan(
			&r.SessionID, &r.AgentID, &r.WorkspaceID, &r.TaskID, &r.Status,
			&r.Iterations, &r.TotalInputTokens, &r.TotalOutputTokens, &r.ToolCalls,
			&r.StopReason, &r.ErrorMessage,
			&r.StartedAt, &r.CompletedAt, &r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent session: %w", err)
		}
		sessions = append(sessions, r)
	}
	if sessions == nil {
		sessions = []AgentSessionRecord{}
	}
	return sessions, rows.Err()
}
