package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrAgentDefinitionNotFound     = errors.New("agent definition not found")
	ErrAgentDefinitionNameConflict = errors.New("agent definition name already exists in workspace")
)

// AgentDefinitionCreateInput is the input for creating an agent definition.
type AgentDefinitionCreateInput struct {
	ID            string
	Name          string
	Provider      string
	Model         string
	SystemPrompt  string
	Tools         []string
	MaxIterations int
	WorkspaceID   string
}

// AgentDefinitionRecord represents a stored agent definition.
type AgentDefinitionRecord struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Provider      string   `json:"provider"`
	Model         string   `json:"model"`
	SystemPrompt  string   `json:"system_prompt"`
	Tools         []string `json:"tools"`
	MaxIterations int      `json:"max_iterations"`
	WorkspaceID   string   `json:"workspace_id"`
	CreatedAt     string   `json:"created_at"`
	UpdatedAt     string   `json:"updated_at"`
}

// CreateAgentDefinition inserts a new agent definition.
func (s *Store) CreateAgentDefinition(ctx context.Context, input AgentDefinitionCreateInput) (*AgentDefinitionRecord, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("create agent definition: name is required")
	}
	if input.WorkspaceID == "" {
		return nil, fmt.Errorf("create agent definition: workspace_id is required")
	}
	if input.Provider != "claude" && input.Provider != "openai" {
		return nil, fmt.Errorf("create agent definition: provider must be \"claude\" or \"openai\", got %q", input.Provider)
	}

	if input.ID == "" {
		input.ID = fmt.Sprintf("agentdef-%d", time.Now().UnixNano())
	}
	if input.MaxIterations <= 0 {
		input.MaxIterations = 50
	}

	tools := input.Tools
	if tools == nil {
		tools = []string{}
	}
	toolsJSON, err := json.Marshal(tools)
	if err != nil {
		return nil, fmt.Errorf("create agent definition: marshal tools: %w", err)
	}

	_, err = s.writeDB.ExecContext(ctx,
		`INSERT INTO agent_definitions (id, name, provider, model, system_prompt, tools_json, max_iterations, workspace_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		input.ID, input.Name, input.Provider, input.Model, input.SystemPrompt,
		string(toolsJSON), input.MaxIterations, input.WorkspaceID,
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			if strings.Contains(err.Error(), "idx_agent_definitions_name_workspace") ||
				(strings.Contains(err.Error(), "agent_definitions.name") && strings.Contains(err.Error(), "agent_definitions.workspace_id")) {
				return nil, ErrAgentDefinitionNameConflict
			}
		}
		return nil, fmt.Errorf("create agent definition: %w", err)
	}

	return s.GetAgentDefinition(ctx, input.ID)
}

// GetAgentDefinition retrieves an agent definition by ID.
func (s *Store) GetAgentDefinition(ctx context.Context, id string) (*AgentDefinitionRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, name, provider, COALESCE(model,''), COALESCE(system_prompt,''),
		        tools_json, max_iterations, workspace_id, created_at, updated_at
		 FROM agent_definitions WHERE id = ?`, id,
	)

	var r AgentDefinitionRecord
	var toolsJSON string
	err := row.Scan(
		&r.ID, &r.Name, &r.Provider, &r.Model, &r.SystemPrompt,
		&toolsJSON, &r.MaxIterations, &r.WorkspaceID, &r.CreatedAt, &r.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrAgentDefinitionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get agent definition: %w", err)
	}

	if err := json.Unmarshal([]byte(toolsJSON), &r.Tools); err != nil {
		return nil, fmt.Errorf("get agent definition: unmarshal tools: %w", err)
	}
	if r.Tools == nil {
		r.Tools = []string{}
	}

	return &r, nil
}

// ListAgentDefinitions returns agent definitions, optionally filtered by workspace.
func (s *Store) ListAgentDefinitions(ctx context.Context, workspaceID string) ([]AgentDefinitionRecord, error) {
	var rows *sql.Rows
	var err error

	if workspaceID == "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT id, name, provider, COALESCE(model,''), COALESCE(system_prompt,''),
			        tools_json, max_iterations, workspace_id, created_at, updated_at
			 FROM agent_definitions ORDER BY created_at DESC`,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT id, name, provider, COALESCE(model,''), COALESCE(system_prompt,''),
			        tools_json, max_iterations, workspace_id, created_at, updated_at
			 FROM agent_definitions WHERE workspace_id = ? ORDER BY created_at DESC`,
			workspaceID,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("list agent definitions: %w", err)
	}
	defer rows.Close()

	var defs []AgentDefinitionRecord
	for rows.Next() {
		var r AgentDefinitionRecord
		var toolsJSON string
		if err := rows.Scan(
			&r.ID, &r.Name, &r.Provider, &r.Model, &r.SystemPrompt,
			&toolsJSON, &r.MaxIterations, &r.WorkspaceID, &r.CreatedAt, &r.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent definition: %w", err)
		}
		if err := json.Unmarshal([]byte(toolsJSON), &r.Tools); err != nil {
			return nil, fmt.Errorf("list agent definitions: unmarshal tools: %w", err)
		}
		if r.Tools == nil {
			r.Tools = []string{}
		}
		defs = append(defs, r)
	}
	if defs == nil {
		defs = []AgentDefinitionRecord{}
	}
	return defs, rows.Err()
}

// DeleteAgentDefinition deletes an agent definition by ID.
func (s *Store) DeleteAgentDefinition(ctx context.Context, id string) error {
	result, err := s.writeDB.ExecContext(ctx,
		`DELETE FROM agent_definitions WHERE id = ?`, id,
	)
	if err != nil {
		return fmt.Errorf("delete agent definition: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete agent definition: %w", err)
	}
	if affected == 0 {
		return ErrAgentDefinitionNotFound
	}
	return nil
}
