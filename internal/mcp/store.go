package mcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/secret"
)

// Store provides CRUD for MCP server registrations and cached tools.
type Store struct {
	db *sql.DB
}

// NewStore creates a new MCP Store.
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// ServerRecord represents a registered MCP server.
type ServerRecord struct {
	ServerID     string `json:"server_id"`
	WorkspaceID  string `json:"workspace_id"`
	DisplayName  string `json:"display_name"`
	Transport    string `json:"transport"`
	URL          string `json:"url,omitempty"`
	Command      string `json:"command,omitempty"`
	ArgsJSON     string `json:"args_json,omitempty"`
	EnvJSON      string `json:"env_json,omitempty"`
	HeadersJSON  string `json:"headers_json,omitempty"`
	Status       string `json:"status"`
	RegisteredBy string `json:"registered_by"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

// ServerToolRecord represents a cached tool from an MCP server.
type ServerToolRecord struct {
	ServerID     string `json:"server_id"`
	ToolName     string `json:"tool_name"`
	Description  string `json:"description"`
	InputSchema  string `json:"input_schema"`
	DiscoveredAt string `json:"discovered_at"`
}

// RegisterInput is input for registering an MCP server.
type RegisterInput struct {
	ServerID     string
	WorkspaceID  string
	DisplayName  string
	Transport    string
	URL          string
	Command      string
	ArgsJSON     string
	EnvJSON      string
	HeadersJSON  string
	RegisteredBy string
}

// RegisterServer registers or updates an MCP server.
func (s *Store) RegisterServer(ctx context.Context, input RegisterInput) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin register mcp server tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.RegisterServerTx(ctx, tx, input); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit register mcp server tx: %w", err)
	}
	return nil
}

// RegisterServerTx registers or updates an MCP server inside an existing transaction.
func (s *Store) RegisterServerTx(ctx context.Context, tx *sql.Tx, input RegisterInput) error {
	if tx == nil {
		return errors.New("register mcp server tx is required")
	}
	serverID := strings.TrimSpace(input.ServerID)
	if serverID == "" {
		return errors.New("server_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	displayName := strings.TrimSpace(input.DisplayName)
	if displayName == "" {
		return errors.New("display_name is required")
	}
	transport := strings.TrimSpace(input.Transport)
	if transport == "" {
		transport = "streamable-http"
	}

	if transport == "stdio" && !registeredByIsSystem(input.RegisteredBy) {
		return errors.New("stdio transport is restricted to system-registered servers for security reasons")
	}

	command := strings.TrimSpace(input.Command)
	if transport == "stdio" && command == "" {
		return errors.New("command is required for stdio transport")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	envJSON := defaultStr(input.EnvJSON, "{}")
	headersJSON := defaultStr(input.HeadersJSON, "{}")

	encEnv, err := secret.EncryptVaultData(envJSON)
	if err != nil {
		return fmt.Errorf("encrypt env json: %w", err)
	}
	encHeaders, err := secret.EncryptVaultData(headersJSON)
	if err != nil {
		return fmt.Errorf("encrypt headers json: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO mcp_servers (server_id, workspace_id, display_name, transport, url, command, args_json, env_json, headers_json, status, registered_by, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?)
		 ON CONFLICT(workspace_id, server_id) DO UPDATE SET
		   display_name = excluded.display_name,
		   transport = excluded.transport,
		   url = excluded.url,
		   command = excluded.command,
		   args_json = excluded.args_json,
		   env_json = excluded.env_json,
		   headers_json = excluded.headers_json,
		   status = 'ACTIVE',
		   updated_at = excluded.updated_at`,
		serverID, workspaceID, displayName, transport,
		strings.TrimSpace(input.URL),
		strings.TrimSpace(input.Command),
		defaultStr(input.ArgsJSON, "[]"),
		encEnv,
		encHeaders,
		strings.TrimSpace(input.RegisteredBy),
		now, now,
	)
	if err != nil {
		return fmt.Errorf("register mcp server: %w", err)
	}
	return nil
}

// ListServers lists MCP servers in a workspace.
func (s *Store) ListServers(ctx context.Context, workspaceID string) ([]ServerRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT server_id, workspace_id, display_name, transport, url, command, args_json, env_json, headers_json, status, registered_by, created_at, updated_at
		 FROM mcp_servers WHERE workspace_id = ? AND status = 'ACTIVE' ORDER BY created_at`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("list mcp servers: %w", err)
	}
	defer rows.Close()

	var servers []ServerRecord
	for rows.Next() {
		var r ServerRecord
		if err := rows.Scan(
			&r.ServerID, &r.WorkspaceID, &r.DisplayName, &r.Transport,
			&r.URL, &r.Command, &r.ArgsJSON, &r.EnvJSON, &r.HeadersJSON,
			&r.Status, &r.RegisteredBy, &r.CreatedAt, &r.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan mcp server: %w", err)
		}

		if decEnv, err := secret.DecryptVaultData(r.EnvJSON); err == nil {
			r.EnvJSON = decEnv
		} else {
			return nil, fmt.Errorf("decrypt env: %w", err)
		}
		if decHeaders, err := secret.DecryptVaultData(r.HeadersJSON); err == nil {
			r.HeadersJSON = decHeaders
		} else {
			return nil, fmt.Errorf("decrypt headers: %w", err)
		}

		servers = append(servers, r)
	}
	return servers, rows.Err()
}

// GetServer gets a single MCP server by ID within a workspace.
func (s *Store) GetServer(ctx context.Context, workspaceID, serverID string) (ServerRecord, error) {
	return s.getServer(ctx, s.db, workspaceID, serverID, true)
}

// GetServerTx gets a single active MCP server by ID within a workspace inside an existing transaction.
func (s *Store) GetServerTx(ctx context.Context, tx *sql.Tx, workspaceID, serverID string) (ServerRecord, error) {
	if tx == nil {
		return ServerRecord{}, errors.New("get mcp server tx is required")
	}
	return s.getServer(ctx, tx, workspaceID, serverID, true)
}

// GetServerAnyStatus gets a single MCP server by ID regardless of active/removed state.
func (s *Store) GetServerAnyStatus(ctx context.Context, workspaceID, serverID string) (ServerRecord, error) {
	return s.getServer(ctx, s.db, workspaceID, serverID, false)
}

func (s *Store) getServer(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, serverID string, activeOnly bool) (ServerRecord, error) {
	var r ServerRecord
	query := `SELECT server_id, workspace_id, display_name, transport, url, command, args_json, env_json, headers_json, status, registered_by, created_at, updated_at
		 FROM mcp_servers WHERE workspace_id = ? AND server_id = ?`
	if activeOnly {
		query += ` AND status = 'ACTIVE'`
	}
	err := queryer.QueryRowContext(ctx, query,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(serverID),
	).Scan(
		&r.ServerID, &r.WorkspaceID, &r.DisplayName, &r.Transport,
		&r.URL, &r.Command, &r.ArgsJSON, &r.EnvJSON, &r.HeadersJSON,
		&r.Status, &r.RegisteredBy, &r.CreatedAt, &r.UpdatedAt,
	)
	if err != nil {
		return ServerRecord{}, fmt.Errorf("get mcp server: %w", err)
	}

	if decEnv, err := secret.DecryptVaultData(r.EnvJSON); err == nil {
		r.EnvJSON = decEnv
	} else {
		return ServerRecord{}, fmt.Errorf("decrypt env: %w", err)
	}
	if decHeaders, err := secret.DecryptVaultData(r.HeadersJSON); err == nil {
		r.HeadersJSON = decHeaders
	} else {
		return ServerRecord{}, fmt.Errorf("decrypt headers: %w", err)
	}

	return r, nil
}

// RemoveServer soft-deletes an MCP server.
func (s *Store) RemoveServer(ctx context.Context, workspaceID, serverID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin remove mcp server tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.RemoveServerTx(ctx, tx, workspaceID, serverID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit remove mcp server tx: %w", err)
	}
	return nil
}

// RemoveServerTx soft-deletes an MCP server inside an existing transaction.
func (s *Store) RemoveServerTx(ctx context.Context, tx *sql.Tx, workspaceID, serverID string) error {
	if tx == nil {
		return errors.New("remove mcp server tx is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := tx.ExecContext(ctx,
		`UPDATE mcp_servers SET status = 'REMOVED', updated_at = ? WHERE workspace_id = ? AND server_id = ?`,
		now, strings.TrimSpace(workspaceID), strings.TrimSpace(serverID),
	)
	if err != nil {
		return fmt.Errorf("remove mcp server: %w", err)
	}
	return nil
}

// SaveDiscoveredTools replaces the cached tools for a server.
func (s *Store) SaveDiscoveredTools(ctx context.Context, workspaceID, serverID string, tools []Tool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.SaveDiscoveredToolsTx(ctx, tx, workspaceID, serverID, tools); err != nil {
		return err
	}
	return tx.Commit()
}

// SaveDiscoveredToolsTx replaces the cached tools for a server inside an existing transaction.
func (s *Store) SaveDiscoveredToolsTx(ctx context.Context, tx *sql.Tx, workspaceID, serverID string, tools []Tool) error {
	if tx == nil {
		return errors.New("save discovered tools tx is required")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	serverID = strings.TrimSpace(serverID)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	if _, err := tx.ExecContext(ctx, `DELETE FROM mcp_server_tools WHERE workspace_id = ? AND server_id = ?`, workspaceID, serverID); err != nil {
		return fmt.Errorf("clear old tools: %w", err)
	}

	for _, tool := range tools {
		schema := "{}"
		if tool.InputSchema != nil {
			schema = string(tool.InputSchema)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO mcp_server_tools (workspace_id, server_id, tool_name, description, input_schema, discovered_at) VALUES (?, ?, ?, ?, ?, ?)`,
			workspaceID, serverID, tool.Name, tool.Description, schema, now,
		); err != nil {
			return fmt.Errorf("insert tool %s: %w", tool.Name, err)
		}
	}
	return nil
}

// ListServerTools lists cached tools for a workspace.
func (s *Store) ListServerTools(ctx context.Context, workspaceID string) ([]ServerToolRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT t.server_id, t.tool_name, t.description, t.input_schema, t.discovered_at
		 FROM mcp_server_tools t
		 JOIN mcp_servers s ON s.workspace_id = t.workspace_id AND s.server_id = t.server_id
		 WHERE s.workspace_id = ? AND s.status = 'ACTIVE'
		 ORDER BY s.display_name, t.tool_name`,
		strings.TrimSpace(workspaceID),
	)
	if err != nil {
		return nil, fmt.Errorf("list mcp tools: %w", err)
	}
	defer rows.Close()

	var tools []ServerToolRecord
	for rows.Next() {
		var t ServerToolRecord
		if err := rows.Scan(&t.ServerID, &t.ToolName, &t.Description, &t.InputSchema, &t.DiscoveredAt); err != nil {
			return nil, fmt.Errorf("scan mcp tool: %w", err)
		}
		tools = append(tools, t)
	}
	return tools, rows.Err()
}

// GetServerHeaders parses the stored headers_json into a map.
func (s *Store) GetServerHeaders(server ServerRecord) map[string]string {
	headers := make(map[string]string)
	if server.HeadersJSON != "" && server.HeadersJSON != "{}" {
		_ = json.Unmarshal([]byte(server.HeadersJSON), &headers)
	}
	return headers
}

// GetServerStdioConfig parses the stdio transport config from a ServerRecord.
func (s *Store) GetServerStdioConfig(server ServerRecord) (command string, args []string, env map[string]string) {
	command = server.Command
	if server.ArgsJSON != "" && server.ArgsJSON != "[]" {
		_ = json.Unmarshal([]byte(server.ArgsJSON), &args)
	}
	env = make(map[string]string)
	if server.EnvJSON != "" && server.EnvJSON != "{}" {
		_ = json.Unmarshal([]byte(server.EnvJSON), &env)
	}
	return
}

func defaultStr(value, fallback string) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return fallback
	}
	return v
}

func registeredByIsSystem(owner string) bool {
	kind, _, typed := splitRegisteredByOwner(owner)
	if typed {
		return strings.EqualFold(kind, "system")
	}
	return strings.EqualFold(strings.TrimSpace(owner), "system")
}

func splitRegisteredByOwner(owner string) (kind string, principalID string, typed bool) {
	owner = strings.TrimSpace(owner)
	parts := strings.SplitN(owner, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	kind = strings.TrimSpace(parts[0])
	principalID = strings.TrimSpace(parts[1])
	if kind == "" || principalID == "" {
		return "", "", false
	}
	return kind, principalID, true
}
