package sqlite_test

import (
	"database/sql"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"testing"
	"time"
)

func TestMigration0074_MCPWorkspaceIsolation(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()
	now := time.Now().Format(time.RFC3339Nano)

	// Try inserting a direct workspace isolated record without failing constraints
	_, err := db.Exec(`INSERT INTO mcp_servers
		(workspace_id, server_id, display_name, transport, registered_by, created_at, updated_at)
		VALUES ('ws-1', 'mcp-1', 'S1', 'stdio', 'u1', ?, ?)`,
		now, now)

	if err != nil {
		t.Fatalf("insert first mcp: %v", err)
	}

	// Insert with same server_id but different workspace_id (Isolation Check)
	_, err = db.Exec(`INSERT INTO mcp_servers
		(workspace_id, server_id, display_name, transport, registered_by, created_at, updated_at)
		VALUES ('ws-2', 'mcp-1', 'S2', 'stdio', 'u2', ?, ?)`,
		now, now)

	if err != nil {
		t.Fatalf("insert second mcp (should allow duplicate server_id across workspaces): %v", err)
	}

	_, err = db.Exec(`INSERT INTO mcp_server_tools
		(workspace_id, server_id, tool_name, discovered_at)
		VALUES ('ws-1', 'mcp-1', 'tool-a', ?)`,
		now)
	if err != nil {
		t.Fatalf("insert first mcp tool: %v", err)
	}
	_, err = db.Exec(`INSERT INTO mcp_server_tools
		(workspace_id, server_id, tool_name, discovered_at)
		VALUES ('ws-2', 'mcp-1', 'tool-b', ?)`,
		now)
	if err != nil {
		t.Fatalf("insert second mcp tool: %v", err)
	}

	var ws1Tools, ws2Tools int
	if err := db.QueryRow(`SELECT COUNT(*) FROM mcp_server_tools WHERE workspace_id = 'ws-1' AND server_id = 'mcp-1'`).Scan(&ws1Tools); err != nil {
		t.Fatalf("count ws-1 tools: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM mcp_server_tools WHERE workspace_id = 'ws-2' AND server_id = 'mcp-1'`).Scan(&ws2Tools); err != nil {
		t.Fatalf("count ws-2 tools: %v", err)
	}
	if ws1Tools != 1 || ws2Tools != 1 {
		t.Fatalf("expected one isolated tool row per workspace, got ws-1=%d ws-2=%d", ws1Tools, ws2Tools)
	}

	if _, err := db.Exec(`DELETE FROM mcp_servers WHERE workspace_id = 'ws-1' AND server_id = 'mcp-1'`); err != nil {
		t.Fatalf("delete ws-1 server: %v", err)
	}

	if err := db.QueryRow(`SELECT COUNT(*) FROM mcp_server_tools WHERE workspace_id = 'ws-1' AND server_id = 'mcp-1'`).Scan(&ws1Tools); err != nil {
		t.Fatalf("recount ws-1 tools: %v", err)
	}
	if ws1Tools != 0 {
		t.Fatalf("expected ws-1 tool rows to cascade-delete, got %d", ws1Tools)
	}

	var survivingTool string
	err = db.QueryRow(`SELECT tool_name FROM mcp_server_tools WHERE workspace_id = 'ws-2' AND server_id = 'mcp-1'`).Scan(&survivingTool)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Fatal("expected ws-2 tool row to survive delete of ws-1 server")
		}
		t.Fatalf("query surviving ws-2 tool: %v", err)
	}
	if survivingTool != "tool-b" {
		t.Fatalf("expected surviving ws-2 tool to remain intact, got %q", survivingTool)
	}
}
