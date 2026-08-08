package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceToolRegisterRemovePromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tool-registry-prompt-context"
		toolID      = "tool-registry-prompt-context"
		operatorID  = "operator-tool-registry"
		manifest    = `{"route":{"kind":"local"}}`
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Registry Prompt Context",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID:                workspaceID,
		ToolID:                     toolID,
		DisplayName:                "Registry Prompt Tool",
		OwnerUserID:                operatorID,
		Kind:                       model.ToolKindIntegration,
		Status:                     model.ToolStatusActive,
		Version:                    "v1",
		AccessLevel:                model.ToolAccessWorkspace,
		Endpoint:                   "local:tool",
		Capabilities:               []string{"tool.call", "workspace.read"},
		ManifestJSON:               manifest,
		PromptContextEnvelope:      sqlite.BuildToolRegistryPromptContextEnvelope("tool.register", "server_rpc", workspaceID, "human", operatorID),
		PromptContextSurface:       "tool.register",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	}); err != nil {
		t.Fatalf("register workspace tool with prompt context: %v", err)
	}
	registered := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	assertWorkspaceToolPromptContextEnvelope(t, registered.PayloadJSON, map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_tool_registry_write",
		"surface":                            "tool.register",
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     "human",
		"principal_id":                       operatorID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
		"tool_id":                            toolID,
		"display_name":                       "Registry Prompt Tool",
		"owner_user_id":                      operatorID,
		"kind":                               model.ToolKindIntegration,
		"status":                             model.ToolStatusActive,
		"version":                            "v1",
		"access_level":                       model.ToolAccessWorkspace,
		"endpoint":                           "local:tool",
		"capabilities_sha256":                testSHA256Hex(mustMarshalPromptContextJSON(t, []string{"tool.call", "workspace.read"})),
		"manifest_sha256":                    testSHA256Hex(manifest),
		"event_type":                         "workspace_tool.registered",
		"entity_type":                        "workspace_tool",
		"entity_id":                          toolID,
		"actor_type":                         "operator",
		"actor_id":                           operatorID,
	})

	if _, err := store.RemoveWorkspaceTool(ctx, sqlite.WorkspaceToolRemoveInput{
		WorkspaceID:                workspaceID,
		ToolID:                     toolID,
		RemovedBy:                  operatorID,
		PromptContextEnvelope:      sqlite.BuildToolRegistryPromptContextEnvelope("tool.remove", "server_rpc", workspaceID, "human", operatorID),
		PromptContextSurface:       "tool.remove",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	}); err != nil {
		t.Fatalf("remove workspace tool with prompt context: %v", err)
	}
	removed := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.removed",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	assertWorkspaceToolPromptContextEnvelope(t, removed.PayloadJSON, map[string]string{
		"surface":        "tool.remove",
		"origin":         "server_rpc",
		"workspace_id":   workspaceID,
		"principal_type": "human",
		"principal_id":   operatorID,
		"tool_id":        toolID,
		"removed_by":     operatorID,
		"event_type":     "workspace_tool.removed",
		"entity_type":    "workspace_tool",
		"entity_id":      toolID,
		"actor_type":     "operator",
		"actor_id":       operatorID,
	})
}

func TestWorkspaceToolRegisterRejectsForgedPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tool-registry-forged-prompt-context"
		toolID      = "tool-registry-forged"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Forged Tool Registry Prompt Context",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID:                workspaceID,
		ToolID:                     toolID,
		DisplayName:                "Forged Registry Tool",
		OwnerUserID:                "operator-a",
		Kind:                       model.ToolKindOther,
		Status:                     model.ToolStatusActive,
		PromptContextEnvelope:      sqlite.BuildToolRegistryPromptContextEnvelope("tool.register", "server_rpc", workspaceID, "human", "operator-b"),
		PromptContextSurface:       "tool.register",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-a",
	})
	if err == nil {
		t.Fatal("expected forged principal in tool registry prompt context to fail closed")
	}
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, toolID); err == nil {
		t.Fatal("expected forged prompt context to roll back tool row")
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no runtime event after forged prompt context, got %+v", events)
	}
}

func TestWorkspaceToolPromptContextRequiresExpectedPrincipalBinding(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tool-registry-missing-principal-binding"
		registerID  = "tool-registry-missing-register-binding"
		removeID    = "tool-registry-missing-remove-binding"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Registry Missing Principal Binding",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID:           workspaceID,
		ToolID:                registerID,
		DisplayName:           "Missing Register Principal Binding",
		OwnerUserID:           "operator-a",
		Kind:                  model.ToolKindOther,
		Status:                model.ToolStatusActive,
		PromptContextEnvelope: sqlite.BuildToolRegistryPromptContextEnvelope("tool.register", "server_rpc", workspaceID, "human", "operator-b"),
		PromptContextSurface:  "tool.register",
	}); err == nil {
		t.Fatal("expected register prompt context without expected principal binding to fail closed")
	}
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, registerID); err == nil {
		t.Fatal("expected failed register to leave no tool row")
	}

	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID: workspaceID,
		ToolID:      removeID,
		DisplayName: "Remove Binding Seed",
		OwnerUserID: "operator-a",
		Kind:        model.ToolKindOther,
		Status:      model.ToolStatusActive,
	}); err != nil {
		t.Fatalf("seed remove tool: %v", err)
	}
	if _, err := store.RemoveWorkspaceTool(ctx, sqlite.WorkspaceToolRemoveInput{
		WorkspaceID:           workspaceID,
		ToolID:                removeID,
		RemovedBy:             "operator-a",
		PromptContextEnvelope: sqlite.BuildToolRegistryPromptContextEnvelope("tool.remove", "server_rpc", workspaceID, "human", "operator-b"),
		PromptContextSurface:  "tool.remove",
	}); err == nil {
		t.Fatal("expected remove prompt context without expected principal binding to fail closed")
	}
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, removeID); err != nil {
		t.Fatalf("expected failed remove to leave tool row: %v", err)
	}
	removedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.removed",
		EntityType:  "workspace_tool",
		EntityID:    removeID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list remove events: %v", err)
	}
	if len(removedEvents) != 0 {
		t.Fatalf("expected no remove runtime event after missing principal binding, got %+v", removedEvents)
	}
}

func TestReconcileMCPWorkspaceToolsStampsProjectionPromptContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-mcp-tool-registry-projection-context"
		serverID    = "notion-projection"
		toolID      = "mcp__notion_projection__search_docs"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP Tool Registry Projection Context",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	fence := sqlite.WorkspaceAuthorityFenceInput{
		WorkspaceID:                   workspaceID,
		Scope:                         "workspace",
		ExpectedHolderAuthorityNodeID: authority.HolderAuthorityNodeID,
		ExpectedLeaseToken:            authority.LeaseToken,
		ExpectedTerm:                  authority.Term,
		ReferenceAt:                   time.Now().UTC().Format(time.RFC3339Nano),
	}
	seedToolRegistryProjectionOperationRun(t, ctx, store, workspaceID, "mcpdiscover-parent-1", "mcp.tool.discover")
	_, err := store.WithFencedWorkspaceAuthority(ctx, fence, func(tx *sql.Tx, checked sqlite.WorkspaceAuthorityRecord) error {
		return store.ReconcileMCPWorkspaceToolsTx(ctx, tx, checked, workspaceID, serverID, "agent:planner", []sqlite.WorkspaceToolInput{{
			WorkspaceID:           workspaceID,
			ToolID:                toolID,
			DisplayName:           "search_docs",
			Description:           "Search docs",
			OwnerUserID:           "planner",
			Kind:                  model.ToolKindIntegration,
			Status:                model.ToolStatusActive,
			AccessLevel:           model.ToolAccessWorkspace,
			Endpoint:              "https://mcp.example.test",
			Capabilities:          []string{"tool.call"},
			ManifestJSON:          `{"route":{"kind":"mcp","server_id":"notion-projection","tool_name":"search_docs"}}`,
			PromptContextEnvelope: sqlite.BuildToolRegistryPromptContextEnvelope("tool.register", "server_rpc", workspaceID, "human", "forged"),
			PromptContextSurface:  "tool.register",
		}}, "mcp.tool.discover", "mcpdiscover-parent-1")
	})
	if err != nil {
		t.Fatalf("reconcile mcp workspace tools: %v", err)
	}
	event := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	assertWorkspaceToolPromptContextEnvelope(t, event.PayloadJSON, map[string]string{
		"context_kind":              "authority_bearing_tool_registry_write",
		"surface":                   "mcp.workspace_tool.project",
		"origin":                    "server_mcp_projection",
		"workspace_id":              workspaceID,
		"principal_type":            "agent",
		"principal_id":              "planner",
		"tool_id":                   toolID,
		"projection_action":         "register",
		"projection_source":         "mcp_workspace_tool_reconcile",
		"projection_source_surface": "mcp.tool.discover",
		"projection_operation_id":   "mcpdiscover-parent-1",
		"mcp_server_id":             serverID,
		"mcp_tool_name":             "search_docs",
	})
}

func TestReconcileMCPWorkspaceToolsRequiresProjectionOperationID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-mcp-tool-registry-projection-operation-required"
		serverID    = "notion-projection-operation-required"
		toolID      = "mcp__notion_projection_operation_required__search_docs"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP Tool Registry Projection Operation Required",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	fence := sqlite.WorkspaceAuthorityFenceInput{
		WorkspaceID:                   workspaceID,
		Scope:                         "workspace",
		ExpectedHolderAuthorityNodeID: authority.HolderAuthorityNodeID,
		ExpectedLeaseToken:            authority.LeaseToken,
		ExpectedTerm:                  authority.Term,
		ReferenceAt:                   time.Now().UTC().Format(time.RFC3339Nano),
	}
	_, err := store.WithFencedWorkspaceAuthority(ctx, fence, func(tx *sql.Tx, checked sqlite.WorkspaceAuthorityRecord) error {
		return store.ReconcileMCPWorkspaceToolsTx(ctx, tx, checked, workspaceID, serverID, "agent:planner", []sqlite.WorkspaceToolInput{{
			WorkspaceID:  workspaceID,
			ToolID:       toolID,
			DisplayName:  "search_docs",
			OwnerUserID:  "planner",
			Kind:         model.ToolKindIntegration,
			Status:       model.ToolStatusActive,
			AccessLevel:  model.ToolAccessWorkspace,
			Endpoint:     "https://mcp.example.test",
			ManifestJSON: `{"route":{"kind":"mcp","server_id":"notion-projection-operation-required","tool_name":"search_docs"}}`,
		}}, "mcp.tool.discover", "")
	})
	if err == nil || !strings.Contains(err.Error(), "projection_operation_id") {
		t.Fatalf("expected projection_operation_id rejection, got %v", err)
	}
	if _, getErr := store.GetWorkspaceTool(ctx, workspaceID, toolID); !errors.Is(getErr, sqlite.ErrToolNotFound) {
		t.Fatalf("expected no workspace tool after missing projection operation id, got %v", getErr)
	}
	events, listErr := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	if listErr != nil {
		t.Fatalf("list events: %v", listErr)
	}
	if len(events) != 0 {
		t.Fatalf("expected no runtime events after missing projection operation id, got %+v", events)
	}
}

func TestReconcileMCPWorkspaceToolsRejectsInvalidProjectionOperationReference(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-mcp-tool-registry-projection-operation-invalid"
		foreignWS   = "ws-mcp-tool-registry-projection-operation-foreign"
		serverID    = "notion-projection-operation-invalid"
	)
	for _, workspaceID := range []string{workspaceID, foreignWS} {
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "MCP Tool Registry Projection Operation Invalid",
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace %s: %v", workspaceID, err)
		}
		claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	}
	seedToolRegistryProjectionOperationRun(t, ctx, store, workspaceID, "wrong-surface-parent", "mcp.server.remove")
	seedToolRegistryProjectionOperationRunRaw(t, ctx, store, workspaceID, "wrong-kind-parent", "mcp.tool.discover", "tool_call", "mcp.tool.discover")
	seedToolRegistryProjectionOperationRunRaw(t, ctx, store, workspaceID, "wrong-capability-parent", "mcp.tool.discover", "mcp_discover", "tool.call")
	seedToolRegistryProjectionOperationRun(t, ctx, store, foreignWS, "foreign-parent", "mcp.tool.discover")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	fence := sqlite.WorkspaceAuthorityFenceInput{
		WorkspaceID:                   workspaceID,
		Scope:                         "workspace",
		ExpectedHolderAuthorityNodeID: authority.HolderAuthorityNodeID,
		ExpectedLeaseToken:            authority.LeaseToken,
		ExpectedTerm:                  authority.Term,
		ReferenceAt:                   time.Now().UTC().Format(time.RFC3339Nano),
	}

	cases := []struct {
		name          string
		toolID        string
		sourceSurface string
		operationID   string
		wantErr       string
	}{
		{
			name:          "unknown operation",
			toolID:        "mcp__notion_projection_operation_invalid__unknown",
			sourceSurface: "mcp.tool.discover",
			operationID:   "not-a-real-operation",
			wantErr:       "not a workspace execution run",
		},
		{
			name:          "invalid source surface",
			toolID:        "mcp__notion_projection_operation_invalid__source",
			sourceSurface: "tool.register",
			operationID:   "wrong-surface-parent",
			wantErr:       "projection_source_surface",
		},
		{
			name:          "foreign workspace operation",
			toolID:        "mcp__notion_projection_operation_invalid__foreign",
			sourceSurface: "mcp.tool.discover",
			operationID:   "foreign-parent",
			wantErr:       "not a workspace execution run",
		},
		{
			name:          "mismatched parent surface",
			toolID:        "mcp__notion_projection_operation_invalid__surface",
			sourceSurface: "mcp.tool.discover",
			operationID:   "wrong-surface-parent",
			wantErr:       "surface",
		},
		{
			name:          "mismatched parent operation kind",
			toolID:        "mcp__notion_projection_operation_invalid__kind",
			sourceSurface: "mcp.tool.discover",
			operationID:   "wrong-kind-parent",
			wantErr:       "operation_kind",
		},
		{
			name:          "mismatched parent requested capability",
			toolID:        "mcp__notion_projection_operation_invalid__capability",
			sourceSurface: "mcp.tool.discover",
			operationID:   "wrong-capability-parent",
			wantErr:       "requested_capability",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := store.WithFencedWorkspaceAuthority(ctx, fence, func(tx *sql.Tx, checked sqlite.WorkspaceAuthorityRecord) error {
				return store.ReconcileMCPWorkspaceToolsTx(ctx, tx, checked, workspaceID, serverID, "agent:planner", []sqlite.WorkspaceToolInput{{
					WorkspaceID:  workspaceID,
					ToolID:       tc.toolID,
					DisplayName:  "search_docs",
					OwnerUserID:  "planner",
					Kind:         model.ToolKindIntegration,
					Status:       model.ToolStatusActive,
					AccessLevel:  model.ToolAccessWorkspace,
					Endpoint:     "https://mcp.example.test",
					ManifestJSON: `{"route":{"kind":"mcp","server_id":"notion-projection-operation-invalid","tool_name":"search_docs"}}`,
				}}, tc.sourceSurface, tc.operationID)
			})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected %q rejection, got %v", tc.wantErr, err)
			}
			if _, getErr := store.GetWorkspaceTool(ctx, workspaceID, tc.toolID); !errors.Is(getErr, sqlite.ErrToolNotFound) {
				t.Fatalf("expected no workspace tool after invalid projection operation, got %v", getErr)
			}
			events, listErr := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "workspace_tool.registered",
				EntityType:  "workspace_tool",
				EntityID:    tc.toolID,
				Limit:       10,
			})
			if listErr != nil {
				t.Fatalf("list events: %v", listErr)
			}
			if len(events) != 0 {
				t.Fatalf("expected no runtime events after invalid projection operation, got %+v", events)
			}
		})
	}
}

func assertWorkspaceToolPromptContextEnvelope(t *testing.T, payloadJSON string, expected map[string]string) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode workspace-tool prompt payload: %v; payload=%q", err, payloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in workspace-tool payload %+v", payload)
	}
	for key, want := range expected {
		if got, _ := envelope[key].(string); got != want {
			t.Fatalf("expected envelope %s=%q, got %q in %+v", key, want, got, envelope)
		}
	}
}

func mustMarshalPromptContextJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal prompt-context test value: %v", err)
	}
	return string(raw)
}

func seedToolRegistryProjectionOperationRun(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, runID, surface string) {
	t.Helper()
	operationKind := map[string]string{
		"mcp.tool.discover":   "mcp_discover",
		"mcp.server.register": "mcp_server_register",
		"mcp.server.remove":   "mcp_server_remove",
	}[surface]
	seedToolRegistryProjectionOperationRunRaw(t, ctx, store, workspaceID, runID, surface, operationKind, surface)
}

func seedToolRegistryProjectionOperationRunRaw(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, runID, surface, operationKind, requestedCapability string) {
	t.Helper()
	_, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       runID,
		Title:       "MCP projection parent operation",
		Status:      "ACTIVE",
		Verification: sqlite.AttachExecutionPromptContextEnvelope(
			map[string]any{
				"operation_ledger": map[string]any{
					"schema":         "operation_ledger.v1",
					"operation_id":   runID,
					"operation_kind": operationKind,
					"capability_snapshot": map[string]any{
						"requested_capability": requestedCapability,
					},
				},
			},
			sqlite.BuildExecutionPromptContextEnvelope(surface, "server_operation_ledger", workspaceID, "system", "mcp_projection_test"),
		),
	})
	if err != nil {
		t.Fatalf("seed projection operation run %s/%s: %v", workspaceID, runID, err)
	}
}
