package server

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryGraphAtlasRejectsWorkspaceIsolationViolation(t *testing.T) {
	store := newServerTestStore(t)
	ctx := context.Background()

	for _, workspaceID := range []string{"ws-memory-atlas-auth", "ws-other-memory-atlas-auth"} {
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "Memory Atlas Auth",
			CreatedBy:   "developer",
		}); err != nil {
			t.Fatalf("create workspace %s: %v", workspaceID, err)
		}
	}

	h := NewHandler(store)
	result, rpcErr := h.workspaceMemoryGraphAtlas(
		testAuthContext("ws-other-memory-atlas-auth", "human", "developer"),
		mustJSONRaw(workspaceMemoryGraphAtlasParams{
			WorkspaceID: "ws-memory-atlas-auth",
			LimitNodes:  20,
			LimitEdges:  20,
		}),
	)
	if rpcErr == nil {
		t.Fatal("expected workspace isolation reject for atlas handler")
	}
	if result != nil {
		t.Fatalf("expected no atlas result on auth reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied, got %+v", rpcErr)
	}
	if rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("expected workspace isolation violation message, got %+v", rpcErr)
	}
}

func TestWorkspaceMemoryGraphAtlasRejectsInvalidEpistemicStatus(t *testing.T) {
	store := newServerTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-atlas-invalid",
		Title:       "Memory Atlas Invalid",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	h := NewHandler(store)
	result, rpcErr := h.workspaceMemoryGraphAtlas(
		testAuthContext("ws-memory-atlas-invalid", "human", "developer"),
		mustJSONRaw(workspaceMemoryGraphAtlasParams{
			WorkspaceID:     "ws-memory-atlas-invalid",
			EpistemicStatus: "UNCERTAIN",
			LimitNodes:      20,
			LimitEdges:      20,
		}),
	)
	if rpcErr == nil {
		t.Fatal("expected invalid epistemic_status reject for atlas handler")
	}
	if result != nil {
		t.Fatalf("expected no atlas result on invalid params, got %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params, got %+v", rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "epistemic_status") {
		t.Fatalf("expected epistemic_status validation message, got %+v", rpcErr)
	}
}

func TestWorkspaceMemoryGraphAtlasRejectsInvalidTypeAndOriginContract(t *testing.T) {
	store := newServerTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-atlas-invalid-contract",
		Title:       "Memory Atlas Invalid Contract",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	h := NewHandler(store)
	cases := []struct {
		name    string
		params  workspaceMemoryGraphAtlasParams
		message string
	}{
		{
			name: "invalid memory type",
			params: workspaceMemoryGraphAtlasParams{
				WorkspaceID: "ws-memory-atlas-invalid-contract",
				MemoryType:  "DECISON",
				LimitNodes:  20,
				LimitEdges:  20,
			},
			message: "memory_type",
		},
		{
			name: "invalid origin kind",
			params: workspaceMemoryGraphAtlasParams{
				WorkspaceID: "ws-memory-atlas-invalid-contract",
				OriginKind:  "workspace_segment",
				LimitNodes:  20,
				LimitEdges:  20,
			},
			message: "origin_kind",
		},
		{
			name: "canonical origin conflict",
			params: workspaceMemoryGraphAtlasParams{
				WorkspaceID:   "ws-memory-atlas-invalid-contract",
				CanonicalOnly: true,
				OriginKind:    "knowledge_claim",
				LimitNodes:    20,
				LimitEdges:    20,
			},
			message: "canonical_only",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, rpcErr := h.workspaceMemoryGraphAtlas(
				testAuthContext("ws-memory-atlas-invalid-contract", "human", "developer"),
				mustJSONRaw(tc.params),
			)
			if rpcErr == nil {
				t.Fatalf("expected invalid params reject for %s", tc.name)
			}
			if result != nil {
				t.Fatalf("expected no atlas result on invalid params, got %+v", result)
			}
			if rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params, got %+v", rpcErr)
			}
			if !strings.Contains(strings.ToLower(rpcErr.Message), strings.ToLower(tc.message)) {
				t.Fatalf("expected message to mention %q, got %+v", tc.message, rpcErr)
			}
		})
	}
}
