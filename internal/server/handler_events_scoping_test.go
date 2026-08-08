package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// ── P1A-004: /events workspace scoping ──────────────────────────────

func TestServeEventsHTTPRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-events-owned",
		Title:       "Events Owned",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	auth, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-events-owned",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-events-scoping",
		DisplayName:       "Agent Events Scoping",
		OwnerUserID:       "developer",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	handler := AuthMiddlewareWithStore(store, h.ServeEventsHTTP())

	// Request events for a different workspace than the authenticated principal
	req := httptest.NewRequest(http.MethodGet, "/events?workspace_id=ws-events-foreign", nil)
	req.Header.Set("Authorization", "Bearer "+auth.Token)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-workspace SSE subscription, got %d body=%s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), "workspace isolation violation") {
		t.Fatalf("expected workspace isolation violation message, got %s", resp.Body.String())
	}
}

func TestServeEventsHTTPAllowsCorrectWorkspace(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-events-correct",
		Title:       "Events Correct",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	auth, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-events-correct",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-events-ok",
		DisplayName:       "Agent Events OK",
		OwnerUserID:       "developer",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	handler := AuthMiddlewareWithStore(store, h.ServeEventsHTTP())

	// Request events for the correct workspace — should start SSE stream
	req := httptest.NewRequest(http.MethodGet, "/events?workspace_id=ws-events-correct", nil)
	req.Header.Set("Authorization", "Bearer "+auth.Token)
	resp := httptest.NewRecorder()

	// Since SSE blocks forever, we cancel the context after checking headers
	cancelCtx, cancel := context.WithCancel(ctx)
	req = req.WithContext(cancelCtx)
	go func() {
		// Wait for the handler to write the initial heartbeat, then cancel
		for resp.Body.Len() == 0 {
			// busy wait briefly
		}
		cancel()
	}()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200 for correct-workspace SSE subscription, got %d body=%s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), "connected to workspace ws-events-correct") {
		t.Fatalf("expected SSE heartbeat, got %s", resp.Body.String())
	}
}

// ── P1A-005: no wildcard SSE origin ─────────────────────────────────

func TestServeEventsHTTPNoWildcardOrigin(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-events-cors",
		Title:       "Events CORS",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	auth, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       "ws-events-cors",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "agent-cors",
		DisplayName:       "Agent CORS",
		OwnerUserID:       "developer",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	handler := AuthMiddlewareWithStore(store, h.ServeEventsHTTP())

	// Test 1: with Origin header → should echo back origin, not *
	cancelCtx, cancel := context.WithCancel(ctx)
	req := httptest.NewRequest(http.MethodGet, "/events?workspace_id=ws-events-cors", nil)
	req.Header.Set("Authorization", "Bearer "+auth.Token)
	req.Header.Set("Origin", "https://dashboard.example.com")
	req = req.WithContext(cancelCtx)
	resp := httptest.NewRecorder()
	go func() {
		for resp.Body.Len() == 0 {
		}
		cancel()
	}()
	handler.ServeHTTP(resp, req)

	acao := resp.Header().Get("Access-Control-Allow-Origin")
	if acao == "*" {
		t.Fatal("expected non-wildcard Access-Control-Allow-Origin, got *")
	}
	if acao != "https://dashboard.example.com" {
		t.Fatalf("expected origin echo, got %q", acao)
	}
	if resp.Header().Get("Vary") != "Origin" {
		t.Fatalf("expected Vary: Origin header, got %q", resp.Header().Get("Vary"))
	}

	// Test 2: without Origin header → no ACAO header at all
	cancelCtx2, cancel2 := context.WithCancel(ctx)
	req2 := httptest.NewRequest(http.MethodGet, "/events?workspace_id=ws-events-cors", nil)
	req2.Header.Set("Authorization", "Bearer "+auth.Token)
	req2 = req2.WithContext(cancelCtx2)
	resp2 := httptest.NewRecorder()
	go func() {
		for resp2.Body.Len() == 0 {
		}
		cancel2()
	}()
	handler.ServeHTTP(resp2, req2)

	if acao2 := resp2.Header().Get("Access-Control-Allow-Origin"); acao2 != "" {
		t.Fatalf("expected no ACAO header without Origin, got %q", acao2)
	}
}
