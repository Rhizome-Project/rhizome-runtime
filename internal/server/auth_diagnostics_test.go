package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAuthMiddlewareWithStoreOrServerTokenAcceptsHumanAccessToken(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	ctx := context.Background()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-auth-diagnostics-human",
		Title:       "Auth Diagnostics Human",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       "ws-auth-diagnostics-human",
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "developer",
		DisplayName:       "Example User",
		Password:          "secret-password",
		IPAddress:         "203.0.113.10",
		UserAgent:         "rhizome-auth-test/1.0",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	handler := AuthMiddlewareWithStoreOrServerToken(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := authPrincipalFromContext(r.Context())
		if !ok {
			t.Fatal("expected authenticated principal on diagnostics request")
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"workspace_id":   principal.WorkspaceID,
			"principal_type": principal.PrincipalType,
			"principal_id":   principal.PrincipalID,
		})
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/diagnostics", nil)
	req.Header.Set("Authorization", "Bearer "+human.Token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}
	if payload["workspace_id"] != human.WorkspaceID || payload["principal_type"] != "human" || payload["principal_id"] != human.UserID {
		t.Fatalf("unexpected authenticated principal payload: %+v", payload)
	}
}

func TestAuthMiddlewareWithStoreOrServerTokenPreservesLegacyServerToken(t *testing.T) {
	t.Setenv("RHIZOME_API_TOKEN", "server-token-123")

	handler := AuthMiddlewareWithStoreOrServerToken(nil, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := authPrincipalFromContext(r.Context()); ok {
			t.Fatal("did not expect store principal when authenticating via legacy server token")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/diagnostics", nil)
	req.Header.Set("Authorization", "Bearer server-token-123")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}
