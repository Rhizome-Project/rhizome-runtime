package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// AuthMiddleware returns an HTTP middleware that validates a Bearer token.
// The expected token is read from the RHIZOME_API_TOKEN environment variable.
// If the variable is empty, all requests are allowed (development mode).
func AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := os.Getenv("RHIZOME_API_TOKEN")
		if expected == "" {
			// Fail-open is disabled: token must be configured.
			writeAuthFailure(w, r, http.StatusUnauthorized, "RHIZOME_API_TOKEN is not configured on the server")
			return
		}

		token, errMsg := extractAuthToken(r)
		if errMsg != "" {
			writeAuthFailure(w, r, http.StatusUnauthorized, errMsg)
			return
		}

		if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
			writeAuthFailure(w, r, http.StatusUnauthorized, "invalid token")
			return
		}

		next.ServeHTTP(w, r)
	})
}

type authPrincipalContextKey struct{}

type requestMetadataContextKey struct{}

type RequestMetadata struct {
	ClientIP  string
	UserAgent string
}

type AuthPrincipal struct {
	WorkspaceID       string
	PrincipalType     string
	PrincipalID       string
	TokenID           string
	TokenPrefix       string
	DisplayName       string
	RuntimeOrigin     string
	TokenMetadataJSON string
}

// AuthMiddlewareWithStore validates Bearer tokens against the SQLite auth store.
// It records token usage metadata and attaches the authenticated principal to the request context.
func AuthMiddlewareWithStore(store *sqlite.Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if store == nil {
			writeAuthFailure(w, r, http.StatusUnauthorized, "auth store is not configured on the server")
			return
		}

		token, errMsg := extractAuthToken(r)
		if errMsg != "" {
			writeAuthFailure(w, r, http.StatusUnauthorized, errMsg)
			return
		}

		meta := RequestMetadata{
			ClientIP:  requestClientIP(r),
			UserAgent: strings.TrimSpace(r.UserAgent()),
		}
		ctx, ok := authenticateStoreToken(r.Context(), store, token, meta)
		if !ok {
			writeAuthFailure(w, r, http.StatusUnauthorized, "invalid token")
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// AuthMiddlewareWithStoreOrServerToken accepts either a scoped access token from
// the SQLite auth store or the legacy RHIZOME_API_TOKEN server token.
func AuthMiddlewareWithStoreOrServerToken(store *sqlite.Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, errMsg := extractAuthToken(r)
		if errMsg != "" {
			writeAuthFailure(w, r, http.StatusUnauthorized, errMsg)
			return
		}

		if serverTokenMatches(token) {
			next.ServeHTTP(w, r)
			return
		}

		if store == nil {
			writeAuthFailure(w, r, http.StatusUnauthorized, "auth store is not configured on the server")
			return
		}

		meta := RequestMetadata{
			ClientIP:  requestClientIP(r),
			UserAgent: strings.TrimSpace(r.UserAgent()),
		}
		ctx, ok := authenticateStoreToken(r.Context(), store, token, meta)
		if !ok {
			writeAuthFailure(w, r, http.StatusUnauthorized, "invalid token")
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func extractAuthToken(r *http.Request) (string, string) {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth != "" {
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			return "", "authorization must use Bearer scheme"
		}
		token := strings.TrimSpace(strings.TrimPrefix(auth, prefix))
		if token == "" {
			return "", "missing Authorization header"
		}
		return token, ""
	}

	return "", "missing Authorization header"
}

func authenticateStoreToken(ctx context.Context, store *sqlite.Store, token string, meta RequestMetadata) (context.Context, bool) {
	if store == nil {
		return nil, false
	}
	record, err := store.AuthenticateAccessToken(ctx, token)
	if err != nil {
		return nil, false
	}
	ctx = context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:       record.WorkspaceID,
		PrincipalType:     record.SubjectType,
		PrincipalID:       record.SubjectID,
		TokenID:           record.TokenID,
		TokenPrefix:       record.TokenPrefix,
		DisplayName:       record.DisplayName,
		RuntimeOrigin:     authPrincipalRuntimeOrigin(record.MetadataJSON),
		TokenMetadataJSON: record.MetadataJSON,
	})
	ctx = context.WithValue(ctx, requestMetadataContextKey{}, meta)
	return ctx, true
}

func authPrincipalRuntimeOrigin(metadataJSON string) string {
	var metadata map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(firstNonEmpty(metadataJSON, "{}"))), &metadata); err != nil {
		return ""
	}
	for _, key := range []string{"runtime_origin", "origin", "capability_class"} {
		if value, _ := metadata[key].(string); strings.EqualFold(strings.TrimSpace(value), "agent_responder") {
			return "agent_responder"
		}
	}
	return ""
}

func serverTokenMatches(token string) bool {
	expected := strings.TrimSpace(os.Getenv("RHIZOME_API_TOKEN"))
	if expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

func writeAuthFailure(w http.ResponseWriter, r *http.Request, status int, message string) {
	if strings.HasPrefix(r.URL.Path, "/rpc") {
		writeRPCError(w, nil, -32000, message)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/events") {
		http.Error(w, message, status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message": message,
	})
}

func requestClientIP(r *http.Request) string {
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		if idx := strings.Index(xff, ","); idx >= 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return xff
	}
	if xri := strings.TrimSpace(r.Header.Get("X-Real-IP")); xri != "" {
		return xri
	}
	return requestTransportPeerIP(r)
}

// requestTransportPeerIP returns the address of the TCP peer without trusting
// forwarding headers supplied by the request. Use it for unauthenticated
// endpoints where attacker-controlled headers must not create fresh buckets.
func requestTransportPeerIP(r *http.Request) string {
	remote := strings.TrimSpace(r.RemoteAddr)
	if remote == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(remote); err == nil {
		return host
	}
	return remote
}

func authPrincipalFromContext(ctx context.Context) (AuthPrincipal, bool) {
	principal, ok := ctx.Value(authPrincipalContextKey{}).(AuthPrincipal)
	return principal, ok
}

func requestMetadataFromContext(ctx context.Context) (RequestMetadata, bool) {
	meta, ok := ctx.Value(requestMetadataContextKey{}).(RequestMetadata)
	return meta, ok
}

func requireWorkspacePrincipal(ctx context.Context, workspaceID string) (AuthPrincipal, *RPCError) {
	principal, ok := authPrincipalFromContext(ctx)
	if !ok {
		return AuthPrincipal{}, &RPCError{Code: errCodePermissionDenied, Message: "unauthorized"}
	}
	if workspaceID != principal.WorkspaceID {
		return AuthPrincipal{}, &RPCError{Code: errCodePermissionDenied, Message: "workspace isolation violation"}
	}
	return principal, nil
}

func requireWorkspacePrincipalIfPresent(ctx context.Context, workspaceID string) (AuthPrincipal, bool, *RPCError) {
	principal, ok := authPrincipalFromContext(ctx)
	if !ok {
		return AuthPrincipal{}, false, nil
	}
	if strings.TrimSpace(workspaceID) != strings.TrimSpace(principal.WorkspaceID) {
		return AuthPrincipal{}, true, &RPCError{Code: errCodePermissionDenied, Message: "workspace isolation violation"}
	}
	return principal, true, nil
}

func requireAgentPrincipal(ctx context.Context, workspaceID, agentID, subjectParam string) (AuthPrincipal, *RPCError) {
	principal, rpcErr := requireWorkspacePrincipal(ctx, workspaceID)
	if rpcErr != nil {
		return AuthPrincipal{}, rpcErr
	}
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return AuthPrincipal{}, &RPCError{Code: errCodePermissionDenied, Message: "agent principal required"}
	}
	if agentID != principal.PrincipalID {
		return AuthPrincipal{}, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match " + subjectParam}
	}
	return principal, nil
}

func requireAgentPrincipalIfPresent(ctx context.Context, workspaceID, agentID, subjectParam string) (AuthPrincipal, bool, *RPCError) {
	principal, ok, rpcErr := requireWorkspacePrincipalIfPresent(ctx, workspaceID)
	if rpcErr != nil || !ok {
		return AuthPrincipal{}, ok, rpcErr
	}
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return principal, ok, nil
	}
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(agentID) != strings.TrimSpace(principal.PrincipalID) {
		return AuthPrincipal{}, true, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match " + subjectParam}
	}
	return principal, true, nil
}

func requireWorkspaceActorPrincipal(ctx context.Context, workspaceID, actorID, subjectParam string) (AuthPrincipal, *RPCError) {
	principal, rpcErr := requireWorkspacePrincipal(ctx, workspaceID)
	if rpcErr != nil {
		return AuthPrincipal{}, rpcErr
	}
	if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "system") {
		return principal, nil
	}
	if strings.TrimSpace(actorID) == "" || strings.TrimSpace(actorID) != strings.TrimSpace(principal.PrincipalID) {
		return AuthPrincipal{}, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match " + subjectParam}
	}
	return principal, nil
}

func requireWorkspaceActorPrincipalIfPresent(ctx context.Context, workspaceID, actorID, subjectParam string) (AuthPrincipal, bool, *RPCError) {
	principal, ok, rpcErr := requireWorkspacePrincipalIfPresent(ctx, workspaceID)
	if rpcErr != nil || !ok {
		return AuthPrincipal{}, ok, rpcErr
	}
	if strings.TrimSpace(actorID) == "" || strings.TrimSpace(actorID) != strings.TrimSpace(principal.PrincipalID) {
		return AuthPrincipal{}, true, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match " + subjectParam}
	}
	return principal, true, nil
}
