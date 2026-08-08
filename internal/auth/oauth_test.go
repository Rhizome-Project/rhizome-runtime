package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestBuildAuthorizeURL(t *testing.T) {
	cfg := OAuthConfig{
		ClientID:     "test-client-id",
		AuthorizeURL: "https://example.com/authorize",
		RedirectURI:  "http://localhost:9999/auth/callback",
		Scope:        "openid profile email",
	}

	u := BuildAuthorizeURL(cfg, "test-challenge", "test-state")

	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatalf("failed to parse URL: %v", err)
	}

	if parsed.Scheme != "https" || parsed.Host != "example.com" || parsed.Path != "/authorize" {
		t.Errorf("unexpected base URL: %s", u)
	}

	q := parsed.Query()
	checks := map[string]string{
		"response_type":         "code",
		"client_id":             "test-client-id",
		"redirect_uri":          "http://localhost:9999/auth/callback",
		"scope":                 "openid profile email",
		"code_challenge":        "test-challenge",
		"code_challenge_method": "S256",
		"state":                 "test-state",
	}
	for key, want := range checks {
		got := q.Get(key)
		if got != want {
			t.Errorf("param %s = %q, want %q", key, got, want)
		}
	}
}

func TestExchangeAuthCode(t *testing.T) {
	expectedCode := "auth-code-123"
	expectedVerifier := "verifier-456"
	expectedClientID := "test-client"
	expectedRedirectURI := "http://localhost:9999/auth/callback"
	mockIDToken := makeTestJWT(map[string]string{"email": "user@example.com"})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		ct := r.Header.Get("Content-Type")
		if ct != "application/x-www-form-urlencoded" {
			t.Errorf("expected application/x-www-form-urlencoded, got %s", ct)
		}

		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}

		if r.FormValue("grant_type") != "authorization_code" {
			t.Errorf("grant_type = %q, want authorization_code", r.FormValue("grant_type"))
		}
		if r.FormValue("code") != expectedCode {
			t.Errorf("code = %q, want %q", r.FormValue("code"), expectedCode)
		}
		if r.FormValue("client_id") != expectedClientID {
			t.Errorf("client_id = %q, want %q", r.FormValue("client_id"), expectedClientID)
		}
		if r.FormValue("redirect_uri") != expectedRedirectURI {
			t.Errorf("redirect_uri = %q, want %q", r.FormValue("redirect_uri"), expectedRedirectURI)
		}
		if r.FormValue("code_verifier") != expectedVerifier {
			t.Errorf("code_verifier = %q, want %q", r.FormValue("code_verifier"), expectedVerifier)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"id_token": mockIDToken})
	}))
	defer srv.Close()

	cfg := OAuthConfig{
		ClientID:    expectedClientID,
		TokenURL:    srv.URL,
		RedirectURI: expectedRedirectURI,
	}

	idToken, err := ExchangeAuthCode(context.Background(), cfg, expectedCode, expectedVerifier)
	if err != nil {
		t.Fatalf("ExchangeAuthCode error: %v", err)
	}
	if idToken != mockIDToken {
		t.Errorf("idToken = %q, want %q", idToken, mockIDToken)
	}
}

func TestExchangeAuthCode_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":"invalid_grant"}`)
	}))
	defer srv.Close()

	cfg := OAuthConfig{
		ClientID: "test",
		TokenURL: srv.URL,
	}

	_, err := ExchangeAuthCode(context.Background(), cfg, "bad-code", "verifier")
	if err == nil {
		t.Fatal("expected error for HTTP 400")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention status code 400: %v", err)
	}
}

func TestExchangeForAPIKey(t *testing.T) {
	mockIDToken := makeTestJWT(map[string]string{"email": "user@example.com"})
	expectedAPIKey := "sk-test-api-key-123"
	expectedClientID := "test-client"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}

		if r.FormValue("grant_type") != "urn:ietf:params:oauth:grant-type:token-exchange" {
			t.Errorf("grant_type = %q", r.FormValue("grant_type"))
		}
		if r.FormValue("client_id") != expectedClientID {
			t.Errorf("client_id = %q, want %q", r.FormValue("client_id"), expectedClientID)
		}
		if r.FormValue("subject_token_type") != "urn:ietf:params:oauth:token-type:id_token" {
			t.Errorf("subject_token_type = %q", r.FormValue("subject_token_type"))
		}
		if r.FormValue("subject_token") != mockIDToken {
			t.Errorf("subject_token mismatch")
		}
		if r.FormValue("requested_token_type") != "urn:openai:token-type:api-key" {
			t.Errorf("requested_token_type = %q", r.FormValue("requested_token_type"))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"access_token": expectedAPIKey})
	}))
	defer srv.Close()

	cfg := OAuthConfig{
		ClientID: expectedClientID,
		TokenURL: srv.URL,
	}

	apiKey, err := ExchangeForAPIKey(context.Background(), cfg, mockIDToken)
	if err != nil {
		t.Fatalf("ExchangeForAPIKey error: %v", err)
	}
	if apiKey != expectedAPIKey {
		t.Errorf("apiKey = %q, want %q", apiKey, expectedAPIKey)
	}
}

func TestCallbackHandler_Success(t *testing.T) {
	state := "expected-state-value"
	resultCh := make(chan callbackResult, 1)
	handler := makeCallbackHandler(state, resultCh)

	req := httptest.NewRequest(http.MethodGet, "/auth/callback?code=test-code&state="+state, nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if !strings.Contains(w.Body.String(), "You can close this tab") {
		t.Error("response body should contain success message")
	}

	select {
	case result := <-resultCh:
		if result.Err != nil {
			t.Fatalf("unexpected error: %v", result.Err)
		}
		if result.Code != "test-code" {
			t.Errorf("code = %q, want %q", result.Code, "test-code")
		}
	default:
		t.Fatal("no result received on channel")
	}
}

func TestCallbackHandler_StateMismatch(t *testing.T) {
	resultCh := make(chan callbackResult, 1)
	handler := makeCallbackHandler("correct-state", resultCh)

	req := httptest.NewRequest(http.MethodGet, "/auth/callback?code=test-code&state=wrong-state", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	select {
	case result := <-resultCh:
		if result.Err == nil {
			t.Fatal("expected error for state mismatch")
		}
		if !strings.Contains(result.Err.Error(), "state mismatch") {
			t.Errorf("error should mention state mismatch: %v", result.Err)
		}
	default:
		t.Fatal("no result received on channel")
	}
}

func TestCallbackHandler_NoCode(t *testing.T) {
	state := "test-state"
	resultCh := make(chan callbackResult, 1)
	handler := makeCallbackHandler(state, resultCh)

	req := httptest.NewRequest(http.MethodGet, "/auth/callback?state="+state, nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	select {
	case result := <-resultCh:
		if result.Err == nil {
			t.Fatal("expected error for missing code")
		}
	default:
		t.Fatal("no result received on channel")
	}
}

func TestExtractEmailFromIDToken(t *testing.T) {
	tests := []struct {
		name  string
		token string
		want  string
	}{
		{
			name:  "valid JWT with email",
			token: makeTestJWT(map[string]string{"email": "alice@example.com"}),
			want:  "alice@example.com",
		},
		{
			name:  "valid JWT without email",
			token: makeTestJWT(map[string]string{"sub": "12345"}),
			want:  "",
		},
		{
			name:  "invalid JWT no dots",
			token: "not-a-jwt",
			want:  "",
		},
		{
			name:  "invalid base64 payload",
			token: "header.!!!invalid!!!.signature",
			want:  "",
		},
		{
			name:  "invalid JSON payload",
			token: "header." + base64.RawURLEncoding.EncodeToString([]byte("not json")) + ".signature",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractEmailFromIDToken(tt.token)
			if got != tt.want {
				t.Errorf("ExtractEmailFromIDToken() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOAuthFlow_Timeout(t *testing.T) {
	// Use a very short timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	cfg := OAuthConfig{
		ClientID:     "test-client",
		AuthorizeURL: "https://example.com/authorize",
		TokenURL:     "https://example.com/token",
		RedirectURI:  "http://localhost:0/auth/callback",
		Scope:        "openid",
		ListenAddr:   ":0",
		OpenBrowser:  false,
	}

	_, err := RunOAuthFlow(ctx, cfg)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("error should mention timeout: %v", err)
	}
}

// makeTestJWT creates a minimal JWT with the given claims for testing.
func makeTestJWT(claims map[string]string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	payload, _ := json.Marshal(claims)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payload)
	return header + "." + payloadB64 + ".test-signature"
}
