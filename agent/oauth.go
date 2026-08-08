package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

const (
	oauthClientID     = "app_EMoamEEZ73f0CkXaXp7hrann"
	oauthAuthorizeURL = "https://auth.openai.com/oauth/authorize"
	oauthTokenURL     = "https://auth.openai.com/oauth/token"
	oauthRedirectURI  = "http://localhost:1455/auth/callback"
	oauthScope        = "openid profile email offline_access"
	oauthListenAddr   = ":1455"
)

// RunBrowserAuth opens a browser for OpenAI OAuth and returns the API key.
func RunBrowserAuth() (string, error) {
	// PKCE
	verifier, err := generateCodeVerifier()
	if err != nil {
		return "", err
	}
	challenge := codeChallenge(verifier)

	state, err := generateState()
	if err != nil {
		return "", err
	}

	// Auth URL
	params := url.Values{
		"response_type":         {"code"},
		"client_id":             {oauthClientID},
		"redirect_uri":          {oauthRedirectURI},
		"scope":                 {oauthScope},
		"code_challenge":        {challenge},
		"code_challenge_method": {"S256"},
		"state":                 {state},
	}
	authURL := oauthAuthorizeURL + "?" + params.Encode()

	// Callback server
	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/auth/callback", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != state {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "<h1>Error: state mismatch</h1>")
			errCh <- fmt.Errorf("state mismatch")
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "<h1>Error: no code</h1>")
			errCh <- fmt.Errorf("no auth code")
			return
		}
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<h1>Success! You can close this tab.</h1>")
		codeCh <- code
	})

	srv := &http.Server{Handler: mux}
	ln, err := net.Listen("tcp", oauthListenAddr)
	if err != nil {
		return "", fmt.Errorf("port %s in use: %w", oauthListenAddr, err)
	}
	go srv.Serve(ln)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	// Open browser
	fmt.Fprintf(os.Stderr, "Opening browser for OpenAI auth...\n")
	fmt.Fprintf(os.Stderr, "URL: %s\n", authURL)
	openBrowser(authURL)

	// Wait for callback
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var code string
	select {
	case code = <-codeCh:
	case err := <-errCh:
		return "", err
	case <-ctx.Done():
		return "", fmt.Errorf("auth timed out (5 min)")
	}

	// Exchange code → id_token
	idToken, err := exchangeAuthCode(ctx, code, verifier)
	if err != nil {
		return "", err
	}

	// Exchange id_token → API key
	apiKey, err := exchangeForAPIKey(ctx, idToken)
	if err != nil {
		return "", err
	}

	email := extractEmail(idToken)
	if email != "" {
		fmt.Fprintf(os.Stderr, "Authenticated as: %s\n", email)
	}

	return apiKey, nil
}

func exchangeAuthCode(ctx context.Context, code, verifier string) (string, error) {
	data := url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {oauthClientID},
		"code":          {code},
		"redirect_uri":  {oauthRedirectURI},
		"code_verifier": {verifier},
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", oauthTokenURL, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("token endpoint %d: %s", resp.StatusCode, body)
	}

	var result struct {
		IDToken string `json:"id_token"`
	}
	json.Unmarshal(body, &result)
	if result.IDToken == "" {
		return "", fmt.Errorf("no id_token in response")
	}
	return result.IDToken, nil
}

func exchangeForAPIKey(ctx context.Context, idToken string) (string, error) {
	data := url.Values{
		"grant_type":         {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"client_id":          {oauthClientID},
		"subject_token_type": {"urn:ietf:params:oauth:token-type:id_token"},
		"subject_token":      {idToken},
		"requested_token":    {"openai-api-key"},
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", oauthTokenURL, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("exchange request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("exchange endpoint %d: %s", resp.StatusCode, body)
	}

	var result struct {
		AccessToken string `json:"access_token"`
	}
	json.Unmarshal(body, &result)
	if result.AccessToken == "" {
		return "", fmt.Errorf("no access_token in response")
	}
	return result.AccessToken, nil
}

func extractEmail(idToken string) string {
	parts := strings.SplitN(idToken, ".", 3)
	if len(parts) < 2 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Email string `json:"email"`
	}
	json.Unmarshal(payload, &claims)
	return claims.Email
}

func generateCodeVerifier() (string, error) {
	buf := make([]byte, 64)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func codeChallenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

func generateState() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func openBrowser(u string) {
	name, args := browserCommandForGOOS(runtime.GOOS, u)
	if name == "" {
		return
	}
	_ = exec.Command(name, args...).Start()
}

func browserCommandForGOOS(goos, u string) (string, []string) {
	switch goos {
	case "darwin":
		return "open", []string{u}
	case "linux":
		return "xdg-open", []string{u}
	case "windows":
		return "rundll32", []string{"url.dll,FileProtocolHandler", u}
	default:
		return "", nil
	}
}
