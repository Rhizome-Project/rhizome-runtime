package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// OAuthConfig holds the configuration for the OAuth authorization code + PKCE flow.
type OAuthConfig struct {
	ClientID     string
	AuthorizeURL string
	TokenURL     string
	RedirectURI  string
	Scope        string
	ListenAddr   string

	// OpenBrowser controls whether the flow attempts to open the browser.
	// Set to false in tests.
	OpenBrowser bool
}

// DefaultOAuthConfig returns an OAuthConfig with production defaults.
func DefaultOAuthConfig() OAuthConfig {
	return OAuthConfig{
		ClientID:     "app_EMoamEEZ73f0CkXaXp7hrann",
		AuthorizeURL: "https://auth.openai.com/oauth/authorize",
		TokenURL:     "https://auth.openai.com/oauth/token",
		RedirectURI:  "http://localhost:1455/auth/callback",
		Scope:        "openid profile email offline_access",
		ListenAddr:   ":1455",
		OpenBrowser:  true,
	}
}

// OAuthResult contains the results of a successful OAuth flow.
type OAuthResult struct {
	APIKey  string
	Email   string
	IDToken string
}

// callbackResult is sent from the HTTP callback handler to the main flow.
type callbackResult struct {
	Code string
	Err  error
}

// BuildAuthorizeURL constructs the authorization URL with all required query parameters.
func BuildAuthorizeURL(cfg OAuthConfig, codeChallenge, state string) string {
	params := url.Values{
		"response_type":         {"code"},
		"client_id":             {cfg.ClientID},
		"redirect_uri":          {cfg.RedirectURI},
		"scope":                 {cfg.Scope},
		"code_challenge":        {codeChallenge},
		"code_challenge_method": {"S256"},
		"state":                 {state},
	}
	return cfg.AuthorizeURL + "?" + params.Encode()
}

// RunOAuthFlow executes the full OAuth Authorization Code + PKCE flow.
// It starts a local HTTP server, opens the browser, waits for the callback,
// exchanges the auth code for tokens, and returns the API key.
func RunOAuthFlow(ctx context.Context, cfg OAuthConfig) (*OAuthResult, error) {
	// Step 1: Generate PKCE codes
	verifier, err := GenerateCodeVerifier()
	if err != nil {
		return nil, fmt.Errorf("generating code verifier: %w", err)
	}
	challenge := CodeChallenge(verifier)

	// Step 2: Generate state
	state, err := GenerateState()
	if err != nil {
		return nil, fmt.Errorf("generating state: %w", err)
	}

	// Step 3: Build authorization URL
	authURL := BuildAuthorizeURL(cfg, challenge, state)

	// Step 4: Set up callback server
	resultCh := make(chan callbackResult, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/auth/callback", makeCallbackHandler(state, resultCh))

	server := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}

	// Start listener explicitly to detect port conflicts early
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("port %s is in use, try --listen-addr with a different port: %w", cfg.ListenAddr, err)
	}

	// Set up 5-minute timeout
	flowCtx, flowCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer flowCancel()

	// Start server in background
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- server.Serve(ln)
	}()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	// Step 5: Open browser
	if cfg.OpenBrowser {
		openBrowser(authURL)
	}
	// Always print URL as fallback
	fmt.Fprintf(getStderr(), "Open this URL in your browser:\n%s\n", authURL)

	// Step 6: Wait for callback
	var code string
	select {
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		code = result.Code
	case <-flowCtx.Done():
		return nil, fmt.Errorf("timed out waiting for OAuth callback (5 minutes)")
	}

	// Step 7: Exchange auth code for id_token
	idToken, err := ExchangeAuthCode(flowCtx, cfg, code, verifier)
	if err != nil {
		return nil, fmt.Errorf("exchanging auth code: %w", err)
	}

	// Step 8: Exchange id_token for API key
	apiKey, err := ExchangeForAPIKey(flowCtx, cfg, idToken)
	if err != nil {
		return nil, fmt.Errorf("exchanging for API key: %w", err)
	}

	// Step 9: Extract email from id_token
	email := ExtractEmailFromIDToken(idToken)

	return &OAuthResult{
		APIKey:  apiKey,
		Email:   email,
		IDToken: idToken,
	}, nil
}

// getStderr returns a writer for informational output. It defaults to io.Discard
// to keep the library quiet; the CLI layer prints the URL itself via os.Stderr.
var getStderr = func() io.Writer {
	return io.Discard
}

// makeCallbackHandler returns an HTTP handler for the OAuth callback endpoint.
func makeCallbackHandler(expectedState string, resultCh chan<- callbackResult) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		receivedState := r.URL.Query().Get("state")
		if receivedState != expectedState {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "<html><body><h1>Error</h1><p>State mismatch. Please try again.</p></body></html>")
			resultCh <- callbackResult{Err: fmt.Errorf("state mismatch: expected %q, got %q", expectedState, receivedState)}
			return
		}

		code := r.URL.Query().Get("code")
		if code == "" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "<html><body><h1>Error</h1><p>No authorization code received.</p></body></html>")
			resultCh <- callbackResult{Err: fmt.Errorf("no authorization code in callback")}
			return
		}

		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html><body><h1>Success!</h1><p>You can close this tab.</p></body></html>")
		resultCh <- callbackResult{Code: code}
	}
}

// ExchangeAuthCode exchanges an authorization code for an id_token.
func ExchangeAuthCode(ctx context.Context, cfg OAuthConfig, code, codeVerifier string) (string, error) {
	data := url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {cfg.ClientID},
		"code":          {code},
		"redirect_uri":  {cfg.RedirectURI},
		"code_verifier": {codeVerifier},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("creating token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		IDToken string `json:"id_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("parsing token response: %w", err)
	}
	if tokenResp.IDToken == "" {
		return "", fmt.Errorf("no id_token in token response")
	}

	return tokenResp.IDToken, nil
}

// ExchangeForAPIKey exchanges an id_token for an OpenAI API key via RFC 8693 token exchange.
func ExchangeForAPIKey(ctx context.Context, cfg OAuthConfig, idToken string) (string, error) {
	data := url.Values{
		"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"client_id":            {cfg.ClientID},
		"subject_token_type":   {"urn:ietf:params:oauth:token-type:id_token"},
		"subject_token":        {idToken},
		"requested_token_type": {"urn:openai:token-type:api-key"},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("creating exchange request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("exchange request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading exchange response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("exchange endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var exchangeResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &exchangeResp); err != nil {
		return "", fmt.Errorf("parsing exchange response: %w", err)
	}
	if exchangeResp.AccessToken == "" {
		return "", fmt.Errorf("no access_token in exchange response")
	}

	return exchangeResp.AccessToken, nil
}

// ExtractEmailFromIDToken extracts the email claim from a JWT id_token
// by base64-decoding the payload segment. No signature verification is performed.
func ExtractEmailFromIDToken(idToken string) string {
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
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}

	return claims.Email
}

// openBrowser opens the given URL in the default browser.
func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	default:
		return
	}
	_ = cmd.Start()
}
