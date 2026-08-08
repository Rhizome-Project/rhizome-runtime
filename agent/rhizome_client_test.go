package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type blockingReadCloser struct {
	ctx context.Context
}

func TestRPCDescribeMethodUnsupportedRecognizesUnexpectedMethod(t *testing.T) {
	err := errors.New("rpc rpc.describe: unexpected method rpc.describe")
	if !rpcDescribeMethodUnsupported(err) {
		t.Fatalf("expected unexpected rpc.describe method error to be treated as unsupported")
	}
}

func TestControlReportClusterAcceptsObjectSignals(t *testing.T) {
	var report ControlReport
	raw := []byte(`{
		"workspace_id":"ws",
		"clusters":[{
			"proto_cluster_id":"cluster-1",
			"signals":{"attention_band":"WATCH","pressure_score":7},
			"suggested_controls":{"priority_focus":"review"}
		}]
	}`)
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("ControlReport unmarshal failed: %v", err)
	}
	if len(report.Clusters) != 1 {
		t.Fatalf("expected one cluster, got %+v", report)
	}
	if len(report.Clusters[0].Signals) != 1 || report.Clusters[0].Signals[0]["attention_band"] != "WATCH" {
		t.Fatalf("expected object signals to normalize into one-entry list, got %+v", report.Clusters[0].Signals)
	}
	if len(report.Clusters[0].SuggestedControls) != 1 || report.Clusters[0].SuggestedControls[0]["priority_focus"] != "review" {
		t.Fatalf("expected object suggested_controls to normalize into one-entry list, got %+v", report.Clusters[0].SuggestedControls)
	}
}

func (b blockingReadCloser) Read(_ []byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b blockingReadCloser) Close() error { return nil }

func TestRhizomeClientRegisterAgent(t *testing.T) {
	var gotMethod string
	var gotAuth string
	var gotParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotMethod, _ = req["method"].(string)
		gotParams, _ = req["params"].(map[string]any)

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"agent_id":     "agent-a",
				"display_name": "Agent A",
				"token":        "agent-token-1",
				"workspace_id": "ws-test",
				"agent": map[string]any{
					"agent_id":         "agent-a",
					"workspace_id":     "ws-test",
					"owner_user_id":    "developer",
					"display_name":     "Agent A",
					"role":             "generalist",
					"status":           "REGISTERED",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "registered",
					"created_at":       "2026-03-23T00:00:00Z",
					"updated_at":       "2026-03-23T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "secret")
	record, err := client.RegisterAgent(context.Background(), AgentRegisterInput{
		WorkspaceID:       "ws-test",
		WorkspaceName:     "Workspace Test",
		WorkspacePassword: "test-workspace-password",
		HostURL:           "https://rhizome.test",
		AgentID:           "agent-a",
		GroupID:           "codex",
		DisplayName:       "Agent A",
		OwnerUserID:       "developer",
	})
	if err != nil {
		t.Fatalf("RegisterAgent() error: %v", err)
	}
	if gotMethod != "workspace.auth.agent.register" {
		t.Fatalf("expected method workspace.auth.agent.register, got %q", gotMethod)
	}
	if gotAuth != "" {
		t.Fatalf("expected self-registration to omit bearer auth, got %q", gotAuth)
	}
	if gotParams["workspace_id"] != "ws-test" {
		t.Fatalf("unexpected params: %+v", gotParams)
	}
	if gotParams["workspace_password"] != "test-workspace-password" {
		t.Fatalf("expected workspace_password in params, got %+v", gotParams)
	}
	if gotParams["group_id"] != "codex" {
		t.Fatalf("expected group_id to be forwarded, got %+v", gotParams)
	}
	if workspaceName, ok := gotParams["workspace_name"]; ok && workspaceName != "" {
		t.Fatalf("expected authoritative workspace_id registration params, got %+v", gotParams)
	}
	if gotParams["host_url"] != "https://rhizome.test" {
		t.Fatalf("expected authoritative workspace_id registration params, got %+v", gotParams)
	}
	if record.Agent.AgentID != "agent-a" || record.Agent.DisplayName != "Agent A" {
		t.Fatalf("unexpected record: %+v", record)
	}
	if record.Token != "agent-token-1" || record.WorkspaceID != "ws-test" || record.WorkspaceName != "Workspace Test" || record.HostURL != "https://rhizome.test" {
		t.Fatalf("unexpected register result: %+v", record)
	}
	if len(record.Agent.Capabilities) != 1 || record.Agent.Capabilities[0] != "tool.call" {
		t.Fatalf("expected authoritative agent capabilities in register result, got %+v", record.Agent)
	}
}

func TestRhizomeClientRespondRequestClampsOversizedResponse(t *testing.T) {
	var gotResponse string
	var gotBodyLen int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		gotBodyLen = len(body)
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		params, _ := req["params"].(map[string]any)
		gotResponse, _ = params["response"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result":  map[string]any{"status": "COMPLETED"},
		})
	}))
	defer server.Close()

	original := strings.Repeat("é", maxAgentRespondResponseBytes)
	clamped := clampAgentRespondResponse(original)
	if !utf8.ValidString(clamped) {
		t.Fatal("clamped response must remain valid UTF-8")
	}
	if strings.ContainsRune(clamped, utf8.RuneError) {
		t.Fatal("clamped response must not contain a replacement rune")
	}
	if err := NewRhizomeClient(server.URL, "token").RespondRequest(context.Background(), "req-1", original); err != nil {
		t.Fatalf("RespondRequest error: %v", err)
	}

	if gotBodyLen >= 1<<20 {
		t.Fatalf("agent.respond request body was not kept below default RPC cap: %d", gotBodyLen)
	}
	if len(gotResponse) > maxAgentRespondResponseBytes {
		t.Fatalf("response len = %d, want <= %d", len(gotResponse), maxAgentRespondResponseBytes)
	}
	if !strings.Contains(gotResponse, "truncated agent.respond response") {
		tailStart := len(gotResponse) - 160
		if tailStart < 0 {
			tailStart = 0
		}
		t.Fatalf("expected truncation marker, got tail %q", gotResponse[tailStart:])
	}
	if !strings.Contains(gotResponse, "é") {
		t.Fatalf("expected preserved UTF-8 prefix")
	}
}

func TestRhizomeClientCallSurfacesTransportTimeoutExplicitly(t *testing.T) {
	client := NewRhizomeClient("https://rhizome.test/rpc", "")
	client.client = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	err := client.call(ctx, "agent.request.result", map[string]any{"workspace_id": "ws-test"}, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded classification, got %v", err)
	}
	if !strings.Contains(err.Error(), "rpc agent.request.result timed out") {
		t.Fatalf("expected explicit timeout error, got %v", err)
	}
}

func TestRhizomeClientCallSurfacesTransportCancellationExplicitly(t *testing.T) {
	client := NewRhizomeClient("https://rhizome.test/rpc", "")
	client.client = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.call(ctx, "agent.request.result", map[string]any{"workspace_id": "ws-test"}, nil)
	if err == nil {
		t.Fatal("expected canceled error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled classification, got %v", err)
	}
	if !strings.Contains(err.Error(), "rpc agent.request.result canceled") {
		t.Fatalf("expected explicit canceled error, got %v", err)
	}
}

func TestRhizomeClientCallSurfacesReadBodyDeadlineExplicitly(t *testing.T) {
	client := NewRhizomeClient("https://rhizome.test/rpc", "")
	client.client = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       blockingReadCloser{ctx: req.Context()},
				Header:     make(http.Header),
			}, nil
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	var out map[string]any
	err := client.call(ctx, "agent.request.result", map[string]any{"workspace_id": "ws-test"}, &out)
	if err == nil {
		t.Fatal("expected read-body timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded classification, got %v", err)
	}
	if !strings.Contains(err.Error(), "rpc agent.request.result timed out") {
		t.Fatalf("expected explicit read-body timeout error, got %v", err)
	}
}

func TestRhizomeClientCallPreservesRPCErrorCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"error": map[string]any{
				"code":    rhizomeRPCCodeDocumentConflict,
				"message": "sha drifted",
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	err := client.call(context.Background(), "workspace.doc.put", map[string]any{"workspace_id": "ws-test"}, nil)
	if err == nil {
		t.Fatal("expected rpc error")
	}
	var rpcErr *RhizomeRPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("expected RhizomeRPCError, got %T %[1]v", err)
	}
	if rpcErr.Method != "workspace.doc.put" || rpcErr.Code != rhizomeRPCCodeDocumentConflict || rpcErr.Message != "sha drifted" {
		t.Fatalf("unexpected rpc error payload: %+v", rpcErr)
	}
	if got := err.Error(); got != "rpc workspace.doc.put: sha drifted" {
		t.Fatalf("expected legacy-compatible error text, got %q", got)
	}
}

func TestRhizomeClientRegisterAgentUsesHTTPAuthEndpointWhenRPCIsProtected(t *testing.T) {
	var sawHTTPAuth bool
	var sawRPC bool
	var gotHTTPAuth map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		switch r.URL.Path {
		case "/api/auth/agent/register":
			sawHTTPAuth = true
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode auth register request: %v", err)
			}
			gotHTTPAuth = req
			if req["workspace_password"] != "test-workspace-password" {
				t.Fatalf("expected workspace password in auth request, got %+v", req)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"workspace_id":   "ws-test",
				"workspace_name": "Workspace Test",
				"agent_id":       "agent-a",
				"display_name":   "Agent A",
				"access_token":   "agent-token-http",
				"agent": map[string]any{
					"agent_id":         "agent-a",
					"workspace_id":     "ws-test",
					"owner_user_id":    "developer",
					"display_name":     "Agent A",
					"role":             "generalist",
					"status":           "REGISTERED",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "registered",
					"created_at":       "2026-03-23T00:00:00Z",
					"updated_at":       "2026-03-23T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
			})
		case "/rpc":
			sawRPC = true
			t.Fatalf("did not expect RPC fallback when HTTP auth endpoint is available")
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL+"/rpc", "")
	record, err := client.RegisterAgent(context.Background(), AgentRegisterInput{
		WorkspaceID:       "ws-test",
		WorkspaceName:     "Workspace Test",
		WorkspacePassword: "test-workspace-password",
		HostURL:           server.URL,
		AgentID:           "agent-a",
		GroupID:           "codex",
		DisplayName:       "Agent A",
		OwnerUserID:       "developer",
	})
	if err != nil {
		t.Fatalf("RegisterAgent() error: %v", err)
	}
	if !sawHTTPAuth {
		t.Fatal("expected HTTP auth endpoint to be used")
	}
	if gotHTTPAuth["group_id"] != "codex" {
		t.Fatalf("expected group_id in auth registration payload, got %+v", gotHTTPAuth)
	}
	if workspaceName, ok := gotHTTPAuth["workspace_name"]; ok && workspaceName != "" {
		t.Fatalf("expected workspace_name to be suppressed for HTTP auth registration, got %+v", gotHTTPAuth)
	}
	if sawRPC {
		t.Fatal("did not expect RPC call when HTTP auth succeeded")
	}
	if record.Token != "agent-token-http" || record.WorkspaceID != "ws-test" || record.HostURL != server.URL {
		t.Fatalf("unexpected auth register result: %+v", record)
	}
	if record.Agent.AgentID != "agent-a" || len(record.Agent.Capabilities) != 1 {
		t.Fatalf("expected http auth register result to include authoritative agent record, got %+v", record)
	}
}

func TestRhizomeClientRegisterAgentSuppressesLegacyWorkspaceNameWhenWorkspaceIDIsPresent(t *testing.T) {
	var gotParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotParams, _ = req["params"].(map[string]any)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"agent_id":       "agent-a",
				"display_name":   "Agent A",
				"token":          "agent-token-1",
				"workspace_id":   "ws-test",
				"workspace_name": "Workspace Test",
				"agent": map[string]any{
					"agent_id":         "agent-a",
					"workspace_id":     "ws-test",
					"owner_user_id":    "developer",
					"display_name":     "Agent A",
					"role":             "generalist",
					"status":           "REGISTERED",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "registered",
					"created_at":       "2026-03-23T00:00:00Z",
					"updated_at":       "2026-03-23T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	_, err := client.RegisterAgent(context.Background(), AgentRegisterInput{
		WorkspaceID:       "ws-test",
		WorkspaceName:     "Workspace One",
		WorkspacePassword: "test-workspace-password",
		HostURL:           "https://rhizome.test",
		AgentID:           "agent-a",
		DisplayName:       "Agent A",
		OwnerUserID:       "developer",
	})
	if err != nil {
		t.Fatalf("RegisterAgent() error: %v", err)
	}
	if workspaceName, ok := gotParams["workspace_name"]; ok && workspaceName != "" {
		t.Fatalf("expected workspace_name to be suppressed when workspace_id is present, got %+v", gotParams)
	}
}

func TestRhizomeClientRegisterAgentFallsBackToLegacyMethod(t *testing.T) {
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if strings.HasPrefix(r.URL.Path, "/api/auth/agent/register") {
			http.NotFound(w, r)
			return
		}

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)

		switch method {
		case "workspace.auth.agent.register":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32601,
					"message": "unknown method: workspace.auth.agent.register",
				},
			})
		case "agent.register":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"agent_id":       "agent-a",
					"display_name":   "Agent A",
					"token":          "agent-token-legacy",
					"workspace_id":   "ws-test",
					"workspace_name": "Workspace Test",
					"agent": map[string]any{
						"agent_id":         "agent-a",
						"workspace_id":     "ws-test",
						"owner_user_id":    "developer",
						"display_name":     "Agent A",
						"role":             "generalist",
						"status":           "REGISTERED",
						"protocol_version": "rnar/v1",
						"capabilities":     []string{"tool.call"},
						"summary":          "online",
						"created_at":       "2026-03-23T00:00:00Z",
						"updated_at":       "2026-03-23T00:00:00Z",
						"is_online":        true,
						"active_tasks":     []any{},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	record, err := client.RegisterAgent(context.Background(), AgentRegisterInput{
		WorkspaceID:       "ws-test",
		WorkspaceName:     "Workspace Test",
		WorkspacePassword: "test-workspace-password",
		HostURL:           "https://rhizome.test",
		AgentID:           "agent-a",
		DisplayName:       "Agent A",
		OwnerUserID:       "developer",
	})
	if err != nil {
		t.Fatalf("RegisterAgent() error: %v", err)
	}
	if len(methods) != 2 || methods[0] != "workspace.auth.agent.register" || methods[1] != "agent.register" {
		t.Fatalf("unexpected method sequence: %+v", methods)
	}
	if record.Agent.AgentID != "agent-a" || record.WorkspaceID != "ws-test" || record.WorkspaceName != "Workspace Test" || record.HostURL != "https://rhizome.test" {
		t.Fatalf("unexpected fallback result: %+v", record)
	}
	if record.Token != "agent-token-legacy" {
		t.Fatalf("expected legacy fallback token to survive normalization, got %+v", record)
	}
}

func TestRhizomeClientRegisterAgentRejectsPartialRegistrationTruth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"agent_id":     "agent-a",
				"display_name": "Agent A",
				"token":        "agent-token-1",
				"workspace_id": "ws-test",
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	_, err := client.RegisterAgent(context.Background(), AgentRegisterInput{
		WorkspaceID:       "ws-test",
		WorkspacePassword: "test-workspace-password",
		HostURL:           "https://rhizome.test",
		AgentID:           "agent-a",
		DisplayName:       "Agent A",
		OwnerUserID:       "developer",
	})
	if err == nil || !strings.Contains(err.Error(), "missing authoritative agent record") {
		t.Fatalf("expected partial registration truth to fail closed, got %v", err)
	}
}

func TestRhizomeClientRegisterAgentRejectsMismatchedRegisteredIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"agent_id":     "agent-b",
				"display_name": "Agent B",
				"token":        "agent-token-1",
				"workspace_id": "ws-test",
				"agent": map[string]any{
					"agent_id":         "agent-b",
					"workspace_id":     "ws-test",
					"owner_user_id":    "developer",
					"display_name":     "Agent B",
					"role":             "generalist",
					"status":           "REGISTERED",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "registered",
					"created_at":       "2026-03-23T00:00:00Z",
					"updated_at":       "2026-03-23T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	_, err := client.RegisterAgent(context.Background(), AgentRegisterInput{
		WorkspaceID:       "ws-test",
		WorkspacePassword: "test-workspace-password",
		HostURL:           "https://rhizome.test",
		AgentID:           "agent-a",
		DisplayName:       "Agent A",
		OwnerUserID:       "developer",
	})
	if err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("expected mismatched registration truth to fail closed, got %v", err)
	}
}

func TestRhizomeClientStateGetMissingReturnsNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      "1",
			"error": map[string]any{
				"code":    -32000,
				"message": "get agent state: sql: no rows in result set",
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	value, ok, err := client.StateGet(context.Background(), "ws", "agent", "missing")
	if err != nil {
		t.Fatalf("StateGet() unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected missing state, got ok=true value=%q", value)
	}
	if value != "" {
		t.Fatalf("expected empty value, got %q", value)
	}
}

func TestRhizomeClientPolicyCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      "1",
			"result": map[string]any{
				"check": map[string]any{
					"workspace_id": "ws",
					"subject_type": "agent",
					"subject_id":   "agent-a",
					"capability":   "tool.call",
					"tool_id":      "shell",
					"verdict":      "ALLOW",
				},
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	check, err := client.CheckPolicy(context.Background(), PolicyCheckInput{
		WorkspaceID: "ws",
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "shell",
	})
	if err != nil {
		t.Fatalf("CheckPolicy() error: %v", err)
	}
	if check.Verdict != "ALLOW" {
		t.Fatalf("expected ALLOW verdict, got %+v", check)
	}
}

func TestRhizomeClientWorkNextAndHydrateTask(t *testing.T) {
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)

		switch method {
		case "agent.work.next":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"generated_at":                 "2026-03-23T00:00:00Z",
					"workspace_id":                 "ws",
					"agent_id":                     "agent-a",
					"has_work":                     true,
					"reason":                       "resume_session",
					"project_id":                   "project-alpha",
					"task_kind":                    "EXECUTION",
					"project_lane":                 "implementation",
					"requires_project_gate":        true,
					"project_gate_block":           map[string]any{"state": "blocked", "reason": "project review pending", "gate_id": "gate-1"},
					"project_coordination":         map[string]any{"lane": "implementation", "peers": []any{"agent-b"}, "write_scope_hints": []any{"agent/**"}},
					"autonomous_execution_allowed": true,
					"profile_gate_reason":          "profile_allows_autonomous_execution",
					"profile_gate_summary":         "Agent profile allows autonomous execution.",
					"packet": map[string]any{
						"work_type":             "resume_session",
						"coordination_state":    "active",
						"preferred_transition":  "continue",
						"why_now":               "resume_session",
						"project_id":            "project-alpha",
						"task_kind":             "EXECUTION",
						"project_lane":          "implementation",
						"requires_project_gate": true,
						"project_gate_block":    map[string]any{"state": "blocked", "reason": "project review pending", "gate_id": "gate-1"},
						"project_coordination":  map[string]any{"lane": "implementation", "peers": []any{"agent-b"}, "write_scope_hints": []any{"agent/**"}},
						"gate": map[string]any{
							"gate_state":  "open",
							"gate_type":   "approval",
							"needed_from": "human",
							"summary":     "need approval",
						},
						"context_hints": map[string]any{"anchor_task_ids": []any{"task-1"}},
						"advisory":      map[string]any{"proto_cluster_id": "cluster-1"},
					},
					"task": map[string]any{
						"task_id":               "task-1",
						"title":                 "Task 1",
						"owner_user_id":         "owner-1",
						"priority":              "HIGH",
						"status":                "PENDING",
						"task_kind":             "general",
						"task_template":         "default",
						"project_id":            "project-alpha",
						"project_lane":          "implementation",
						"requires_project_gate": true,
						"linked_by":             "system",
						"linked_at":             "2026-03-23T00:00:00Z",
					},
					"session": map[string]any{
						"session_id":   "sess-1",
						"workspace_id": "ws",
						"agent_id":     "agent-a",
						"task_id":      "task-1",
						"status":       "ACTIVE",
						"summary":      "resume",
						"updated_at":   "2026-03-23T00:00:00Z",
						"started_at":   "2026-03-23T00:00:00Z",
					},
				},
			})
		case "agent.task.hydrate":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"bundle": map[string]any{
						"generated_at": "2026-03-23T00:00:00Z",
						"workspace_task": map[string]any{
							"task_id":               "task-1",
							"title":                 "Task 1",
							"owner_user_id":         "owner-1",
							"priority":              "HIGH",
							"status":                "PENDING",
							"task_kind":             "general",
							"task_template":         "default",
							"project_id":            "project-alpha",
							"project_lane":          "implementation",
							"requires_project_gate": true,
							"linked_by":             "system",
							"linked_at":             "2026-03-23T00:00:00Z",
						},
						"task": map[string]any{
							"task_id":               "task-1",
							"title":                 "Task 1",
							"owner_user_id":         "owner-1",
							"priority":              "HIGH",
							"status":                "PENDING",
							"task_kind":             "general",
							"task_template":         "default",
							"project_id":            "project-alpha",
							"project_lane":          "implementation",
							"requires_project_gate": true,
							"node_counts":           map[string]any{},
							"nodes":                 []any{},
						},
						"docs":          []any{},
						"task_links":    []any{},
						"related_tasks": []any{},
						"artifacts":     []any{},
						"updates":       []any{},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	work, err := client.WorkNext(context.Background(), WorkNextInput{
		WorkspaceID: "ws",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("WorkNext() error: %v", err)
	}
	if !work.HasWork || work.Task == nil || work.Task.TaskID != "task-1" || work.Session == nil || work.Session.SessionID != "sess-1" {
		t.Fatalf("unexpected work result: %+v", work)
	}
	if work.Packet == nil || work.Packet.WorkType != "resume_session" || work.Packet.Advisory == nil || work.Packet.Advisory.ProtoClusterID != "cluster-1" {
		t.Fatalf("expected typed work packet, got %+v", work.Packet)
	}
	if !work.AutonomousExecutionAllowed || work.ProfileGateReason != "profile_allows_autonomous_execution" || work.ProfileGateSummary != "Agent profile allows autonomous execution." {
		t.Fatalf("expected typed profile gate fields, got %+v", work)
	}
	if work.Packet.Gate == nil || work.Packet.Gate.GateType != "approval" || work.Packet.Gate.NeededFrom != "human" {
		t.Fatalf("expected typed gate packet, got %+v", work.Packet)
	}
	if work.ProjectID != "project-alpha" || work.TaskKind != "EXECUTION" || work.ProjectLane != "implementation" || work.RequiresProjectGate == nil || !*work.RequiresProjectGate {
		t.Fatalf("expected top-level project digest fields, got %+v", work)
	}
	if !strings.Contains(string(work.ProjectGateBlock), "project review pending") || !strings.Contains(string(work.ProjectCoordination), "agent/**") {
		t.Fatalf("expected raw top-level project digest payloads to be preserved, gate=%s coordination=%s", work.ProjectGateBlock, work.ProjectCoordination)
	}
	if work.Packet.ProjectID != "project-alpha" || work.Packet.TaskKind != "EXECUTION" || work.Packet.ProjectLane != "implementation" || work.Packet.RequiresProjectGate == nil || !*work.Packet.RequiresProjectGate {
		t.Fatalf("expected packet project digest fields, got %+v", work.Packet)
	}
	if !strings.Contains(string(work.Packet.ProjectGateBlock), "project review pending") || !strings.Contains(string(work.Packet.ProjectCoordination), "agent-b") {
		t.Fatalf("expected raw packet project payloads to be preserved, gate=%s coordination=%s", work.Packet.ProjectGateBlock, work.Packet.ProjectCoordination)
	}
	if work.Task == nil || work.Task.ProjectID != "project-alpha" || work.Task.ProjectLane != "implementation" || work.Task.RequiresProjectGate == nil || !*work.Task.RequiresProjectGate {
		t.Fatalf("expected task project fields, got %+v", work.Task)
	}
	encodedWork, err := json.Marshal(work)
	if err != nil {
		t.Fatalf("marshal work result: %v", err)
	}
	if !strings.Contains(string(encodedWork), `"project_gate_block"`) || !strings.Contains(string(encodedWork), `"project_coordination"`) || !strings.Contains(string(encodedWork), `"write_scope_hints"`) {
		t.Fatalf("expected project raw payloads to round-trip, got %s", encodedWork)
	}

	hydration, err := client.HydrateTask(context.Background(), TaskHydrationInput{
		WorkspaceID: "ws",
		TaskID:      "task-1",
	})
	if err != nil {
		t.Fatalf("HydrateTask() error: %v", err)
	}
	if hydration.Task.TaskID != "task-1" {
		t.Fatalf("unexpected hydration bundle: %+v", hydration)
	}
	if hydration.Task.ProjectID != "project-alpha" || hydration.Task.ProjectLane != "implementation" || hydration.Task.RequiresProjectGate == nil || !*hydration.Task.RequiresProjectGate {
		t.Fatalf("expected hydrated task project fields, got %+v", hydration.Task)
	}
	if hydration.WorkspaceTask == nil || hydration.WorkspaceTask.ProjectID != "project-alpha" || hydration.WorkspaceTask.ProjectLane != "implementation" || hydration.WorkspaceTask.RequiresProjectGate == nil || !*hydration.WorkspaceTask.RequiresProjectGate {
		t.Fatalf("expected hydrated workspace task project fields, got %+v", hydration.WorkspaceTask)
	}
	if len(methods) != 2 || methods[0] != "agent.work.next" || methods[1] != "agent.task.hydrate" {
		t.Fatalf("unexpected method sequence: %+v", methods)
	}
}

func TestRhizomeClientWorkNextRetriesHTTP429(t *testing.T) {
	previousDelay := rhizomeWorkNextRetryDelay
	rhizomeWorkNextRetryDelay = func(int) time.Duration { return 0 }
	t.Cleanup(func() { rhizomeWorkNextRetryDelay = previousDelay })

	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		req := decodeRPCRequest(t, r)
		if req.Method != "agent.work.next" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		attempts++
		if attempts < 3 {
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		writeRPCResult(w, req, map[string]any{
			"generated_at": "2026-06-08T00:00:00Z",
			"workspace_id": "ws",
			"agent_id":     "agent-a",
			"has_work":     false,
			"reason":       "idle",
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	work, err := client.WorkNext(context.Background(), WorkNextInput{
		WorkspaceID: "ws",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("WorkNext() error after retryable 429s: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if work.WorkspaceID != "ws" || work.AgentID != "agent-a" || work.HasWork {
		t.Fatalf("unexpected work.next result after retry: %+v", work)
	}
}

func TestRhizomeClientCoordinationReads(t *testing.T) {
	var methods []string
	var frontierParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)
		params, _ := req["params"].(map[string]any)

		switch method {
		case "workspace.instrumentation.control.cluster":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"detail": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id":        "cluster-1",
							"resolution_kind":         "task",
							"task_ids":                []any{"task-1"},
							"doc_keys":                []any{"task.task-1"},
							"artifact_refs":           []any{"doc:deliverable"},
							"summary":                 "Review pressure dominates this task cluster",
							"basis_stale":             false,
							"metrics":                 map[string]any{"event_count": 4, "active_session_count": 1},
							"signals":                 map[string]any{"attention_band": "WATCH", "pressure_score": 7},
							"suggested_controls":      map[string]any{"priority_focus": "review"},
							"confirmed_tension_count": 1,
							"pending_tension_count":   0,
						},
						"tensions": []any{
							map[string]any{
								"tension_id":       "tension-1",
								"workspace_id":     "ws",
								"proto_cluster_id": "cluster-1",
								"tension_type":     "gap",
								"review_status":    "CONFIRMED",
								"lifecycle_state":  "ACTIVE",
								"title":            "Need acceptance evidence",
								"anchor_kind":      "task",
								"anchor_ref":       "task-1",
								"surface_score":    9,
								"task_ids":         []any{"task-1"},
								"created_at":       "2026-03-23T00:00:00Z",
								"updated_at":       "2026-03-23T00:00:00Z",
							},
						},
					},
				},
			})
		case "workspace.instrumentation.corridor.cluster":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"detail": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id":      "cluster-1",
							"resolution_kind":       "task",
							"task_class_hint":       "PROOF",
							"task_class_source":     "authored",
							"corridor_catalog_hint": "proof.review",
							"corridor_readiness":    "READY",
							"summary":               "This cluster is ready for proof-like work",
							"task_class_confidence": 0.92,
							"readiness_confidence":  0.88,
							"metrics":               map[string]any{"event_count": 4, "active_session_count": 1},
						},
					},
				},
			})
		case "workspace.tension.frontier":
			frontierParams = params
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []any{
						map[string]any{
							"tension_id":       "tension-1",
							"proto_cluster_id": "cluster-1",
							"tension_type":     "gap",
							"review_status":    "CONFIRMED",
							"title":            "Need acceptance evidence",
							"summary":          "Acceptance evidence is still missing",
							"surface_score":    9,
							"evidence_count":   2,
							"last_seen_at":     "2026-03-23T00:00:00Z",
						},
					},
				},
			})
		case "workspace.tension.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tension": map[string]any{
						"tension_id":       "tension-1",
						"workspace_id":     "ws",
						"proto_cluster_id": "cluster-1",
						"tension_type":     "gap",
						"review_status":    "CONFIRMED",
						"lifecycle_state":  "ACTIVE",
						"title":            "Need acceptance evidence",
						"summary":          "Acceptance evidence is still missing",
						"anchor_kind":      "task",
						"anchor_ref":       "task-1",
						"surface_score":    9,
						"task_ids":         []any{"task-1"},
						"doc_keys":         []any{"task.task-1"},
						"artifact_refs":    []any{"doc:deliverable"},
						"created_at":       "2026-03-23T00:00:00Z",
						"updated_at":       "2026-03-23T00:00:00Z",
					},
					"docs": []any{map[string]any{"doc_key": "task.task-1", "title": "Task 1"}},
					"artifacts": []any{map[string]any{
						"artifact_id":  "artifact-1",
						"workspace_id": "ws",
						"title":        "Deliverable",
						"artifact_ref": "doc:deliverable",
						"kind":         "workspace_doc",
						"content_type": "text/markdown",
						"created_by":   "agent-1",
						"created_at":   "2026-03-23T00:00:00Z",
					}},
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	control, err := client.GetControlClusterDetail(context.Background(), ControlClusterInput{
		WorkspaceID:    "ws",
		ProtoClusterID: "cluster-1",
	})
	if err != nil {
		t.Fatalf("GetControlClusterDetail() error: %v", err)
	}
	if control.Cluster.ProtoClusterID != "cluster-1" || control.Cluster.Signals.AttentionBand != "WATCH" {
		t.Fatalf("unexpected control detail: %+v", control)
	}

	corridor, err := client.GetCorridorClusterDetail(context.Background(), CorridorClusterInput{
		WorkspaceID:    "ws",
		ProtoClusterID: "cluster-1",
	})
	if err != nil {
		t.Fatalf("GetCorridorClusterDetail() error: %v", err)
	}
	if corridor.Cluster.CorridorReadiness != "READY" || corridor.Cluster.TaskClassHint != "PROOF" {
		t.Fatalf("unexpected corridor detail: %+v", corridor)
	}

	frontier, err := client.ListTensionFrontier(context.Background(), TensionFrontierInput{
		WorkspaceID: "ws",
		TaskID:      "task-1",
		Limit:       3,
	})
	if err != nil {
		t.Fatalf("ListTensionFrontier() error: %v", err)
	}
	if frontierParams["task_id"] != "task-1" || frontierParams["limit"] != float64(3) {
		t.Fatalf("unexpected frontier params: %+v", frontierParams)
	}
	if len(frontier) != 1 || frontier[0].TensionID != "tension-1" {
		t.Fatalf("unexpected frontier: %+v", frontier)
	}

	tension, err := client.GetTension(context.Background(), "ws", "tension-1")
	if err != nil {
		t.Fatalf("GetTension() error: %v", err)
	}
	if tension.Tension.TensionID != "tension-1" || tension.Tension.AnchorRef != "task-1" || len(tension.Artifacts) != 1 {
		t.Fatalf("unexpected tension detail: %+v", tension)
	}

	if want := []string{
		"workspace.instrumentation.control.cluster",
		"workspace.instrumentation.corridor.cluster",
		"workspace.tension.frontier",
		"workspace.tension.get",
	}; len(methods) != len(want) {
		t.Fatalf("unexpected methods: %+v", methods)
	} else {
		for i := range want {
			if methods[i] != want[i] {
				t.Fatalf("unexpected methods: %+v", methods)
			}
		}
	}
}

func TestRhizomeClientGetLocusBundle(t *testing.T) {
	var gotMethod string
	var gotParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotMethod, _ = req["method"].(string)
		gotParams, _ = req["params"].(map[string]any)

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"bundle": map[string]any{
					"workspace_id":     "ws",
					"generated_at":     "2026-03-23T10:00:00Z",
					"resolved":         true,
					"resolved_from":    "task_id",
					"match_score":      8,
					"proto_cluster_id": "cluster-1",
					"control": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id": "cluster-1",
							"resolution_kind":  "task",
							"task_ids":         []any{"task-1"},
							"signals":          map[string]any{"attention_band": "WATCH", "pressure_score": 7},
							"summary":          "Review pressure dominates this task cluster",
						},
					},
					"control_state": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id": "cluster-1",
							"summary":          "cluster state summary",
						},
						"state": map[string]any{
							"proto_cluster_id":          "cluster-1",
							"resolution_kind":           "task",
							"heuristic_profile_context": map[string]any{"profile": "integration"},
							"state": map[string]any{
								"workspace_id":            "ws",
								"proto_cluster_id":        "cluster-1",
								"resolution_kind":         "task",
								"heuristic_profile":       "integration",
								"epoch":                   2,
								"stabilized_mode_hint":    "STEADY",
								"candidate_mode_hint":     "COHERENCE",
								"dominant_signal_kind":    "review",
								"attention_band":          "WATCH",
								"pressure_score":          7,
								"operator_hints":          map[string]any{"priority_focus": "review"},
								"signal_deviation_vector": map[string]any{"review": 0.3},
								"created_at":              "2026-03-23T00:00:00Z",
								"updated_at":              "2026-03-23T00:00:00Z",
							},
							"metrics":            map[string]any{"event_count": 4},
							"signals":            map[string]any{"attention_band": "WATCH", "pressure_score": 7},
							"suggested_controls": map[string]any{"priority_focus": "review"},
						},
					},
					"corridor": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id":      "cluster-1",
							"resolution_kind":       "task",
							"task_ids":              []any{"task-1"},
							"task_class_hint":       "PROOF",
							"task_class_source":     "authored",
							"corridor_catalog_hint": "proof.review",
							"corridor_readiness":    "READY",
							"summary":               "This cluster is ready for proof-like work",
						},
					},
					"corridor_fit": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id":      "cluster-1",
							"resolution_kind":       "task",
							"task_class_hint":       "PROOF",
							"corridor_catalog_hint": "proof.review",
							"corridor_readiness":    "READY",
							"fit_status":            "IN_CORRIDOR",
							"fit_score":             0,
						},
					},
					"frontier": []any{
						map[string]any{
							"tension_id":       "tension-1",
							"proto_cluster_id": "cluster-1",
							"tension_type":     "gap",
							"review_status":    "CONFIRMED",
							"title":            "Need acceptance evidence",
							"surface_score":    9,
						},
					},
					"dominant_tension": map[string]any{
						"tension": map[string]any{
							"tension_id":       "tension-1",
							"workspace_id":     "ws",
							"proto_cluster_id": "cluster-1",
							"tension_type":     "gap",
							"review_status":    "CONFIRMED",
							"lifecycle_state":  "ACTIVE",
							"title":            "Need acceptance evidence",
							"anchor_kind":      "task",
							"anchor_ref":       "task-1",
							"surface_score":    9,
							"created_at":       "2026-03-23T00:00:00Z",
							"updated_at":       "2026-03-23T00:00:00Z",
						},
					},
				},
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	bundle, err := client.GetLocusBundle(context.Background(), LocusBundleInput{
		WorkspaceID:   "ws",
		TaskID:        "task-1",
		SessionID:     "sess-1",
		DocKeys:       []string{"task.task-1"},
		ArtifactRefs:  []string{"doc:deliverable"},
		FrontierLimit: 3,
	})
	if err != nil {
		t.Fatalf("GetLocusBundle() error: %v", err)
	}
	if gotMethod != "workspace.instrumentation.locus.bundle" {
		t.Fatalf("unexpected method %q", gotMethod)
	}
	if gotParams["task_id"] != "task-1" || gotParams["frontier_limit"] != float64(3) {
		t.Fatalf("unexpected params: %+v", gotParams)
	}
	if !bundle.Resolved || bundle.ProtoClusterID != "cluster-1" {
		t.Fatalf("unexpected bundle: %+v", bundle)
	}
	if bundle.Control == nil || bundle.ControlState == nil || bundle.Corridor == nil || bundle.DominantTension == nil {
		t.Fatalf("expected bundled locus surfaces, got %+v", bundle)
	}
}

func TestRhizomeClientRequestExternalGate(t *testing.T) {
	var methods []string
	var gotParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)
		gotParams, _ = req["params"].(map[string]any)

		switch method {
		case "workspace.ops.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32602,
					"message": "operator queue item not found",
				},
			})
		case "workspace.ops.request":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"status": "REQUESTED"},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	err := client.RequestExternalGate(context.Background(), ExternalGateRequestInput{
		WorkspaceID:       "ws",
		RequestKey:        "rnar.task.task-1",
		GateType:          "PAYMENT_BILLING",
		Title:             "Complete payment",
		Summary:           "Subscription checkout is required",
		Details:           "Open the checkout page and complete payment",
		AssignedTo:        "owner-1",
		SourceKind:        "session",
		SourceID:          "session-1",
		TaskID:            "task-1",
		SessionID:         "session-1",
		AgentID:           "agent-1",
		KeepSessionActive: boolPtr(true),
	})
	if err != nil {
		t.Fatalf("RequestExternalGate() error: %v", err)
	}
	if len(methods) != 2 || methods[0] != "workspace.ops.get" || methods[1] != "workspace.ops.request" {
		t.Fatalf("unexpected method sequence: %+v", methods)
	}
	if gotParams["gate_type"] != "PAYMENT_BILLING" || gotParams["request_key"] != "rnar.task.task-1" {
		t.Fatalf("unexpected request params: %+v", gotParams)
	}
	if gotParams["assigned_to"] != "owner-1" || gotParams["session_id"] != "session-1" || gotParams["agent_id"] != "agent-1" {
		t.Fatalf("expected ownership and session fields, got %+v", gotParams)
	}
}

func TestRhizomeClientGetOperatorQueueTreatsNotFoundCodeAsMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if method, _ := req["method"].(string); method != "workspace.ops.get" {
			t.Fatalf("unexpected method %q", method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"error": map[string]any{
				"code":    rhizomeRPCCodeOperatorQueueNotFound,
				"message": "queue lookup missed",
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	item, ok, err := client.GetOperatorQueue(context.Background(), "ws", "", "queue-missing")
	if err != nil {
		t.Fatalf("GetOperatorQueue() error = %v", err)
	}
	if ok || item.QueueID != "" || item.QueueKey != "" {
		t.Fatalf("expected code-based missing queue result, got ok=%v item=%+v", ok, item)
	}
}

func TestRhizomeClientRequestExternalGateHydratesBaseVersion(t *testing.T) {
	var methods []string
	var requestParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)

		switch method {
		case "workspace.ops.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"item": map[string]any{
						"queue_id":     "queue-1",
						"workspace_id": "ws",
						"queue_key":    "external_gate:explicit_approval:rnar.task.task-2",
						"revision":     7,
						"updated_at":   "2026-04-16T00:00:00Z",
					},
				},
			})
		case "workspace.ops.request":
			requestParams, _ = req["params"].(map[string]any)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"status": "REQUESTED"},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	err := client.RequestExternalGate(context.Background(), ExternalGateRequestInput{
		WorkspaceID: "ws",
		RequestKey:  "rnar.task.task-2",
		GateType:    "EXPLICIT_APPROVAL",
		Title:       "Approve change",
	})
	if err != nil {
		t.Fatalf("RequestExternalGate() error: %v", err)
	}
	if len(methods) != 2 || methods[0] != "workspace.ops.get" || methods[1] != "workspace.ops.request" {
		t.Fatalf("unexpected method sequence: %+v", methods)
	}
	if requestParams["current_revision"] != float64(7) {
		t.Fatalf("expected hydrated current_revision, got %+v", requestParams)
	}
	if requestParams["current_updated_at"] != "2026-04-16T00:00:00Z" {
		t.Fatalf("expected hydrated current_updated_at, got %+v", requestParams)
	}
}

func TestRhizomeClientRequestExternalGateFallsBackToLegacyUpsert(t *testing.T) {
	var methods []string
	var fallbackParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)

		switch method {
		case "workspace.ops.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32602,
					"message": "operator queue item not found",
				},
			})
		case "workspace.ops.request":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32601,
					"message": "unknown method: workspace.ops.request",
				},
			})
		case "workspace.ops.upsert":
			fallbackParams, _ = req["params"].(map[string]any)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"status": "UPSERTED"},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	err := client.RequestExternalGate(context.Background(), ExternalGateRequestInput{
		WorkspaceID: "ws",
		RequestKey:  "rnar.task.task-9",
		GateType:    "EXPLICIT_APPROVAL",
		Title:       "Approve privileged action",
		Summary:     "Privileged tool approval is required",
		TaskID:      "task-9",
		AgentID:     "agent-9",
	})
	if err != nil {
		t.Fatalf("RequestExternalGate() error: %v", err)
	}
	if len(methods) != 3 || methods[0] != "workspace.ops.get" || methods[1] != "workspace.ops.request" || methods[2] != "workspace.ops.upsert" {
		t.Fatalf("unexpected method sequence: %+v", methods)
	}
	if fallbackParams["queue_key"] != "external_gate:explicit_approval:rnar.task.task-9" {
		t.Fatalf("unexpected fallback queue key: %+v", fallbackParams)
	}
	if fallbackParams["queue_type"] != "DECISION" {
		t.Fatalf("expected approval fallback to DECISION queue, got %+v", fallbackParams)
	}
}

func TestRhizomeClientRequestExternalGateFailsClosedWhenQueueLookupUnavailable(t *testing.T) {
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)

		switch method {
		case "workspace.ops.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32601,
					"message": "unknown method: workspace.ops.get",
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	err := client.RequestExternalGate(context.Background(), ExternalGateRequestInput{
		WorkspaceID: "ws",
		RequestKey:  "rnar.task.task-10",
		GateType:    "EXPLICIT_APPROVAL",
		Title:       "Approve privileged action",
	})
	if err == nil {
		t.Fatal("expected strict queue hydration failure")
	}
	if !strings.Contains(err.Error(), "operator queue lookup unavailable") {
		t.Fatalf("expected strict hydration error, got %v", err)
	}
	if len(methods) != 1 || methods[0] != "workspace.ops.get" {
		t.Fatalf("expected lookup-only method sequence, got %+v", methods)
	}
}

func TestRhizomeClientResolveOperatorQueueHydratesBaseVersion(t *testing.T) {
	var methods []string
	var resolveParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)

		switch method {
		case "workspace.ops.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"item": map[string]any{
						"queue_id":     "queue-5",
						"workspace_id": "ws",
						"queue_key":    "external_gate:payment_billing:rnar.task.task-5",
						"revision":     9,
						"updated_at":   "2026-04-16T00:05:00Z",
					},
				},
			})
		case "workspace.ops.resolve":
			resolveParams, _ = req["params"].(map[string]any)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"status": "RESOLVED"},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	err := client.ResolveOperatorQueue(context.Background(), OperatorQueueResolveInput{
		WorkspaceID: "ws",
		QueueKey:    "external_gate:payment_billing:rnar.task.task-5",
		Status:      "RESOLVED",
		ResolvedBy:  "agent-5",
	})
	if err != nil {
		t.Fatalf("ResolveOperatorQueue() error: %v", err)
	}
	if len(methods) != 2 || methods[0] != "workspace.ops.get" || methods[1] != "workspace.ops.resolve" {
		t.Fatalf("unexpected method sequence: %+v", methods)
	}
	if resolveParams["current_revision"] != float64(9) {
		t.Fatalf("expected hydrated current_revision, got %+v", resolveParams)
	}
	if resolveParams["current_updated_at"] != "2026-04-16T00:05:00Z" {
		t.Fatalf("expected hydrated current_updated_at, got %+v", resolveParams)
	}
}

func TestRhizomeClientResolveOperatorQueueTreatsAlreadyClosedAsIdempotent(t *testing.T) {
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{
				"item": map[string]any{
					"queue_id":     "queue-5",
					"workspace_id": "ws",
					"queue_key":    "external_gate:payment_billing:rnar.task.task-5",
					"revision":     9,
					"updated_at":   "2026-04-16T00:05:00Z",
				},
			})
		case "workspace.ops.resolve":
			writeRPCError(w, req, -32602, "operator queue item is not open: queue-5")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	err := client.ResolveOperatorQueue(context.Background(), OperatorQueueResolveInput{
		WorkspaceID: "ws",
		QueueKey:    "external_gate:payment_billing:rnar.task.task-5",
		Status:      "RESOLVED",
		ResolvedBy:  "agent-5",
	})
	if err != nil {
		t.Fatalf("ResolveOperatorQueue() error: %v", err)
	}
	if len(methods) != 2 || methods[0] != "workspace.ops.get" || methods[1] != "workspace.ops.resolve" {
		t.Fatalf("unexpected method sequence: %+v", methods)
	}
}

func TestRhizomeClientResolveOperatorQueueFailsClosedWhenQueueLookupUnavailable(t *testing.T) {
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)

		switch method {
		case "workspace.ops.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32601,
					"message": "unknown method: workspace.ops.get",
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	err := client.ResolveOperatorQueue(context.Background(), OperatorQueueResolveInput{
		WorkspaceID: "ws",
		QueueKey:    "external_gate:payment_billing:rnar.task.task-11",
		Status:      "RESOLVED",
		ResolvedBy:  "agent-11",
	})
	if err == nil {
		t.Fatal("expected strict queue hydration failure")
	}
	if !strings.Contains(err.Error(), "operator queue lookup unavailable") {
		t.Fatalf("expected strict hydration error, got %v", err)
	}
	if len(methods) != 1 || methods[0] != "workspace.ops.get" {
		t.Fatalf("expected lookup-only method sequence, got %+v", methods)
	}
}

func TestRhizomeClientWriteArtifactAndList(t *testing.T) {
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)

		switch method {
		case "workspace.artifact.write":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"artifact": map[string]any{
						"artifact_id":   "artifact-1",
						"workspace_id":  "ws",
						"task_id":       "task-1",
						"title":         "Deliverable Brief",
						"artifact_ref":  "doc:deliverable.brief",
						"kind":          "workspace_doc",
						"content_type":  "text/markdown",
						"created_by":    "agent-1",
						"metadata_json": `{"run_id":"run-1"}`,
						"created_at":    "2026-03-23T00:00:00Z",
					},
				},
			})
		case "workspace.artifact.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []any{
						map[string]any{
							"artifact_id":   "artifact-1",
							"workspace_id":  "ws",
							"task_id":       "task-1",
							"title":         "Deliverable Brief",
							"artifact_ref":  "doc:deliverable.brief",
							"kind":          "workspace_doc",
							"content_type":  "text/markdown",
							"created_by":    "agent-1",
							"metadata_json": `{"run_id":"run-1"}`,
							"created_at":    "2026-03-23T00:00:00Z",
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	record, err := client.WriteArtifact(context.Background(), WorkspaceArtifactWriteInput{
		WorkspaceID:  "ws",
		TaskID:       "task-1",
		Title:        "Deliverable Brief",
		ArtifactRef:  "doc:deliverable.brief",
		Kind:         "workspace_doc",
		ContentType:  "text/markdown",
		CreatedBy:    "agent-1",
		MetadataJSON: `{"run_id":"run-1"}`,
	})
	if err != nil {
		t.Fatalf("WriteArtifact() error: %v", err)
	}
	if record.ArtifactID != "artifact-1" || record.ArtifactRef != "doc:deliverable.brief" {
		t.Fatalf("unexpected artifact record: %+v", record)
	}

	items, err := client.ListArtifacts(context.Background(), WorkspaceArtifactListInput{
		WorkspaceID: "ws",
		TaskID:      "task-1",
	})
	if err != nil {
		t.Fatalf("ListArtifacts() error: %v", err)
	}
	if len(items) != 1 || items[0].ArtifactID != "artifact-1" {
		t.Fatalf("unexpected artifacts: %+v", items)
	}
	if len(methods) != 2 || methods[0] != "workspace.artifact.write" || methods[1] != "workspace.artifact.list" {
		t.Fatalf("unexpected method sequence: %+v", methods)
	}
}

func TestRhizomeClientListWorkspaceTools(t *testing.T) {
	var gotMethod string
	var gotParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotMethod, _ = req["method"].(string)
		gotParams, _ = req["params"].(map[string]any)

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"tools": []any{
					map[string]any{
						"tool_id":       "mcp__notion__search_docs",
						"workspace_id":  "ws",
						"display_name":  "Search Docs",
						"description":   "Search docs through routed tool.call",
						"owner_user_id": "developer",
						"kind":          "INTEGRATION",
						"status":        "ACTIVE",
						"version":       "v1",
						"access_level":  "WORKSPACE",
						"manifest_json": `{"route":{"kind":"mcp","server_id":"notion","tool_name":"search_docs"},"input_schema":{"type":"object","properties":{"query":{"type":"string"}}}}`,
						"created_at":    "2026-03-23T00:00:00Z",
						"updated_at":    "2026-03-23T00:00:00Z",
					},
				},
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	tools, err := client.ListWorkspaceTools(context.Background(), "ws")
	if err != nil {
		t.Fatalf("ListWorkspaceTools() error: %v", err)
	}
	if gotMethod != "tool.list" {
		t.Fatalf("expected tool.list, got %q", gotMethod)
	}
	if gotParams["workspace_id"] != "ws" {
		t.Fatalf("unexpected params: %+v", gotParams)
	}
	if len(tools) != 1 || tools[0].ToolID != "mcp__notion__search_docs" || tools[0].Status != "ACTIVE" {
		t.Fatalf("unexpected workspace tools: %+v", tools)
	}
}

func TestRhizomeClientCallWorkspaceTool(t *testing.T) {
	var gotMethod string
	var gotParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotMethod, _ = req["method"].(string)
		gotParams, _ = req["params"].(map[string]any)

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"tool_id":     "mcp__notion__search_docs",
				"stdout":      "found matching docs",
				"stderr":      "",
				"exit_code":   0,
				"timed_out":   false,
				"router_kind": "mcp",
				"is_error":    false,
				"content": []any{
					map[string]any{"type": "text", "text": "found matching docs"},
				},
				"server_id": "notion",
				"tool_name": "search_docs",
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "")
	result, err := client.CallWorkspaceTool(context.Background(), WorkspaceToolCallInput{
		WorkspaceID:         "ws",
		ToolID:              "mcp__notion__search_docs",
		Arguments:           map[string]any{"query": "runtime state"},
		ActorType:           "agent",
		ActorID:             "agent-1",
		RequestedCapability: "tool.call",
		TaskID:              "task-1",
		SessionID:           "session-1",
		RunID:               "run-1",
	})
	if err != nil {
		t.Fatalf("CallWorkspaceTool() error: %v", err)
	}
	if gotMethod != "tool.call" {
		t.Fatalf("expected tool.call, got %q", gotMethod)
	}
	if gotParams["tool_id"] != "mcp__notion__search_docs" || gotParams["workspace_id"] != "ws" {
		t.Fatalf("unexpected params: %+v", gotParams)
	}
	if gotParams["actor_type"] != "agent" || gotParams["actor_id"] != "agent-1" || gotParams["requested_capability"] != "tool.call" {
		t.Fatalf("expected actor/policy params, got %+v", gotParams)
	}
	if gotParams["task_id"] != "task-1" || gotParams["session_id"] != "session-1" || gotParams["run_id"] != "run-1" {
		t.Fatalf("expected task/session/run binding params, got %+v", gotParams)
	}
	if result.RouterKind != "mcp" || result.ServerID != "notion" || result.ToolName != "search_docs" {
		t.Fatalf("unexpected routed tool result: %+v", result)
	}
	if result.Stdout != "found matching docs" || result.ExitCode != 0 || result.IsError {
		t.Fatalf("unexpected tool output: %+v", result)
	}
}

func TestRhizomeClientRequestAgentAndGetResult(t *testing.T) {
	var methods []string
	var gotRequest map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)

		switch method {
		case "agent.request":
			gotRequest, _ = req["params"].(map[string]any)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "areq-1",
					"workspace_id": "ws-test",
					"to_agent_id":  "agent-b",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":    "areq-1",
					"workspace_id":  "ws-test",
					"from_agent_id": "agent-a",
					"to_agent_id":   "agent-b",
					"method":        "runtime.pause",
					"payload":       `{"reason":"pause"}`,
					"status":        "COMPLETED",
					"response":      `{"paused":true}`,
					"created_at":    "2026-03-27T00:00:00Z",
					"responded_at":  "2026-03-27T00:00:01Z",
					"timeout_sec":   300,
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "token")
	sent, err := client.RequestAgent(context.Background(), AgentRequestInput{
		WorkspaceID: "ws-test",
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "runtime.pause",
		PayloadJSON: `{"reason":"pause"}`,
		TimeoutSec:  300,
	})
	if err != nil {
		t.Fatalf("RequestAgent() error: %v", err)
	}
	if sent.RequestID != "areq-1" || sent.Status != "PENDING" {
		t.Fatalf("unexpected send result: %+v", sent)
	}
	if gotRequest["from_agent_id"] != "agent-a" || gotRequest["to_agent_id"] != "agent-b" {
		t.Fatalf("unexpected request payload: %+v", gotRequest)
	}

	result, err := client.GetAgentRequestResult(context.Background(), "ws-test", "areq-1")
	if err != nil {
		t.Fatalf("GetAgentRequestResult() error: %v", err)
	}
	if result.Status != "COMPLETED" || result.Response != `{"paused":true}` {
		t.Fatalf("unexpected request result: %+v", result)
	}
	if len(methods) != 2 || methods[0] != "agent.request" || methods[1] != "agent.request.result" {
		t.Fatalf("unexpected method sequence: %+v", methods)
	}
}

func TestRhizomeClientUpdateAgentProfile(t *testing.T) {
	var gotMethod string
	var gotParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotMethod, _ = req["method"].(string)
		gotParams, _ = req["params"].(map[string]any)

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"agent_id": "observer",
				"status":   "UPDATED",
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "token")
	err := client.UpdateAgentProfile(context.Background(), AgentProfileUpdateInput{
		WorkspaceID:    "ws-test",
		AgentID:        "observer",
		ActorID:        "observer",
		Bio:            "Analyze global system dynamics without direct participation.",
		Specialization: "meta-analysis",
		Tags:           []string{"generalist", "observer"},
		ToolsAccess:    []string{"local shell", "local filesystem"},
		Metadata: map[string]any{
			"default_work_mode": "observer",
		},
	})
	if err != nil {
		t.Fatalf("UpdateAgentProfile() error: %v", err)
	}
	if gotMethod != "agent.profile.update" {
		t.Fatalf("expected agent.profile.update, got %q", gotMethod)
	}
	if gotParams["workspace_id"] != "ws-test" || gotParams["agent_id"] != "observer" {
		t.Fatalf("unexpected profile params: %+v", gotParams)
	}
	if gotParams["actor_id"] != "observer" {
		t.Fatalf("expected actor_id to be forwarded, got %+v", gotParams)
	}
	if gotParams["specialization"] != "meta-analysis" {
		t.Fatalf("expected specialization to be forwarded, got %+v", gotParams)
	}
}

func TestRhizomeClientRequestAgentRejectsPartialCreateTruth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		if method != "agent.request" {
			t.Fatalf("unexpected method %q", method)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"workspace_id": "ws-test",
				"to_agent_id":  "agent-b",
				"status":       "PENDING",
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "token")
	_, err := client.RequestAgent(context.Background(), AgentRequestInput{
		WorkspaceID: "ws-test",
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "runtime.pause",
	})
	if err == nil {
		t.Fatal("expected partial create truth error")
	}
	if !strings.Contains(err.Error(), "agent.request returned partial result: missing request_id") {
		t.Fatalf("expected explicit partial create error, got %v", err)
	}
}

func TestRhizomeClientRequestAgentRejectsMismatchedTargetTruth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		if method != "agent.request" {
			t.Fatalf("unexpected method %q", method)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"request_id":   "areq-1",
				"workspace_id": "ws-test",
				"to_agent_id":  "agent-z",
				"status":       "PENDING",
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "token")
	_, err := client.RequestAgent(context.Background(), AgentRequestInput{
		WorkspaceID: "ws-test",
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "runtime.pause",
	})
	if err == nil {
		t.Fatal("expected mismatched target truth error")
	}
	if !strings.Contains(err.Error(), `agent.request returned mismatched to_agent_id "agent-z" (wanted "agent-b")`) {
		t.Fatalf("expected explicit mismatched target error, got %v", err)
	}
}

func TestRhizomeClientGetAgentRequestResultRejectsPartialTruth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		if method != "agent.request.result" {
			t.Fatalf("unexpected method %q", method)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"workspace_id": "ws-test",
				"status":       "PENDING",
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "token")
	_, err := client.GetAgentRequestResult(context.Background(), "ws-test", "areq-1")
	if err == nil {
		t.Fatal("expected partial request-result truth error")
	}
	if !strings.Contains(err.Error(), "agent.request.result returned partial result: missing request_id") {
		t.Fatalf("expected explicit partial request-result error, got %v", err)
	}
}

func TestRhizomeClientGetAgentRequestResultRejectsMismatchedRequestTruth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		if method != "agent.request.result" {
			t.Fatalf("unexpected method %q", method)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"request_id":   "areq-z",
				"workspace_id": "ws-test",
				"status":       "PENDING",
			},
		})
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "token")
	_, err := client.GetAgentRequestResult(context.Background(), "ws-test", "areq-1")
	if err == nil {
		t.Fatal("expected mismatched request-result truth error")
	}
	if !strings.Contains(err.Error(), `agent.request.result returned mismatched request_id "areq-z" (wanted "areq-1")`) {
		t.Fatalf("expected explicit mismatched request-result error, got %v", err)
	}
}
