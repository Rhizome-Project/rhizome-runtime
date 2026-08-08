package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRPCBodyLimitRejectsOversizedRequestClearly(t *testing.T) {
	t.Parallel()

	resp := callRPCBodyLimitTest(t, strings.NewReader(strings.Repeat("x", int(rpcMaterializationMaxRequestBodyBytes)+1)))

	if resp.Error == nil || resp.Error.Code != errCodeInvalidRequest || !strings.Contains(resp.Error.Message, "request body exceeds maximum allowed size") {
		t.Fatalf("expected clear hard request-size RPC error, got %+v", resp.Error)
	}
}

func TestRPCBodyLimitKeepsGenericMethodsSmall(t *testing.T) {
	t.Parallel()

	body := mustMarshalRPCBodyLimitTest(t, map[string]any{
		"jsonrpc": "2.0",
		"method":  "workspace.tasks.list",
		"params": map[string]any{
			"padding": strings.Repeat("x", int(rpcDefaultMaxRequestBodyBytes)+1),
		},
		"id": 1,
	})
	resp := callRPCBodyLimitTest(t, bytes.NewReader(body))

	if resp.Error == nil || resp.Error.Code != errCodeInvalidRequest ||
		!strings.Contains(resp.Error.Message, "request body exceeds default maximum allowed size") ||
		!strings.Contains(resp.Error.Message, "workspace.tasks.list") {
		t.Fatalf("expected generic method to keep default request-size cap, got %+v", resp.Error)
	}
}

func TestRPCBodyLimitAllowsMaterializationEnvelope(t *testing.T) {
	t.Parallel()

	body := mustMarshalRPCBodyLimitTest(t, map[string]any{
		"jsonrpc": "2.0",
		"method":  rpcLargeBodyMaterializationMethod,
		"params": map[string]any{
			"workspace_id": "",
			"padding":      strings.Repeat("x", int(rpcDefaultMaxRequestBodyBytes)+1),
		},
		"id": 1,
	})
	resp := callRPCBodyLimitTest(t, bytes.NewReader(body))

	if resp.Error == nil || strings.Contains(resp.Error.Message, "request body exceeds") {
		t.Fatalf("expected materialization method to pass transport size gate and fail later, got %+v", resp.Error)
	}
}

func callRPCBodyLimitTest(t *testing.T, body io.Reader) RPCResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/rpc", body)
	rec := httptest.NewRecorder()

	(&Handler{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want JSON-RPC 200", rec.Code)
	}
	var resp RPCResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode RPC response: %v", err)
	}
	return resp
}

func mustMarshalRPCBodyLimitTest(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal RPC request body: %v", err)
	}
	if int64(len(body)) <= rpcDefaultMaxRequestBodyBytes {
		t.Fatalf("test body length = %d, want over default cap %d", len(body), rpcDefaultMaxRequestBodyBytes)
	}
	if int64(len(body)) > rpcMaterializationMaxRequestBodyBytes {
		t.Fatalf("test body length = %d, want within materialization cap %d", len(body), rpcMaterializationMaxRequestBodyBytes)
	}
	return body
}
