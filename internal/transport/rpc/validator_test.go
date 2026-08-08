package rpc

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestValidateFinopsRequestPayload_Valid(t *testing.T) {
	payload := mustJSON(t, map[string]any{
		"jsonrpc": "2.0",
		"method":  "finops.request_resource",
		"id":      "req-1",
		"params": map[string]any{
			"request_id":         "rr-1",
			"task_id":            "task-1",
			"node_id":            "node-1",
			"owner_user_id":      "developer",
			"resource_type":      "api_key",
			"service_id":         "2captcha",
			"estimated_cost_usd": 0.05,
			"justification":      "solve captcha",
			"idempotency_key":    "task-1-node-1-rr-1",
		},
	})

	if err := ValidateFinopsRequestPayload(payload); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidateFinopsRequestPayload_InvalidCases(t *testing.T) {
	t.Parallel()

	valid := map[string]any{
		"jsonrpc": "2.0",
		"method":  "finops.request_resource",
		"id":      "req-1",
		"params": map[string]any{
			"request_id":         "rr-1",
			"task_id":            "task-1",
			"node_id":            "node-1",
			"owner_user_id":      "developer",
			"resource_type":      "api_key",
			"service_id":         "2captcha",
			"estimated_cost_usd": 0.05,
			"justification":      "solve captcha",
			"idempotency_key":    "task-1-node-1-rr-1",
		},
	}

	cases := []struct {
		name        string
		payload     []byte
		wantErr     error
		wantContain string
	}{
		{
			name:    "invalid json",
			payload: []byte("{"),
			wantErr: ErrInvalidJSON,
		},
		{
			name: "invalid jsonrpc",
			payload: mustJSON(t, mutate(valid, func(m map[string]any) {
				m["jsonrpc"] = "1.0"
			})),
			wantErr: ErrInvalidJSONRPC,
		},
		{
			name: "invalid method",
			payload: mustJSON(t, mutate(valid, func(m map[string]any) {
				m["method"] = "finops.wrong"
			})),
			wantErr: ErrInvalidMethod,
		},
		{
			name: "missing id",
			payload: mustJSON(t, mutate(valid, func(m map[string]any) {
				delete(m, "id")
			})),
			wantErr: ErrMissingID,
		},
		{
			name: "missing required field",
			payload: mustJSON(t, mutate(valid, func(m map[string]any) {
				params := m["params"].(map[string]any)
				delete(params, "justification")
			})),
			wantErr:     ErrMissingField,
			wantContain: "justification",
		},
		{
			name: "invalid estimated_cost_usd type",
			payload: mustJSON(t, mutate(valid, func(m map[string]any) {
				params := m["params"].(map[string]any)
				params["estimated_cost_usd"] = "0.05"
			})),
			wantErr:     ErrInvalidFieldValue,
			wantContain: "estimated_cost_usd",
		},
		{
			name: "empty string field",
			payload: mustJSON(t, mutate(valid, func(m map[string]any) {
				params := m["params"].(map[string]any)
				params["resource_type"] = "   "
			})),
			wantErr:     ErrInvalidFieldValue,
			wantContain: "resource_type",
		},
		{
			name: "invalid params shape",
			payload: mustJSON(t, mutate(valid, func(m map[string]any) {
				m["params"] = nil
			})),
			wantErr: ErrInvalidParams,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateFinopsRequestPayload(tc.payload)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
			if tc.wantContain != "" && !strings.Contains(err.Error(), tc.wantContain) {
				t.Fatalf("expected error to contain %q, got %q", tc.wantContain, err.Error())
			}
		})
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return raw
}

func mutate(source map[string]any, fn func(map[string]any)) map[string]any {
	next := cloneMap(source)
	fn(next)
	return next
}

func cloneMap(source map[string]any) map[string]any {
	out := make(map[string]any, len(source))
	for k, v := range source {
		switch typed := v.(type) {
		case map[string]any:
			out[k] = cloneMap(typed)
		default:
			out[k] = typed
		}
	}
	return out
}
