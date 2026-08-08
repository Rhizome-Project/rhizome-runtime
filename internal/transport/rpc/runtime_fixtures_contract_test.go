package rpc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

type fixtureEnvelope struct {
	JSONRPC string         `json:"jsonrpc"`
	Result  map[string]any `json:"result"`
	Error   *fixtureError  `json:"error"`
	ID      any            `json:"id"`
}

type fixtureError struct {
	Code    int            `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

func TestRuntimeFixturesContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filename     string
		expectError  bool
		errorCode    int
		errorMsg     string
		resultStatus string
	}{
		{
			name:         "success response",
			filename:     "success_response.json",
			expectError:  false,
			resultStatus: "SUCCESS",
		},
		{
			name:        "timeout response",
			filename:    "timeout_response.json",
			expectError: true,
			errorCode:   -32017,
			errorMsg:    "executor_timeout",
		},
		{
			name:        "runtime error response",
			filename:    "runtime_error_response.json",
			expectError: true,
			errorCode:   -32018,
			errorMsg:    "executor_runtime_error",
		},
		{
			name:        "captcha blocked response",
			filename:    "captcha_blocked_response.json",
			expectError: true,
			errorCode:   -32018,
			errorMsg:    "executor_runtime_error",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw := readFixture(t, tc.filename)

			var env fixtureEnvelope
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatalf("decode fixture %s: %v", tc.filename, err)
			}

			if env.JSONRPC != "2.0" {
				t.Fatalf("expected jsonrpc 2.0, got %q", env.JSONRPC)
			}
			if env.ID == nil {
				t.Fatalf("expected id field in fixture %s", tc.filename)
			}

			if !tc.expectError {
				if env.Error != nil {
					t.Fatalf("expected result fixture, got error: %+v", env.Error)
				}
				if env.Result == nil {
					t.Fatalf("expected result object")
				}
				if got, _ := env.Result["status"].(string); got != tc.resultStatus {
					t.Fatalf("expected result.status=%q, got %q", tc.resultStatus, got)
				}
				return
			}

			if env.Error == nil {
				t.Fatalf("expected error fixture, got nil error")
			}
			if env.Error.Code != tc.errorCode {
				t.Fatalf("expected error.code=%d, got %d", tc.errorCode, env.Error.Code)
			}
			if env.Error.Message != tc.errorMsg {
				t.Fatalf("expected error.message=%q, got %q", tc.errorMsg, env.Error.Message)
			}
		})
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}

	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))
	path := filepath.Join(root, "internal", "executor", "fixtures", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return raw
}
