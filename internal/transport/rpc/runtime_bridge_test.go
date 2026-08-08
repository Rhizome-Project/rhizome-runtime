package rpc

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestParseExecutorRunNodeResponse_Fixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		filename        string
		expectedID      string
		expectError     bool
		expectedStatus  string
		expectedErrCode int
		expectedErrMsg  string
	}{
		{
			name:           "success fixture",
			filename:       "success_response.json",
			expectedID:     "exec-001",
			expectError:    false,
			expectedStatus: "SUCCESS",
		},
		{
			name:            "timeout fixture",
			filename:        "timeout_response.json",
			expectedID:      "exec-002",
			expectError:     true,
			expectedErrCode: -32017,
			expectedErrMsg:  "executor_timeout",
		},
		{
			name:            "runtime error fixture",
			filename:        "runtime_error_response.json",
			expectedID:      "exec-003",
			expectError:     true,
			expectedErrCode: -32018,
			expectedErrMsg:  "executor_runtime_error",
		},
		{
			name:            "captcha fixture",
			filename:        "captcha_blocked_response.json",
			expectedID:      "exec-004",
			expectError:     true,
			expectedErrCode: -32018,
			expectedErrMsg:  "executor_runtime_error",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw := mustReadExecutorFixture(t, tc.filename)
			parsed, err := ParseExecutorRunNodeResponse(raw, tc.expectedID)
			if err != nil {
				t.Fatalf("ParseExecutorRunNodeResponse failed: %v", err)
			}

			if tc.expectError {
				if parsed.Error == nil {
					t.Fatalf("expected error payload, got nil")
				}
				if parsed.Error.Code != tc.expectedErrCode {
					t.Fatalf("expected error.code=%d, got %d", tc.expectedErrCode, parsed.Error.Code)
				}
				if parsed.Error.Message != tc.expectedErrMsg {
					t.Fatalf("expected error.message=%q, got %q", tc.expectedErrMsg, parsed.Error.Message)
				}
				mapped := MapExecutorRuntimeErrorCode(parsed.Error.Code, parsed.Error.Message)
				if mapped != tc.expectedErrMsg {
					t.Fatalf("expected mapped error=%q, got %q", tc.expectedErrMsg, mapped)
				}
				return
			}

			if parsed.Result == nil {
				t.Fatalf("expected result payload, got nil")
			}
			if parsed.Result.Status != tc.expectedStatus {
				t.Fatalf("expected result.status=%q, got %q", tc.expectedStatus, parsed.Result.Status)
			}
		})
	}
}

func TestParseExecutorRunNodeResponse_IDValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       []byte
		expectedID string
		wantErr    error
	}{
		{
			name:       "matching id",
			body:       []byte(`{"jsonrpc":"2.0","id":"exec-123","result":{"status":"SUCCESS"}}`),
			expectedID: "exec-123",
		},
		{
			name:       "mismatched id",
			body:       []byte(`{"jsonrpc":"2.0","id":"exec-other","result":{"status":"SUCCESS"}}`),
			expectedID: "exec-123",
			wantErr:    ErrMismatchedID,
		},
		{
			name:       "missing id",
			body:       []byte(`{"jsonrpc":"2.0","result":{"status":"SUCCESS"}}`),
			expectedID: "exec-123",
			wantErr:    ErrMissingID,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseExecutorRunNodeResponse(tc.body, tc.expectedID)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("ParseExecutorRunNodeResponse failed: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestParseExecutorRunNodeResponse_InvalidCases(t *testing.T) {
	t.Parallel()

	invalidBodies := []struct {
		name    string
		body    []byte
		wantErr error
	}{
		{
			name:    "invalid json",
			body:    []byte("{"),
			wantErr: ErrInvalidJSON,
		},
		{
			name:    "missing id",
			body:    []byte(`{"jsonrpc":"2.0","result":{"status":"SUCCESS"}}`),
			wantErr: ErrMissingID,
		},
		{
			name:    "missing result and error",
			body:    []byte(`{"jsonrpc":"2.0","id":"x"}`),
			wantErr: ErrInvalidRuntimeEnvelope,
		},
		{
			name:    "invalid result status",
			body:    []byte(`{"jsonrpc":"2.0","id":"x","result":{"status":"FAILED"}}`),
			wantErr: ErrInvalidRuntimeResult,
		},
		{
			name:    "invalid error shape",
			body:    []byte(`{"jsonrpc":"2.0","id":"x","error":{"code":-32018,"message":"executor_runtime_error","details":{}}}`),
			wantErr: ErrInvalidRuntimeError,
		},
	}

	for _, tc := range invalidBodies {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseExecutorRunNodeResponse(tc.body, "x")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestMapExecutorRuntimeErrorCode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		code    int
		message string
		want    string
	}{
		{-32017, "executor_timeout", "executor_timeout"},
		{-32018, "executor_runtime_error", "executor_runtime_error"},
		{-32019, "state_transition_invalid", "state_transition_invalid"},
		{123, "  budget_exceeded  ", "budget_exceeded"},
		{123, "", "internal_error"},
	}

	for _, tc := range cases {
		got := MapExecutorRuntimeErrorCode(tc.code, tc.message)
		if got != tc.want {
			t.Fatalf("MapExecutorRuntimeErrorCode(%d, %q) = %q, want %q", tc.code, tc.message, got, tc.want)
		}
	}
}

func mustReadExecutorFixture(t *testing.T, name string) []byte {
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
