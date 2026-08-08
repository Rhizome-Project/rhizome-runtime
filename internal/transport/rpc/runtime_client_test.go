package rpc

import (
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestLastJSONLine(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		out     string
		want    string
		wantErr bool
	}{
		{
			name: "single json line",
			out:  `{"jsonrpc":"2.0","id":"1","result":{"status":"SUCCESS"}}`,
			want: `{"jsonrpc":"2.0","id":"1","result":{"status":"SUCCESS"}}`,
		},
		{
			name: "logs before response",
			out: "hello\nworld\n" +
				`{"jsonrpc":"2.0","id":"1","error":{"code":-32018,"message":"executor_runtime_error","details":{"status":"FAILED","error_message":"x"}}}`,
			want: `{"jsonrpc":"2.0","id":"1","error":{"code":-32018,"message":"executor_runtime_error","details":{"status":"FAILED","error_message":"x"}}}`,
		},
		{
			name:    "no json response",
			out:     "plain text only",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := lastJSONLine(tc.out)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("lastJSONLine failed: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("unexpected json line: got %q, want %q", string(got), tc.want)
			}
		})
	}
}

func TestStdioRuntimeClientRejectsMismatchedResponseID(t *testing.T) {
	t.Parallel()

	client, err := NewStdioRuntimeClient(StdioRuntimeClientConfig{
		PythonBin:    os.Args[0],
		BridgeScript: "-test.run=TestRuntimeClientMismatchedIDHelper",
		WorkDir:      t.TempDir(),
		Env: map[string]string{
			"RHIZOME_RUNTIME_CLIENT_HELPER": "1",
		},
	})
	if err != nil {
		t.Fatalf("NewStdioRuntimeClient failed: %v", err)
	}

	_, err = client.RunNode(t.Context(), NodeRunRequest{
		TaskID:         "task-1",
		NodeID:         "node-1",
		RuntimeProfile: "default",
		ScriptRef:      "script.py",
	})
	if !errors.Is(err, ErrMismatchedID) {
		t.Fatalf("expected ErrMismatchedID, got %v", err)
	}
}

func TestRuntimeClientMismatchedIDHelper(t *testing.T) {
	if os.Getenv("RHIZOME_RUNTIME_CLIENT_HELPER") != "1" {
		return
	}

	_, _ = io.ReadAll(os.Stdin)
	fmt.Println(`{"jsonrpc":"2.0","id":"wrong-id","result":{"status":"SUCCESS"}}`)
	os.Exit(0)
}
