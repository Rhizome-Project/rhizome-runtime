package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if mode := os.Getenv("RHIZOME_MCP_STDIO_HELPER"); mode != "" {
		runMCPStdioClientTestHelper(mode)
		return
	}
	os.Exit(m.Run())
}

func TestCallToolUsesCallerContextDeadline(t *testing.T) {
	client := NewClient()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      "tool-call-1",
			"result": map[string]any{
				"content": []map[string]any{{"type": "text", "text": "ok"}},
			},
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.CallTool(ctx, server.URL, nil, "sleepy", map[string]any{"q": "test"})
	if err == nil {
		t.Fatal("expected context deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

func TestStdioClientMatchesNumericJSONRPCID(t *testing.T) {
	sc := NewStdioClient()
	if err := sc.Start(os.Args[0], nil, map[string]string{"RHIZOME_MCP_STDIO_HELPER": "echo"}); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	defer sc.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := sc.CallTool(ctx, "echo", map[string]any{"value": "ready"})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if len(result.Content) != 1 || result.Content[0].Text != "stdio-ok" {
		t.Fatalf("unexpected stdio result %+v", result)
	}
}

func TestStdioClientCapturesStderrAndFailsOnChildExit(t *testing.T) {
	sc := NewStdioClient()
	if err := sc.Start(os.Args[0], nil, map[string]string{"RHIZOME_MCP_STDIO_HELPER": "exit_with_stderr"}); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	defer sc.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := time.Now()
	_, err := sc.CallTool(ctx, "will-fail", map[string]any{})
	if err == nil {
		t.Fatal("expected stdio child death error")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("child death took too long to surface: %s err=%v", elapsed, err)
	}
	diagnostics, ok := StdioDiagnosticsFromError(err)
	if !ok {
		t.Fatalf("expected stdio diagnostics in error %T: %v", err, err)
	}
	if !diagnostics.StdoutClosed {
		t.Fatalf("expected stdout_closed diagnostic, got %+v", diagnostics)
	}
	if !strings.Contains(diagnostics.Stderr, "fatal child crash") {
		t.Fatalf("stderr diagnostic = %q, want fatal child crash", diagnostics.Stderr)
	}
}

func TestStdioClientTimeoutIncludesStderrDiagnostic(t *testing.T) {
	sc := NewStdioClient()
	if err := sc.Start(os.Args[0], nil, map[string]string{"RHIZOME_MCP_STDIO_HELPER": "hang_with_stderr"}); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	defer sc.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := sc.CallTool(ctx, "will-timeout", map[string]any{})
	if err == nil {
		t.Fatal("expected stdio timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	diagnostics, ok := StdioDiagnosticsFromError(err)
	if !ok {
		t.Fatalf("expected stdio diagnostics in timeout error %T: %v", err, err)
	}
	if !strings.Contains(diagnostics.Stderr, "still working before hang") {
		t.Fatalf("stderr diagnostic = %q, want still working before hang", diagnostics.Stderr)
	}
}

func runMCPStdioClientTestHelper(mode string) {
	switch mode {
	case "echo":
		runEchoMCPStdioHelper()
	case "exit_with_stderr":
		fmt.Fprintln(os.Stderr, "fatal child crash")
		os.Exit(7)
	case "hang_with_stderr":
		fmt.Fprintln(os.Stderr, "still working before hang")
		for {
			time.Sleep(time.Hour)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q\n", mode)
		os.Exit(2)
	}
}

func runEchoMCPStdioHelper() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var req JSONRPCRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			continue
		}
		if req.ID == nil {
			continue
		}
		switch req.Method {
		case "tools/call":
			_ = json.NewEncoder(os.Stdout).Encode(JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result: mustTestRawMessage(map[string]any{
					"content": []map[string]any{{"type": "text", "text": "stdio-ok"}},
				}),
			})
		default:
			_ = json.NewEncoder(os.Stdout).Encode(JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result:  mustTestRawMessage(map[string]any{}),
			})
		}
	}
}

func mustTestRawMessage(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}
