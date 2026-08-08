package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestQwenExecLLMChatParsesFinalResponse(t *testing.T) {
	workdir := t.TempDir()
	llm := &QwenExecLLM{
		executablePath: "qwen",
		baseArgs:       []string{"--profile", "prod"},
		workdir:        workdir,
		model:          "qwen3-coder-plus",
		runner: func(_ context.Context, executablePath string, args []string, stdin string, gotWorkdir string) ([]byte, error) {
			if executablePath != "qwen" {
				t.Fatalf("unexpected executable path: %s", executablePath)
			}
			if gotWorkdir == workdir {
				t.Fatalf("expected isolated qwen backend workdir, got configured repo workdir %q", gotWorkdir)
			}
			if base := filepath.Base(gotWorkdir); !strings.HasPrefix(base, "rhizome-agent-qwen-") {
				t.Fatalf("expected isolated qwen temp workdir, got %q", gotWorkdir)
			}
			if _, err := os.Stat(gotWorkdir); err != nil {
				t.Fatalf("expected isolated qwen workdir to exist during call: %v", err)
			}
			if len(args) < 2 || args[0] != "--profile" || args[1] != "prod" {
				t.Fatalf("expected bridge args prefix, got %v", args)
			}
			if containsArg(args, "--bare") {
				t.Fatalf("did not expect --bare by default because it skips native qwen settings: %v", args)
			}
			if got := argValue(args, "--approval-mode"); got != qwenExecApprovalModeDefault {
				t.Fatalf("expected default approval mode %q, got %q in %v", qwenExecApprovalModeDefault, got, args)
			}
			if got := argValue(args, "--output-format"); got != "json" {
				t.Fatalf("expected json output format, got %q in %v", got, args)
			}
			schemaArg := argValue(args, "--json-schema")
			if !strings.HasPrefix(schemaArg, "@") {
				t.Fatalf("expected @schema path, got %q in %v", schemaArg, args)
			}
			if _, err := os.Stat(strings.TrimPrefix(schemaArg, "@")); err != nil {
				t.Fatalf("expected schema file to exist: %v", err)
			}
			if got := argValue(args, "-m"); got != "qwen3-coder-plus" {
				t.Fatalf("expected model flag, got %q in %v", got, args)
			}
			if got := argValue(args, "-p"); !strings.Contains(got, "structured JSON") {
				t.Fatalf("expected headless prompt flag, got %q in %v", got, args)
			}
			for _, want := range []string{"Qwen Code built-in tools", "ledgered actions", "Available outer tools JSON"} {
				if !strings.Contains(stdin, want) {
					t.Fatalf("stdin prompt missing %q:\n%s", want, stdin)
				}
			}
			return []byte(`[{"type":"result","subtype":"success","is_error":false,"result":{"kind":"final","content":"done","tool_calls":[]}}]`), nil
		},
	}

	resp, err := llm.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, nil)
	if err != nil {
		t.Fatalf("QwenExecLLM.Chat() error = %v", err)
	}
	if resp.Content != "done" {
		t.Fatalf("response content = %q", resp.Content)
	}
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("expected no tool calls, got %+v", resp.ToolCalls)
	}
}

func TestQwenExecLLMChatParsesStringResultToolCalls(t *testing.T) {
	llm := &QwenExecLLM{
		executablePath: "qwen",
		workdir:        t.TempDir(),
		model:          "qwen3-coder-plus",
		runner: func(_ context.Context, _ string, _ []string, _ string, _ string) ([]byte, error) {
			return []byte(`[{"type":"result","subtype":"success","is_error":false,"result":"{\"kind\":\"tool_calls\",\"content\":\"\",\"tool_calls\":[{\"name\":\"capture\",\"arguments_json\":\"{\\\"value\\\":\\\"abc\\\"}\"}]}"}]`), nil
		},
	}

	resp, err := llm.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, []ToolDef{{
		Type: "function",
		Function: ToolFunctionDef{
			Name:       "capture",
			Parameters: map[string]any{"type": "object"},
		},
	}})
	if err != nil {
		t.Fatalf("QwenExecLLM.Chat() error = %v", err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %+v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].ID != "qwen-call-1" || resp.ToolCalls[0].Function.Name != "capture" {
		t.Fatalf("unexpected tool call: %+v", resp.ToolCalls[0])
	}
	if resp.ToolCalls[0].Function.Arguments != `{"value":"abc"}` {
		t.Fatalf("unexpected tool call arguments: %s", resp.ToolCalls[0].Function.Arguments)
	}
}

func TestQwenExecApprovalModeHonorsExplicitOverride(t *testing.T) {
	t.Setenv(qwenExecApprovalModeEnvFlag, "auto")
	if got := qwenExecApprovalMode(); got != "auto" {
		t.Fatalf("expected explicit qwen approval mode to win, got %q", got)
	}
	t.Setenv(qwenExecApprovalModeEnvFlag, "unsupported")
	if got := qwenExecApprovalMode(); got != qwenExecApprovalModeDefault {
		t.Fatalf("expected unsupported approval mode to fall back to default, got %q", got)
	}
}

func TestQwenExecBareModeIsOptIn(t *testing.T) {
	t.Setenv(qwenExecBareEnvFlag, "")
	if qwenExecBareMode() {
		t.Fatal("expected qwen bare mode to be disabled by default")
	}
	t.Setenv(qwenExecBareEnvFlag, "1")
	if !qwenExecBareMode() {
		t.Fatal("expected qwen bare mode opt-in env to enable bare mode")
	}
}

func TestQwenExecCommandWrapsWindowsBatchFiles(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("windows batch wrapper is only used on Windows")
	}
	commandPath, commandArgs := qwenExecCommand(`C:\Program Files\npm\qwen.cmd`, []string{"--bare", "value with space"})
	if !strings.HasSuffix(strings.ToLower(commandPath), "cmd.exe") {
		t.Fatalf("expected qwen .cmd wrapper to use cmd.exe, got path=%q args=%v", commandPath, commandArgs)
	}
	if len(commandArgs) < 5 || commandArgs[0] != "/d" || commandArgs[1] != "/s" || commandArgs[2] != "/c" {
		t.Fatalf("unexpected cmd.exe args: %v", commandArgs)
	}
	if commandArgs[3] != `C:\Program Files\npm\qwen.cmd` || commandArgs[5] != "value with space" {
		t.Fatalf("expected batch command and original args to survive, got %v", commandArgs)
	}
}

func TestQwenExecLLMChatSurfacesTimeoutExplicitly(t *testing.T) {
	llm := &QwenExecLLM{
		executablePath: "qwen",
		workdir:        t.TempDir(),
		model:          "qwen3-coder-plus",
		runner: func(ctx context.Context, _ string, _ []string, _ string, _ string) ([]byte, error) {
			<-ctx.Done()
			return []byte("partial qwen output"), ctx.Err()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_, err := llm.Chat(ctx, []Message{{Role: "user", Content: "hello"}}, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded classification, got %v", err)
	}
	if !strings.Contains(err.Error(), "qwen exec timed out") || !strings.Contains(err.Error(), "partial qwen output") {
		t.Fatalf("expected explicit timeout error, got %v", err)
	}
}

func TestParseQwenStructuredOutputAcceptsFencedJSONResult(t *testing.T) {
	resp, err := parseQwenStructuredOutput([]byte(`[{"type":"result","subtype":"success","result":"` + "```json\\n{\\\"kind\\\":\\\"final\\\",\\\"content\\\":\\\"done\\\",\\\"tool_calls\\\":[]}\\n```" + `"}]`))
	if err != nil {
		t.Fatalf("parseQwenStructuredOutput() error = %v", err)
	}
	if resp.Content != "done" {
		t.Fatalf("response content = %q", resp.Content)
	}
}

func TestParseQwenStructuredOutputSalvagesAssistantJSONBeforeSchemaError(t *testing.T) {
	raw := []byte(`[
  {"type":"system","subtype":"init","model":"qwen3-coder-plus"},
  {"type":"assistant","message":{"content":[{"type":"text","text":"{\"kind\":\"final\",\"content\":\"qwen-smoke-ok\",\"tool_calls\":[]}"}]}},
  {"type":"result","subtype":"error_during_execution","is_error":true,"error":{"message":"Model produced plain text instead of calling structured_output"}}
]`)

	resp, err := parseQwenStructuredOutput(raw)
	if err != nil {
		t.Fatalf("parseQwenStructuredOutput() error = %v", err)
	}
	if resp.Content != "qwen-smoke-ok" {
		t.Fatalf("response content = %q", resp.Content)
	}
}
