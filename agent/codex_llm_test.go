package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestResolveLLMBackendAutoPrefersCodexChatGPTSession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)

	authPath := filepath.Join(home, codexConfigDir, "auth.json")
	if err := os.MkdirAll(filepath.Dir(authPath), 0o755); err != nil {
		t.Fatalf("mkdir auth dir: %v", err)
	}
	if err := os.WriteFile(authPath, []byte(`{"auth_mode":"chatgpt","tokens":{"access_token":"test-token"}}`), 0o600); err != nil {
		t.Fatalf("write auth.json: %v", err)
	}

	executableName := "codex"
	if runtime.GOOS == "windows" {
		executableName += ".exe"
	}
	executablePath := filepath.Join(home, codexConfigDir, ".sandbox-bin", executableName)
	if err := os.MkdirAll(filepath.Dir(executablePath), 0o755); err != nil {
		t.Fatalf("mkdir sandbox bin dir: %v", err)
	}
	if err := os.WriteFile(executablePath, []byte("stub"), 0o600); err != nil {
		t.Fatalf("write codex stub: %v", err)
	}

	backend, err := resolveLLMBackend(RuntimeConfig{LLMBackend: llmBackendAuto})
	if err != nil {
		t.Fatalf("resolveLLMBackend() error = %v", err)
	}
	if backend != llmBackendCodex {
		t.Fatalf("expected codex backend, got %q", backend)
	}
}

func TestResolveLLMBackendAutoPrefersExplicitAPIKey(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "env-key")
	backend, err := resolveLLMBackend(RuntimeConfig{LLMBackend: llmBackendAuto})
	if err != nil {
		t.Fatalf("resolveLLMBackend() error = %v", err)
	}
	if backend != llmBackendOpenAI {
		t.Fatalf("expected openai backend, got %q", backend)
	}
}

func TestCodexExecLLMChatParsesFinalResponse(t *testing.T) {
	workdir := t.TempDir()
	llm := &CodexExecLLM{
		executablePath: "codex",
		workdir:        workdir,
		model:          "gpt-test",
		runner: func(_ context.Context, executablePath string, args []string, stdin string) ([]byte, error) {
			if executablePath != "codex" {
				t.Fatalf("unexpected executable path: %s", executablePath)
			}
			if len(args) < 3 || args[0] != "--ask-for-approval" || args[1] != "never" || args[2] != "exec" {
				t.Fatalf("unexpected codex arg prefix: %v", args)
			}
			if got := argValue(args, "--sandbox"); got != codexExecSandboxReadOnly {
				t.Fatalf("expected default codex sandbox %q, got %q in %v", codexExecSandboxReadOnly, got, args)
			}
			if !containsArg(args, "--json") {
				t.Fatalf("codex exec args must request JSONL events for usage accounting: %v", args)
			}
			if !strings.Contains(stdin, "Conversation JSON:") {
				t.Fatalf("stdin prompt missing conversation block: %s", stdin)
			}
			outputPath := argValue(args, "-o")
			if outputPath == "" {
				t.Fatalf("missing output path in args: %v", args)
			}
			if err := os.WriteFile(outputPath, []byte(`{"kind":"final","content":"done","tool_calls":[]}`), 0o600); err != nil {
				t.Fatalf("write output file: %v", err)
			}
			return []byte(`{"type":"response.completed","response":{"usage":{"input_tokens":11,"output_tokens":7,"total_tokens":18}}}` + "\n"), nil
		},
	}

	resp, err := llm.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, nil)
	if err != nil {
		t.Fatalf("CodexExecLLM.Chat() error = %v", err)
	}
	if resp.Content != "done" {
		t.Fatalf("response content = %q", resp.Content)
	}
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("expected no tool calls, got %+v", resp.ToolCalls)
	}
	if resp.Usage.PromptTokens != 11 || resp.Usage.CompletionTokens != 7 || resp.Usage.TotalTokens != 18 {
		t.Fatalf("usage = %+v, want prompt=11 completion=7 total=18", resp.Usage)
	}
}

func TestParseCodexExecTokenUsageJSONL(t *testing.T) {
	raw := strings.Join([]string{
		`{"type":"turn.started"}`,
		`{"type":"token_count","usage":{"prompt_tokens":3,"completion_tokens":4}}`,
		`{"type":"response.completed","response":{"usage":{"input_tokens":11,"output_tokens":7,"total_tokens":18}}}`,
	}, "\n")
	usage := parseCodexExecTokenUsage([]byte(raw))
	if usage.PromptTokens != 11 || usage.CompletionTokens != 7 || usage.TotalTokens != 18 {
		t.Fatalf("usage = %+v, want prompt=11 completion=7 total=18", usage)
	}
}

func TestParseCodexExecTokenUsageNestedMsgInfo(t *testing.T) {
	raw := strings.Join([]string{
		`{"type":"msg","msg":{"total_token_usage":{"input_tokens":8,"output_tokens":5,"total_tokens":13}}}`,
		`{"type":"info","info":{"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}}`,
	}, "\n")
	usage := parseCodexExecTokenUsage([]byte(raw))
	if usage.PromptTokens != 8 || usage.CompletionTokens != 5 || usage.TotalTokens != 13 {
		t.Fatalf("usage = %+v, want prompt=8 completion=5 total=13", usage)
	}
}

func TestCodexStructuredResponseCarriesUsageFallback(t *testing.T) {
	response, err := (codexStructuredResponse{
		Kind:      "final",
		Content:   "done",
		ToolCalls: []codexStructuredCall{},
		Usage:     &TokenUsage{PromptTokens: 5, CompletionTokens: 6},
	}).toLLMResponse()
	if err != nil {
		t.Fatalf("toLLMResponse() error = %v", err)
	}
	if response.Usage.PromptTokens != 5 || response.Usage.CompletionTokens != 6 || response.Usage.TotalTokens != 11 {
		t.Fatalf("usage = %+v, want prompt=5 completion=6 total=11", response.Usage)
	}
}

func TestCodexExecSandboxModeUsesDangerForTrustedManagedRuntime(t *testing.T) {
	t.Setenv(codexExecSandboxEnvFlag, "")
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "1")
	t.Setenv(managedAgentAllowLocalMutationFlag, "1")

	if got := codexExecSandboxMode(); got != codexExecSandboxDanger {
		t.Fatalf("trusted managed runtime should use %q codex sandbox, got %q", codexExecSandboxDanger, got)
	}
}

func TestCodexExecSandboxModeHonorsExplicitOverride(t *testing.T) {
	t.Setenv(codexExecSandboxEnvFlag, codexExecSandboxWorkspace)
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "1")
	t.Setenv(managedAgentAllowLocalMutationFlag, "1")

	if got := codexExecSandboxMode(); got != codexExecSandboxWorkspace {
		t.Fatalf("explicit sandbox override should win, got %q", got)
	}
}

func TestVerifyCodexExecVersionPin(t *testing.T) {
	t.Setenv(codexExecVersionPinEnvFlag, "codex-cli 0.139.0")
	old := codexExecVersionOutputFunc
	t.Cleanup(func() { codexExecVersionOutputFunc = old })
	codexExecVersionOutputFunc = func(executablePath string) ([]byte, error) {
		if executablePath != "codex-test" {
			t.Fatalf("unexpected executable path %q", executablePath)
		}
		return []byte("codex-cli 0.139.0\n"), nil
	}
	if err := verifyCodexExecVersionPin("codex-test"); err != nil {
		t.Fatalf("verifyCodexExecVersionPin() error = %v", err)
	}

	codexExecVersionOutputFunc = func(string) ([]byte, error) {
		return []byte("codex-cli 0.140.0\n"), nil
	}
	if err := verifyCodexExecVersionPin("codex-test"); err == nil || !strings.Contains(err.Error(), "version mismatch") {
		t.Fatalf("expected version mismatch, got %v", err)
	}
}

func TestBuildCodexExecPromptRoutesImplementationThroughOuterTools(t *testing.T) {
	prompt, err := buildCodexExecPrompt([]Message{{Role: "user", Content: "implement app"}}, []ToolDef{
		{Type: "function", Function: ToolFunctionDef{Name: "shell", Parameters: map[string]any{"type": "object"}}},
		{Type: "function", Function: ToolFunctionDef{Name: "write_file", Parameters: map[string]any{"type": "object"}}},
	})
	if err != nil {
		t.Fatalf("buildCodexExecPrompt() error = %v", err)
	}
	for _, want := range []string{"ledgered actions", "OUTER shell/write_file/project tools", "fresh read-only OUTER project tools", "Available outer tools JSON"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt, "Do not execute shell commands") {
		t.Fatalf("prompt should not globally forbid shell commands:\n%s", prompt)
	}
}

func TestCodexExecLLMChatParsesToolCalls(t *testing.T) {
	llm := &CodexExecLLM{
		executablePath: "codex",
		workdir:        t.TempDir(),
		model:          "gpt-test",
		runner: func(_ context.Context, _ string, args []string, _ string) ([]byte, error) {
			outputPath := argValue(args, "-o")
			if outputPath == "" {
				t.Fatalf("missing output path in args: %v", args)
			}
			if err := os.WriteFile(outputPath, []byte(`{"kind":"tool_calls","content":"","tool_calls":[{"name":"capture","arguments_json":"{\"value\":\"abc\"}"}]}`), 0o600); err != nil {
				t.Fatalf("write output file: %v", err)
			}
			return []byte("ok"), nil
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
		t.Fatalf("CodexExecLLM.Chat() error = %v", err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %+v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].Function.Name != "capture" {
		t.Fatalf("unexpected tool call name: %+v", resp.ToolCalls[0])
	}
	if resp.ToolCalls[0].Function.Arguments != `{"value":"abc"}` {
		t.Fatalf("unexpected tool call arguments: %s", resp.ToolCalls[0].Function.Arguments)
	}
}

func TestCodexExecLLMChatReturnsRecoverableInvalidToolArguments(t *testing.T) {
	llm := &CodexExecLLM{
		executablePath: "codex",
		workdir:        t.TempDir(),
		model:          "gpt-test",
		runner: func(_ context.Context, _ string, args []string, _ string) ([]byte, error) {
			outputPath := argValue(args, "-o")
			if outputPath == "" {
				t.Fatalf("missing output path in args: %v", args)
			}
			raw, err := json.Marshal(codexStructuredResponse{
				Kind:    "tool_calls",
				Content: "",
				ToolCalls: []codexStructuredCall{{
					Name:          "write_file",
					ArgumentsJSON: `{"path":"app/index.html","content":"bad\+escape"}`,
				}},
			})
			if err != nil {
				t.Fatalf("marshal structured response: %v", err)
			}
			if err := os.WriteFile(outputPath, raw, 0o600); err != nil {
				t.Fatalf("write output file: %v", err)
			}
			return []byte("ok"), nil
		},
	}

	resp, err := llm.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, []ToolDef{{
		Type: "function",
		Function: ToolFunctionDef{
			Name:       "write_file",
			Parameters: map[string]any{"type": "object"},
		},
	}})
	if err != nil {
		t.Fatalf("CodexExecLLM.Chat() error = %v", err)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Function.Name != "write_file" {
		t.Fatalf("expected write_file tool call, got %+v", resp.ToolCalls)
	}
	if !strings.Contains(resp.ToolCalls[0].Function.Arguments, codexInvalidArgumentsJSONKey) ||
		!strings.Contains(resp.ToolCalls[0].Function.Arguments, codexInvalidArgumentsErrorKey) {
		t.Fatalf("expected recoverable invalid arguments payload, got %s", resp.ToolCalls[0].Function.Arguments)
	}
}

func TestCodexExecLLMChatSurfacesTimeoutExplicitly(t *testing.T) {
	llm := &CodexExecLLM{
		executablePath: "codex",
		workdir:        t.TempDir(),
		model:          "gpt-test",
		runner: func(ctx context.Context, _ string, _ []string, _ string) ([]byte, error) {
			<-ctx.Done()
			return []byte("partial output"), ctx.Err()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	resp, err := llm.Chat(ctx, []Message{{Role: "user", Content: "hello"}}, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if resp != nil {
		t.Fatalf("expected no partial response without usage, got %+v", resp)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded classification, got %v", err)
	}
	if !strings.Contains(err.Error(), "codex exec timed out") || !strings.Contains(err.Error(), "partial output") {
		t.Fatalf("expected explicit timeout error, got %v", err)
	}
}

func TestCodexExecLLMChatReturnsPartialUsageOnTimeout(t *testing.T) {
	llm := &CodexExecLLM{
		executablePath: "codex",
		workdir:        t.TempDir(),
		model:          "gpt-test",
		runner: func(ctx context.Context, _ string, _ []string, _ string) ([]byte, error) {
			<-ctx.Done()
			return []byte(`{"type":"response.completed","response":{"usage":{"input_tokens":17,"output_tokens":9,"total_tokens":26}}}`), ctx.Err()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	resp, err := llm.Chat(ctx, []Message{{Role: "user", Content: "hello"}}, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded classification, got %v", err)
	}
	if resp == nil {
		t.Fatal("expected partial usage response")
	}
	if resp.Usage.PromptTokens != 17 || resp.Usage.CompletionTokens != 9 || resp.Usage.TotalTokens != 26 {
		t.Fatalf("usage = %+v, want prompt=17 completion=9 total=26", resp.Usage)
	}
}

func TestRunCodexExecTerminatesRealProcessOnTimeout(t *testing.T) {
	t.Setenv("RHIZOME_AGENT_CODEX_EXEC_HELPER", "spawn-child")
	// The deadline must fire AFTER the child process has actually started, so the
	// ctx.Done() cleanup branch (the code under test, which emits the process-tree
	// kill evidence) runs. A 50ms budget routinely expired during env construction
	// before cmd.Start(), returning an empty buffer via the early ctx.Err() guard
	// and flaking line "expected cleanup evidence" (~6/8 runs). 2s reliably starts
	// the process while staying far below the helper child's 10s sleep; the kill is
	// still asserted prompt by the return-elapsed bound below.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	out, err := runCodexExec(ctx, os.Args[0], []string{"-test.run=TestCodexExecHelperProcess", "-test.v=false"}, "")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error from helper process")
	}
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected helper context deadline, got ctx=%v err=%v output=%s", ctx.Err(), err, out)
	}
	// Returns well before the helper child's 10s sleep: proves we kill the process
	// tree promptly on deadline rather than waiting for the child to exit. The
	// margin above the 2s deadline absorbs process-tree teardown on slow/Windows CI.
	if elapsed > 6*time.Second {
		t.Fatalf("codex exec timeout did not return promptly, elapsed=%s output=%s err=%v", elapsed, out, err)
	}
	if !strings.Contains(string(out), "process tree kill attempted") {
		t.Fatalf("expected cleanup evidence in output, got %s", out)
	}
}

func TestCodexExecHelperProcess(t *testing.T) {
	switch os.Getenv("RHIZOME_AGENT_CODEX_EXEC_HELPER") {
	case "spawn-child":
		cmd := exec.Command(os.Args[0], "-test.run=TestCodexExecHelperProcess", "-test.v=false")
		cmd.Env = append(os.Environ(), "RHIZOME_AGENT_CODEX_EXEC_HELPER=child-sleep")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start child helper: %v", err)
		}
		time.Sleep(10 * time.Second)
	case "child-sleep":
		time.Sleep(10 * time.Second)
	default:
		return
	}
}

func TestCodexExecLLMChatSalvagesStructuredOutputOnTimeout(t *testing.T) {
	llm := &CodexExecLLM{
		executablePath: "codex",
		workdir:        t.TempDir(),
		model:          "gpt-test",
		runner: func(ctx context.Context, _ string, args []string, _ string) ([]byte, error) {
			outputPath := argValue(args, "-o")
			if outputPath == "" {
				t.Fatalf("missing output path in args: %v", args)
			}
			if err := os.WriteFile(outputPath, []byte(`{"kind":"tool_calls","content":"","tool_calls":[{"name":"write_file","arguments_json":"{\"path\":\"app/index.html\",\"content\":\"ok\"}"}]}`), 0o600); err != nil {
				t.Fatalf("write output file: %v", err)
			}
			<-ctx.Done()
			return []byte("partial output after structured response"), ctx.Err()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	resp, err := llm.Chat(ctx, []Message{{Role: "user", Content: "hello"}}, []ToolDef{{
		Type: "function",
		Function: ToolFunctionDef{
			Name:       "write_file",
			Parameters: map[string]any{"type": "object"},
		},
	}})
	if err != nil {
		t.Fatalf("expected structured timeout output to be salvaged, got %v", err)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Function.Name != "write_file" {
		t.Fatalf("expected salvaged write_file tool call, got %+v", resp.ToolCalls)
	}
	if !strings.Contains(resp.ToolCalls[0].Function.Arguments, "app/index.html") {
		t.Fatalf("expected salvaged tool arguments, got %+v", resp.ToolCalls[0])
	}
}

func TestCodexExecLLMChatSurfacesCancellationExplicitly(t *testing.T) {
	llm := &CodexExecLLM{
		executablePath: "codex",
		workdir:        t.TempDir(),
		model:          "gpt-test",
		runner: func(ctx context.Context, _ string, _ []string, _ string) ([]byte, error) {
			<-ctx.Done()
			return []byte("partial output"), ctx.Err()
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := llm.Chat(ctx, []Message{{Role: "user", Content: "hello"}}, nil)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled classification, got %v", err)
	}
	if !strings.Contains(err.Error(), "codex exec canceled") || !strings.Contains(err.Error(), "partial output") {
		t.Fatalf("expected explicit canceled error, got %v", err)
	}
}

func TestCodexExecLLMChatPreservesNonContextExecFailure(t *testing.T) {
	llm := &CodexExecLLM{
		executablePath: "codex",
		workdir:        t.TempDir(),
		model:          "gpt-test",
		runner: func(_ context.Context, _ string, _ []string, _ string) ([]byte, error) {
			return []byte("stderr output"), errors.New("exit status 1")
		},
	}

	_, err := llm.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, nil)
	if err == nil {
		t.Fatal("expected exec failure")
	}
	if !strings.Contains(err.Error(), "codex exec failed") || !strings.Contains(err.Error(), "stderr output") {
		t.Fatalf("expected generic exec failure, got %v", err)
	}
}

func TestCodexExecErrorOutputKeepsHeadAndTail(t *testing.T) {
	output := codexExecErrorOutput([]byte("head-" + strings.Repeat("x", 1400) + "-tail"))
	if !strings.Contains(output, "head-") || !strings.Contains(output, "-tail") || !strings.Contains(output, "\n...\n") {
		t.Fatalf("expected long codex output to preserve head and tail, got %q", output)
	}
}

func TestNormalizeCodexArgumentsRejectsNonObjectJSON(t *testing.T) {
	if _, err := normalizeCodexArguments(`["not","an","object"]`); err == nil {
		t.Fatal("expected error for non-object arguments_json")
	}
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func argValue(args []string, key string) string {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == key {
			return args[i+1]
		}
	}
	return ""
}

func TestParseCodexExecTokenUsageCachedInputTokens(t *testing.T) {
	raw := []byte(`{"type":"token_count","usage":{"input_tokens":1000,"cached_input_tokens":800,"output_tokens":50}}`)
	usage := parseCodexExecTokenUsage(raw)
	if usage.PromptTokens != 1000 {
		t.Fatalf("prompt tokens = %d, want 1000", usage.PromptTokens)
	}
	if usage.CachedPromptTokens != 800 {
		t.Fatalf("cached prompt tokens = %d, want 800", usage.CachedPromptTokens)
	}
	if usage.CompletionTokens != 50 {
		t.Fatalf("completion tokens = %d, want 50", usage.CompletionTokens)
	}
	if usage.TotalTokens != 1050 {
		t.Fatalf("total tokens = %d, want 1050", usage.TotalTokens)
	}
}
