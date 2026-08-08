package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const codexStructuredResponseSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "kind": {
      "type": "string",
      "enum": ["final", "tool_calls"]
    },
    "content": {
      "type": "string"
    },
    "tool_calls": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "name": {
            "type": "string"
          },
          "arguments_json": {
            "type": "string"
          }
        },
        "required": ["name", "arguments_json"]
      }
    }
  },
  "required": ["kind", "content", "tool_calls"]
}`

type codexExecRunner func(ctx context.Context, executablePath string, args []string, stdin string) ([]byte, error)

type CodexExecLLM struct {
	executablePath string
	baseArgs       []string
	workdir        string
	model          string
	runner         codexExecRunner
}

type codexStructuredResponse struct {
	Kind      string                `json:"kind"`
	Content   string                `json:"content"`
	ToolCalls []codexStructuredCall `json:"tool_calls"`
	Usage     *TokenUsage           `json:"usage,omitempty"`
}

type codexStructuredCall struct {
	Name          string `json:"name"`
	ArgumentsJSON string `json:"arguments_json"`
}

const (
	codexInvalidArgumentsJSONKey  = "_codex_invalid_arguments_json"
	codexInvalidArgumentsErrorKey = "_codex_arguments_parse_error"
	codexExecSandboxEnvFlag       = "RHIZOME_AGENT_CODEX_SANDBOX"
	codexExecVersionPinEnvFlag    = "RHIZOME_AGENT_CODEX_CLI_VERSION"
	codexExecSandboxReadOnly      = "read-only"
	codexExecSandboxWorkspace     = "workspace-write"
	codexExecSandboxDanger        = "danger-full-access"
	codexExecKillWaitBudget       = 5 * time.Second
	codexExecOutputLimitBytes     = 1024 * 1024
	codexExecVersionCheckTimeout  = 5 * time.Second
)

var codexExecVersionOutputFunc = codexExecVersionOutput

func NewCodexExecLLM(executablePath, workdir, model string, baseArgs ...string) *CodexExecLLM {
	return &CodexExecLLM{
		executablePath: executablePath,
		baseArgs:       append([]string(nil), baseArgs...),
		workdir:        workdir,
		model:          model,
		runner:         runCodexExec,
	}
}

func verifyCodexExecVersionPin(executablePath string) error {
	pin := strings.TrimSpace(os.Getenv(codexExecVersionPinEnvFlag))
	if pin == "" {
		return nil
	}
	output, err := codexExecVersionOutputFunc(executablePath)
	if err != nil {
		return fmt.Errorf("codex cli version pin %q could not be verified: %w", pin, err)
	}
	version := strings.TrimSpace(string(output))
	if !strings.Contains(version, pin) {
		return fmt.Errorf("codex cli version mismatch: got %q, want substring %q", version, pin)
	}
	return nil
}

func codexExecVersionOutput(executablePath string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), codexExecVersionCheckTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, executablePath, "--version")
	cmd.Env = codexExecEnv()
	output, err := cmd.CombinedOutput()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return output, fmt.Errorf("%w while running %s --version", context.DeadlineExceeded, executablePath)
	}
	if err != nil {
		return output, fmt.Errorf("%w (output: %s)", err, truncate(strings.TrimSpace(string(output)), 400))
	}
	return output, nil
}

func runCodexExec(ctx context.Context, executablePath string, args []string, stdin string) ([]byte, error) {
	output := newBoundedShellOutputBuffer(codexExecOutputLimitBytes)
	cmd := exec.Command(executablePath, args...)
	cmd.Env = codexExecEnv()
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Stdout = output
	cmd.Stderr = output
	configureShellCommandProcess(cmd)

	if err := ctx.Err(); err != nil {
		return output.Bytes(), err
	}
	if err := cmd.Start(); err != nil {
		return output.Bytes(), err
	}

	processHandle, err := attachShellCommandProcessTree(cmd)
	if err != nil {
		output.WriteString(fmt.Sprintf("[codex exec containment] process tree containment degraded: %v; timeout cleanup will use pid fallback\n", err))
		processHandle = fallbackShellCommandProcessHandle(cmd)
	}
	if processHandle != nil {
		defer processHandle.release()
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	select {
	case err := <-waitCh:
		return output.Bytes(), err
	case <-ctx.Done():
		output.WriteString(fmt.Sprintf("\n[codex exec cleanup] context ended; process tree kill attempted; wait grace %s\n", codexExecKillWaitBudget))
		var killErr error
		if processHandle != nil {
			killErr = processHandle.terminate()
		} else if cmd.Process != nil {
			killErr = killShellCommandProcessTreeByPID(cmd.Process.Pid)
		}
		select {
		case waitErr := <-waitCh:
			if killErr != nil {
				output.WriteString(fmt.Sprintf("[codex exec cleanup] process tree kill error: %v\n", killErr))
			}
			if waitErr != nil {
				output.WriteString(fmt.Sprintf("[codex exec cleanup] process wait after context ended: %v\n", waitErr))
			}
			return output.Bytes(), ctx.Err()
		case <-time.After(codexExecKillWaitBudget):
			if killErr != nil {
				output.WriteString(fmt.Sprintf("[codex exec cleanup] process tree kill error: %v\n", killErr))
			}
			output.WriteString(fmt.Sprintf("[codex exec cleanup] process wait grace expired after %s\n", codexExecKillWaitBudget))
			return output.Bytes(), ctx.Err()
		}
	}
}

func codexExecEnv() []string {
	env := append([]string(nil), os.Environ()...)
	if root := strings.TrimSpace(os.Getenv(managedAgentCodexHomeFlag)); root != "" {
		env = upsertEnvValue(env, "CODEX_HOME", cleanManagedRuntimeRoot(root))
	}
	env = appendKnownBrowserDirsToEnvPath(env)
	return env
}

func upsertEnvValue(env []string, key, value string) []string {
	key = strings.TrimSpace(key)
	if key == "" {
		return env
	}
	entryValue := key + "=" + value
	for i, entry := range env {
		existingKey, _, ok := strings.Cut(entry, "=")
		if ok && strings.EqualFold(strings.TrimSpace(existingKey), key) {
			env[i] = entryValue
			return env
		}
	}
	return append(env, entryValue)
}

func (l *CodexExecLLM) Chat(ctx context.Context, messages []Message, tools []ToolDef) (*LLMResponse, error) {
	tmpDir, err := os.MkdirTemp("", "rhizome-agent-codex-*")
	if err != nil {
		return nil, fmt.Errorf("create codex temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	prompt, _, err := buildCodexExecPromptWithSections(messages, tools)
	if err != nil {
		return nil, err
	}

	schemaPath := filepath.Join(tmpDir, "schema.json")
	outputPath := filepath.Join(tmpDir, "output.json")
	if err := os.WriteFile(schemaPath, []byte(codexStructuredResponseSchema), 0o600); err != nil {
		return nil, fmt.Errorf("write codex schema: %w", err)
	}

	args := append([]string(nil), l.baseArgs...)
	args = append(args, "--ask-for-approval", "never", "exec", "--json", "--sandbox", codexExecSandboxMode(), "--skip-git-repo-check", "-C", l.workdir)
	if strings.TrimSpace(l.model) != "" {
		args = append(args, "-m", l.model)
	}
	args = append(args, "--output-schema", schemaPath, "-o", outputPath, "-")

	log.Printf("[llm] calling codex exec backend (%d messages, %d tools)", len(messages), len(tools))
	commandOutput, err := l.runner(ctx, l.executablePath, args, prompt)
	usage := parseCodexExecTokenUsage(commandOutput)
	if err != nil {
		output := codexExecErrorOutput(commandOutput)
		if !errors.Is(err, context.Canceled) && !errors.Is(ctx.Err(), context.Canceled) {
			if response, parseErr := readCodexStructuredOutput(outputPath, usage); parseErr == nil {
				log.Printf("[llm] codex exec returned valid structured output despite process error: %v", err)
				return response, nil
			}
		}
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
			return codexPartialUsageResponse(usage), fmt.Errorf("codex exec timed out: %w (output: %s)", context.DeadlineExceeded, output)
		case errors.Is(err, context.Canceled), errors.Is(ctx.Err(), context.Canceled):
			return codexPartialUsageResponse(usage), fmt.Errorf("codex exec canceled: %w (output: %s)", context.Canceled, output)
		default:
			return codexPartialUsageResponse(usage), fmt.Errorf("codex exec failed: %w (output: %s)", err, output)
		}
	}

	return readCodexStructuredOutput(outputPath, usage)
}

func codexPartialUsageResponse(usage TokenUsage) *LLMResponse {
	if codexTokenUsageTotal(usage) <= 0 {
		return nil
	}
	return &LLMResponse{Usage: usage}
}

func codexExecSandboxMode() string {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(codexExecSandboxEnvFlag))) {
	case codexExecSandboxReadOnly:
		return codexExecSandboxReadOnly
	case codexExecSandboxWorkspace:
		return codexExecSandboxWorkspace
	case codexExecSandboxDanger:
		return codexExecSandboxDanger
	}
	if isManagedAgentProcess() &&
		envFlagEnabled(managedAgentAllowLocalShellFlag) &&
		envFlagEnabled(managedAgentAllowLocalMutationFlag) {
		return codexExecSandboxDanger
	}
	return codexExecSandboxReadOnly
}

func envFlagEnabled(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func readCodexStructuredOutput(outputPath string, usage TokenUsage) (*LLMResponse, error) {
	raw, err := os.ReadFile(outputPath)
	if err != nil {
		return nil, fmt.Errorf("read codex output: %w", err)
	}
	var structured codexStructuredResponse
	if err := json.Unmarshal(raw, &structured); err != nil {
		return nil, fmt.Errorf("parse codex output: %w (body: %s)", err, truncate(strings.TrimSpace(string(raw)), 400))
	}
	response, err := structured.toLLMResponse()
	if err != nil {
		return nil, err
	}
	if codexTokenUsageTotal(response.Usage) == 0 && codexTokenUsageTotal(usage) > 0 {
		response.Usage = usage
	}
	return response, nil
}

func codexExecErrorOutput(raw []byte) string {
	output := strings.TrimSpace(string(raw))
	if len(output) <= 1200 {
		return output
	}
	return truncate(output, 400) + "\n...\n" + output[len(output)-800:]
}

// codexStaticPreamble is the backend-injected static instruction text emitted
// by buildCodexExecPrompt before the conversation/tools JSON. It is the
// static_preamble section per the TE-02 prompt telemetry schema; its byte
// length is a measurable constant used for section attribution.
func codexStaticPreamble() string {
	var b strings.Builder
	b.WriteString("You are the reasoning backend for an outer agent.\n")
	b.WriteString("Do not call Codex built-in tools or MCP tools directly from this backend process; the outer runtime needs ledgered actions.\n")
	b.WriteString("When implementation requires shell commands, file edits, package managers, git, tests, or project materialization, choose the supplied OUTER shell/write_file/project tools if they are listed below.\n")
	b.WriteString("When a blocked/complete decision depends on current project coordination, prefer fresh read-only OUTER project tools or fresh packet evidence over stale docs, peer answers, or local memory.\n")
	b.WriteString("Your only job is to decide the OUTER agent's next action from the supplied conversation and outer-tool definitions.\n")
	b.WriteString("Return JSON only, matching the provided schema.\n\n")
	b.WriteString("Decision rules:\n")
	b.WriteString("- Use kind=\"final\" when the outer agent should answer directly.\n")
	b.WriteString("- Use kind=\"tool_calls\" when the outer agent should invoke one or more of the supplied tools.\n")
	b.WriteString("- Use only tool names from the supplied list.\n")
	b.WriteString("- Each tool call arguments_json value must be a JSON string encoding an object, for example {\"name\":\"tool\",\"arguments_json\":\"{\\\"path\\\":\\\"README.md\\\"}\"}.\n")
	b.WriteString("- Prefer the minimum number of tool calls needed for the next step.\n")
	b.WriteString("- Never wrap the JSON in markdown fences.\n\n")
	return b.String()
}

func buildCodexExecPrompt(messages []Message, tools []ToolDef) (string, error) {
	prompt, _, err := buildCodexExecPromptWithSections(messages, tools)
	return prompt, err
}

// buildCodexExecPromptWithSections builds the codex stdin prompt and returns the
// per-call prompt section byte breakdown (TE-02 telemetry). The breakdown is
// computed from the exact built prompt: static_preamble is the measured static
// header, total is the exact len of the returned prompt string.
func buildCodexExecPromptWithSections(messages []Message, tools []ToolDef) (string, PromptSectionBytes, error) {
	// TE-31 (economy, E1-ranked): compact JSON instead of MarshalIndent. The E1 byte budget
	// showed tools_json (31.7%) + system/packet (44.5%, carried inside messageJSON) dominate
	// per-call cost; two-space indentation on deeply nested tool schemas and message arrays
	// inflated every call by roughly a quarter to a third for zero semantic value. The model
	// parses JSON either way.
	messageJSON, err := json.Marshal(messages)
	if err != nil {
		return "", PromptSectionBytes{}, fmt.Errorf("marshal messages for codex backend: %w", err)
	}
	toolJSON, err := json.Marshal(tools)
	if err != nil {
		return "", PromptSectionBytes{}, fmt.Errorf("marshal tools for codex backend: %w", err)
	}

	preamble := codexStaticPreamble()
	var b strings.Builder
	b.WriteString(preamble)
	b.WriteString("Conversation JSON:\n")
	b.Write(messageJSON)
	b.WriteString("\n\nAvailable outer tools JSON:\n")
	b.Write(toolJSON)
	b.WriteString("\n")
	prompt := b.String()

	sections := computePromptSectionBytes(messages, marshalToolsForTelemetry(tools), len(preamble), len(prompt))
	logPromptSectionBytes("codex", sections)
	return prompt, sections, nil
}

func (r codexStructuredResponse) toLLMResponse() (*LLMResponse, error) {
	switch strings.TrimSpace(r.Kind) {
	case "final":
		return &LLMResponse{
			Content: strings.TrimSpace(r.Content),
			Usage:   codexStructuredTokenUsage(r.Usage),
		}, nil
	case "tool_calls":
		toolCalls := make([]ToolCall, 0, len(r.ToolCalls))
		for i, call := range r.ToolCalls {
			name := strings.TrimSpace(call.Name)
			if name == "" {
				return nil, fmt.Errorf("codex tool call %d missing name", i)
			}
			rawArgs, err := normalizeCodexArguments(call.ArgumentsJSON)
			if err != nil {
				rawArgs = codexInvalidArgumentsPayload(call.ArgumentsJSON, err)
			}
			toolCalls = append(toolCalls, ToolCall{
				ID:   fmt.Sprintf("codex-call-%d", i+1),
				Type: "function",
				Function: FunctionCall{
					Name:      name,
					Arguments: string(rawArgs),
				},
			})
		}
		if len(toolCalls) == 0 {
			return nil, fmt.Errorf("codex backend returned kind=tool_calls with no tool calls")
		}
		return &LLMResponse{
			Content:   strings.TrimSpace(r.Content),
			ToolCalls: toolCalls,
			Usage:     codexStructuredTokenUsage(r.Usage),
		}, nil
	default:
		return nil, fmt.Errorf("unknown codex response kind %q", r.Kind)
	}
}

func codexStructuredTokenUsage(usage *TokenUsage) TokenUsage {
	if usage == nil {
		return TokenUsage{}
	}
	out := *usage
	if out.TotalTokens <= 0 && (out.PromptTokens > 0 || out.CompletionTokens > 0) {
		out.TotalTokens = out.PromptTokens + out.CompletionTokens
	}
	return out
}

func parseCodexExecTokenUsage(raw []byte) TokenUsage {
	var best TokenUsage
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			continue
		}
		usage, ok := codexTokenUsageFromMap(decoded)
		if !ok {
			continue
		}
		best = codexPreferLargerTokenUsage(best, usage)
	}
	return best
}

func codexTokenUsageFromMap(payload map[string]any) (TokenUsage, bool) {
	if len(payload) == 0 {
		return TokenUsage{}, false
	}
	for _, key := range []string{"usage", "token_usage", "tokenUsage", "total_token_usage", "totalTokenUsage"} {
		if nested, ok := payload[key].(map[string]any); ok {
			if usage, found := codexTokenUsageFields(nested); found {
				return usage, true
			}
			if usage, found := codexTokenUsageFromMap(nested); found {
				return usage, true
			}
		}
	}
	if usage, found := codexTokenUsageFields(payload); found {
		return usage, true
	}
	for _, key := range []string{"response", "result", "message", "msg", "info", "payload", "data", "event"} {
		if nested, ok := payload[key].(map[string]any); ok {
			if usage, found := codexTokenUsageFromMap(nested); found {
				return usage, true
			}
		}
	}
	return TokenUsage{}, false
}

func codexTokenUsageFields(payload map[string]any) (TokenUsage, bool) {
	prompt := codexTokenUsageInt(payload, "prompt_tokens", "input_tokens", "inputTokens", "total_input_tokens", "totalInputTokens")
	completion := codexTokenUsageInt(payload, "completion_tokens", "output_tokens", "outputTokens", "total_output_tokens", "totalOutputTokens")
	total := codexTokenUsageInt(payload, "total_tokens", "totalTokens", "tokens", "total")
	// TE-20 finding: codex exec token_count events report the cached-input
	// portion separately; record it so cache hits become visible in telemetry
	// (billing math intentionally unchanged).
	cached := codexTokenUsageInt(payload, "cached_input_tokens", "cachedInputTokens", "total_cached_input_tokens", "totalCachedInputTokens", "cached_tokens")
	if total <= 0 && (prompt > 0 || completion > 0) {
		total = prompt + completion
	}
	if prompt <= 0 && completion <= 0 && total <= 0 {
		return TokenUsage{}, false
	}
	return TokenUsage{
		PromptTokens:       prompt,
		CompletionTokens:   completion,
		TotalTokens:        total,
		CachedPromptTokens: cached,
	}, true
}

func codexTokenUsageInt(payload map[string]any, keys ...string) int {
	for _, key := range keys {
		value, ok := payload[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case float64:
			if typed > 0 {
				return int(typed)
			}
		case int:
			if typed > 0 {
				return typed
			}
		case int64:
			if typed > 0 {
				return int(typed)
			}
		case json.Number:
			if parsed, err := typed.Int64(); err == nil && parsed > 0 {
				return int(parsed)
			}
		case string:
			trimmed := strings.TrimSpace(typed)
			if trimmed == "" {
				continue
			}
			var parsed int
			if _, err := fmt.Sscanf(trimmed, "%d", &parsed); err == nil && parsed > 0 {
				return parsed
			}
		}
	}
	return 0
}

func codexPreferLargerTokenUsage(current, candidate TokenUsage) TokenUsage {
	if codexTokenUsageTotal(candidate) > codexTokenUsageTotal(current) {
		return candidate
	}
	if codexTokenUsageTotal(candidate) == codexTokenUsageTotal(current) && candidate.PromptTokens+candidate.CompletionTokens > current.PromptTokens+current.CompletionTokens {
		return candidate
	}
	return current
}

func codexTokenUsageTotal(usage TokenUsage) int {
	if usage.TotalTokens > 0 {
		return usage.TotalTokens
	}
	return usage.PromptTokens + usage.CompletionTokens
}

func normalizeCodexArguments(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "{}", nil
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return "", err
	}
	if decoded == nil {
		decoded = map[string]any{}
	}

	normalized, err := json.Marshal(decoded)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}

func codexInvalidArgumentsPayload(raw string, cause error) string {
	payload := map[string]any{
		codexInvalidArgumentsJSONKey: strings.TrimSpace(raw),
	}
	if cause != nil {
		payload[codexInvalidArgumentsErrorKey] = cause.Error()
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Sprintf(`{"%s":true}`, codexInvalidArgumentsErrorKey)
	}
	return string(encoded)
}
