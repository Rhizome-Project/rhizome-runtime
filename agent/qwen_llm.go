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
	"runtime"
	"strings"
	"time"
)

type qwenExecRunner func(ctx context.Context, executablePath string, args []string, stdin string, workdir string) ([]byte, error)

type QwenExecLLM struct {
	executablePath string
	baseArgs       []string
	workdir        string
	model          string
	runner         qwenExecRunner
}

type qwenOutputEvent struct {
	Type    string          `json:"type"`
	Subtype string          `json:"subtype,omitempty"`
	IsError bool            `json:"is_error,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
	Message struct {
		Content json.RawMessage `json:"content,omitempty"`
	} `json:"message,omitempty"`
}

const (
	qwenExecApprovalModeEnvFlag  = "RHIZOME_AGENT_QWEN_APPROVAL_MODE"
	qwenExecBareEnvFlag          = "RHIZOME_AGENT_QWEN_BARE"
	qwenExecApprovalModeDefault  = "plan"
	qwenInvalidArgumentsJSONKey  = "_qwen_invalid_arguments_json"
	qwenInvalidArgumentsErrorKey = "_qwen_arguments_parse_error"
)

func NewQwenExecLLM(executablePath, workdir, model string, baseArgs ...string) *QwenExecLLM {
	return &QwenExecLLM{
		executablePath: executablePath,
		baseArgs:       append([]string(nil), baseArgs...),
		workdir:        workdir,
		model:          model,
		runner:         runQwenExec,
	}
}

func runQwenExec(ctx context.Context, executablePath string, args []string, stdin string, workdir string) ([]byte, error) {
	output := newBoundedShellOutputBuffer(codexExecOutputLimitBytes)
	commandPath, commandArgs := qwenExecCommand(executablePath, args)
	cmd := exec.Command(commandPath, commandArgs...)
	cmd.Env = qwenExecEnv()
	if strings.TrimSpace(workdir) != "" {
		cmd.Dir = workdir
	}
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
		output.WriteString(fmt.Sprintf("[qwen exec containment] process tree containment degraded: %v; timeout cleanup will use pid fallback\n", err))
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
		output.WriteString(fmt.Sprintf("\n[qwen exec cleanup] context ended; process tree kill attempted; wait grace %s\n", codexExecKillWaitBudget))
		var killErr error
		if processHandle != nil {
			killErr = processHandle.terminate()
		} else if cmd.Process != nil {
			killErr = killShellCommandProcessTreeByPID(cmd.Process.Pid)
		}
		select {
		case waitErr := <-waitCh:
			if killErr != nil {
				output.WriteString(fmt.Sprintf("[qwen exec cleanup] process tree kill error: %v\n", killErr))
			}
			if waitErr != nil {
				output.WriteString(fmt.Sprintf("[qwen exec cleanup] process wait after context ended: %v\n", waitErr))
			}
			return output.Bytes(), ctx.Err()
		case <-time.After(codexExecKillWaitBudget):
			if killErr != nil {
				output.WriteString(fmt.Sprintf("[qwen exec cleanup] process tree kill error: %v\n", killErr))
			}
			output.WriteString(fmt.Sprintf("[qwen exec cleanup] process wait grace expired after %s\n", codexExecKillWaitBudget))
			return output.Bytes(), ctx.Err()
		}
	}
}

func qwenExecEnv() []string {
	env := append([]string(nil), os.Environ()...)
	env = appendKnownBrowserDirsToEnvPath(env)
	return env
}

func qwenExecCommand(executablePath string, args []string) (string, []string) {
	if runtime.GOOS != "windows" {
		return executablePath, args
	}
	switch strings.ToLower(filepath.Ext(executablePath)) {
	case ".bat", ".cmd":
		comspec := strings.TrimSpace(os.Getenv("COMSPEC"))
		if comspec == "" {
			comspec = "cmd.exe"
		}
		commandArgs := []string{"/d", "/s", "/c", executablePath}
		commandArgs = append(commandArgs, args...)
		return comspec, commandArgs
	default:
		return executablePath, args
	}
}

func (l *QwenExecLLM) Chat(ctx context.Context, messages []Message, tools []ToolDef) (*LLMResponse, error) {
	tmpDir, err := os.MkdirTemp("", "rhizome-agent-qwen-*")
	if err != nil {
		return nil, fmt.Errorf("create qwen temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	prompt, _, err := buildQwenExecPromptWithSections(messages, tools)
	if err != nil {
		return nil, err
	}

	schemaPath := filepath.Join(tmpDir, "schema.json")
	if err := os.WriteFile(schemaPath, []byte(codexStructuredResponseSchema), 0o600); err != nil {
		return nil, fmt.Errorf("write qwen schema: %w", err)
	}

	args := append([]string(nil), l.baseArgs...)
	if qwenExecBareMode() {
		args = append(args, "--bare")
	}
	args = append(args, "--approval-mode", qwenExecApprovalMode(), "--output-format", "json", "--json-schema", "@"+schemaPath)
	if strings.TrimSpace(l.model) != "" {
		args = append(args, "-m", l.model)
	}
	args = append(args, "-p", "Read the full backend request from stdin and return the structured JSON response.")

	// Qwen Code auto-discovers repository state from cwd. The outer Rhizome
	// runtime supplies all context and ledgered tools through stdin, so keep the
	// backend in an isolated non-repo cwd to avoid host git/status side effects.
	execWorkdir := tmpDir

	log.Printf("[llm] calling qwen code backend (%d messages, %d tools)", len(messages), len(tools))
	commandOutput, err := l.runner(ctx, l.executablePath, args, prompt, execWorkdir)
	if err != nil {
		output := codexExecErrorOutput(commandOutput)
		if !errors.Is(err, context.Canceled) && !errors.Is(ctx.Err(), context.Canceled) {
			if response, parseErr := parseQwenStructuredOutput(commandOutput); parseErr == nil {
				log.Printf("[llm] qwen exec returned valid structured output despite process error: %v", err)
				return response, nil
			}
		}
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
			return nil, fmt.Errorf("qwen exec timed out: %w (output: %s)", context.DeadlineExceeded, output)
		case errors.Is(err, context.Canceled), errors.Is(ctx.Err(), context.Canceled):
			return nil, fmt.Errorf("qwen exec canceled: %w (output: %s)", context.Canceled, output)
		default:
			return nil, fmt.Errorf("qwen exec failed: %w (output: %s)", err, output)
		}
	}

	return parseQwenStructuredOutput(commandOutput)
}

func qwenExecApprovalMode() string {
	switch mode := strings.ToLower(strings.TrimSpace(os.Getenv(qwenExecApprovalModeEnvFlag))); mode {
	case "plan", "default", "auto-edit", "auto", "yolo":
		return mode
	default:
		return qwenExecApprovalModeDefault
	}
}

func qwenExecBareMode() bool {
	return envFlagEnabled(qwenExecBareEnvFlag)
}

// qwenStaticPreamble is the backend-injected static instruction text emitted by
// buildQwenExecPrompt before the conversation/tools JSON. It is the
// static_preamble section per the TE-02 prompt telemetry schema; its byte
// length is a measurable constant used for section attribution.
func qwenStaticPreamble() string {
	var b strings.Builder
	b.WriteString("You are the reasoning backend for an outer agent.\n")
	b.WriteString("Do not call Qwen Code built-in tools or MCP tools directly from this backend process; the outer runtime needs ledgered actions.\n")
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

func buildQwenExecPrompt(messages []Message, tools []ToolDef) (string, error) {
	prompt, _, err := buildQwenExecPromptWithSections(messages, tools)
	return prompt, err
}

// buildQwenExecPromptWithSections builds the qwen stdin prompt and returns the
// per-call prompt section byte breakdown (TE-02 telemetry). The breakdown is
// computed from the exact built prompt: static_preamble is the measured static
// header, total is the exact len of the returned prompt string.
func buildQwenExecPromptWithSections(messages []Message, tools []ToolDef) (string, PromptSectionBytes, error) {
	// TE-31: compact JSON, mirroring the codex builder - see buildCodexExecPromptWithSections.
	messageJSON, err := json.Marshal(messages)
	if err != nil {
		return "", PromptSectionBytes{}, fmt.Errorf("marshal messages for qwen backend: %w", err)
	}
	toolJSON, err := json.Marshal(tools)
	if err != nil {
		return "", PromptSectionBytes{}, fmt.Errorf("marshal tools for qwen backend: %w", err)
	}

	preamble := qwenStaticPreamble()
	var b strings.Builder
	b.WriteString(preamble)
	b.WriteString("Conversation JSON:\n")
	b.Write(messageJSON)
	b.WriteString("\n\nAvailable outer tools JSON:\n")
	b.Write(toolJSON)
	b.WriteString("\n")
	prompt := b.String()

	sections := computePromptSectionBytes(messages, marshalToolsForTelemetry(tools), len(preamble), len(prompt))
	logPromptSectionBytes("qwen", sections)
	return prompt, sections, nil
}

func parseQwenStructuredOutput(raw []byte) (*LLMResponse, error) {
	body := strings.TrimSpace(string(raw))
	if body == "" {
		return nil, fmt.Errorf("qwen output is empty")
	}

	var lastErr error
	for _, candidate := range qwenJSONCandidates(body) {
		response, err := parseQwenJSONPayload(candidate)
		if err == nil {
			return response, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no JSON payload found")
	}
	return nil, fmt.Errorf("parse qwen output: %w (body: %s)", lastErr, truncate(body, 400))
}

func parseQwenJSONPayload(raw []byte) (*LLMResponse, error) {
	var events []qwenOutputEvent
	if err := json.Unmarshal(raw, &events); err == nil && events != nil {
		return qwenEventsToLLMResponse(events)
	}
	return qwenStructuredResultToLLMResponse(raw)
}

func qwenEventsToLLMResponse(events []qwenOutputEvent) (*LLMResponse, error) {
	var lastErr error
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if !qwenRawMessageEmpty(event.Result) {
			response, err := qwenStructuredResultToLLMResponse(event.Result)
			if err == nil {
				return response, nil
			}
			lastErr = err
		}
		if !qwenRawMessageEmpty(event.Message.Content) {
			response, err := qwenMessageContentToLLMResponse(event.Message.Content)
			if err == nil {
				return response, nil
			}
			lastErr = err
		}
		if event.IsError && lastErr == nil {
			lastErr = fmt.Errorf("qwen result error: %s", qwenRawMessageSummary(firstQwenRawMessage(event.Error, event.Result)))
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("qwen json output missing structured result")
}

func qwenStructuredResultToLLMResponse(raw json.RawMessage) (*LLMResponse, error) {
	raw = json.RawMessage(strings.TrimSpace(string(raw)))
	if qwenRawMessageEmpty(raw) {
		return nil, fmt.Errorf("qwen structured result is empty")
	}

	var structured codexStructuredResponse
	if err := json.Unmarshal(raw, &structured); err == nil && strings.TrimSpace(structured.Kind) != "" {
		return qwenStructuredResponseToLLMResponse(structured)
	}

	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return qwenStructuredTextToLLMResponse(text)
	}

	return nil, fmt.Errorf("qwen structured result is not a supported JSON shape")
}

type qwenMessageContentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func qwenMessageContentToLLMResponse(raw json.RawMessage) (*LLMResponse, error) {
	if response, err := qwenStructuredResultToLLMResponse(raw); err == nil {
		return response, nil
	}

	var parts []qwenMessageContentPart
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, err
	}
	var textParts []string
	for _, part := range parts {
		if strings.TrimSpace(part.Text) == "" {
			continue
		}
		textParts = append(textParts, part.Text)
	}
	if len(textParts) == 0 {
		return nil, fmt.Errorf("qwen assistant message content has no text parts")
	}
	return qwenStructuredTextToLLMResponse(strings.Join(textParts, "\n"))
}

func qwenStructuredTextToLLMResponse(text string) (*LLMResponse, error) {
	text = stripQwenJSONFence(strings.TrimSpace(text))
	var lastErr error
	for _, candidate := range qwenJSONCandidates(text) {
		var structured codexStructuredResponse
		if err := json.Unmarshal(candidate, &structured); err != nil {
			lastErr = err
			continue
		}
		if strings.TrimSpace(structured.Kind) == "" {
			lastErr = fmt.Errorf("qwen structured text missing kind")
			continue
		}
		return qwenStructuredResponseToLLMResponse(structured)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("qwen structured text is not JSON")
	}
	return nil, lastErr
}

func qwenStructuredResponseToLLMResponse(r codexStructuredResponse) (*LLMResponse, error) {
	switch strings.TrimSpace(r.Kind) {
	case "final":
		return &LLMResponse{
			Content: strings.TrimSpace(r.Content),
		}, nil
	case "tool_calls":
		toolCalls := make([]ToolCall, 0, len(r.ToolCalls))
		for i, call := range r.ToolCalls {
			name := strings.TrimSpace(call.Name)
			if name == "" {
				return nil, fmt.Errorf("qwen tool call %d missing name", i)
			}
			rawArgs, err := normalizeCodexArguments(call.ArgumentsJSON)
			if err != nil {
				rawArgs = qwenInvalidArgumentsPayload(call.ArgumentsJSON, err)
			}
			toolCalls = append(toolCalls, ToolCall{
				ID:   fmt.Sprintf("qwen-call-%d", i+1),
				Type: "function",
				Function: FunctionCall{
					Name:      name,
					Arguments: string(rawArgs),
				},
			})
		}
		if len(toolCalls) == 0 {
			return nil, fmt.Errorf("qwen backend returned kind=tool_calls with no tool calls")
		}
		return &LLMResponse{
			Content:   strings.TrimSpace(r.Content),
			ToolCalls: toolCalls,
		}, nil
	default:
		return nil, fmt.Errorf("unknown qwen response kind %q", r.Kind)
	}
}

func qwenInvalidArgumentsPayload(raw string, cause error) string {
	payload := map[string]any{
		qwenInvalidArgumentsJSONKey: strings.TrimSpace(raw),
	}
	if cause != nil {
		payload[qwenInvalidArgumentsErrorKey] = cause.Error()
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Sprintf(`{"%s":true}`, qwenInvalidArgumentsErrorKey)
	}
	return string(encoded)
}

func qwenJSONCandidates(body string) [][]byte {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}
	values := []string{body}
	if extracted := qwenExtractJSONCandidate(body); extracted != "" && extracted != body {
		values = append(values, extracted)
	}
	candidates := make([][]byte, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		candidates = append(candidates, []byte(value))
	}
	return candidates
}

func qwenExtractJSONCandidate(body string) string {
	first := -1
	for _, marker := range []string{"[", "{"} {
		if idx := strings.Index(body, marker); idx >= 0 && (first == -1 || idx < first) {
			first = idx
		}
	}
	if first < 0 {
		return ""
	}
	last := -1
	for _, marker := range []string{"]", "}"} {
		if idx := strings.LastIndex(body, marker); idx > last {
			last = idx
		}
	}
	if last < first {
		return ""
	}
	return strings.TrimSpace(body[first : last+1])
}

func stripQwenJSONFence(text string) string {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "```") {
		return text
	}
	lines := strings.Split(text, "\n")
	if len(lines) < 2 {
		return text
	}
	if !strings.HasPrefix(strings.TrimSpace(lines[0]), "```") {
		return text
	}
	end := len(lines) - 1
	if !strings.HasPrefix(strings.TrimSpace(lines[end]), "```") {
		return text
	}
	return strings.TrimSpace(strings.Join(lines[1:end], "\n"))
}

func firstQwenRawMessage(values ...json.RawMessage) json.RawMessage {
	for _, value := range values {
		if !qwenRawMessageEmpty(value) {
			return value
		}
	}
	return nil
}

func qwenRawMessageEmpty(raw json.RawMessage) bool {
	value := strings.TrimSpace(string(raw))
	return value == "" || value == "null"
}

func qwenRawMessageSummary(raw json.RawMessage) string {
	if qwenRawMessageEmpty(raw) {
		return "unknown error"
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return truncate(strings.TrimSpace(text), 400)
	}
	return truncate(strings.TrimSpace(string(raw)), 400)
}
