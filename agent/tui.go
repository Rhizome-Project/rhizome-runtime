package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

const tuiJournalRedacted = "[REDACTED]"

var (
	tuiJournalSecretAssignmentPattern = regexp.MustCompile(`(?i)(["']?(?:(?:[a-z0-9_-]+[_-])?(?:api[_-]?key|secret|token|password|credential)|secret[_-][a-z0-9_-]+|token[_-][a-z0-9_-]+)["']?\s*(?::|=)\s*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s,;}\]]+)`)
	tuiJournalSecretValuePatterns     = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\b(?:sk-ant-|sk-|ghp_|ghu_|glpat-|xoxb-|xoxp-)[a-z0-9_-]{8,}`),
		regexp.MustCompile(`(?i)\bbearer\s+[a-z0-9._~+/=-]{8,}`),
	}
)

func RunTUI(ctx context.Context, cfg RuntimeConfig, llm ChatLLM, input io.Reader, output io.Writer) error {
	agent := &Agent{
		LLM:     llm,
		Workdir: cfg.Workdir,
	}
	if strings.TrimSpace(cfg.RhizomeRPC) != "" {
		agent.Client = NewRhizomeClient(cfg.RhizomeRPC, cfg.RhizomeToken)
		agent.WorkspaceID = cfg.WorkspaceID
		agent.AgentID = cfg.AgentID
	}
	agent.Init()
	if agent.Client != nil && strings.TrimSpace(cfg.WorkspaceID) != "" {
		if err := agent.RefreshRhizomeWorkspaceTools(ctx, agent.Client, cfg.WorkspaceID, cfg.AgentID); err != nil {
			fmt.Fprintf(output, "system> warning: could not load Rhizome tools: %v\n", err)
		}
	}
	backend, err := resolveLLMBackend(cfg)
	if err != nil {
		backend = firstNonEmpty(cfg.LLMBackend, llmBackendAuto)
	}

	session := agent.NewChatSession(AgentTaskContext{
		Mode:        "tui",
		WorkspaceID: cfg.WorkspaceID,
		AgentID:     cfg.AgentID,
		Summary:     fmt.Sprintf("Interactive terminal chat with the local operator. LLM backend: %s. Requested model: %s.", backend, cfg.Model),
	})

	ui := &TerminalChatUI{
		cfg:     cfg,
		agent:   agent,
		session: session,
		in:      bufio.NewReader(input),
		out:     output,
	}
	return ui.Run(ctx)
}

type TerminalChatUI struct {
	cfg     RuntimeConfig
	agent   *Agent
	session *ChatSession
	in      *bufio.Reader
	out     io.Writer
}

func (ui *TerminalChatUI) Run(ctx context.Context) error {
	ui.printBanner()

	for {
		if ctx.Err() != nil {
			return nil
		}

		fmt.Fprint(ui.out, "\nyou> ")
		line, err := ui.readLine()
		if err != nil {
			if err == io.EOF {
				fmt.Fprintln(ui.out)
				return nil
			}
			return err
		}

		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		exit, handled := ui.handleCommand(trimmed)
		if handled {
			if exit {
				return nil
			}
			continue
		}

		fmt.Fprintln(ui.out, "agent> thinking...")
		turn, err := ui.session.Send(ctx, trimmed)
		if err != nil {
			fmt.Fprintf(ui.out, "error> %v\n", err)
			continue
		}

		ui.printToolEvents(turn.ToolEvents)
		fmt.Fprintf(ui.out, "agent> %s\n", strings.TrimSpace(turn.Response))
	}
}

func (ui *TerminalChatUI) readLine() (string, error) {
	line, err := ui.in.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	if err == io.EOF && line == "" {
		return "", io.EOF
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (ui *TerminalChatUI) handleCommand(input string) (exit bool, handled bool) {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "/help":
		ui.printHelp()
		return false, true
	case "/tools":
		ui.printTools()
		return false, true
	case "/reset", "/clear":
		ui.session.Reset()
		fmt.Fprintln(ui.out, "system> chat history reset")
		return false, true
	case "/exit", "/quit":
		fmt.Fprintln(ui.out, "system> bye")
		return true, true
	default:
		return false, false
	}
}

func (ui *TerminalChatUI) printBanner() {
	backend, err := resolveLLMBackend(ui.cfg)
	if err != nil {
		backend = firstNonEmpty(ui.cfg.LLMBackend, llmBackendAuto)
	}
	fmt.Fprintf(ui.out, "%s tui\n", appCommandName)
	fmt.Fprintf(ui.out, "workdir: %s\n", ui.cfg.Workdir)
	fmt.Fprintf(ui.out, "backend: %s\n", backend)
	fmt.Fprintf(ui.out, "model: %s\n", ui.cfg.Model)
	if strings.TrimSpace(ui.cfg.WorkspaceID) != "" {
		fmt.Fprintf(ui.out, "workspace: %s\n", ui.cfg.WorkspaceID)
	}
	if strings.TrimSpace(ui.cfg.AgentID) != "" {
		fmt.Fprintf(ui.out, "agent: %s\n", ui.cfg.AgentID)
	}
	ui.printHelp()
}

func (ui *TerminalChatUI) printHelp() {
	fmt.Fprintln(ui.out, "commands: /help /tools /reset /exit")
}

func (ui *TerminalChatUI) printTools() {
	names := make([]string, 0, len(ui.agent.registry.tools))
	for name := range ui.agent.registry.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		fmt.Fprintln(ui.out, "system> no tools registered")
		return
	}
	fmt.Fprintln(ui.out, "system> tools:")
	for _, name := range names {
		fmt.Fprintf(ui.out, "- %s\n", name)
	}
}

func (ui *TerminalChatUI) printToolEvents(events []ChatToolEvent) {
	if len(events) == 0 {
		return
	}
	fmt.Fprintln(ui.out, "tools>")
	for _, event := range events {
		status := "ok"
		if event.IsError {
			status = "error"
		}
		args := truncate(strings.TrimSpace(redactTUIJournalPayload(event.Arguments)), 120)
		if args == "" {
			args = "{}"
		}
		fmt.Fprintf(ui.out, "- %s %s %s\n", event.Name, status, args)
		if output := strings.TrimSpace(redactTUIJournalPayload(event.Output)); output != "" {
			fmt.Fprintf(ui.out, "  %s\n", truncate(output, 200))
		}
	}
}

func redactTUIJournalPayload(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.UseNumber()
	var payload any
	if err := decoder.Decode(&payload); err == nil {
		var trailing any
		if err := decoder.Decode(&trailing); err == io.EOF {
			redacted, marshalErr := json.Marshal(deepRedactTUIJournalValue(payload))
			if marshalErr == nil {
				return string(redacted)
			}
		}
	}

	return redactTUIJournalText(trimmed)
}

func deepRedactTUIJournalValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		redacted := make(map[string]any, len(typed))
		for key, child := range typed {
			if isTUIJournalSecretField(key) {
				redacted[key] = tuiJournalRedacted
				continue
			}
			redacted[key] = deepRedactTUIJournalValue(child)
		}
		return redacted
	case []any:
		redacted := make([]any, len(typed))
		for i, child := range typed {
			redacted[i] = deepRedactTUIJournalValue(child)
		}
		return redacted
	case string:
		return redactTUIJournalText(typed)
	default:
		return value
	}
}

func isTUIJournalSecretField(field string) bool {
	normalized := strings.ToLower(strings.TrimSpace(field))
	normalized = strings.NewReplacer("-", "_", " ", "_", ".", "_").Replace(normalized)
	normalized = strings.Trim(normalized, "_")
	if normalized == "" {
		return false
	}

	switch normalized {
	case "claim_token", "lease_token",
		"api_key", "apikey", "secret", "token", "password", "passwd",
		"credential", "credentials", "authorization", "auth_token", "bearer_token",
		"access_token", "refresh_token", "id_token", "client_secret", "private_key", "access_key":
		return true
	}

	for _, suffix := range []string{"_api_key", "_secret", "_token", "_password", "_credential"} {
		if strings.HasSuffix(normalized, suffix) {
			return true
		}
	}
	return strings.HasPrefix(normalized, "secret_") || strings.HasPrefix(normalized, "token_")
}

func redactTUIJournalText(value string) string {
	redacted := tuiJournalSecretAssignmentPattern.ReplaceAllString(value, `${1}"`+tuiJournalRedacted+`"`)
	for _, pattern := range tuiJournalSecretValuePatterns {
		redacted = pattern.ReplaceAllString(redacted, tuiJournalRedacted)
	}
	return redacted
}
