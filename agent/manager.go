package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unicode"
)

func runManager(args []string) error {
	if len(args) > 0 {
		switch strings.ToLower(strings.TrimSpace(args[0])) {
		case "-h", "--help", "help":
			printManagerUsage(os.Stdout)
			return nil
		default:
			ui := &ManagerUI{
				in:  bufio.NewReader(os.Stdin),
				out: os.Stdout,
			}
			exit, err := ui.handleFields(context.Background(), args)
			if err != nil {
				return err
			}
			if exit {
				return nil
			}
			return nil
		}
	}
	if isInteractiveTerminal(os.Stdin, os.Stdout) {
		return runManagerVisor(context.Background())
	}
	ui := &ManagerUI{
		in:  bufio.NewReader(os.Stdin),
		out: os.Stdout,
	}
	return ui.Run(context.Background())
}

func printManagerUsage(w io.Writer) {
	fmt.Fprintf(w, "usage: %s [manager]\n", appCommandName)
	fmt.Fprintf(w, "       %s daemon [runtime flags]\n", appCommandName)
	fmt.Fprintf(w, "       %s tui [runtime flags]\n", appCommandName)
	fmt.Fprintf(w, "       %s onboard [--workdir PATH]\n", appCommandName)
	fmt.Fprintf(w, "       %s web [--host LOOPBACK_HOST] [--port PORT] [--no-open]\n", appCommandName)
	fmt.Fprintf(w, "       %s list | show <agent> | start <agent> | stop <agent> | restart <agent> | status <agent> | logs <agent>\n", appCommandName)
	fmt.Fprintf(w, "       %s chat <agent> | attach <agent> | install [--dir PATH] [--force]\n", appCommandName)
	fmt.Fprintf(w, "       %s refresh-credentials [--roster-json PATH] | sync-roster --roster-json PATH [--prune]\n", appCommandName)
	fmt.Fprintf(w, "       %s defaults | set-default <field> <value> | clear-default <field>\n", appCommandName)
	fmt.Fprintln(w, "without arguments, opens the global agent manager TUI")
}

type ManagerUI struct {
	in  *bufio.Reader
	out io.Writer
}

func (ui *ManagerUI) Run(ctx context.Context) error {
	ui.printBanner()
	ui.printAgentTable()
	ui.printHelp()

	for {
		if ctx.Err() != nil {
			return nil
		}

		fmt.Fprint(ui.out, "\nmanager> ")
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

		exit, err := ui.handleCommand(ctx, trimmed)
		if err != nil {
			fmt.Fprintf(ui.out, "error> %v\n", err)
		}
		if exit {
			return nil
		}
	}
}

func (ui *ManagerUI) readLine() (string, error) {
	line, err := ui.in.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	if err == io.EOF && line == "" {
		return "", io.EOF
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (ui *ManagerUI) handleCommand(ctx context.Context, input string) (bool, error) {
	fields, err := splitManagerCommand(input)
	if err != nil {
		return false, err
	}
	return ui.handleFields(ctx, fields)
}

func (ui *ManagerUI) handleFields(ctx context.Context, fields []string) (bool, error) {
	if len(fields) == 0 {
		return false, nil
	}

	switch strings.ToLower(fields[0]) {
	case "help", "/help":
		ui.printHelp()
	case "list", "ls":
		ui.printAgentTable()
	case "defaults":
		ui.printDefaults()
	case "refresh":
		ui.printAgentTable()
	case "show":
		record, err := ui.resolveAgentRef(fields[1:])
		if err != nil {
			return false, err
		}
		ui.printAgentDetails(record)
	case "start":
		record, err := ui.resolveAgentRef(fields[1:])
		if err != nil {
			return false, err
		}
		state, err := StartManagedAgent(record)
		if err != nil {
			return false, err
		}
		fmt.Fprintf(ui.out, "system> started %s pid=%d\n", record.AgentID, state.PID)
	case "stop":
		record, err := ui.resolveAgentRef(fields[1:])
		if err != nil {
			return false, err
		}
		if err := StopManagedAgent(record); err != nil {
			return false, err
		}
		fmt.Fprintf(ui.out, "system> stopped %s\n", record.AgentID)
	case "restart":
		record, err := ui.resolveAgentRef(fields[1:])
		if err != nil {
			return false, err
		}
		state, err := RestartManagedAgent(record)
		if err != nil {
			return false, err
		}
		fmt.Fprintf(ui.out, "system> restarted %s pid=%d\n", record.AgentID, state.PID)
	case "status":
		record, err := ui.resolveAgentRef(fields[1:])
		if err != nil {
			return false, err
		}
		ui.printAgentProcessStatus(record)
	case "logs":
		lines := 40
		refArgs := fields[1:]
		if len(fields) > 2 {
			if parsed, err := strconv.Atoi(fields[len(fields)-1]); err == nil && parsed > 0 {
				lines = parsed
				refArgs = fields[1 : len(fields)-1]
			}
		}
		record, err := ui.resolveAgentRef(refArgs)
		if err != nil {
			return false, err
		}
		ui.printAgentLogs(record, lines)
	case "chat":
		record, err := ui.resolveAgentRef(fields[1:])
		if err != nil {
			return false, err
		}
		if err := ui.openAgentChat(record); err != nil {
			return false, err
		}
		ui.printBanner()
		ui.printAgentTable()
	case "attach":
		record, err := ui.resolveAgentRef(fields[1:])
		if err != nil {
			return false, err
		}
		if err := runManagedAgentAttach(ctx, record, ui.in, ui.out); err != nil {
			return false, err
		}
		ui.printBanner()
		ui.printAgentTable()
	case "remove":
		record, err := ui.resolveAgentRef(fields[1:])
		if err != nil {
			return false, err
		}
		if status := InspectManagedAgentProcess(record); status.Running {
			return false, fmt.Errorf("agent %s is still running; stop it before removing it from the registry", record.AgentID)
		}
		if err := RemoveManagedAgent(record.AgentID); err != nil {
			return false, err
		}
		fmt.Fprintf(ui.out, "system> removed %s from registry\n", record.AgentID)
	case "onboard":
		args := []string{}
		if len(fields) > 1 {
			args = append(args, "--workdir", strings.Join(fields[1:], " "))
		}
		if err := runOnboardWithIO(args, ui.in, ui.out); err != nil {
			return false, err
		}
		ui.printBanner()
		ui.printAgentTable()
	case "install":
		if err := runInstallWithWriter(fields[1:], ui.out); err != nil {
			return false, err
		}
	case "set-default":
		if len(fields) < 3 {
			return false, fmt.Errorf("usage: set-default <field> <value>")
		}
		if err := SetManagerDefault(fields[1], strings.Join(fields[2:], " ")); err != nil {
			return false, err
		}
		ui.printDefaults()
	case "clear-default":
		if len(fields) < 2 {
			return false, fmt.Errorf("usage: clear-default <field>")
		}
		if err := ClearManagerDefault(fields[1]); err != nil {
			return false, err
		}
		ui.printDefaults()
	case "quit", "exit", "/exit", "/quit":
		fmt.Fprintln(ui.out, "system> bye")
		return true, nil
	default:
		return false, fmt.Errorf("unknown command %q", fields[0])
	}

	return false, nil
}

func (ui *ManagerUI) printBanner() {
	registry := LoadBotRegistry()
	fmt.Fprintf(ui.out, "%s manager\n", appCommandName)
	fmt.Fprintf(ui.out, "defaults: workspace=%s host=%s backend=%s model=%s\n",
		registry.Defaults.WorkspaceID,
		registry.Defaults.HostURL,
		registry.Defaults.LLMBackend,
		registry.Defaults.Model,
	)
}

func (ui *ManagerUI) printHelp() {
	fmt.Fprintln(ui.out, "commands: list show <agent> start <agent> stop <agent> restart <agent> status <agent> logs <agent> chat <agent> attach <agent> remove <agent> defaults set-default <field> <value> clear-default <field> install [--dir PATH] [--force] onboard [path] refresh exit")
}

func (ui *ManagerUI) printDefaults() {
	registry := LoadBotRegistry()
	fmt.Fprintln(ui.out, "system> defaults:")
	fmt.Fprintf(ui.out, "- host_url: %s\n", registry.Defaults.HostURL)
	fmt.Fprintf(ui.out, "- workspace_id: %s\n", registry.Defaults.WorkspaceID)
	fmt.Fprintf(ui.out, "- workspace_password: %s\n", configuredSecretLabel(registry.Defaults.WorkspacePassword))
	fmt.Fprintf(ui.out, "- owner_user_id: %s\n", registry.Defaults.OwnerUserID)
	fmt.Fprintf(ui.out, "- llm_backend: %s\n", registry.Defaults.LLMBackend)
	fmt.Fprintf(ui.out, "- model: %s\n", registry.Defaults.Model)
	fmt.Fprintf(ui.out, "- coordination_mode: %s\n", firstNonEmpty(registry.Defaults.CoordinationMode, defaultCoordinationMode))
	fmt.Fprintf(ui.out, "- role: %s\n", registry.Defaults.Role)
	fmt.Fprintf(ui.out, "- protocol_version: %s\n", registry.Defaults.ProtocolVersion)
	if len(registry.Defaults.Capabilities) > 0 {
		fmt.Fprintf(ui.out, "- capabilities: %s\n", strings.Join(registry.Defaults.Capabilities, ","))
	}
}

func configuredSecretLabel(value string) string {
	if strings.TrimSpace(value) == "" {
		return "[not set]"
	}
	return "[set]"
}

func (ui *ManagerUI) printAgentTable() {
	registry := LoadBotRegistry()
	if len(registry.Agents) == 0 {
		fmt.Fprintln(ui.out, "system> no managed agents registered yet")
		return
	}
	fmt.Fprintln(ui.out, "system> agents:")
	for idx, record := range registry.Agents {
		status := ManagedAgentRuntimeStatus(record)
		fmt.Fprintf(ui.out, "%d. %s [%s] %s (%s)\n", idx+1, record.AgentID, status, record.DisplayName, record.Workdir)
	}
}

func (ui *ManagerUI) printAgentDetails(record ManagedAgentRecord) {
	record = normalizeManagedAgentRecord(record)
	local := LoadLocalRuntimeProfile(record.Workdir)
	profile := LoadAgentProfile(record.Workdir)
	process := LoadAgentProcessState(record.Workdir)
	processStatus := InspectManagedAgentProcess(record)

	fmt.Fprintf(ui.out, "system> agent %s\n", firstNonEmpty(local.effectiveAgentID(), record.AgentID))
	fmt.Fprintf(ui.out, "- display_name: %s\n", firstNonEmpty(local.effectiveDisplayName(), profile.DisplayName, record.DisplayName))
	fmt.Fprintf(ui.out, "- workdir: %s\n", record.Workdir)
	fmt.Fprintf(ui.out, "- status: %s\n", processStatus.State)
	fmt.Fprintf(ui.out, "- host_url: %s\n", firstNonEmpty(local.HostURL, record.HostURL))
	fmt.Fprintf(ui.out, "- workspace_id: %s\n", firstNonEmpty(local.effectiveWorkspaceID(), record.WorkspaceID))
	fmt.Fprintf(ui.out, "- role: %s\n", firstNonEmpty(local.effectiveRole(), profile.Role, record.Role))
	fmt.Fprintf(ui.out, "- llm_backend: %s\n", firstNonEmpty(local.LLMBackend, record.LLMBackend))
	fmt.Fprintf(ui.out, "- model: %s\n", firstNonEmpty(local.Model, record.Model))
	if process.PID > 0 {
		fmt.Fprintf(ui.out, "- pid: %d\n", process.PID)
		fmt.Fprintf(ui.out, "- log_out: %s\n", process.LogOutPath)
		fmt.Fprintf(ui.out, "- log_err: %s\n", process.LogErrPath)
	}
}

func (ui *ManagerUI) printAgentProcessStatus(record ManagedAgentRecord) {
	status := InspectManagedAgentProcess(record)
	fmt.Fprintf(ui.out, "system> agent %s status\n", record.AgentID)
	fmt.Fprintf(ui.out, "- state: %s\n", status.State)
	if status.PID > 0 {
		fmt.Fprintf(ui.out, "- pid: %d\n", status.PID)
	}
	if status.StartedAt != "" {
		fmt.Fprintf(ui.out, "- started_at: %s\n", status.StartedAt)
	}
	if status.LogOutPath != "" {
		fmt.Fprintf(ui.out, "- stdout: %s\n", status.LogOutPath)
	}
	if status.LogErrPath != "" {
		fmt.Fprintf(ui.out, "- stderr: %s\n", status.LogErrPath)
	}
}

func (ui *ManagerUI) printAgentLogs(record ManagedAgentRecord, lines int) {
	tail, err := TailManagedAgentLogs(record, lines)
	if err != nil {
		fmt.Fprintf(ui.out, "error> %v\n", err)
		return
	}
	fmt.Fprintf(ui.out, "system> logs for %s (last %d lines)\n", record.AgentID, lines)
	fmt.Fprintf(ui.out, "stdout: %s\n", tail.LogOutPath)
	for _, line := range tail.Stdout {
		fmt.Fprintln(ui.out, line)
	}
	fmt.Fprintf(ui.out, "stderr: %s\n", tail.LogErrPath)
	for _, line := range tail.Stderr {
		fmt.Fprintln(ui.out, line)
	}
}

func (ui *ManagerUI) resolveAgentRef(args []string) (ManagedAgentRecord, error) {
	if len(args) > 1 {
		switch strings.ToLower(strings.TrimSpace(args[0])) {
		case "show", "start", "stop", "restart", "status", "logs", "chat", "attach", "remove":
			args = args[1:]
		}
	}
	if len(args) == 0 {
		return ManagedAgentRecord{}, fmt.Errorf("agent reference is required")
	}
	return ResolveManagedAgentReference(strings.TrimSpace(strings.Join(args, " ")))
}

func (ui *ManagerUI) openAgentChat(record ManagedAgentRecord) error {
	cfg, err := loadInlineLocalChatConfig(record)
	if err != nil {
		return err
	}
	return runWithConfig(cfg)
}

func runListAgents() error {
	registry := LoadBotRegistry()
	if len(registry.Agents) == 0 {
		fmt.Println("no managed agents registered")
		return nil
	}
	for _, record := range registry.Agents {
		fmt.Printf("%s\t%s\t%s\t%s\n", record.AgentID, ManagedAgentRuntimeStatus(record), record.DisplayName, record.Workdir)
	}
	return nil
}

func runShowAgent(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: %s show <agent>", appCommandName)
	}
	record, err := ResolveManagedAgentReference(strings.Join(args, " "))
	if err != nil {
		return err
	}
	ui := &ManagerUI{out: os.Stdout}
	ui.printAgentDetails(record)
	return nil
}

func runStartAgent(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: %s start <agent>", appCommandName)
	}
	record, err := ResolveManagedAgentReference(strings.Join(args, " "))
	if err != nil {
		return err
	}
	state, err := StartManagedAgent(record)
	if err != nil {
		return err
	}
	fmt.Printf("started %s pid=%d\n", record.AgentID, state.PID)
	return nil
}

func runStopAgent(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: %s stop <agent>", appCommandName)
	}
	record, err := ResolveManagedAgentReference(strings.Join(args, " "))
	if err != nil {
		return err
	}
	if err := StopManagedAgent(record); err != nil {
		return err
	}
	fmt.Printf("stopped %s\n", record.AgentID)
	return nil
}

func runRestartAgent(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: %s restart <agent>", appCommandName)
	}
	record, err := ResolveManagedAgentReference(strings.Join(args, " "))
	if err != nil {
		return err
	}
	state, err := RestartManagedAgent(record)
	if err != nil {
		return err
	}
	fmt.Printf("restarted %s pid=%d\n", record.AgentID, state.PID)
	return nil
}

func runStatusAgent(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: %s status <agent>", appCommandName)
	}
	record, err := ResolveManagedAgentReference(strings.Join(args, " "))
	if err != nil {
		return err
	}
	ui := &ManagerUI{out: os.Stdout}
	ui.printAgentProcessStatus(record)
	return nil
}

func runLogsAgent(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: %s logs <agent> [lines]", appCommandName)
	}
	lines := 40
	refArgs := args
	if len(args) > 1 {
		if parsed, err := strconv.Atoi(args[1]); err == nil && parsed > 0 {
			lines = parsed
			refArgs = args[:1]
		}
	}
	record, err := ResolveManagedAgentReference(strings.Join(refArgs, " "))
	if err != nil {
		return err
	}
	ui := &ManagerUI{out: os.Stdout}
	ui.printAgentLogs(record, lines)
	return nil
}

func loadInlineLocalChatConfig(record ManagedAgentRecord) (RuntimeConfig, error) {
	if err := ensureManagedRecordAllowsInlineLocalChat(record); err != nil {
		return RuntimeConfig{}, err
	}
	return loadRuntimeConfigFromArgs([]string{"--workdir", record.Workdir, "--mode", string(RuntimeModeTUI)})
}

func runChatAgent(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: %s chat <agent>", appCommandName)
	}
	record, err := ResolveManagedAgentReference(strings.Join(args, " "))
	if err != nil {
		return err
	}
	cfg, err := loadInlineLocalChatConfig(record)
	if err != nil {
		return err
	}
	return runWithConfig(cfg)
}

func runDefaults(args []string) error {
	if len(args) == 0 {
		ui := &ManagerUI{out: os.Stdout}
		ui.printDefaults()
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(args[0])) {
	case "set":
		if len(args) < 3 {
			return fmt.Errorf("usage: %s defaults set <field> <value>", appCommandName)
		}
		return SetManagerDefault(args[1], strings.Join(args[2:], " "))
	case "clear":
		if len(args) < 2 {
			return fmt.Errorf("usage: %s defaults clear <field>", appCommandName)
		}
		return ClearManagerDefault(args[1])
	default:
		return fmt.Errorf("unknown defaults subcommand %q", args[0])
	}
}

func splitManagerCommand(input string) ([]string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, nil
	}
	var (
		fields  []string
		current strings.Builder
		quote   rune
	)
	flush := func() {
		if current.Len() == 0 {
			return
		}
		fields = append(fields, current.String())
		current.Reset()
	}
	for _, r := range input {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				current.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote = r
		case unicode.IsSpace(r):
			flush()
		default:
			current.WriteRune(r)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quoted string")
	}
	flush()
	return fields, nil
}
