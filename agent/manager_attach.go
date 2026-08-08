package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

func runAttachAgent(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: %s attach <agent>", appCommandName)
	}
	record, err := ResolveManagedAgentReference(strings.Join(args, " "))
	if err != nil {
		return err
	}
	return runManagedAgentAttach(context.Background(), record, os.Stdin, os.Stdout)
}

func runManagedAgentAttach(ctx context.Context, record ManagedAgentRecord, input io.Reader, output io.Writer) error {
	record = normalizeManagedAgentRecord(record)
	status := InspectManagedAgentProcess(record)
	if !status.Running {
		return fmt.Errorf("agent %s is not running; start it before using live attach", record.AgentID)
	}
	control, err := managedAgentControlClientForRecord(record)
	if err != nil {
		return err
	}

	fmt.Fprintf(output, "%s attach %s\n", appCommandName, record.AgentID)
	fmt.Fprintln(output, "commands: /status /pause [reason] /resume [reason] /switch-task <task_id> [session_id] [reason] /switch-tension <tension_id> [attach|detach|lifecycle] [role] [state] [reason] /exit")
	if statusMsg, err := executeAttachLine(ctx, control, "/status"); err == nil && strings.TrimSpace(statusMsg) != "" {
		fmt.Fprintln(output, statusMsg)
	}

	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fmt.Fprint(output, "\nattach> ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			fmt.Fprintln(output)
			return nil
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		msg, exit, err := executeAttachLineDetailed(ctx, control, line)
		if err != nil {
			fmt.Fprintf(output, "error> %v\n", err)
			continue
		}
		if strings.TrimSpace(msg) != "" {
			fmt.Fprintln(output, msg)
		}
		if exit {
			return nil
		}
	}
}

func executeAttachLine(ctx context.Context, control ManagedRuntimeControlClient, line string) (string, error) {
	msg, _, err := executeAttachLineDetailed(ctx, control, line)
	return msg, err
}

func executeAttachLineDetailed(ctx context.Context, control ManagedRuntimeControlClient, line string) (string, bool, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", false, nil
	}
	if !strings.HasPrefix(line, "/") {
		record, err := control.Request(ctx, "model.ask", line, 2*time.Minute)
		if err != nil {
			return "", false, err
		}
		return formatAgentRequestResponse(record), false, nil
	}

	fields, err := splitManagerCommand(strings.TrimSpace(strings.TrimPrefix(line, "/")))
	if err != nil {
		return "", false, err
	}
	if len(fields) == 0 {
		return "", false, nil
	}

	switch strings.ToLower(fields[0]) {
	case "help":
		return "commands: /status /pause [reason] /resume [reason] /switch-task <task_id> [session_id] [reason] /switch-tension <tension_id> [attach|detach|lifecycle] [role] [state] [reason] /exit", false, nil
	case "exit", "quit":
		return "detach closed", true, nil
	case "status":
		record, err := control.Status(ctx, 30*time.Second, "interactive attach status")
		if err != nil {
			return "", false, err
		}
		return formatAgentRequestResponse(record), false, nil
	case "pause":
		record, err := control.Pause(ctx, firstNonEmpty(strings.Join(fields[1:], " "), "interactive attach pause"), 30*time.Second)
		if err != nil {
			return "", false, err
		}
		return formatAgentRequestResponse(record), false, nil
	case "resume":
		record, err := control.Resume(ctx, firstNonEmpty(strings.Join(fields[1:], " "), "interactive attach resume"), "", "", 30*time.Second)
		if err != nil {
			return "", false, err
		}
		return formatAgentRequestResponse(record), false, nil
	case "switch-task":
		if len(fields) < 2 {
			return "", false, fmt.Errorf("usage: /switch-task <task_id> [session_id] [reason]")
		}
		taskID := fields[1]
		sessionID := ""
		reasonStart := 2
		if len(fields) > 2 {
			sessionID = fields[2]
			reasonStart = 3
		}
		record, err := control.SwitchTask(ctx, taskID, sessionID, firstNonEmpty(strings.Join(fields[reasonStart:], " "), "interactive attach switch task"), 30*time.Second)
		if err != nil {
			return "", false, err
		}
		return formatAgentRequestResponse(record), false, nil
	case "switch-tension":
		if len(fields) < 2 {
			return "", false, fmt.Errorf("usage: /switch-tension <tension_id> [attach|detach|lifecycle] [role] [state] [reason]")
		}
		tensionID := fields[1]
		action := "attach"
		idx := 2
		if len(fields) > 2 {
			switch strings.ToLower(fields[2]) {
			case "attach", "focus", "detach", "release", "lifecycle", "update":
				action = fields[2]
				idx = 3
			}
		}
		role := ""
		lifecycleState := ""
		reason := ""
		switch strings.ToLower(action) {
		case "attach", "focus":
			if len(fields) > idx {
				role = fields[idx]
				idx++
			}
			if len(fields) > idx {
				lifecycleState = fields[idx]
				idx++
			}
			reason = strings.Join(fields[idx:], " ")
		case "lifecycle", "update":
			if len(fields) > idx {
				lifecycleState = fields[idx]
				idx++
			}
			reason = strings.Join(fields[idx:], " ")
		default:
			reason = strings.Join(fields[idx:], " ")
		}
		record, err := control.SwitchTension(ctx, tensionID, action, role, lifecycleState, firstNonEmpty(reason, "interactive attach switch tension"), 30*time.Second)
		if err != nil {
			return "", false, err
		}
		return formatAgentRequestResponse(record), false, nil
	default:
		return "", false, fmt.Errorf("unknown attach command %q", fields[0])
	}
}

func formatAgentRequestResponse(record AgentRequestRecord) string {
	if pretty := prettyJSONText(strings.TrimSpace(record.Response)); pretty != "" {
		return pretty
	}
	response := strings.TrimSpace(record.Response)
	if response != "" {
		return response
	}
	if status := strings.TrimSpace(record.Status); status != "" {
		return "status: " + status
	}
	return "request completed"
}

func prettyJSONText(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var payload any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return ""
	}
	formatted, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return ""
	}
	return string(formatted)
}
