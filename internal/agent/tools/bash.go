package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const maxOutputBytes = 1 << 20 // 1MB
const bashUnavailableMessage = "bash is not available on this host"
const bashOutputTruncationMessage = "\n... (output truncated)"

type bashTool struct {
	cfg BuiltinConfig
}

func (t *bashTool) Name() string        { return "bash" }
func (t *bashTool) Description() string { return "Execute a shell command via bash" }

func (t *bashTool) Schema() Schema {
	return Schema{
		Type: "object",
		Properties: map[string]Property{
			"command": {Type: "string", Description: "The shell command to execute"},
			"timeout": {Type: "integer", Description: "Timeout in seconds (default 120, max 600)"},
		},
		Required: []string{"command"},
	}
}

func (t *bashTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Command string `json:"command"`
		Timeout int    `json:"timeout"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("parse input: %w", err)
	}

	if params.Command == "" {
		return "Command is required", nil
	}
	if err := t.cfg.EnsureShellCommandReadOnly(t.Name(), params.Command); err != nil {
		return fmt.Sprintf("Permission Denied: %v", err), nil
	}

	timeout := params.Timeout
	if timeout <= 0 {
		timeout = 120
	}
	if timeout > 600 {
		timeout = 600
	}

	execCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	shellPath, err := exec.LookPath("bash")
	var cmd *exec.Cmd
	var fallbackWarning string
	if err != nil {
		if runtime.GOOS == "windows" {
			shellPath, err = exec.LookPath("powershell")
			if err != nil {
				return bashUnavailableMessage, nil
			}
			cmd = exec.CommandContext(execCtx, shellPath, "-Command", params.Command)
		} else {
			shellPath, err = exec.LookPath("sh")
			if err != nil {
				return bashUnavailableMessage, nil
			}
			cmd = exec.CommandContext(execCtx, shellPath, "-c", params.Command)
		}
		fallbackWarning = fmt.Sprintf("[Substrate Warning: bash absent, executing via graceful fallback shell: %s]\n\n", filepath.Base(shellPath))
	} else {
		cmd = exec.CommandContext(execCtx, shellPath, "-c", params.Command)
	}

	if t.cfg.WorkspaceDir != "" {
		cmd.Dir = t.cfg.WorkspaceDir
	} else {
		cmd.Dir, _ = os.Getwd()
	}

	stdout := newBoundedCapture(maxOutputBytes / 2)
	stderr := newBoundedCapture(maxOutputBytes / 2)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err = cmd.Run()

	output := normalizeBashOutput(stdout.String() + stderr.String())
	if len(output) > maxOutputBytes {
		output = output[:maxOutputBytes]
	}
	if stdout.Truncated() || stderr.Truncated() || len(output) == maxOutputBytes {
		output += bashOutputTruncationMessage
	}

	if err != nil {
		if execCtx.Err() == context.DeadlineExceeded {
			return fmt.Sprintf("%sCommand timed out after %ds\n%s", fallbackWarning, timeout, output), nil
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if bashExecutionUnavailable(output) {
				return fallbackWarning + bashUnavailableMessage + "\n" + output, nil
			}
			return fmt.Sprintf("%sExit code: %d\n%s", fallbackWarning, exitErr.ExitCode(), output), nil
		}
		if bashExecutionUnavailable(err.Error()) {
			return fallbackWarning + bashUnavailableMessage + "\n" + normalizeBashOutput(err.Error()), nil
		}
		return "", fmt.Errorf("exec: %w", err)
	}

	return fallbackWarning + output, nil
}

func normalizeBashOutput(output string) string {
	output = strings.ReplaceAll(output, "\x00", "")
	return strings.TrimSpace(output)
}

func bashExecutionUnavailable(output string) bool {
	normalized := strings.ToLower(normalizeBashOutput(output))
	if normalized == "" {
		return false
	}
	return strings.Contains(normalized, "bash/service/createinstance/e_accessdenied") ||
		strings.Contains(normalized, "access is denied") ||
		strings.Contains(normalized, "no such file or directory") ||
		strings.Contains(normalized, "executable file not found")
}
