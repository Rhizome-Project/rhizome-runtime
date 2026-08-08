//go:build windows

package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

func processCommandLine(pid int) (string, error) {
	if pid <= 0 {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	script := fmt.Sprintf("(Get-CimInstance Win32_Process -Filter \"ProcessId=%d\").CommandLine", pid)
	out, err := exec.CommandContext(ctx, "powershell", "-NoProfile", "-NonInteractive", "-Command", script).Output()
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
