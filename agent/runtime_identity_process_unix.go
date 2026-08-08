//go:build !windows

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
	out, err := exec.CommandContext(ctx, "ps", "-p", fmt.Sprintf("%d", pid), "-o", "command=").Output()
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
