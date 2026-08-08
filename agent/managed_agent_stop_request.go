package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const managedAgentStopRequestFilename = "agent.stop"

func managedAgentStopRequestPath(workdir string) string {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return ""
	}
	return filepath.Join(workdir, managedAgentStopRequestFilename)
}

func requestManagedAgentGracefulStop(workdir string) error {
	path := managedAgentStopRequestPath(workdir)
	if path == "" {
		return fmt.Errorf("managed agent workdir is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	body := []byte(time.Now().UTC().Format(time.RFC3339Nano) + "\n")
	return os.WriteFile(path, body, 0o600)
}

func clearManagedAgentGracefulStop(workdir string) {
	if path := managedAgentStopRequestPath(workdir); path != "" {
		_ = os.Remove(path)
	}
}

func managedAgentGracefulStopRequested(workdir string) bool {
	path := managedAgentStopRequestPath(workdir)
	if path == "" {
		return false
	}
	if _, err := os.Stat(path); err == nil {
		return true
	}
	return false
}

func exitIfManagedAgentStopRequested(workdir string) {
	if managedAgentGracefulStopRequested(workdir) {
		fmt.Fprintln(os.Stderr, "[managed-agent stop] graceful stop requested")
		os.Exit(0)
	}
}
