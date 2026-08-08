package tools

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func installNativeTestTool(t *testing.T, executor *Executor, workspaceID, toolID string) {
	t.Helper()

	binaryPath := buildExecutorTestToolBinary(t)
	entryPoint := filepath.Base(binaryPath)
	dir := filepath.Join(executor.toolsDir, sanitize(workspaceID), sanitize(toolID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create native test tool dir: %v", err)
	}

	binaryRaw, err := os.ReadFile(binaryPath)
	if err != nil {
		t.Fatalf("read native test tool binary: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, entryPoint), binaryRaw, 0o755); err != nil {
		t.Fatalf("write native test tool binary: %v", err)
	}

	meta := map[string]string{
		"tool_id":      toolID,
		"workspace_id": workspaceID,
		"runtime":      "native",
		"entry_point":  entryPoint,
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal native test tool metadata: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tool.json"), metaJSON, 0o644); err != nil {
		t.Fatalf("write native test tool metadata: %v", err)
	}
}

func buildExecutorTestToolBinary(t *testing.T) string {
	t.Helper()

	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "main.go")
	outputPath := filepath.Join(tempDir, "tool-helper")
	if runtime.GOOS == "windows" {
		outputPath += ".exe"
	}

	source := `package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"
)

type input struct {
	PIDFile       string ` + "`json:\"pid_file\"`" + `
	SleepSec      int    ` + "`json:\"sleep_sec\"`" + `
	SpawnChild    bool   ` + "`json:\"spawn_child\"`" + `
	ChildSleepSec int    ` + "`json:\"child_sleep_sec\"`" + `
}

func main() {
	if len(os.Args) >= 3 && os.Args[1] == "--child" {
		sleepSec, _ := strconv.Atoi(os.Args[2])
		time.Sleep(time.Duration(sleepSec) * time.Second)
		return
	}

	var in input
	if err := json.NewDecoder(os.Stdin).Decode(&in); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if in.SpawnChild {
		cmd := exec.Command(os.Args[0], "--child", strconv.Itoa(in.ChildSleepSec))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		if in.PIDFile != "" {
			if err := os.WriteFile(in.PIDFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}
		}
	}

	if in.SleepSec > 0 {
		time.Sleep(time.Duration(in.SleepSec) * time.Second)
	}

	fmt.Print("done")
}`

	if err := os.WriteFile(sourcePath, []byte(source), 0o644); err != nil {
		t.Fatalf("write native test tool source: %v", err)
	}

	cmd := exec.Command("go", "build", "-o", outputPath, sourcePath)
	cmd.Dir = tempDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build native test tool: %v\n%s", err, string(output))
	}

	return outputPath
}
