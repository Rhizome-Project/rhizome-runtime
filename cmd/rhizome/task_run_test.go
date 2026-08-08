package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestTaskRunWait_EndToEndWithFakeBridge(t *testing.T) {
	_, workspaceRoot := setupFakeBridgeEnv(t)
	const workspaceID = "ws-task-run"
	createExecutionTestWorkspace(t, workspaceID)

	const taskID = "task-run-e2e"
	if err := runTaskSubmit([]string{
		"--task-id", taskID,
		"--owner-user-id", "developer",
		"--workspace-id", workspaceID,
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}
	writeExecutionNodeScript(t, workspaceRoot, taskID, "node-1")

	if err := runTaskRun([]string{
		"--task-id", taskID,
		"--wait",
		"--timeout-sec", "30",
		"--poll-ms", "50",
	}); err != nil {
		t.Fatalf("runTaskRun --wait failed: %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	status, err := store.GetTaskStatus(ctx, "", taskID)
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}

	if status.Status != model.TaskStatusResolved {
		t.Fatalf("expected task status %s, got %s", model.TaskStatusResolved, status.Status)
	}
	if len(status.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(status.Nodes))
	}
	if status.Nodes[0].Status != model.NodeStatusResolved {
		t.Fatalf("expected node status %s, got %s", model.NodeStatusResolved, status.Nodes[0].Status)
	}
}

func TestDaemonRunOnce_JSONLFormat(t *testing.T) {
	_, workspaceRoot := setupFakeBridgeEnv(t)
	const workspaceID = "ws-daemon-jsonl"
	createExecutionTestWorkspace(t, workspaceID)

	const taskID = "task-daemon-jsonl"
	if err := runTaskSubmit([]string{
		"--task-id", taskID,
		"--owner-user-id", "developer",
		"--workspace-id", workspaceID,
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}
	writeExecutionNodeScript(t, workspaceRoot, taskID, "node-1")

	out, err := captureStdout(t, func() error {
		return runDaemon([]string{
			"run",
			"--once",
			"--format", "jsonl",
			"--max-nodes", "10",
			"--node-timeout-sec", "30",
		})
	})
	if err != nil {
		t.Fatalf("runDaemon failed: %v", err)
	}

	line := strings.TrimSpace(out)
	if line == "" {
		t.Fatalf("expected daemon jsonl output, got empty string")
	}

	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("decode daemon jsonl output: %v; output=%q", err, line)
	}
	if got, _ := event["event"].(string); got != "daemon_tick" {
		t.Fatalf("expected event daemon_tick, got %q", got)
	}
	if got, _ := event["mode"].(string); got != "once" {
		t.Fatalf("expected mode once, got %q", got)
	}
	if got, _ := event["trace_id"].(string); strings.TrimSpace(got) == "" {
		t.Fatalf("expected non-empty trace_id, got %q", got)
	}
	if _, ok := event["processed_nodes"]; !ok {
		t.Fatalf("expected processed_nodes field in output")
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	status, err := store.GetTaskStatus(context.Background(), "", taskID)
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.Status != model.TaskStatusResolved {
		t.Fatalf("expected task status %s, got %s", model.TaskStatusResolved, status.Status)
	}
}

func TestTaskRunWait_JSONLFormat(t *testing.T) {
	_, workspaceRoot := setupFakeBridgeEnv(t)
	const workspaceID = "ws-task-run-jsonl"
	createExecutionTestWorkspace(t, workspaceID)

	const taskID = "task-run-jsonl"
	if err := runTaskSubmit([]string{
		"--task-id", taskID,
		"--owner-user-id", "developer",
		"--workspace-id", workspaceID,
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}
	writeExecutionNodeScript(t, workspaceRoot, taskID, "node-1")

	out, err := captureStdout(t, func() error {
		return runTaskRun([]string{
			"--task-id", taskID,
			"--wait",
			"--timeout-sec", "30",
			"--poll-ms", "50",
			"--format", "jsonl",
		})
	})
	if err != nil {
		t.Fatalf("runTaskRun --wait --format jsonl failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) == 0 {
		t.Fatalf("expected jsonl output lines, got empty string")
	}

	var final map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &final); err != nil {
		t.Fatalf("decode final jsonl line: %v; line=%q", err, lines[len(lines)-1])
	}
	if got, _ := final["event"].(string); got != "task_run_result" {
		t.Fatalf("expected final event task_run_result, got %q", got)
	}
	if got, _ := final["status"].(string); got != model.TaskStatusResolved {
		t.Fatalf("expected final status %s, got %q", model.TaskStatusResolved, got)
	}
	if got, _ := final["wait"].(bool); !got {
		t.Fatalf("expected wait=true in final event")
	}
}

func TestTaskRunWaitRejectsExecutionTaskWithoutWorkspaceAttachment(t *testing.T) {
	setupFakeBridgeEnv(t)

	const taskID = "task-run-no-workspace"
	store, err := openStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	graph := dag.NormalizeGraph(dag.DefaultGraph())
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate default graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateGeneric,
	}, graph); err != nil {
		t.Fatalf("create orphan task fixture: %v", err)
	}

	err = runTaskRun([]string{
		"--task-id", taskID,
		"--wait",
		"--timeout-sec", "5",
		"--poll-ms", "50",
	})
	if err == nil {
		t.Fatal("expected task run to reject missing execution workspace")
	}
	if !strings.Contains(err.Error(), "not attached to an execution workspace") {
		t.Fatalf("expected missing execution workspace error, got %v", err)
	}
}

func setupFakeBridgeEnv(t *testing.T) (dbPath string, workspaceRoot string) {
	t.Helper()

	dbPath = filepath.Join(t.TempDir(), "rhizome-task-run.db")
	workspaceRoot = filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	repoRoot := filepath.Clean(filepath.Join(wd, "..", ".."))
	fakeBridge := filepath.Join(repoRoot, "tests", "fixtures", "fake_executor_bridge.py")
	if _, err := os.Stat(fakeBridge); err != nil {
		t.Fatalf("fake bridge not found: %v", err)
	}

	t.Setenv("RHIZOME_DB", dbPath)
	t.Setenv("RHIZOME_WORKSPACE_ROOT", workspaceRoot)
	t.Setenv("RHIZOME_WORKSPACE_PASSWORD", "cli-test-workspace-password")
	t.Setenv("RHIZOME_EXECUTOR_PYTHON", "python")
	t.Setenv("RHIZOME_EXECUTOR_BRIDGE_SCRIPT", fakeBridge)

	return dbPath, workspaceRoot
}

func createExecutionTestWorkspace(t *testing.T, workspaceID string) {
	t.Helper()

	createCLITestWorkspace(t, workspaceID)
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}

	os.Stdout = writer
	var buf bytes.Buffer
	readDone := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(&buf, reader)
		readDone <- copyErr
	}()
	runErr := fn()
	_ = writer.Close()
	os.Stdout = oldStdout
	readErr := <-readDone
	_ = reader.Close()
	if readErr != nil {
		t.Fatalf("read captured stdout: %v", readErr)
	}

	return buf.String(), runErr
}

func writeExecutionNodeScript(t *testing.T, workspaceRoot, taskID, nodeID string) {
	t.Helper()

	dir := filepath.Join(workspaceRoot, "shared", taskID, nodeID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir shared dir: %v", err)
	}
	scriptPath := filepath.Join(dir, "_rhizome_node.py")
	if err := os.WriteFile(scriptPath, []byte("print('ok')\n"), 0o644); err != nil {
		t.Fatalf("write node script: %v", err)
	}
}
