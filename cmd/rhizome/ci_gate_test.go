package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestTaskCommands_AutoApplyMigrationsOnEmptyDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rhizome-zero-state.db")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	t.Setenv("RHIZOME_DB", dbPath)
	t.Setenv("RHIZOME_WORKSPACE_ROOT", workspaceRoot)
	t.Setenv("RHIZOME_WORKSPACE_PASSWORD", "ci-test-workspace-password")

	const taskID = "ci-zero-state-task"
	const workspaceID = "ci-zero-state-workspace"

	createCLITestWorkspace(t, workspaceID)

	submitOut, err := captureStdout(t, func() error {
		return runTaskSubmit([]string{
			"--task-id", taskID,
			"--owner-user-id", "ci-user",
			"--workspace-id", workspaceID,
		})
	})
	if err != nil {
		t.Fatalf("runTaskSubmit failed on empty DB: %v", err)
	}

	var submitPayload map[string]any
	if err := json.Unmarshal([]byte(submitOut), &submitPayload); err != nil {
		t.Fatalf("decode task submit output: %v; output=%q", err, submitOut)
	}
	if got, _ := submitPayload["task_id"].(string); got != taskID {
		t.Fatalf("expected task_id %q, got %q", taskID, got)
	}
	if got, _ := submitPayload["status"].(string); got != model.TaskStatusPending {
		t.Fatalf("expected status %q, got %q", model.TaskStatusPending, got)
	}
	if got, _ := submitPayload["workspace_id"].(string); got != workspaceID {
		t.Fatalf("expected workspace_id %q, got %q", workspaceID, got)
	}
	if got, _ := submitPayload["runtime_event_id"].(string); strings.TrimSpace(got) == "" {
		t.Fatalf("expected non-empty runtime_event_id in task submit output")
	}
	requireCLITaskRuntimeEvent(t, workspaceID, taskID, "task.created", "task.submit")

	statusOut, err := captureStdout(t, func() error {
		return runTaskStatus([]string{
			"--task-id", taskID,
		})
	})
	if err != nil {
		t.Fatalf("runTaskStatus failed after submit: %v", err)
	}

	var statusPayload struct {
		TraceID string `json:"trace_id"`
		Task    struct {
			TaskID string `json:"task_id"`
			Status string `json:"status"`
		} `json:"task"`
	}
	if err := json.Unmarshal([]byte(statusOut), &statusPayload); err != nil {
		t.Fatalf("decode task status output: %v; output=%q", err, statusOut)
	}
	if statusPayload.Task.TaskID != taskID {
		t.Fatalf("expected task_id %q, got %q", taskID, statusPayload.Task.TaskID)
	}
	if statusPayload.Task.Status != model.TaskStatusPending {
		t.Fatalf("expected task status %q, got %q", model.TaskStatusPending, statusPayload.Task.Status)
	}
	if strings.TrimSpace(statusPayload.TraceID) == "" {
		t.Fatalf("expected non-empty trace_id in task status output")
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("open store after auto-migration commands: %v", err)
	}
	defer func() { _ = store.Close() }()

	status, err := store.GetTaskStatus(context.Background(), "", taskID)
	if err != nil {
		t.Fatalf("get task status from store: %v", err)
	}
	if status.Status != model.TaskStatusPending {
		t.Fatalf("expected persisted task status %q, got %q", model.TaskStatusPending, status.Status)
	}
}

func TestRunApprovalDecide_RejectsUnauthorizedActorBeforeDBOpen(t *testing.T) {
	t.Setenv("RHIZOME_OPERATOR_IDS", "alice,bob")
	t.Setenv("RHIZOME_DB", filepath.Join(t.TempDir(), "missing", "nested", "rhizome.db"))

	err := runApprovalDecide([]string{
		"--approval-id", "approval-ci-guard",
		"--decision", "approve",
		"--actor", "carol",
	})
	if err == nil {
		t.Fatalf("expected unauthorized actor error")
	}
	if !strings.Contains(err.Error(), "approval_action_forbidden") {
		t.Fatalf("expected approval_action_forbidden error, got %v", err)
	}
}

func TestRunApprovalPatchQueueEnableRejectsUnauthorizedActorBeforeDBOpen(t *testing.T) {
	t.Setenv("RHIZOME_OPERATOR_IDS", "alice,bob")
	t.Setenv("RHIZOME_PATCH_QUEUE_CLAIM_TOKEN", "claim-token-ci-guard")
	t.Setenv("RHIZOME_DB", filepath.Join(t.TempDir(), "missing", "nested", "rhizome.db"))

	err := runApprovalPatchQueueEnable([]string{
		"--workspace-id", "ws-ci-guard",
		"--project-id", "project-ci-guard",
		"--queue-id", "patchq-ci-guard",
		"--item-id", "patchitem-ci-guard",
		"--actor", "carol",
	})
	if err == nil {
		t.Fatalf("expected unauthorized operator enablement actor error")
	}
	if !strings.Contains(err.Error(), "approval_action_forbidden") {
		t.Fatalf("expected approval_action_forbidden error, got %v", err)
	}
}

func TestRunApprovalPatchQueueEnableRequiresExplicitOperatorIDsBeforeDBOpen(t *testing.T) {
	t.Setenv("RHIZOME_OPERATOR_IDS", "")
	t.Setenv("RHIZOME_PATCH_QUEUE_CLAIM_TOKEN", "claim-token-ci-guard")
	t.Setenv("RHIZOME_DB", filepath.Join(t.TempDir(), "missing", "nested", "rhizome.db"))

	err := runApprovalPatchQueueEnable([]string{
		"--workspace-id", "ws-ci-guard",
		"--project-id", "project-ci-guard",
		"--queue-id", "patchq-ci-guard",
		"--item-id", "patchitem-ci-guard",
		"--actor", "operator-1",
	})
	if err == nil {
		t.Fatalf("expected missing explicit operator ids error")
	}
	if !strings.Contains(err.Error(), "approval_action_forbidden") || !strings.Contains(err.Error(), "RHIZOME_OPERATOR_IDS is required") {
		t.Fatalf("expected explicit operator ids error before DB open, got %v", err)
	}
}

func TestRunApprovalPatchQueueEnableRejectsClaimTokenInCommandLine(t *testing.T) {
	err := runApprovalPatchQueueEnable([]string{"--claim-token", "must-not-enter-argv"})
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("runApprovalPatchQueueEnable(--claim-token) error = %v, want unsupported flag", err)
	}
}

func TestApprovalPatchQueueEnableSuccessPayloadRedactsClaimToken(t *testing.T) {
	const sentinel = "sentinel-claim-token-must-not-enter-cli-output"
	item := sqlite.ProjectPatchQueueItemRecord{
		QueueID:    "patchq-security",
		ItemID:     "patchitem-security",
		State:      sqlite.ProjectPatchQueueStateClaimed,
		ClaimToken: sentinel,
	}
	event := sqlite.RuntimeEventRecord{
		EventID:   "event-security",
		EventType: "project.patch_queue.operator_enablement_recorded",
	}

	payload := approvalPatchQueueEnableSuccessPayload(
		"trace-security",
		"ws-security",
		"project-security",
		item.QueueID,
		item.ItemID,
		"operator-security",
		item,
		event,
	)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal approval patch queue enable success payload: %v", err)
	}
	if strings.Contains(string(raw), sentinel) || strings.Contains(string(raw), `"claim_token"`) {
		t.Fatalf("approval CLI success payload disclosed patch queue claim token: %s", raw)
	}
	if item.ClaimToken != sentinel {
		t.Fatalf("redaction mutated internal fencing record: got %q", item.ClaimToken)
	}
}
