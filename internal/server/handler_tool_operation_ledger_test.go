package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestMain(m *testing.M) {
	if mode := os.Getenv("RHIZOME_SERVER_MCP_STDIO_HELPER"); mode != "" {
		runServerMCPStdioTestHelper(mode)
		return
	}
	os.Exit(m.Run())
}

func TestToolDeployRecordsDurableOperationLedgerLifecycle(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := uniqueOperationLedgerTestID("ws-tool-deploy-ledger")
	toolID := uniqueOperationLedgerTestID("tool-deploy-ledger-ok")
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool deploy ledger",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(toolDeployParams{
		ToolID:      toolID,
		WorkspaceID: workspaceID,
		Runtime:     "node",
		SourceCode:  `console.log("deploy-ledger-ok")`,
		EntryPoint:  "main.js",
		DeployedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal deploy params: %v", err)
	}
	result, rpcErr := h.toolDeploy(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("tool deploy failed: %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected deploy result type %T", result)
	}
	operationID, ok := resultMap["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("missing operation_id in deploy result %+v", resultMap)
	}
	if got := strings.TrimSpace(resultMap["status"].(string)); got != "DEPLOYED" {
		t.Fatalf("deploy status = %q", got)
	}

	run := latestToolDeployOperationRun(t, ctx, store, workspaceID)
	if run.RunID != operationID {
		t.Fatalf("run id = %q, want response operation_id %q", run.RunID, operationID)
	}
	if run.Status != "COMPLETED" || run.Outcome != "COMPLETED" {
		t.Fatalf("unexpected deploy run terminal state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	assertOperationLedgerPromptContext(t, run, "tool.deploy", workspaceID, "human", "developer")
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "operation_kind"); got != "tool_deploy" {
		t.Fatalf("operation_kind = %q", got)
	}
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	if got := stringLedgerField(t, details, "execution_kind"); got != "tool_deploy" {
		t.Fatalf("execution_kind = %q", got)
	}
	if got := stringLedgerField(t, details, "mutation_kind"); got != "tool_filesystem_deploy" {
		t.Fatalf("mutation_kind = %q", got)
	}
	if got := stringLedgerField(t, details, "source_redaction"); got != "source_code_omitted" {
		t.Fatalf("source_redaction = %q", got)
	}
	if got := stringLedgerField(t, details, "source_sha256"); got != toolSourceSHA256(`console.log("deploy-ledger-ok")`) {
		t.Fatalf("source_sha256 = %q", got)
	}
	if previous, ok := details["previous_deployment"].(bool); !ok || previous {
		t.Fatalf("expected previous_deployment=false in request details, got %+v", details["previous_deployment"])
	}
	if _, hasSource := details["source_code"]; hasSource {
		t.Fatalf("deploy ledger leaked source_code: %+v", details)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "operation_id"); got != operationID {
		t.Fatalf("ledger result operation_id = %q, want %q", got, operationID)
	}
	if got := stringLedgerField(t, resultLedger, "status"); got != "DEPLOYED" {
		t.Fatalf("ledger result status = %q", got)
	}
	if previous, ok := resultLedger["previous_deployment"].(bool); !ok || previous {
		t.Fatalf("expected result previous_deployment=false, got %+v", resultLedger["previous_deployment"])
	}
}

func TestToolDeployFailureRecordsTerminalOperationLedger(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := uniqueOperationLedgerTestID("ws-tool-deploy-ledger-failure")
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool deploy ledger failure",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(toolDeployParams{
		ToolID:      "tool-deploy-ledger-empty-source",
		WorkspaceID: workspaceID,
		Runtime:     "node",
		SourceCode:  "   ",
		EntryPoint:  "main.js",
		DeployedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal deploy params: %v", err)
	}
	if result, rpcErr := h.toolDeploy(ctx, raw); rpcErr == nil {
		t.Fatalf("expected deploy failure, got result=%+v", result)
	}

	run := latestToolDeployOperationRun(t, ctx, store, workspaceID)
	if run.Status != "FAILED" || run.Outcome != "FAILED" {
		t.Fatalf("unexpected deploy failure run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "failed" {
		t.Fatalf("ledger status = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "operation_id"); got != run.RunID {
		t.Fatalf("failure result operation_id = %q, want %q", got, run.RunID)
	}
	if isError, ok := resultLedger["is_error"].(bool); !ok || !isError {
		t.Fatalf("expected is_error=true, got %+v", resultLedger["is_error"])
	}
	if got := stringLedgerField(t, resultLedger, "summary"); got != "source_code is required" {
		t.Fatalf("failure summary = %q", got)
	}
}

func TestToolRedeployRecordsPreviousDeploymentTrue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := uniqueOperationLedgerTestID("ws-tool-redeploy-ledger")
	toolID := uniqueOperationLedgerTestID("tool-redeploy-ledger")
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool redeploy ledger",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	deployToolForOperationLedgerTest(t, h, ctx, workspaceID, toolID, `console.log("first")`)

	raw, err := json.Marshal(toolDeployParams{
		ToolID:      toolID,
		WorkspaceID: workspaceID,
		Runtime:     "node",
		SourceCode:  `console.log("second")`,
		EntryPoint:  "main.js",
		DeployedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal redeploy params: %v", err)
	}
	result, rpcErr := h.toolDeploy(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("tool redeploy failed: %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected redeploy result type %T", result)
	}
	operationID, ok := resultMap["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("missing operation_id in redeploy result %+v", resultMap)
	}
	if previous, ok := resultMap["previous_deployment"].(bool); !ok || !previous {
		t.Fatalf("expected redeploy previous_deployment=true, got %+v", resultMap["previous_deployment"])
	}

	run := latestToolDeployOperationRun(t, ctx, store, workspaceID)
	if run.RunID != operationID {
		t.Fatalf("run id = %q, want response operation_id %q", run.RunID, operationID)
	}
	ledger := operationLedgerFromRun(t, run)
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	if previous, ok := details["previous_deployment"].(bool); !ok || !previous {
		t.Fatalf("expected ledger previous_deployment=true, got %+v", details["previous_deployment"])
	}
	if _, hasSource := details["source_code"]; hasSource {
		t.Fatalf("redeploy ledger leaked source_code: %+v", details)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if previous, ok := resultLedger["previous_deployment"].(bool); !ok || !previous {
		t.Fatalf("expected result previous_deployment=true, got %+v", resultLedger["previous_deployment"])
	}
}

func TestToolUndeployRecordsDurableOperationLedgerLifecycle(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := uniqueOperationLedgerTestID("ws-tool-undeploy-ledger")
	toolID := uniqueOperationLedgerTestID("tool-undeploy-ledger-ok")
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool undeploy ledger",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	deployToolForOperationLedgerTest(t, h, ctx, workspaceID, toolID, `console.log("ready")`)

	raw, err := json.Marshal(toolUndeployParams{
		ToolID:      toolID,
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("marshal undeploy params: %v", err)
	}
	result, rpcErr := h.toolUndeploy(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("tool undeploy failed: %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected undeploy result type %T", result)
	}
	operationID, ok := resultMap["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("missing operation_id in undeploy result %+v", resultMap)
	}

	run := latestToolUndeployOperationRun(t, ctx, store, workspaceID)
	if run.RunID != operationID {
		t.Fatalf("run id = %q, want response operation_id %q", run.RunID, operationID)
	}
	if run.Status != "COMPLETED" || run.Outcome != "COMPLETED" {
		t.Fatalf("unexpected undeploy run terminal state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	assertOperationLedgerPromptContext(t, run, "tool.undeploy", workspaceID, "human", "developer")
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "operation_kind"); got != "tool_undeploy" {
		t.Fatalf("operation_kind = %q", got)
	}
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	if got := stringLedgerField(t, details, "execution_kind"); got != "tool_undeploy" {
		t.Fatalf("execution_kind = %q", got)
	}
	if got := stringLedgerField(t, details, "mutation_kind"); got != "tool_filesystem_undeploy" {
		t.Fatalf("mutation_kind = %q", got)
	}
	if previous, ok := details["previous_deployment"].(bool); !ok || !previous {
		t.Fatalf("expected undeploy previous_deployment=true, got %+v", details["previous_deployment"])
	}
	fence := nestedLedgerMap(t, ledger, "fence")
	if allowed, ok := fence["canonical_mutation_allowed"].(bool); !ok || allowed {
		t.Fatalf("expected lifecycle canonical_mutation_allowed=false, got %+v", fence["canonical_mutation_allowed"])
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "status"); got != "UNDEPLOYED" {
		t.Fatalf("ledger result status = %q", got)
	}
	if removed, ok := resultLedger["removed"].(bool); !ok || !removed {
		t.Fatalf("expected result removed=true, got %+v", resultLedger["removed"])
	}
}

func TestToolUndeployMissingToolRecordsRemovedFalse(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := uniqueOperationLedgerTestID("ws-tool-undeploy-missing-ledger")
	toolID := uniqueOperationLedgerTestID("tool-undeploy-missing")
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool undeploy missing ledger",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(toolUndeployParams{
		ToolID:      toolID,
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("marshal undeploy params: %v", err)
	}
	result, rpcErr := h.toolUndeploy(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("missing tool undeploy should be idempotent, rpcErr=%+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected undeploy result type %T", result)
	}
	operationID, ok := resultMap["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("missing operation_id in undeploy result %+v", resultMap)
	}
	if previous, ok := resultMap["previous_deployment"].(bool); !ok || previous {
		t.Fatalf("expected previous_deployment=false, got %+v", resultMap["previous_deployment"])
	}
	if removed, ok := resultMap["removed"].(bool); !ok || removed {
		t.Fatalf("expected removed=false, got %+v", resultMap["removed"])
	}

	run := latestToolUndeployOperationRun(t, ctx, store, workspaceID)
	if run.RunID != operationID {
		t.Fatalf("run id = %q, want response operation_id %q", run.RunID, operationID)
	}
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "completed" {
		t.Fatalf("ledger status = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if removed, ok := resultLedger["removed"].(bool); !ok || removed {
		t.Fatalf("expected ledger removed=false, got %+v", resultLedger["removed"])
	}
}

func TestToolUndeployBlankToolIDRecordsFailureWithoutRemovingDefaultTool(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := uniqueOperationLedgerTestID("ws-tool-undeploy-blank-ledger")
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool undeploy blank ledger",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	deployToolForOperationLedgerTest(t, h, ctx, workspaceID, "default", `console.log("must remain")`)

	raw, err := json.Marshal(toolUndeployParams{
		ToolID:      "   ",
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("marshal undeploy params: %v", err)
	}
	if result, rpcErr := h.toolUndeploy(ctx, raw); rpcErr == nil {
		t.Fatalf("expected blank tool_id undeploy failure, got result=%+v", result)
	}
	stillDeployed, err := h.toolExec.IsDeployed(workspaceID, "default")
	if err != nil {
		t.Fatalf("check default tool deployment: %v", err)
	}
	if !stillDeployed {
		t.Fatalf("blank tool_id undeploy removed sanitized default tool")
	}

	run := latestToolUndeployOperationRun(t, ctx, store, workspaceID)
	if run.Status != "FAILED" || run.Outcome != "FAILED" {
		t.Fatalf("unexpected blank undeploy run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromRun(t, run)
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "operation_id"); got != run.RunID {
		t.Fatalf("failure result operation_id = %q, want %q", got, run.RunID)
	}
	if got := stringLedgerField(t, resultLedger, "summary"); got != "tool_id is required" {
		t.Fatalf("blank undeploy summary = %q", got)
	}
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	if got := stringLedgerField(t, details, "previous_deployment_check_error"); got != "tool_id is required" {
		t.Fatalf("previous deployment check error = %q", got)
	}
}

func TestToolLifecycleLedgerDegradesWithoutWorkspaceAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := uniqueOperationLedgerTestID("ws-tool-lifecycle-ledger-degraded")
	toolID := uniqueOperationLedgerTestID("tool-lifecycle-degraded")
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool lifecycle degraded ledger",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	deployRaw, err := json.Marshal(toolDeployParams{
		ToolID:      toolID,
		WorkspaceID: workspaceID,
		Runtime:     "node",
		SourceCode:  `console.log("degraded ledger still deploys")`,
		EntryPoint:  "main.js",
		DeployedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal deploy params: %v", err)
	}
	var deployOperationID string
	if result, rpcErr := h.toolDeploy(ctx, deployRaw); rpcErr != nil {
		t.Fatalf("tool.deploy should not be gated by operation ledger authority, rpcErr=%+v", rpcErr)
	} else if resultMap, ok := result.(map[string]any); !ok {
		t.Fatalf("unexpected deploy result type %T", result)
	} else {
		deployOperationID, _ = resultMap["operation_id"].(string)
		if strings.TrimSpace(deployOperationID) == "" {
			t.Fatalf("unexpected deploy result %+v", result)
		}
		if degraded, ok := resultMap["operation_ledger_degraded"].(bool); !ok || !degraded {
			t.Fatalf("expected deploy operation_ledger_degraded=true, got %+v in %+v", resultMap["operation_ledger_degraded"], resultMap)
		}
	}

	undeployRaw, err := json.Marshal(toolUndeployParams{
		ToolID:      toolID,
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("marshal undeploy params: %v", err)
	}
	var undeployOperationID string
	if result, rpcErr := h.toolUndeploy(ctx, undeployRaw); rpcErr != nil {
		t.Fatalf("tool.undeploy should not be gated by operation ledger authority, rpcErr=%+v", rpcErr)
	} else if resultMap, ok := result.(map[string]any); !ok {
		t.Fatalf("unexpected undeploy result type %T", result)
	} else {
		undeployOperationID, _ = resultMap["operation_id"].(string)
		if strings.TrimSpace(undeployOperationID) == "" {
			t.Fatalf("unexpected undeploy result %+v", result)
		}
		if degraded, ok := resultMap["operation_ledger_degraded"].(bool); !ok || !degraded {
			t.Fatalf("expected undeploy operation_ledger_degraded=true, got %+v in %+v", resultMap["operation_ledger_degraded"], resultMap)
		}
	}
	runs, err := store.ListExecutionRuns(ctx, sqlite.ExecutionRunFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("list execution runs: %v", err)
	}
	for _, run := range runs {
		if run.RunID == deployOperationID || run.RunID == undeployOperationID {
			t.Fatalf("missing-authority degraded operation id unexpectedly resolved to durable run: %+v", run)
		}
	}
}

func TestToolCallRecordsDurableOperationLedgerLifecycle(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-tool-operation-ledger"
	ctx := testAuthContext(workspaceID, "agent", "agent-ledger")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool operation ledger",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerToolLedgerAgent(t, ctx, store, workspaceID, "agent-ledger")
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-ledger", Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      "task-ledger",
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      "task-ledger",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      "task-ledger",
		AgentID:     "agent-ledger",
		Summary:     "claim before tool ledger session",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "session-ledger",
		AgentID:     "agent-ledger",
		WorkspaceID: workspaceID,
		TaskID:      "task-ledger",
		StartedAt:   "2026-04-24T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if _, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		RunID:                 "run-ledger",
		WorkspaceID:           workspaceID,
		TaskID:                "task-ledger",
		SessionID:             "session-ledger",
		AgentID:               "agent-ledger",
		Title:                 "Parent ledger run",
		Status:                "ACTIVE",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", workspaceID, "agent", "agent-ledger"),
	}); err != nil {
		t.Fatalf("create parent execution run: %v", err)
	}

	deployToolForOperationLedgerTest(t, h, ctx, workspaceID, "tool-ledger-ok", `
let body = "";
process.stdin.on("data", chunk => body += chunk);
process.stdin.on("end", () => {
  const args = JSON.parse(body || "{}");
  console.log("ok:" + String(args.value || ""));
});
`)

	raw, err := json.Marshal(toolCallParams{
		ToolID:      "tool-ledger-ok",
		WorkspaceID: workspaceID,
		Arguments:   map[string]any{"value": "ready"},
		ActorType:   "agent",
		ActorID:     "agent-ledger",
		TaskID:      "task-ledger",
		SessionID:   "session-ledger",
		RunID:       "run-ledger",
	})
	if err != nil {
		t.Fatalf("marshal tool call: %v", err)
	}
	result, rpcErr := h.toolCall(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("tool call failed: %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected tool call result type %T", result)
	}
	if stdout := strings.TrimSpace(resultMap["stdout"].(string)); stdout != "ok:ready" {
		t.Fatalf("unexpected stdout %q in result %+v", stdout, resultMap)
	}

	run := latestToolOperationRun(t, ctx, store, workspaceID)
	if run.Status != "COMPLETED" || run.Outcome != "COMPLETED" {
		t.Fatalf("unexpected execution run terminal state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	if run.TaskID != "task-ledger" || run.SessionID != "session-ledger" {
		t.Fatalf("tool execution run lost task/session binding: task=%q session=%q", run.TaskID, run.SessionID)
	}
	assertOperationLedgerPromptContext(t, run, "tool.call", workspaceID, "agent", "agent-ledger")
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "schema"); got != "operation_ledger.v1" {
		t.Fatalf("ledger schema = %q", got)
	}
	if got := stringLedgerField(t, ledger, "operation_kind"); got != "tool_call" {
		t.Fatalf("operation_kind = %q", got)
	}
	if got := stringLedgerField(t, ledger, "status"); got != "completed" {
		t.Fatalf("ledger status = %q", got)
	}
	if terminal, ok := ledger["terminal"].(bool); !ok || !terminal {
		t.Fatalf("expected terminal ledger, got %+v", ledger["terminal"])
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "tool_id"); got != "tool-ledger-ok" {
		t.Fatalf("ledger result tool_id = %q", got)
	}
	binding := nestedLedgerMap(t, ledger, "binding")
	if got := stringLedgerField(t, binding, "task_id"); got != "task-ledger" {
		t.Fatalf("ledger binding task_id = %q", got)
	}
	if got := stringLedgerField(t, binding, "session_id"); got != "session-ledger" {
		t.Fatalf("ledger binding session_id = %q", got)
	}
	if got := stringLedgerField(t, binding, "parent_run_id"); got != "run-ledger" {
		t.Fatalf("ledger binding parent_run_id = %q", got)
	}
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	if got := stringLedgerField(t, details, "execution_kind"); got != "tool_call" {
		t.Fatalf("execution_kind = %q", got)
	}
	if got := stringLedgerField(t, details, "mutation_kind"); got != "none" {
		t.Fatalf("mutation_kind = %q", got)
	}
	if got := stringLedgerField(t, details, "task_id"); got != "task-ledger" {
		t.Fatalf("request details task_id = %q", got)
	}
	if got := stringLedgerField(t, details, "session_id"); got != "session-ledger" {
		t.Fatalf("request details session_id = %q", got)
	}
	if got := stringLedgerField(t, details, "parent_run_id"); got != "run-ledger" {
		t.Fatalf("request details parent_run_id = %q", got)
	}
	fence := nestedLedgerMap(t, ledger, "fence")
	if allowed, ok := fence["canonical_mutation_allowed"].(bool); !ok || allowed {
		t.Fatalf("expected tool.call canonical_mutation_allowed=false, got %+v", fence["canonical_mutation_allowed"])
	}
	runtimeEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    "tool-ledger-ok",
		Limit:       10,
	})
	if runtimeEvent.AgentID != "agent-ledger" || runtimeEvent.SessionID != "session-ledger" || runtimeEvent.TaskID != "task-ledger" {
		t.Fatalf("tool.call runtime event lost row-level execution binding: agent=%q session=%q task=%q", runtimeEvent.AgentID, runtimeEvent.SessionID, runtimeEvent.TaskID)
	}
}

func TestToolCallRecordsTimedOutOperationLedger(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-tool-operation-timeout"
	ctx := testAuthContext(workspaceID, "agent", "agent-timeout")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool operation timeout",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerToolLedgerAgent(t, ctx, store, workspaceID, "agent-timeout")

	deployToolForOperationLedgerTest(t, h, ctx, workspaceID, "tool-ledger-timeout", `
process.stdout.write("partial-before-timeout\n");
setTimeout(() => {}, 2000);
`)

	raw, err := json.Marshal(toolCallParams{
		ToolID:      "tool-ledger-timeout",
		WorkspaceID: workspaceID,
		Arguments:   map[string]any{},
		TimeoutSec:  1,
		ActorType:   "agent",
		ActorID:     "agent-timeout",
	})
	if err != nil {
		t.Fatalf("marshal tool call: %v", err)
	}
	result, rpcErr := h.toolCall(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("tool call failed: %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected tool call result type %T", result)
	}
	if timedOut, ok := resultMap["timed_out"].(bool); !ok || !timedOut {
		t.Fatalf("expected timed_out result, got %+v in result %+v", resultMap["timed_out"], resultMap)
	}
	operationID, ok := resultMap["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("timeout result missing operation_id: %+v", resultMap)
	}
	if stdout := strings.TrimSpace(resultMap["stdout"].(string)); stdout != "partial-before-timeout" {
		t.Fatalf("expected partial stdout to be captured before timeout, got %q", stdout)
	}

	run := latestToolOperationRun(t, ctx, store, workspaceID)
	if run.Status != "TIMED_OUT" || run.Outcome != "TIMED_OUT" {
		t.Fatalf("unexpected timeout run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	if run.Summary != "tool call timed out" {
		t.Fatalf("timeout run summary = %q, want tool call timed out", run.Summary)
	}
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "timed_out" {
		t.Fatalf("ledger status = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if timedOut, ok := resultLedger["timed_out"].(bool); !ok || !timedOut {
		t.Fatalf("expected ledger timed_out result, got %+v", resultLedger["timed_out"])
	}
	runtimeEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    "tool-ledger-timeout",
		Limit:       10,
	})
	assertServerRuntimeEventAuthorityMetadata(t, runtimeEvent, authority)
	assertToolCallRuntimePromptContext(t, runtimeEvent, "tool.call.executed", workspaceID, "agent", "agent-timeout", "agent", "agent-timeout", "tool-ledger-timeout", "tool.call", operationID)
}

func TestMCPRoutedToolTimeoutRecordsTimedOutOperationLedger(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-mcp-operation-timeout"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP operation timeout",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newSlowToolCallMCPServer(t)
	defer mcpServer.Close()

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "slow-mcp",
		WorkspaceID:  workspaceID,
		DisplayName:  "Slow MCP",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}
	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "slow-mcp"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	if _, rpcErr := h.mcpToolDiscover(ctx, discoverRaw); rpcErr != nil {
		t.Fatalf("mcpToolDiscover rpc error: %+v", rpcErr)
	}

	callRaw, err := json.Marshal(toolCallParams{
		ToolID:      mcpWorkspaceToolID("slow-mcp", "slow_tool"),
		WorkspaceID: workspaceID,
		Arguments:   map[string]any{"query": "hang"},
		TimeoutSec:  1,
	})
	if err != nil {
		t.Fatalf("marshal tool call params: %v", err)
	}
	if result, rpcErr := h.toolCall(ctx, callRaw); rpcErr == nil {
		t.Fatalf("expected routed MCP timeout error, got result=%+v", result)
	}

	run := latestToolOperationRun(t, ctx, store, workspaceID)
	if run.Status != "TIMED_OUT" || run.Outcome != "TIMED_OUT" {
		t.Fatalf("unexpected MCP timeout run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	if run.Summary != "tool call timed out" {
		t.Fatalf("timeout run summary = %q, want tool call timed out", run.Summary)
	}
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "timed_out" {
		t.Fatalf("ledger status = %q", got)
	}
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	if got := stringLedgerField(t, details, "mcp_transport"); got != "streamable-http" {
		t.Fatalf("mcp_transport = %q, want streamable-http", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "mcp_transport"); got != "streamable-http" {
		t.Fatalf("result mcp_transport = %q, want streamable-http", got)
	}
	if timedOut, ok := resultLedger["timed_out"].(bool); !ok || !timedOut {
		t.Fatalf("expected MCP ledger timed_out result, got %+v", resultLedger["timed_out"])
	}
	if exitCode, ok := resultLedger["exit_code"].(float64); !ok || int(exitCode) != -1 {
		t.Fatalf("expected timeout exit_code -1 after JSON roundtrip, got %T %+v", resultLedger["exit_code"], resultLedger["exit_code"])
	}
}

func TestMCPToolCallTimeoutRecordsTerminalLedgerAfterCallerDeadline(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-mcp-direct-timeout-ledger"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP direct timeout ledger",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newSlowToolCallMCPServer(t)
	defer mcpServer.Close()

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "slow-direct-mcp",
		WorkspaceID:  workspaceID,
		DisplayName:  "Slow Direct MCP",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}
	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "slow-direct-mcp"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	if _, rpcErr := h.mcpToolDiscover(ctx, discoverRaw); rpcErr != nil {
		t.Fatalf("mcpToolDiscover rpc error: %+v", rpcErr)
	}

	callRaw, err := json.Marshal(mcpToolCallParams{
		ServerID:  "slow-direct-mcp",
		ToolName:  "slow_tool",
		Arguments: map[string]any{"query": "hang"},
	})
	if err != nil {
		t.Fatalf("marshal mcp tool call params: %v", err)
	}
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	var callErr *RPCError
	if result, rpcErr := h.mcpToolCall(shortCtx, callRaw); rpcErr == nil {
		t.Fatalf("expected direct MCP timeout error, got result=%+v", result)
	} else {
		callErr = rpcErr
	}

	run := latestToolOperationRun(t, ctx, store, workspaceID)
	if run.Status != "TIMED_OUT" || run.Outcome != "TIMED_OUT" {
		t.Fatalf("unexpected direct MCP timeout run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "timed_out" {
		t.Fatalf("ledger status = %q", got)
	}
	operationID := stringLedgerField(t, ledger, "operation_id")
	errorDetails, ok := callErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("direct MCP timeout error details type = %T, want map", callErr.Details)
	}
	if got, _ := errorDetails["operation_id"].(string); got != operationID {
		t.Fatalf("direct MCP timeout error operation_id = %q, want %q", got, operationID)
	}
	if timedOut, ok := errorDetails["timed_out"].(bool); !ok || !timedOut {
		t.Fatalf("direct MCP timeout error timed_out = %+v, want true", errorDetails["timed_out"])
	}
	if got, _ := errorDetails["mcp_transport"].(string); got != "streamable-http" {
		t.Fatalf("direct MCP timeout error mcp_transport = %q, want streamable-http", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "operation_id"); got != operationID {
		t.Fatalf("result operation_id = %q, want %q", got, operationID)
	}
	if got := stringLedgerField(t, resultLedger, "mcp_transport"); got != "streamable-http" {
		t.Fatalf("result mcp_transport = %q, want streamable-http", got)
	}
	if timedOut, ok := resultLedger["timed_out"].(bool); !ok || !timedOut {
		t.Fatalf("expected direct MCP timed_out result, got %+v", resultLedger["timed_out"])
	}
	if exitCode, ok := resultLedger["exit_code"].(float64); !ok || int(exitCode) != -1 {
		t.Fatalf("expected timeout exit_code -1 after JSON roundtrip, got %T %+v", resultLedger["exit_code"], resultLedger["exit_code"])
	}
}

func TestMCPToolDiscoverTimeoutRecordsTimedOutOperationLedger(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-mcp-discover-timeout"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP discover timeout",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newSlowDiscoverMCPServer(t)
	defer mcpServer.Close()

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "slow-discover-mcp",
		WorkspaceID:  workspaceID,
		DisplayName:  "Slow Discover MCP",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}
	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "slow-discover-mcp"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if result, rpcErr := h.mcpToolDiscover(shortCtx, discoverRaw); rpcErr == nil {
		t.Fatalf("expected mcp discover timeout error, got result=%+v", result)
	}

	run := latestMCPDiscoverOperationRun(t, ctx, store, workspaceID)
	if run.Status != "TIMED_OUT" || run.Outcome != "TIMED_OUT" {
		t.Fatalf("unexpected discover timeout run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	assertOperationLedgerPromptContext(t, run, "mcp.tool.discover", workspaceID, "human", "developer")
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "timed_out" {
		t.Fatalf("ledger status = %q", got)
	}
	if got := stringLedgerField(t, ledger, "operation_kind"); got != "mcp_discover" {
		t.Fatalf("operation_kind = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "mcp_transport"); got != "streamable-http" {
		t.Fatalf("result mcp_transport = %q, want streamable-http", got)
	}
	if got := stringLedgerField(t, resultLedger, "phase"); got != "initialize" {
		t.Fatalf("result phase = %q, want initialize", got)
	}
	if timedOut, ok := resultLedger["timed_out"].(bool); !ok || !timedOut {
		t.Fatalf("expected discover timed_out result, got %+v", resultLedger["timed_out"])
	}
	if exitCode, ok := resultLedger["exit_code"].(float64); !ok || int(exitCode) != -1 {
		t.Fatalf("expected timeout exit_code -1 after JSON roundtrip, got %T %+v", resultLedger["exit_code"], resultLedger["exit_code"])
	}
}

func TestMCPToolDiscoverStdioChildDeathRecordsDiagnosticsOperationLedger(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-mcp-discover-stdio-child-death"
	ctx := testAuthContext(workspaceID, "system", "tests")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP discover stdio child death",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	envRaw, err := json.Marshal(map[string]string{
		"RHIZOME_SERVER_MCP_STDIO_HELPER": "crash_on_initialize",
	})
	if err != nil {
		t.Fatalf("marshal helper env: %v", err)
	}
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "stdio-discover-crash",
		WorkspaceID:  workspaceID,
		DisplayName:  "Crashing Discover Stdio MCP",
		Transport:    "stdio",
		Command:      os.Args[0],
		EnvJSON:      string(envRaw),
		RegisteredBy: "system:tests",
	}); err != nil {
		t.Fatalf("register stdio mcp server: %v", err)
	}
	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "stdio-discover-crash"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	started := time.Now()
	if result, rpcErr := h.mcpToolDiscover(ctx, discoverRaw); rpcErr == nil {
		t.Fatalf("expected stdio discover child-death error, got result=%+v", result)
	} else if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("stdio discover child death took too long to surface: %s rpcErr=%+v", elapsed, rpcErr)
	}

	run := latestMCPDiscoverOperationRun(t, ctx, store, workspaceID)
	if run.Status != "FAILED" || run.Outcome != "FAILED" {
		t.Fatalf("unexpected discover child-death run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "failed" {
		t.Fatalf("ledger status = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "mcp_transport"); got != "stdio" {
		t.Fatalf("result mcp_transport = %q, want stdio", got)
	}
	if got := stringLedgerField(t, resultLedger, "phase"); got != "initialize" {
		t.Fatalf("result phase = %q, want initialize", got)
	}
	if stdoutClosed, ok := resultLedger["child_stdout_closed"].(bool); !ok || !stdoutClosed {
		t.Fatalf("expected child_stdout_closed=true, got %+v", resultLedger["child_stdout_closed"])
	}
	if got := stringLedgerField(t, resultLedger, "stderr"); !strings.Contains(got, "fatal stdio crash during initialize") {
		t.Fatalf("stderr = %q, want fatal stdio crash during initialize", got)
	}
	if got := stringLedgerField(t, resultLedger, "stderr_ref"); got != "inline_bounded_stderr" {
		t.Fatalf("stderr_ref = %q, want inline_bounded_stderr", got)
	}
}

func TestToolCallErrorResultMapPreservesStdioDiagnostics(t *testing.T) {
	err := &mcp.StdioTransportError{
		Operation: "connection closed",
		Cause:     context.Canceled,
		Diagnostics: mcp.StdioTransportDiagnostics{
			Stderr:       "fatal stdio child crash",
			StdoutClosed: true,
		},
	}

	result := toolCallErrorResultMap("mcp__stdio__crashy", err)
	if got := result["mcp_transport"]; got != "stdio" {
		t.Fatalf("mcp_transport = %+v, want stdio", got)
	}
	if got := result["stderr"]; got != "fatal stdio child crash" {
		t.Fatalf("stderr = %+v, want fatal stdio child crash", got)
	}
	if got := result["stderr_ref"]; got != "inline_bounded_stderr" {
		t.Fatalf("stderr_ref = %+v, want inline_bounded_stderr", got)
	}
	if got := result["child_stdout_closed"]; got != true {
		t.Fatalf("child_stdout_closed = %+v, want true", got)
	}
	if got := result["canceled"]; got != true {
		t.Fatalf("canceled = %+v, want true", got)
	}
	status, outcome, _, canceled := toolCallTerminalStateFromResult(result, err)
	if status != "CANCELLED" || outcome != "CANCELLED" || !canceled {
		t.Fatalf("terminal state = %s/%s canceled=%v, want CANCELLED/CANCELLED true", status, outcome, canceled)
	}
}

func TestMCPRoutedStdioChildDeathRecordsDiagnosticsOperationLedger(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-mcp-stdio-child-death-ledger"
	const serverID = "stdio-crash-mcp"
	const toolName = "crashy_tool"
	toolID := mcpWorkspaceToolID(serverID, toolName)
	ctx := testAuthContext(workspaceID, "system", "tests")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP stdio child death ledger",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	envRaw, err := json.Marshal(map[string]string{
		"RHIZOME_SERVER_MCP_STDIO_HELPER": "crash_on_call",
	})
	if err != nil {
		t.Fatalf("marshal helper env: %v", err)
	}
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     serverID,
		WorkspaceID:  workspaceID,
		DisplayName:  "Crashing Stdio MCP",
		Transport:    "stdio",
		Command:      os.Args[0],
		EnvJSON:      string(envRaw),
		RegisteredBy: "system:tests",
	}); err != nil {
		t.Fatalf("register stdio mcp server: %v", err)
	}

	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: serverID})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	if _, rpcErr := h.mcpToolDiscover(ctx, discoverRaw); rpcErr != nil {
		t.Fatalf("mcpToolDiscover rpc error: %+v", rpcErr)
	}
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "system",
		SubjectID:   "tests",
		Capability:  "tool.call",
		ToolID:      toolID,
		Effect:      "ALLOW",
		Reason:      "test allows stdio MCP child-death path",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("put capability policy: %v", err)
	}

	callRaw, err := json.Marshal(toolCallParams{
		ToolID:      toolID,
		WorkspaceID: workspaceID,
		Arguments:   map[string]any{"query": "crash"},
		TimeoutSec:  5,
	})
	if err != nil {
		t.Fatalf("marshal tool call params: %v", err)
	}
	started := time.Now()
	if result, rpcErr := h.toolCall(ctx, callRaw); rpcErr == nil {
		t.Fatalf("expected routed stdio child-death error, got result=%+v", result)
	} else if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("stdio child death took too long to surface: %s rpcErr=%+v", elapsed, rpcErr)
	}

	run := latestToolOperationRun(t, ctx, store, workspaceID)
	if run.Status != "FAILED" || run.Outcome != "FAILED" {
		t.Fatalf("unexpected stdio child-death run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "failed" {
		t.Fatalf("ledger status = %q", got)
	}
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	if got := stringLedgerField(t, details, "mcp_transport"); got != "stdio" {
		t.Fatalf("mcp_transport = %q, want stdio", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "mcp_transport"); got != "stdio" {
		t.Fatalf("result mcp_transport = %q, want stdio", got)
	}
	if timedOut, ok := resultLedger["timed_out"].(bool); !ok || timedOut {
		t.Fatalf("expected non-timeout stdio child death, got timed_out=%+v", resultLedger["timed_out"])
	}
	if stdoutClosed, ok := resultLedger["child_stdout_closed"].(bool); !ok || !stdoutClosed {
		t.Fatalf("expected child_stdout_closed=true, got %+v", resultLedger["child_stdout_closed"])
	}
	if got := stringLedgerField(t, resultLedger, "stderr"); !strings.Contains(got, "fatal stdio crash during tool call") {
		t.Fatalf("stderr = %q, want fatal stdio crash during tool call", got)
	}
	if got := stringLedgerField(t, resultLedger, "stderr_ref"); got != "inline_bounded_stderr" {
		t.Fatalf("stderr_ref = %q, want inline_bounded_stderr", got)
	}
}

func deployToolForOperationLedgerTest(t *testing.T, h *Handler, ctx context.Context, workspaceID, toolID, source string) {
	t.Helper()
	raw, err := json.Marshal(toolDeployParams{
		ToolID:      toolID,
		WorkspaceID: workspaceID,
		Runtime:     "node",
		SourceCode:  strings.TrimSpace(source),
		EntryPoint:  "main.js",
		DeployedBy:  "test",
	})
	if err != nil {
		t.Fatalf("marshal deploy params: %v", err)
	}
	if _, rpcErr := h.toolDeploy(ctx, raw); rpcErr != nil {
		t.Fatalf("deploy tool %s: %+v", toolID, rpcErr)
	}
}

func runServerMCPStdioTestHelper(mode string) {
	switch mode {
	case "crash_on_call":
		runServerMCPStdioCrashOnCallHelper()
	case "crash_on_initialize":
		fmt.Fprintln(os.Stderr, "fatal stdio crash during initialize")
		os.Exit(7)
	default:
		fmt.Fprintf(os.Stderr, "unknown server MCP stdio helper mode %q\n", mode)
		os.Exit(2)
	}
}

func runServerMCPStdioCrashOnCallHelper() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var req mcp.JSONRPCRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			continue
		}
		if req.ID == nil {
			continue
		}
		switch req.Method {
		case "initialize":
			_ = json.NewEncoder(os.Stdout).Encode(mcp.JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result: mustServerMCPRawMessage(map[string]any{
					"protocolVersion": "2025-03-26",
					"serverInfo": map[string]any{
						"name":    "stdio-crash-mcp",
						"version": "1.0.0",
					},
				}),
			})
		case "tools/list":
			_ = json.NewEncoder(os.Stdout).Encode(mcp.JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result: mustServerMCPRawMessage(map[string]any{
					"tools": []map[string]any{{
						"name":        "crashy_tool",
						"description": "Crashes during tools/call after writing stderr.",
						"inputSchema": map[string]any{"type": "object"},
					}},
				}),
			})
		case "tools/call":
			fmt.Fprintln(os.Stderr, "fatal stdio crash during tool call")
			os.Exit(7)
		default:
			_ = json.NewEncoder(os.Stdout).Encode(mcp.JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result:  mustServerMCPRawMessage(map[string]any{}),
			})
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "stdin scanner error: %v\n", err)
		os.Exit(3)
	}
}

func mustServerMCPRawMessage(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}

func registerToolLedgerAgent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		OwnerUserID:     "developer",
		DisplayName:     agentID,
		Role:            "worker",
		ProtocolVersion: "test",
		Capabilities:    []string{"tool.call"},
	}); err != nil {
		t.Fatalf("register agent %s: %v", agentID, err)
	}
}

func newSlowToolCallMCPServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req struct {
			Method string `json:"method"`
			ID     any    `json:"id"`
		}
		var envelope map[string]any
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Fatalf("decode mcp request: %v", err)
		}
		req.Method, _ = envelope["method"].(string)
		req.ID = envelope["id"]

		switch req.Method {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"serverInfo": map[string]any{
						"name":    "slow-mcp",
						"version": "1.0.0",
					},
				},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result": map[string]any{
					"tools": []map[string]any{{
						"name":        "slow_tool",
						"description": "Sleeps past caller timeout",
						"inputSchema": map[string]any{"type": "object"},
					}},
				},
			})
		case "tools/call":
			time.Sleep(2 * time.Second)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result": map[string]any{
					"content": []map[string]any{{"type": "text", "text": "late"}},
				},
			})
		default:
			t.Fatalf("unexpected fake MCP method %q", req.Method)
		}
	}))
}

func newSlowDiscoverMCPServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var envelope map[string]any
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Fatalf("decode mcp request: %v", err)
		}
		method, _ := envelope["method"].(string)
		id := envelope["id"]
		switch method {
		case "initialize":
			time.Sleep(250 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"serverInfo": map[string]any{
						"name":    "slow-discover-mcp",
						"version": "1.0.0",
					},
				},
			})
		default:
			t.Fatalf("unexpected slow discover method %q", method)
		}
	}))
}

func latestToolOperationRun(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) sqlite.ExecutionRunRecord {
	t.Helper()
	return latestExecutionRunWithPrefix(t, ctx, store, workspaceID, "toolcall_")
}

func latestToolDeployOperationRun(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) sqlite.ExecutionRunRecord {
	t.Helper()
	return latestExecutionRunWithPrefix(t, ctx, store, workspaceID, "tooldeploy_")
}

func latestToolUndeployOperationRun(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) sqlite.ExecutionRunRecord {
	t.Helper()
	return latestExecutionRunWithPrefix(t, ctx, store, workspaceID, "toolundeploy_")
}

func latestMCPDiscoverOperationRun(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) sqlite.ExecutionRunRecord {
	t.Helper()
	return latestExecutionRunWithPrefix(t, ctx, store, workspaceID, "mcpdiscover_")
}

func latestExecutionRunWithPrefix(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, prefix string) sqlite.ExecutionRunRecord {
	t.Helper()
	runs, err := store.ListExecutionRuns(ctx, sqlite.ExecutionRunFilter{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("list execution runs: %v", err)
	}
	for _, run := range runs {
		if strings.HasPrefix(run.RunID, prefix) {
			return run
		}
	}
	t.Fatalf("expected %s execution run, got %+v", prefix, runs)
	return sqlite.ExecutionRunRecord{}
}

func uniqueOperationLedgerTestID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func operationLedgerFromRun(t *testing.T, run sqlite.ExecutionRunRecord) map[string]any {
	t.Helper()
	raw, ok := run.VerificationJSON["operation_ledger"]
	if !ok {
		t.Fatalf("missing operation_ledger in %+v", run.VerificationJSON)
	}
	ledger, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("operation_ledger has type %T, want map[string]any", raw)
	}
	return ledger
}

func assertOperationLedgerPromptContext(t *testing.T, run sqlite.ExecutionRunRecord, wantSurface, wantWorkspaceID, wantPrincipalType, wantPrincipalID string) {
	t.Helper()
	raw, ok := run.VerificationJSON["prompt_context_envelope"]
	if !ok {
		t.Fatalf("missing prompt_context_envelope in %+v", run.VerificationJSON)
	}
	envelope, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("prompt_context_envelope has type %T, want map[string]any", raw)
	}
	assertOperationLedgerPromptContextField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertOperationLedgerPromptContextField(t, envelope, "context_kind", "authority_bearing_execution_write")
	assertOperationLedgerPromptContextField(t, envelope, "surface", wantSurface)
	assertOperationLedgerPromptContextField(t, envelope, "origin", "server_operation_ledger")
	assertOperationLedgerPromptContextField(t, envelope, "workspace_id", wantWorkspaceID)
	assertOperationLedgerPromptContextField(t, envelope, "principal_type", wantPrincipalType)
	assertOperationLedgerPromptContextField(t, envelope, "principal_id", wantPrincipalID)
	assertOperationLedgerPromptContextField(t, envelope, "authority_model", "workspace_authority")
	assertOperationLedgerPromptContextField(t, envelope, "compiler_status", "non_daemon_context_envelope")
	assertOperationLedgerPromptContextField(t, envelope, "daemon_prompt_compiler_convergence", "not_claimed")
	assertOperationLedgerPromptContextField(t, envelope, "prompt_capability_evidence", "not_present")
}

func assertOperationLedgerPromptContextField(t *testing.T, envelope map[string]any, key, want string) {
	t.Helper()
	got, ok := envelope[key].(string)
	if !ok {
		t.Fatalf("prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
	}
	if got != want {
		t.Fatalf("prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
	}
}

func nestedLedgerMap(t *testing.T, values map[string]any, key string) map[string]any {
	t.Helper()
	raw, ok := values[key]
	if !ok {
		t.Fatalf("missing nested field %q in %+v", key, values)
	}
	nested, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("nested field %q has type %T, want map[string]any", key, raw)
	}
	return nested
}

func stringLedgerField(t *testing.T, values map[string]any, key string) string {
	t.Helper()
	raw, ok := values[key]
	if !ok {
		t.Fatalf("missing field %q in %+v", key, values)
	}
	got, ok := raw.(string)
	if !ok {
		t.Fatalf("field %q has type %T, want string", key, raw)
	}
	return got
}
