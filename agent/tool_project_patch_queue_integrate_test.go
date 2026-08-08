package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestProjectPatchQueueIntegrateToolMergesAcceptedBranchIntoCanonicalMain(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-scaffold"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>remote candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	calls := make([]string, 0, 4)
	var checkoutParams map[string]any
	var integrationBranchParams map[string]any
	var sourceBranchParams map[string]any
	integrationReceipts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
		case "project.patch_queue.integration_record":
			integrationReceipts++
			if got := rpcString(req.Params, "outcome"); got != "admitted" && got != "integrated" {
				t.Fatalf("unexpected integration receipt outcome %q", got)
			} else if got == "integrated" {
				if remoteHead := rpcString(req.Params, "remote_target_head_after"); remoteHead == "" || remoteHead != rpcString(req.Params, "target_head_after") {
					t.Fatalf("integrated receipt must include canonical remote target proof, got %+v", req.Params)
				}
			}
			writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
		case "project.checkout.register":
			checkoutParams = req.Params
			if got := rpcString(req.Params, "checkout_kind"); got != "integration" {
				t.Fatalf("checkout_kind = %q", got)
			}
			if got := rpcString(req.Params, "branch_name"); got != "main" {
				t.Fatalf("integration checkout branch_name = %q", got)
			}
			if got := rpcString(req.Params, "base_sha"); got != initialMain {
				t.Fatalf("checkout base_sha = %q, want initial main %s", got, initialMain)
			}
			if got := rpcString(req.Params, "head_sha"); got == "" || got == initialMain {
				t.Fatalf("checkout head_sha should be merged head, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-integration",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "integration",
				"branch_name":   "main",
				"base_branch":   "main",
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case "project.branch.register":
			switch rpcString(req.Params, "branch_name") {
			case "main":
				integrationBranchParams = req.Params
				if got := rpcString(req.Params, "branch_kind"); got != "integration" {
					t.Fatalf("integration branch_kind = %q", got)
				}
				if got := rpcString(req.Params, "status"); got != "MERGED" {
					t.Fatalf("integration status = %q", got)
				}
				writeRPCResult(w, req, map[string]any{"branch": map[string]any{
					"branch_id":        "branch-integration",
					"workspace_id":     "ws",
					"project_id":       "project-subpixel",
					"repo_id":          "projrepo-1",
					"checkout_id":      "checkout-integration",
					"agent_id":         "agent-alpha",
					"branch_name":      "main",
					"branch_kind":      "integration",
					"base_branch":      "main",
					"head_sha":         rpcString(req.Params, "head_sha"),
					"base_sha":         rpcString(req.Params, "base_sha"),
					"write_scope_json": `{"paths":["**"]}`,
					"status":           "MERGED",
				}})
			case sourceBranch:
				sourceBranchParams = req.Params
				if got := rpcString(req.Params, "branch_id"); got != "branch-1" {
					t.Fatalf("source branch_id = %q", got)
				}
				if got := rpcString(req.Params, "status"); got != "MERGED" {
					t.Fatalf("source status = %q", got)
				}
				if got := rpcString(req.Params, "active_task_id"); got != "" {
					t.Fatalf("terminal source branch must clear active_task_id, got %q", got)
				}
				if got := rpcString(req.Params, "active_claim_id"); got != "" {
					t.Fatalf("terminal source branch must clear active_claim_id, got %q", got)
				}
				writeRPCResult(w, req, map[string]any{"branch": map[string]any{
					"branch_id":        "branch-1",
					"workspace_id":     "ws",
					"project_id":       "project-subpixel",
					"repo_id":          "projrepo-1",
					"checkout_id":      "checkout-1",
					"agent_id":         "agent-beta",
					"branch_name":      sourceBranch,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"head_sha":         sourceHead,
					"base_sha":         initialMain,
					"write_scope_json": `{"paths":["web/**"]}`,
					"status":           "MERGED",
				}})
			default:
				t.Fatalf("unexpected branch.register params: %+v", req.Params)
			}
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected integration success, got %+v", result)
	}
	for _, want := range []string{`"integrated": true`, `"merge_performed": true`, `"push_succeeded": true`, `"source_branch_status": "MERGED"`, `"target_branch_name": "main"`, `"target_branch_status": "MERGED"`, `"remote_target_head_after":`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if checkoutParams == nil || integrationBranchParams == nil || sourceBranchParams == nil || integrationReceipts != 2 {
		t.Fatalf("expected checkout, integration/source branch registrations, and two integration receipts; receipts=%d", integrationReceipts)
	}
	runGit(t, remote, "merge-base", "--is-ancestor", sourceHead, "main")
	if got := gitOutput(t, remote, "rev-parse", "main"); got == initialMain {
		t.Fatalf("remote main did not advance")
	}
	if got := gitOutput(t, remote, "show", "main:web/index.html"); got != "<h1>remote candidate</h1>" {
		t.Fatalf("remote main missing integrated candidate file: %q", got)
	}
	if got, want := strings.Join(calls, ","), "project.coordination.get,project.patch_queue.integration_record,project.checkout.register,project.branch.register,project.patch_queue.integration_record,project.branch.register"; got != want {
		t.Fatalf("unexpected RPC call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueIntegrateToolPushFalseRecordsRepairNotIntegrated(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-local-only"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>local-only candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	calls := make([]string, 0, 3)
	repairReceipts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
		case "project.patch_queue.integration_record":
			if got := rpcString(req.Params, "outcome"); got != "admitted" {
				t.Fatalf("push=false must not record integrated receipt, got %+v", req.Params)
			}
			writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
		case "project.patch_queue.integration_repair":
			repairReceipts++
			if got := rpcString(req.Params, "repair_reason"); !strings.Contains(got, "canonical remote target verification failed") {
				t.Fatalf("repair reason must preserve canonical remote verification failure, got %q", got)
			}
			writePatchQueueIntegrationReceiptResult(w, req, "BLOCKED")
		case "project.checkout.register", "project.branch.register":
			t.Fatalf("local-only integration must not register canonical target evidence before remote proof: %s %+v", req.Method, req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
		"push":       false,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, `"repair_receipt_recorded": true`) {
		t.Fatalf("expected push=false local merge to record repair, got %+v", result)
	}
	if repairReceipts != 1 {
		t.Fatalf("expected one repair receipt, got %d", repairReceipts)
	}
	if got := gitOutput(t, remote, "rev-parse", "main"); got != initialMain {
		t.Fatalf("push=false must not mutate canonical remote main, got %s want %s", got, initialMain)
	}
	if strings.Join(calls, ",") != "project.coordination.get,project.patch_queue.integration_record,project.patch_queue.integration_repair" {
		t.Fatalf("unexpected RPC call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueIntegrateToolTreatsRepairDedupAsTerminalRepairReceipt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.patch_queue.integration_repair" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"error": map[string]any{
				"code":    -32000,
				"message": `dedup_key "project.patch_queue.integration:ws:queue-1:item-1:head-1" already exists`,
			},
		})
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", t.TempDir())
	result := tool.projectPatchQueueIntegrationRepairResult(context.Background(), projectPatchQueueIntegrateOutput{
		ProjectID:        "project-1",
		RepoID:           "repo-1",
		QueueID:          "queue-1",
		ItemID:           "item-1",
		SourceBranchID:   "branch-1",
		SourceHeadSHA:    "head-1",
		TargetBranchName: "main",
		AuthorityMode:    "project_role",
		IntegrationMode:  "direct_merge",
	}, projectPatchQueueIntegrationReceiptInput{
		WorkspaceID:     "ws",
		ProjectID:       "project-1",
		ActorID:         "agent-alpha",
		QueueID:         "queue-1",
		ItemID:          "item-1",
		RepoID:          "repo-1",
		SourceBranchID:  "branch-1",
		SourceHeadSHA:   "head-1",
		TargetBranch:    "main",
		IntegrationMode: "direct_merge",
		RepairReason:    "accepted head has unresolved same-head reviewer defect evidence",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected terminal repair error result, got %+v", result)
	}
	for _, want := range []string{`"repair_receipt_recorded": true`, `"repair_receipt_already_recorded": true`, `"repair_recording_dedup_key_already_exists": true`, `"required_transition": "project_patch_queue_followup_revision_before_integration_retry"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
}

func TestProjectPatchQueueIntegrateToolRefusesSameHeadBlockedReviewerAdvisoryBeforeAdmission(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-lexer"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "lexer.go"), "package lexer\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	calls := make([]string, 0, 6)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueIntegrateCoordinationResultWithSameHeadBlockedAdvisory(remote, sourceBranch, sourceHead, initialMain))
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "project_lane"); got != "implementation" {
				t.Fatalf("revision follow-up project_lane=%q", got)
			}
			if got := strings.Join(rpcStringSlice(req.Params, "write_scope_hints"), ","); got != "" {
				t.Fatalf("revision follow-up must not lock candidate pathset, got %q", got)
			}
			tags := strings.Join(rpcStringSlice(req.Params, "tags"), ",")
			for _, want := range []string{"owner-bound", "owner-bound-kind:patch_queue_revision", "owner-branch:branch-1", "required-agent:agent-beta"} {
				if !strings.Contains(tags, want) {
					t.Fatalf("revision follow-up tags missing %q: %s", want, tags)
				}
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{"pre_integration_defeater_for_accepted_item", "queue_id: patchq-project-subpixel-projrepo-1", "item_id: patchitem-branch-1-negative"} {
				if !strings.Contains(description, want) {
					t.Fatalf("revision follow-up description missing %q:\n%s", want, description)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-followup"})
		case "project.patch_queue.integration_record", "project.patch_queue.integration_repair", "project.checkout.register", "project.branch.register":
			t.Fatalf("defeated acceptance must not admit integration or mutate git evidence: %s %+v", req.Method, req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"queue_id":   "patchq-project-subpixel-projrepo-1",
		"item_id":    "patchitem-branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected same-head advisory to refuse integration, got %+v", result)
	}
	for _, want := range []string{`"pre_integration_gate": "defeasible_acceptance"`, `"defeating_item_id": "patchitem-branch-1-negative"`, `"integrated": false`, `"no_canonical_mutation": true`, `"followup_kind": "revision"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if got := gitOutput(t, remote, "rev-parse", "main"); got != initialMain {
		t.Fatalf("canonical main changed despite defeated acceptance: got %s want %s", got, initialMain)
	}
	if got := strings.Join(calls, ","); !strings.Contains(got, "task.submit") || strings.Contains(got, "project.patch_queue.integration_record") {
		t.Fatalf("unexpected call sequence: %s", got)
	}
}

func TestProjectPatchQueueIntegrateToolReceiptFailureWritesRepairAndDoesNotReportSuccess(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-receipt-failure"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>receipt candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	integratedReceiptAttempts := 0
	repairReceipts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
		case "project.patch_queue.integration_record":
			switch rpcString(req.Params, "outcome") {
			case "admitted":
				writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
			case "integrated":
				integratedReceiptAttempts++
				writeRPCError(w, req, -32000, "injected integration receipt failure")
			default:
				t.Fatalf("unexpected integration receipt params: %+v", req.Params)
			}
		case "project.patch_queue.integration_repair":
			repairReceipts++
			if got := rpcString(req.Params, "repair_reason"); !strings.Contains(got, "failed recording durable integrated receipt") {
				t.Fatalf("repair reason did not preserve receipt failure: %q", got)
			}
			writePatchQueueIntegrationReceiptResult(w, req, "BLOCKED")
		case "project.checkout.register":
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-integration",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "integration",
				"branch_name":   "main",
				"base_branch":   "main",
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case "project.branch.register":
			branchName := rpcString(req.Params, "branch_name")
			branchID := "branch-integration"
			branchKind := rpcString(req.Params, "branch_kind")
			if branchName == sourceBranch {
				branchID = "branch-1"
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        branchID,
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      rpcString(req.Params, "checkout_id"),
				"agent_id":         rpcString(req.Params, "agent_id"),
				"branch_name":      branchName,
				"branch_kind":      branchKind,
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": rpcString(req.Params, "write_scope_json"),
				"status":           "MERGED",
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected receipt failure to return repair error, got %+v", result)
	}
	for _, want := range []string{`"integrated": false`, `"repair_receipt_recorded": true`, "injected integration receipt failure"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected repair output to contain %q, got %q", want, result.Output)
		}
	}
	if integratedReceiptAttempts != 1 || repairReceipts != 1 {
		t.Fatalf("expected one integrated receipt attempt and one repair receipt, got integrated=%d repair=%d", integratedReceiptAttempts, repairReceipts)
	}
	runGit(t, remote, "merge-base", "--is-ancestor", sourceHead, "main")
}

// CT-05: prove that when the source-branch close (project.branch.register MERGED)
// fails AFTER the durable integrated receipt is recorded, the tool (a) has already
// recorded the integrated receipt before attempting the close, (b) writes a durable
// repair receipt that preserves the close-failure lineage, and (c) does NOT report
// success. This closes the "process/RPC fails between integrated receipt and source
// branch close" interleaving so the merge can always be rediscovered downstream.
func TestProjectPatchQueueIntegrateToolSourceCloseFailureWritesRepairAfterIntegratedReceipt(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-source-close-failure"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>source close candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	var events []string
	integratedReceipts := 0
	repairReceipts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
		case "project.patch_queue.integration_record":
			switch rpcString(req.Params, "outcome") {
			case "admitted":
				writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
			case "integrated":
				integratedReceipts++
				events = append(events, "integrated_receipt")
				writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
			default:
				t.Fatalf("unexpected integration receipt params: %+v", req.Params)
			}
		case "project.patch_queue.integration_repair":
			repairReceipts++
			events = append(events, "repair_receipt")
			if got := rpcString(req.Params, "repair_reason"); !strings.Contains(got, "source_branch_close_failed_after_integrated_receipt") {
				t.Fatalf("repair reason must preserve source-branch close failure lineage, got %q", got)
			}
			writePatchQueueIntegrationReceiptResult(w, req, "BLOCKED")
		case "project.checkout.register":
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-integration",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "integration",
				"branch_name":   "main",
				"base_branch":   "main",
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case "project.branch.register":
			branchName := rpcString(req.Params, "branch_name")
			if branchName == sourceBranch {
				// Inject the source-branch close failure AFTER the integrated receipt.
				events = append(events, "source_close_attempt")
				writeRPCError(w, req, -32000, "injected source branch close failure")
				return
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-integration",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      rpcString(req.Params, "checkout_id"),
				"agent_id":         rpcString(req.Params, "agent_id"),
				"branch_name":      branchName,
				"branch_kind":      rpcString(req.Params, "branch_kind"),
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": rpcString(req.Params, "write_scope_json"),
				"status":           "MERGED",
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected source-close failure to return repair error, got %+v", result)
	}
	for _, want := range []string{`"repair_receipt_recorded": true`, "source branch close", "injected source branch close failure"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected repair output to contain %q, got %q", want, result.Output)
		}
	}
	if integratedReceipts != 1 || repairReceipts != 1 {
		t.Fatalf("expected one integrated receipt and one repair receipt, got integrated=%d repair=%d", integratedReceipts, repairReceipts)
	}
	// Ordering invariant: integrated receipt is durable BEFORE the close is attempted,
	// and the repair receipt is durable AFTER the close fails.
	if strings.Join(events, ",") != "integrated_receipt,source_close_attempt,repair_receipt" {
		t.Fatalf("expected integrated-receipt-before-close then repair ordering, got %+v", events)
	}
}

func TestProjectPatchQueueIntegrateToolRejectsNonAcceptedItemBeforeCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-scaffold"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>remote candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, initialMain, "CLAIMED"))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "ACCEPTED") {
		t.Fatalf("expected accepted-state error, got %+v", result)
	}
	if _, err := os.Stat(filepath.Join(workdir, "project-integration")); !os.IsNotExist(err) {
		t.Fatalf("expected no integration checkout after preflight rejection, err=%v", err)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call, got %d", calls)
	}
}

func TestProjectPatchQueueIntegrateControlledRequiresMaterializationOrDirectMerge(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		QueueID:           "patchq-1",
		ItemID:            "patchitem-1",
		RepoAuthorityMode: "repoauthority_controlled_queue",
	}
	if _, err := projectPatchQueueIntegrationModeForItem(item, ""); err == nil || !strings.Contains(err.Error(), "requires durable materialization") {
		t.Fatalf("expected controlled item without materialization to require explicit mode, got %v", err)
	}
	if mode, err := projectPatchQueueIntegrationModeForItem(item, "direct_merge"); err != nil || mode != "direct_merge" {
		t.Fatalf("expected explicit direct_merge mode, mode=%q err=%v", mode, err)
	}
	item.MaterializationAccepted = true
	item.MaterializationSchema = "patch_materialization.v1"
	item.MaterializationDigest = "sha256:materialized"
	if mode, err := projectPatchQueueIntegrationModeForItem(item, ""); err != nil || mode != "materialization" {
		t.Fatalf("expected materialized controlled item to default to materialization, mode=%q err=%v", mode, err)
	}
}

func TestProjectPatchQueueIntegrateToolUsesRuntimeTaskDirectMergeForAcceptedControlledItem(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-scaffold"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>controlled candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	taskReq, err := json.Marshal(map[string]any{
		"schema":                "task_requirements.v1",
		"patch_queue_task_kind": "integration",
		"required_tool":         "project_patch_queue_integrate",
		"project_id":            "project-subpixel",
		"queue_id":              "patchq-project-subpixel-projrepo-1",
		"item_id":               "patchitem-branch-1",
		"branch_id":             "branch-1",
		"head_sha":              sourceHead,
	})
	if err != nil {
		t.Fatalf("marshal task requirements: %v", err)
	}

	integrationReceipts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED")
			coordination := result["coordination"].(map[string]any)
			items := coordination["patch_queue_items"].([]map[string]any)
			items[0]["repo_authority_mode"] = "repoauthority_controlled_queue"
			items[0]["materialization_accepted"] = false
			items[0]["materialization_schema"] = ""
			items[0]["materialization_digest"] = ""
			coordination["tasks"] = []map[string]any{{
				"task_id":                "task-integrate-controlled",
				"workspace_id":           "ws",
				"project_id":             "project-subpixel",
				"project_lane":           "integration",
				"status":                 "RUNNING",
				"task_requirements_json": string(taskReq),
			}}
			writeRPCResult(w, req, result)
		case "project.patch_queue.integration_record":
			integrationReceipts++
			if got := rpcString(req.Params, "integration_mode"); got != "direct_merge" {
				t.Fatalf("expected task-bound direct_merge integration mode, got %+v", req.Params)
			}
			writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
		case "project.checkout.register":
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-integration",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"agent_id":      "agent-alpha",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "integration",
				"branch_name":   "main",
				"base_branch":   "main",
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case "project.branch.register":
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        firstNonEmpty(rpcString(req.Params, "branch_id"), "branch-integration"),
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      firstNonEmpty(rpcString(req.Params, "checkout_id"), "checkout-integration"),
				"agent_id":         rpcString(req.Params, "agent_id"),
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      rpcString(req.Params, "branch_kind"),
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           rpcString(req.Params, "status"),
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-integrate-controlled", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected task-bound direct merge success, got %+v", result)
	}
	if integrationReceipts != 2 {
		t.Fatalf("expected admitted and integrated receipts, got %d", integrationReceipts)
	}
	if got := gitOutput(t, remote, "show", "main:web/index.html"); got != "<h1>controlled candidate</h1>" {
		t.Fatalf("remote main missing task-bound direct merge content: %q", got)
	}
}

func TestProjectPatchQueueIntegrateToolRejectsControlledItemWithoutModeOrRuntimeTask(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-scaffold"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>controlled candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method after controlled preflight %q", req.Method)
		}
		result := patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED")
		coordination := result["coordination"].(map[string]any)
		items := coordination["patch_queue_items"].([]map[string]any)
		items[0]["repo_authority_mode"] = "repoauthority_controlled_queue"
		items[0]["materialization_accepted"] = false
		items[0]["materialization_schema"] = ""
		items[0]["materialization_digest"] = ""
		writeRPCResult(w, req, result)
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "requires durable materialization") {
		t.Fatalf("expected controlled materialization/direct_merge error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination preflight before rejection, got %d calls", calls)
	}
}

func TestProjectPatchQueueIntegrateToolStrictRejectsAcceptedItemWithoutActiveRoleBeforeCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-scaffold"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>remote candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueIntegrateCoordinationResultWithoutAuthority(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "active INTEGRATOR role or strategic lead lease") {
		t.Fatalf("expected strict authority error, got %+v", result)
	}
	if _, err := os.Stat(filepath.Join(workdir, "project-integration")); !os.IsNotExist(err) {
		t.Fatalf("expected no integration checkout after strict preflight rejection, err=%v", err)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call, got %d", calls)
	}
}

func TestProjectPatchQueueIntegrateToolTrustFirstRejectsCanonicalPushWithoutActiveRole(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-scaffold"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>trust first candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueIntegrateCoordinationResultWithoutAuthority(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma", "owner-1", workdir).WithCoordinationMode(CoordinationModeTrustFirst)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "trust_first authority is advisory") {
		t.Fatalf("expected trust_first authority rejection, got %+v", result)
	}
	if got := gitOutput(t, remote, "rev-parse", "main"); got == initialMain {
		// expected: no canonical mutation without server/integration authority
	} else {
		t.Fatalf("remote main advanced without active integration authority: got %s want %s", got, initialMain)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination preflight, got %d calls", calls)
	}
}

func TestProjectPatchQueueIntegrateToolAlreadyIntegratedWithoutSourceBranchEvidence(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-scaffold"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>already integrated candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	integrator := filepath.Join(t.TempDir(), "integrator")
	runGitNoDir(t, "clone", remote, integrator)
	runGit(t, integrator, "config", "user.name", "Rhizome Test")
	runGit(t, integrator, "config", "user.email", "rhizome-test@example.invalid")
	runGit(t, integrator, "checkout", "main")
	runGit(t, integrator, "merge", "--no-ff", "--no-gpg-sign", "-m", "Integrate candidate", "origin/"+sourceBranch)
	runGit(t, integrator, "push", "origin", "main")
	mergedMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	branchRegisters := 0
	integrationReceipts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueIntegrateCoordinationResultMissingSourceBranch(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
		case "project.patch_queue.integration_record":
			integrationReceipts++
			writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
		case "project.checkout.register":
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-integration",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "integration",
				"branch_name":   "main",
				"base_branch":   "main",
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case "project.branch.register":
			branchRegisters++
			if got := rpcString(req.Params, "branch_name"); got != "main" {
				t.Fatalf("missing-source idempotent path should only register integration branch, got %+v", req.Params)
			}
			if got := rpcString(req.Params, "head_sha"); got != mergedMain {
				t.Fatalf("integration head_sha = %q, want merged main %s", got, mergedMain)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-integration",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      rpcString(req.Params, "checkout_id"),
				"agent_id":         rpcString(req.Params, "agent_id"),
				"branch_name":      "main",
				"branch_kind":      "integration",
				"base_branch":      "main",
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": rpcString(req.Params, "write_scope_json"),
				"status":           "MERGED",
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected already-integrated success without visible source branch, got %+v", result)
	}
	for _, want := range []string{`"already_integrated": true`, `"source_branch_evidence_missing": true`, `"merge_performed": false`, `"integrated": true`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if branchRegisters != 1 {
		t.Fatalf("expected only integration branch registration, got %d", branchRegisters)
	}
	if integrationReceipts != 2 {
		t.Fatalf("expected admission and integrated receipts, got %d", integrationReceipts)
	}
	runGit(t, remote, "merge-base", "--is-ancestor", sourceHead, "main")
}

func TestProjectPatchQueueIntegrateToolRecordsRepairWhenSourceBranchCloseFailsAfterIntegratedReceipt(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-scaffold"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>already integrated candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	integrator := filepath.Join(t.TempDir(), "integrator")
	runGitNoDir(t, "clone", remote, integrator)
	runGit(t, integrator, "config", "user.name", "Rhizome Test")
	runGit(t, integrator, "config", "user.email", "rhizome-test@example.invalid")
	runGit(t, integrator, "checkout", "main")
	runGit(t, integrator, "merge", "--no-ff", "--no-gpg-sign", "-m", "Integrate candidate", "origin/"+sourceBranch)
	runGit(t, integrator, "push", "origin", "main")
	mergedMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	sourceBranchCloseAttempted := false
	integrationReceipts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
		case "project.patch_queue.integration_record":
			integrationReceipts++
			writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
		case "project.patch_queue.integration_repair":
			integrationReceipts++
			if got := rpcString(req.Params, "repair_reason"); !strings.Contains(got, "source_branch_close_failed_after_integrated_receipt") {
				t.Fatalf("expected source branch close repair reason, got %+v", req.Params)
			}
			writePatchQueueIntegrationReceiptResult(w, req, "BLOCKED")
		case "project.checkout.register":
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-integration",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "integration",
				"branch_name":   "main",
				"base_branch":   "main",
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case "project.branch.register":
			switch rpcString(req.Params, "branch_name") {
			case "main":
				if got := rpcString(req.Params, "head_sha"); got != mergedMain {
					t.Fatalf("integration head_sha = %q, want merged main %s", got, mergedMain)
				}
				writeRPCResult(w, req, map[string]any{"branch": map[string]any{
					"branch_id":        "branch-integration",
					"workspace_id":     "ws",
					"project_id":       "project-subpixel",
					"repo_id":          "projrepo-1",
					"checkout_id":      rpcString(req.Params, "checkout_id"),
					"agent_id":         rpcString(req.Params, "agent_id"),
					"branch_name":      "main",
					"branch_kind":      "integration",
					"base_branch":      "main",
					"head_sha":         rpcString(req.Params, "head_sha"),
					"base_sha":         rpcString(req.Params, "base_sha"),
					"write_scope_json": rpcString(req.Params, "write_scope_json"),
					"status":           "MERGED",
				}})
			case sourceBranch:
				sourceBranchCloseAttempted = true
				writeRPCError(w, req, -32000, "project branch scope mismatch: branch branch-1 belongs to agent beta")
			default:
				t.Fatalf("unexpected branch.register params: %+v", req.Params)
			}
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected source-close failure to return repair-needed error, got %+v", result)
	}
	for _, want := range []string{`"already_integrated": true`, `"integrated": true`, `"repair_receipt_recorded": true`, `"repair_reason": "source_branch_close_failed_after_integrated_receipt: rpc project.branch.register: project branch scope mismatch`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if !sourceBranchCloseAttempted {
		t.Fatal("expected source branch MERGED close to be attempted")
	}
	if integrationReceipts != 3 {
		t.Fatalf("expected admission, integrated, and repair receipts, got %d", integrationReceipts)
	}
	runGit(t, remote, "merge-base", "--is-ancestor", sourceHead, "main")
}

func TestProjectPatchQueueIntegrateToolMissingSourceBranchEvidenceFailsWhenHeadNotIntegrated(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-scaffold"
	remote := seedBareGitRepoWithBranch(t, sourceBranch, filepath.Join("web", "index.html"), "<h1>not integrated candidate</h1>\n")
	sourceHead := gitOutput(t, remote, "rev-parse", sourceBranch)
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	workdir := t.TempDir()

	branchRegisters := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueIntegrateCoordinationResultMissingSourceBranch(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
		case "project.branch.register":
			branchRegisters++
			t.Fatalf("missing-source not-integrated path should not register branch evidence: %+v", req.Params)
		default:
			writeRPCResult(w, req, map[string]any{})
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "requires source branch evidence") || !strings.Contains(result.Output, "not present in canonical target") {
		t.Fatalf("expected stale source branch evidence error, got %+v", result)
	}
	if branchRegisters != 0 {
		t.Fatalf("expected no branch registrations, got %d", branchRegisters)
	}
	if got := gitOutput(t, remote, "rev-parse", "main"); got != initialMain {
		t.Fatalf("remote main changed unexpectedly: got %s want %s", got, initialMain)
	}
}

func TestProjectPatchQueueIntegrateToolGuidesRecoveryWhenReadyLocalRemoteMissing(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-upload-convert-export"
	missingRemotePath := filepath.Join(t.TempDir(), "project-remotes", "project-subpixel", "subpixel-lab.git")
	remoteURL, err := projectRepoMaterializeFileURL(missingRemotePath)
	if err != nil {
		t.Fatalf("file URL: %v", err)
	}
	sourceHead := strings.Repeat("a", 40)
	initialMain := strings.Repeat("b", 40)
	workdir := t.TempDir()

	repairReceipts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method == "project.patch_queue.integration_record" {
			writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
			return
		}
		if req.Method == "project.patch_queue.integration_repair" {
			repairReceipts++
			if got := rpcString(req.Params, "repair_reason"); !strings.Contains(got, "git checkout failed") {
				t.Fatalf("repair reason must preserve checkout failure, got %q", got)
			}
			writePatchQueueIntegrationReceiptResult(w, req, "BLOCKED")
			return
		}
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueIntegrateCoordinationResult(remoteURL, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected missing local remote failure, got %+v", result)
	}
	for _, want := range []string{"git checkout failed", "recovery_guidance", "project_repo_materialize", "regenerate and republish", "Prior branch refs/objects are not recovered"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if repairReceipts != 1 {
		t.Fatalf("expected post-admission checkout failure to record repair receipt, got %d", repairReceipts)
	}
}

func TestProjectPatchQueueIntegrateToolGuidesRebuildWhenAcceptedHeadMissing(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-upload-convert-export"
	remote := seedBareGitRepoWithBranch(t, "agent/agent-seed/project-subpixel/seed", filepath.Join("README.md"), "seed\n")
	initialMain := gitOutput(t, remote, "rev-parse", "main")
	sourceHead := strings.Repeat("c", 40)
	workdir := t.TempDir()

	repairReceipts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method == "project.patch_queue.integration_record" {
			writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
			return
		}
		if req.Method == "project.patch_queue.integration_repair" {
			repairReceipts++
			if got := rpcString(req.Params, "repair_reason"); !strings.Contains(got, "failed checking ancestry") {
				t.Fatalf("repair reason must preserve missing-head ancestry failure, got %q", got)
			}
			writePatchQueueIntegrationReceiptResult(w, req, "BLOCKED")
			return
		}
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, initialMain, "ACCEPTED"))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected missing head failure, got %+v", result)
	}
	for _, want := range []string{"failed checking ancestry", "recovery_guidance", "git cat-file -e", "followup_kind=rebuild", "fresh equivalent review-ready candidate"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if repairReceipts != 1 {
		t.Fatalf("expected post-admission ancestry failure to record repair receipt, got %d", repairReceipts)
	}
}

func TestProjectPatchQueueIntegrateToolMergeConflictLeavesNoPartialBaseline(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	sourceBranch := "agent/agent-beta/project-subpixel/task-conflicting-candidate"
	remote, sourceHead, targetHead := seedBareGitRepoWithConflictingBranch(t, sourceBranch)
	workdir := t.TempDir()
	localPath := filepath.Join(workdir, "integration-checkout")

	calls := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, targetHead, "ACCEPTED"))
		case "project.patch_queue.integration_record":
			writePatchQueueIntegrationReceiptResult(w, req, "ACCEPTED")
		case "project.patch_queue.integration_repair":
			if got := rpcString(req.Params, "repair_reason"); !strings.Contains(got, "git merge failed") {
				t.Fatalf("repair reason must preserve merge conflict, got %q", got)
			}
			writePatchQueueIntegrationReceiptResult(w, req, "BLOCKED")
		case "project.checkout.register", "project.branch.register":
			t.Fatalf("failed merge must not register checkout or branch evidence: method=%s params=%+v", req.Method, req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueIntegrateTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
		"local_path": localPath,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "git merge failed") {
		t.Fatalf("expected merge conflict error, got %+v", result)
	}
	if status := gitOutput(t, localPath, "status", "--porcelain"); status != "" {
		t.Fatalf("merge conflict path should abort and leave integration checkout clean, status=%q", status)
	}
	if got := gitOutput(t, remote, "rev-parse", "main"); got != targetHead {
		t.Fatalf("remote main changed despite failed merge: got %s want %s", got, targetHead)
	}
	if out, err := gitCombined(context.Background(), localPath, "merge-base", "--is-ancestor", sourceHead, "HEAD"); err == nil {
		t.Fatalf("source head should not be integrated after conflict, merge-base output=%q", out)
	}
	if strings.Join(calls, ",") != "project.coordination.get,project.patch_queue.integration_record,project.patch_queue.integration_repair" {
		t.Fatalf("unexpected RPC calls after failed merge: %+v", calls)
	}
}

func patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, baseSHA, state string) map[string]any {
	result := branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-beta",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  sourceHead,
		BranchBaseSHA:  baseSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
	})
	coordination := result["coordination"].(map[string]any)
	repositories := coordination["repositories"].([]map[string]any)
	repositories[0]["integration_branch"] = "main"
	coordination["repositories"] = repositories
	coordination["roles"] = []map[string]any{{
		"role_id":      "role-integrator",
		"workspace_id": "ws",
		"project_id":   "project-subpixel",
		"agent_id":     "agent-alpha",
		"role_type":    "INTEGRATOR",
		"status":       "ACTIVE",
	}}
	branches := coordination["branches"].([]map[string]any)
	branches[0]["agent_id"] = "agent-beta"
	branches[0]["active_task_id"] = "task-beta-scaffold"
	branches[0]["active_claim_id"] = "task-beta-scaffold"
	branches[0]["branch_name"] = sourceBranch
	branches[0]["head_sha"] = sourceHead
	branches[0]["base_sha"] = baseSHA
	branches[0]["write_scope_json"] = `{"paths":["web/**"]}`
	coordination["branches"] = branches
	coordination["patch_queue_items"] = []map[string]any{patchQueueLifecycleItem(map[string]any{
		"state":          state,
		"repo_id":        "projrepo-1",
		"branch_id":      "branch-1",
		"base_ref":       "main",
		"base_sha":       baseSHA,
		"head_sha":       sourceHead,
		"pathset":        []string{"web/**"},
		"pathset_json":   `{"paths":["web/**"]}`,
		"review_doc_key": "project.project-subpixel.branch.branch-1.review",
	})}
	return result
}

func patchQueueIntegrateCoordinationResultWithSameHeadBlockedAdvisory(remote, sourceBranch, sourceHead, baseSHA string) map[string]any {
	result := patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, baseSHA, "ACCEPTED")
	coordination := result["coordination"].(map[string]any)
	accepted := coordination["patch_queue_items"].([]map[string]any)[0]
	blocked := patchQueueLifecycleItem(map[string]any{
		"queue_id":                   accepted["queue_id"],
		"item_id":                    "patchitem-branch-1-negative",
		"state":                      "BLOCKED",
		"repo_id":                    accepted["repo_id"],
		"branch_id":                  accepted["branch_id"],
		"base_ref":                   accepted["base_ref"],
		"base_sha":                   accepted["base_sha"],
		"head_sha":                   accepted["head_sha"],
		"pathset":                    accepted["pathset"],
		"pathset_json":               accepted["pathset_json"],
		"review_doc_key":             "project.project-subpixel.branch.branch-1.negative-review",
		"decision_summary":           "same-head lexer defect requires repair before integration",
		"reviewer_advisory_accepted": true,
		"reviewer_advisory_digest":   "sha256-negative",
		"reviewer_advisory": map[string]any{
			"schema":             "repo_patch_queue_reviewer_advisory.v1",
			"mode":               "reviewer_advisory",
			"verdict":            "repair_required",
			"scope":              "lane_correctness",
			"head_sha":           sourceHead,
			"defeats_acceptance": true,
			"reviewer_id":        "agent-epsilon",
			"review_doc_key":     "project.project-subpixel.branch.branch-1.negative-review",
			"summary":            "lexer emits incorrect token positions",
		},
	})
	coordination["patch_queue_items"] = []map[string]any{accepted, blocked}
	return result
}

func seedBareGitRepoWithConflictingBranch(t *testing.T, branchName string) (remote, sourceHead, targetHead string) {
	t.Helper()
	remote = seedBareGitRepo(t)
	source := filepath.Join(t.TempDir(), "conflict-source")
	runGitNoDir(t, "clone", remote, source)
	runGit(t, source, "config", "user.name", "Rhizome Test")
	runGit(t, source, "config", "user.email", "rhizome-test@example.invalid")
	conflictPath := filepath.Join("web", "index.html")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(source, conflictPath)), 0o755); err != nil {
		t.Fatalf("mkdir conflict parent: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, conflictPath), []byte("<h1>base</h1>\n"), 0o644); err != nil {
		t.Fatalf("write conflict base: %v", err)
	}
	runGit(t, source, "add", conflictPath)
	runGit(t, source, "commit", "-m", "Add shared app shell")
	runGit(t, source, "push", "origin", "main")
	runGit(t, source, "checkout", "-b", branchName)
	if err := os.WriteFile(filepath.Join(source, conflictPath), []byte("<h1>candidate</h1>\n"), 0o644); err != nil {
		t.Fatalf("write candidate conflict: %v", err)
	}
	runGit(t, source, "add", conflictPath)
	runGit(t, source, "commit", "-m", "Candidate changes shell")
	runGit(t, source, "push", "origin", branchName)
	sourceHead = gitOutput(t, source, "rev-parse", "HEAD")
	runGit(t, source, "checkout", "main")
	if err := os.WriteFile(filepath.Join(source, conflictPath), []byte("<h1>canonical</h1>\n"), 0o644); err != nil {
		t.Fatalf("write canonical conflict: %v", err)
	}
	runGit(t, source, "add", conflictPath)
	runGit(t, source, "commit", "-m", "Canonical changes shell")
	runGit(t, source, "push", "origin", "main")
	targetHead = gitOutput(t, source, "rev-parse", "HEAD")
	return remote, sourceHead, targetHead
}

func patchQueueIntegrateCoordinationResultMissingSourceBranch(remote, sourceBranch, sourceHead, baseSHA, state string) map[string]any {
	result := patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, baseSHA, state)
	coordination := result["coordination"].(map[string]any)
	coordination["branches"] = []map[string]any{}
	return result
}

func patchQueueIntegrateCoordinationResultWithoutAuthority(remote, sourceBranch, sourceHead, baseSHA, state string) map[string]any {
	result := patchQueueIntegrateCoordinationResult(remote, sourceBranch, sourceHead, baseSHA, state)
	coordination := result["coordination"].(map[string]any)
	coordination["roles"] = []map[string]any{}
	delete(coordination, "strategic_lead")
	return result
}

func writePatchQueueIntegrationReceiptResult(w http.ResponseWriter, req rpcRequest, state string) {
	writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
		"state":          state,
		"repo_id":        "projrepo-1",
		"branch_id":      "branch-1",
		"base_ref":       "main",
		"base_sha":       strings.Repeat("a", 40),
		"head_sha":       firstNonEmpty(rpcString(req.Params, "source_head_sha"), strings.Repeat("b", 40)),
		"pathset":        []string{"web/**"},
		"pathset_json":   `{"paths":["web/**"]}`,
		"review_doc_key": "project.project-subpixel.branch.branch-1.review",
	})})
}
