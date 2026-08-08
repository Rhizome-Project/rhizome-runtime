package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProjectImplementationCompletionGateAllowsCleanMatchingReviewReadyHead(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
		PatchQueueItems: []map[string]any{
			projectCompletionGatePatchQueueItem("PROPOSED", headSHA, "fresh implementation candidate submitted", "2026-05-08T02:10:00Z"),
		},
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if applied {
		t.Fatalf("expected clean matching review-ready branch to pass without demotion, got %+v", gated)
	}
	if normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("expected completed outcome to remain, got %+v", gated)
	}
}

func TestProjectImplementationCompletionGateDemotesReadyBranchWithoutPatchQueueSubmit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		BranchBaseSHA:  strings.Repeat("a", 40),
		ReviewDocKey:   "project.project-subpixel.branch.branch-1.review",
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "ready for review"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected missing patch queue handoff to defer ordinary implementation completion, got applied=%v result=%+v", applied, gated)
	}
	for _, want := range []string{"project_patch_queue_submit", "Review packet evidence is not the queue handoff receipt", "READY_FOR_REVIEW", "project_id: project-subpixel", "repo_id: projrepo-1", "branch_id: branch-1", "review_doc_key: project.project-subpixel.branch.branch-1.review", `pathset_json: {"paths":["web/**"]}`, "controlled_queue: true"} {
		if !strings.Contains(gated.Details+"\n"+gated.NextAction, want) {
			t.Fatalf("expected patch queue handoff guidance to contain %q, got %+v", want, gated)
		}
	}
}

func TestProjectImplementationCompletionGateDemotesPatchQueueRevisionWithoutSubmit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
		PatchQueueItems: []map[string]any{
			projectCompletionGatePatchQueueItemForBranch("BLOCKED", "branch-source", strings.Repeat("1", 40), "old source branch still blocked", "2026-05-08T02:00:00Z"),
		},
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGatePatchQueueRevisionTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "ready for validation"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected missing patch queue handoff to defer completion, got applied=%v result=%+v", applied, gated)
	}
	for _, want := range []string{"project_patch_queue_submit", "Review packet evidence is not the queue handoff receipt", "validation-only"} {
		if !strings.Contains(gated.Details+"\n"+gated.NextAction, want) {
			t.Fatalf("expected patch queue handoff guidance to contain %q, got %+v", want, gated)
		}
	}
}

func TestProjectImplementationCompletionGateAllowsPatchQueueRevisionAfterSubmit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
		PatchQueueItems: []map[string]any{
			projectCompletionGatePatchQueueItem("PROPOSED", headSHA, "fresh revision candidate submitted", "2026-05-08T02:10:00Z"),
		},
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGatePatchQueueRevisionTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "submitted to patch queue"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if applied {
		t.Fatalf("expected submitted patch queue revision to pass completion gate, got %+v", gated)
	}
	if normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("expected completed outcome to remain, got %+v", gated)
	}
}

func TestProjectCompletionGateSkipsBranchGateForPatchQueueIntegrationConvergenceWithoutClaimEvidence(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-ambient-icon-sprite-forge-f8c1cf4e0a170765",
		Title:       "Integrate accepted Icon Sprite Forge patch candidate into canonical branch",
		Description: "Call project_patch_queue_integrate for the ACCEPTED patch queue item, then publish canonical validation evidence.",
		Status:      "RUNNING",
		TaskKind:    "EXECUTION",
		ProjectID:   "icon-sprite-forge",
		ProjectLane: "implementation",
		Tags:        []string{"patch-queue", "integration"},
	}
	if runtimeProjectCompletionEvidenceGateRequired(task) {
		t.Fatal("patch queue integration convergence without owned branch evidence should not require implementation branch publication gate")
	}

	task.ClaimWriteScopeJSON = stringPtr(`{"paths":["src/**"]}`)
	if !runtimeProjectCompletionEvidenceGateRequired(task) {
		t.Fatal("explicit write-scope evidence must still require implementation branch publication gate")
	}
}

// R49: a structured docs/spec-fidelity planning-evidence task (lane=implementation, review/synthesis
// work modes, docs/spec-fidelity tags, empty write-scope) must NOT require the implementation
// completion-evidence gate - otherwise it loops forever on an "owned git publication step" it can
// never produce (round-48 ambient planning carrier). Mirrors the R46 claim-admission bypass.
func TestProjectCompletionGateBypassesStructuredPlanningEvidenceTask(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-ambient-project-signal01-rq-s1-84dc75ae6d755732",
		Title:                "Materialize missing product_contract and plan_review planning evidence",
		Status:               "RUNNING",
		TaskKind:             "EXECUTION",
		ProjectID:            "project-signal01-rq-s1",
		ProjectLane:          "implementation",
		Tags:                 []string{"docs", "review", "spec-fidelity"},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","required_work_modes":["implementation","review","synthesis"]}`,
	}
	if runtimeProjectCompletionEvidenceGateRequired(task) {
		t.Fatal("structured docs/spec-fidelity planning-evidence task must not require the implementation completion-evidence gate (R49)")
	}

	// A real implementation task (write-scope present, no structured docs contract) must STILL gate.
	impl := task
	impl.Tags = []string{"implementation"}
	impl.TaskRequirementsJSON = `{"schema":"task_requirements.v1","required_work_modes":["implementation"]}`
	impl.WriteScopeHints = []string{"src/**"}
	if !runtimeProjectCompletionEvidenceGateRequired(impl) {
		t.Fatal("a real implementation task with write-scope must still require the completion-evidence gate")
	}
}

func TestProjectPatchQueueIntegrationAssemblyGateDemotesMissingFullProductValidation(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-integrate",
		Title:       "Integrate accepted patch queue candidate",
		Description: "Call project_patch_queue_integrate for the ACCEPTED patch queue item, then publish canonical validation evidence.",
		Status:      "RUNNING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-tools",
		ProjectLane: "integration",
		Tags:        []string{"patch-queue", "integration"},
	}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Integrated accepted lane",
		Details: "project_patch_queue_integrate completed target_head_after=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}

	gated, applied, err := runtime.enforceProjectPatchQueueIntegrationAssemblyGate(task, result, &TaskRunTrace{SuccessfulToolCalls: []string{"project_patch_queue_integrate"}})
	if err != nil {
		t.Fatalf("assembly gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected missing validation to defer completion, applied=%v result=%+v", applied, gated)
	}
	for _, want := range []string{"full_product_verdict", "build/test/smoke", "project_patch_queue_integrate"} {
		if !strings.Contains(gated.Details+"\n"+gated.NextAction, want) {
			t.Fatalf("expected integration assembly guidance to contain %q, got %+v", want, gated)
		}
	}
}

func TestProjectPatchQueueIntegrationAssemblyGateAllowsIntegratedBuildVerdict(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-integrate",
		Title:       "Integrate accepted patch queue candidate",
		Description: "Call project_patch_queue_integrate for the ACCEPTED patch queue item, then publish canonical validation evidence.",
		Status:      "RUNNING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-tools",
		ProjectLane: "integration",
		Tags:        []string{"patch-queue", "integration"},
	}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Integrated and validated assembled product",
		Details: strings.Join([]string{
			"integration_receipt: project.patch_queue.integrated",
			"canonical_target_head: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"build_or_test_command: go test ./...",
			"exit_code: 0",
			"full_product_verdict: pass",
		}, "\n"),
	}

	gated, applied, err := runtime.enforceProjectPatchQueueIntegrationAssemblyGate(task, result, &TaskRunTrace{SuccessfulToolCalls: []string{"project_patch_queue_integrate", "shell"}})
	if err != nil {
		t.Fatalf("assembly gate: %v", err)
	}
	if applied || normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("expected complete assembled validation to pass, applied=%v result=%+v", applied, gated)
	}
}

func TestProjectPatchQueueIntegrationAssemblyGateRequiresExplicitFullProductVerdict(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-integrate",
		Title:       "Integrate accepted patch queue candidate",
		Description: "Call project_patch_queue_integrate for the ACCEPTED patch queue item, then publish canonical validation evidence.",
		Status:      "RUNNING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-tools",
		ProjectLane: "integration",
		Tags:        []string{"patch-queue", "integration"},
	}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Integrated and smoke passed",
		Details: strings.Join([]string{
			"integration_receipt: project.patch_queue.integrated",
			"canonical_target_head: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"build_or_test_command: go test ./...",
			"exit_code: 0",
			"validation_verdict: pass",
			"tests passed",
		}, "\n"),
	}

	gated, applied, err := runtime.enforceProjectPatchQueueIntegrationAssemblyGate(task, result, &TaskRunTrace{SuccessfulToolCalls: []string{"project_patch_queue_integrate", "shell"}})
	if err != nil {
		t.Fatalf("assembly gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected missing explicit full_product_verdict to defer completion, applied=%v result=%+v", applied, gated)
	}
}

func TestProjectPatchQueueIntegrationAssemblyGateRequiresExplicitCommandEvidence(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-integrate",
		Title:       "Integrate accepted patch queue candidate",
		Description: "Call project_patch_queue_integrate for the ACCEPTED patch queue item, then publish canonical validation evidence.",
		Status:      "RUNNING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-tools",
		ProjectLane: "integration",
		Tags:        []string{"patch-queue", "integration"},
	}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Integrated and validated assembled product",
		Details: strings.Join([]string{
			"integration_receipt: project.patch_queue.integrated",
			"canonical_target_head: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"exit_code: 0",
			"full_product_verdict: pass",
		}, "\n"),
	}

	gated, applied, err := runtime.enforceProjectPatchQueueIntegrationAssemblyGate(task, result, &TaskRunTrace{SuccessfulToolCalls: []string{"project_patch_queue_integrate", "shell"}})
	if err != nil {
		t.Fatalf("assembly gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected missing explicit build/test/smoke command to defer completion, applied=%v result=%+v", applied, gated)
	}
}

// EP-02 (static-audit 2026-06-02): an integrator that actually merged (real
// project_patch_queue_integrate) AND ran `go test ./...` green (a non-error shell receipt
// with go-test pass lines) must complete even without hand-typing the literal
// `full_product_verdict: pass` token, which no tool emits.
func TestProjectPatchQueueIntegrationAssemblyGateAcceptsVerifiedTraceTestPass(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-integrate",
		Title:       "Integrate accepted patch queue candidate",
		Status:      "RUNNING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-tools",
		ProjectLane: "integration",
		Tags:        []string{"patch-queue", "integration"},
	}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Integrated the accepted lane into canonical main and ran the full suite green.",
		Details: "project_patch_queue_integrate recorded; assembled product builds and passes.",
	}
	trace := &TaskRunTrace{
		SuccessfulToolCalls: []string{"project_patch_queue_integrate", "shell"},
		ToolReceipts: []TaskRunToolReceipt{
			{ToolName: "project_patch_queue_integrate", IsError: false, Output: "integrated"},
			{ToolName: "shell", IsError: false, Output: "ok  \tgithub.com/example/rq/internal/lexer\t0.123s\nok  \tgithub.com/example/rq/cmd/rq\t0.045s\n"},
		},
	}

	gated, applied, err := runtime.enforceProjectPatchQueueIntegrationAssemblyGate(task, result, trace)
	if err != nil {
		t.Fatalf("assembly gate: %v", err)
	}
	if applied || normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("expected verified trace test-pass to satisfy the assembly gate, applied=%v result=%+v", applied, gated)
	}
}

// EP-02 safety: a FAILING test run (shell receipt IsError=true, the shell tool flags any
// non-zero exit) must NOT satisfy the verdict — a broken product cannot complete via the
// trace path, even with a real integration receipt.
func TestProjectPatchQueueIntegrationAssemblyGateAcceptsGoNoTestFilesTracePass(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-integrate",
		Title:       "Integrate accepted patch queue candidate",
		Status:      "RUNNING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-tools",
		ProjectLane: "integration",
		Tags:        []string{"patch-queue", "integration"},
	}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Integrated the accepted lane into canonical main and ran go test.",
		Details: "project_patch_queue_integrate recorded; assembled product build/test command completed.",
	}
	trace := &TaskRunTrace{
		SuccessfulToolCalls: []string{"project_patch_queue_integrate", "shell"},
		ToolReceipts: []TaskRunToolReceipt{
			{ToolName: "project_patch_queue_integrate", IsError: false, Output: `{"integrated":true}`},
			{ToolName: "shell", IsError: false, Output: "8bd01d68fc88b82ced6d349b88fe160b7564c37d\n?   \tsignal01fixture/cmd/rq\t[no test files]\n?   \tsignal01fixture/internal/parser\t[no test files]\n?   \tsignal01fixture/internal/evaluator\t[no test files]\n"},
		},
	}

	gated, applied, err := runtime.enforceProjectPatchQueueIntegrationAssemblyGate(task, result, trace)
	if err != nil {
		t.Fatalf("assembly gate: %v", err)
	}
	if applied || normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("expected go test no-test-files trace to satisfy the assembly gate, applied=%v result=%+v", applied, gated)
	}
}

func TestProjectPatchQueueIntegrationAssemblyGateRejectsFailedTraceTestRun(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-integrate",
		Title:       "Integrate accepted patch queue candidate",
		Status:      "RUNNING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-tools",
		ProjectLane: "integration",
		Tags:        []string{"patch-queue", "integration"},
	}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Integrated the accepted lane; ran go test.",
		Details: "project_patch_queue_integrate recorded.",
	}
	trace := &TaskRunTrace{
		SuccessfulToolCalls: []string{"project_patch_queue_integrate"},
		ToolReceipts: []TaskRunToolReceipt{
			{ToolName: "project_patch_queue_integrate", IsError: false, Output: "integrated"},
			{ToolName: "shell", IsError: true, Output: "ok  \tgithub.com/example/rq/internal/lexer\t0.1s\nFAIL\tgithub.com/example/rq/internal/eval\t0.2s\nexit status 1"},
		},
	}

	gated, applied, err := runtime.enforceProjectPatchQueueIntegrationAssemblyGate(task, result, trace)
	if err != nil {
		t.Fatalf("assembly gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected a failed trace test run to defer completion, applied=%v result=%+v", applied, gated)
	}
}

// EP-03 (static-audit 2026-06-02): an incidental narrative phrase ("validation not run")
// must not force-demote a completion that carries a real pass verdict + evidence. Only a
// verdict-keyed negative may defer.
func TestProjectPatchQueueIntegrationAssemblyGateIgnoresIncidentalNegativeNarrative(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-integrate",
		Title:       "Integrate accepted patch queue candidate",
		Status:      "RUNNING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-tools",
		ProjectLane: "integration",
		Tags:        []string{"patch-queue", "integration"},
	}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Integrated and validated assembled product",
		Details: strings.Join([]string{
			"Earlier the validation was not run; this cycle resolves the missing full-product evidence.",
			"integration_receipt: project.patch_queue.integrated",
			"build_or_test_command: go test ./...",
			"full_product_verdict: pass",
		}, "\n"),
	}

	gated, applied, err := runtime.enforceProjectPatchQueueIntegrationAssemblyGate(task, result, &TaskRunTrace{SuccessfulToolCalls: []string{"project_patch_queue_integrate", "shell"}})
	if err != nil {
		t.Fatalf("assembly gate: %v", err)
	}
	if applied || normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("expected incidental negative narrative to be ignored when a real pass verdict is present, applied=%v result=%+v", applied, gated)
	}
}

func TestProjectImplementationCompletionGateDemotesSameHeadRejectedPatchQueueDecision(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:         "agent-alpha",
		RepoURL:         remote,
		BranchStatus:    "READY_FOR_REVIEW",
		BranchHeadSHA:   headSHA,
		WriteScopeJSON:  `{"paths":["web/**"]}`,
		CheckoutPath:    checkoutPath,
		PatchQueueItems: []map[string]any{projectCompletionGatePatchQueueItem("REJECTED", headSHA, "UI advertises 24x24 but renders 16x16", "2026-05-08T02:00:00Z")},
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected same-head rejection to defer completion, got applied=%v result=%+v", applied, gated)
	}
	for _, want := range []string{"REJECTED", "24x24", "new commit/head", "project_patch_queue_submit", "revision follow-up"} {
		if !strings.Contains(gated.Details+"\n"+gated.NextAction, want) {
			t.Fatalf("expected rejected decision guidance to contain %q, got %+v", want, gated)
		}
	}
}

func TestProjectImplementationCompletionGateDemotesSameHeadBlockedPatchQueueDecision(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:         "agent-alpha",
		RepoURL:         remote,
		BranchStatus:    "READY_FOR_REVIEW",
		BranchHeadSHA:   headSHA,
		WriteScopeJSON:  `{"paths":["web/**"]}`,
		CheckoutPath:    checkoutPath,
		PatchQueueItems: []map[string]any{projectCompletionGatePatchQueueItem("BLOCKED", headSHA, "Missing browser smoke evidence", "2026-05-08T02:00:00Z")},
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected same-head blocked decision to defer completion, got applied=%v result=%+v", applied, gated)
	}
	for _, want := range []string{"BLOCKED", "browser smoke", "evidence", "patch queue follow-up"} {
		if !strings.Contains(gated.Details+"\n"+gated.NextAction, want) {
			t.Fatalf("expected blocked decision guidance to contain %q, got %+v", want, gated)
		}
	}
}

func TestProjectImplementationCompletionGateIgnoresOldHeadRejectedPatchQueueDecision(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
		PatchQueueItems: []map[string]any{
			projectCompletionGatePatchQueueItem("REJECTED", strings.Repeat("1", 40), "old rejected head", "2026-05-08T02:00:00Z"),
			projectCompletionGatePatchQueueItem("PROPOSED", headSHA, "current head queued", "2026-05-08T02:10:00Z"),
		},
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if applied || normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("expected old-head rejection not to block current completion, got applied=%v result=%+v", applied, gated)
	}
}

func TestProjectImplementationCompletionGateIgnoresAcceptedPatchQueueDecision(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:         "agent-alpha",
		RepoURL:         remote,
		BranchStatus:    "READY_FOR_REVIEW",
		BranchHeadSHA:   headSHA,
		WriteScopeJSON:  `{"paths":["web/**"]}`,
		CheckoutPath:    checkoutPath,
		PatchQueueItems: []map[string]any{projectCompletionGatePatchQueueItem("ACCEPTED", headSHA, "accepted for integration", "2026-05-08T02:00:00Z")},
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if applied || normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("expected accepted decision not to block completion, got applied=%v result=%+v", applied, gated)
	}
}

func TestProjectImplementationCompletionGateDemotesDirtyCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const dirty = true;\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected dirty checkout to defer completion into continue, got applied=%v result=%+v", applied, gated)
	}
	if !strings.Contains(gated.Details, "uncommitted changes") {
		t.Fatalf("expected dirty-checkout detail, got %+v", gated)
	}
	if len(gated.BlockedOn) != 0 {
		t.Fatalf("expected self-actionable git evidence gap to stay out of blocked_on, got %+v", gated.BlockedOn)
	}
	if !strings.Contains(gated.NextAction, "project_branch_commit") || !strings.Contains(gated.NextAction, "project_branch_review_ready") {
		t.Fatalf("expected concrete git publication next action, got %+v", gated)
	}
}

func TestProjectImplementationCompletionGatePrefersClaimCheckoutOverStaleBranchCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, claimCheckoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	staleCheckoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "stale-default")
	runGitNoDir(t, "clone", remote, staleCheckoutPath)
	runGit(t, staleCheckoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/stale-default")
	if err := os.MkdirAll(filepath.Join(staleCheckoutPath, "internal", "eval"), 0o755); err != nil {
		t.Fatalf("mkdir stale dirty path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staleCheckoutPath, "internal", "eval", "eval_contract_test.go"), []byte("package eval\n"), 0o644); err != nil {
		t.Fatalf("write stale dirty file: %v", err)
	}

	coordination := branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   claimCheckoutPath,
		PatchQueueItems: []map[string]any{
			projectCompletionGatePatchQueueItem("PROPOSED", headSHA, "fresh implementation candidate submitted", "2026-05-08T02:10:00Z"),
		},
	})
	body := coordination["coordination"].(map[string]any)
	body["checkouts"] = []map[string]any{
		{
			"checkout_id":     "checkout-1",
			"workspace_id":    "ws",
			"project_id":      "project-subpixel",
			"repo_id":         "projrepo-1",
			"agent_id":        "agent-alpha",
			"local_path":      claimCheckoutPath,
			"branch_name":     "agent/agent-alpha/project-subpixel/task-build",
			"active_task_id":  "task-build",
			"active_claim_id": "task-build",
			"dirty_state":     "clean",
			"status":          "ACTIVE",
		},
		{
			"checkout_id":     "checkout-stale",
			"workspace_id":    "ws",
			"project_id":      "project-subpixel",
			"repo_id":         "projrepo-1",
			"agent_id":        "agent-alpha",
			"local_path":      staleCheckoutPath,
			"branch_name":     "agent/agent-alpha/project-subpixel/stale-default",
			"active_task_id":  "task-build",
			"active_claim_id": "task-build",
			"dirty_state":     "dirty",
			"status":          "ACTIVE",
		},
	}
	branches := body["branches"].([]map[string]any)
	branches[0]["checkout_id"] = "checkout-stale"

	server := newProjectCompletionGateServer(t, coordination)
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if applied || normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("stale branch checkout must not override clean task claim checkout, applied=%v result=%+v", applied, gated)
	}
	if directive := runtime.projectImplementationUncommittedCommitDirective(context.Background(), task); directive != "" {
		t.Fatalf("stale branch checkout must not create an uncommitted-work directive for the clean claim checkout, got %q", directive)
	}
}

// TestProjectImplementationUncommittedCommitDirectiveOnDirtyCheckout is the G-SUB-1
// regression: on the continue path (not completion), an implementer with uncommitted
// product work in its owned checkout must get a deterministic project_branch_commit
// directive surfaced into its next-cycle advisory context.
func TestProjectImplementationUncommittedCommitDirectiveOnDirtyCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const dirty = true;\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-alpha", Workdir: workdir},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()

	// Dirty checkout + write_file in trace -> directive surfaces.
	directive := runtime.projectImplementationUncommittedCommitDirective(context.Background(), task)
	if !strings.Contains(directive, "project_branch_commit") || !strings.Contains(directive, "UNCOMMITTED WORK") {
		t.Fatalf("expected uncommitted-work commit directive, got %q", directive)
	}
	if !traceTouchedProjectFiles(&TaskRunTrace{SuccessfulToolCalls: []string{"shell", "write_file"}}) {
		t.Fatalf("traceTouchedProjectFiles must detect write_file")
	}
	if !traceTouchedProjectFiles(&TaskRunTrace{SuccessfulToolCalls: []string{"shell", "workspace_doc_put"}}) {
		t.Fatalf("traceTouchedProjectFiles must treat shell as a possible project checkout mutation")
	}
	if traceTouchedProjectFiles(&TaskRunTrace{SuccessfulToolCalls: []string{"workspace_doc_put"}}) {
		t.Fatalf("traceTouchedProjectFiles must not fire on doc-only progress")
	}
}

func TestRuntimeContinueTaskCycleShellDirtyCheckoutQueuesCommitDirective(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, baseSHA := setupProjectCompletionGateCheckoutUnpublished(t)
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const dirty = true;\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}

	var stateValues []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:        "agent-alpha",
				RepoURL:        remote,
				BranchStatus:   "ACTIVE",
				BranchBaseSHA:  baseSHA,
				ActiveTaskID:   "task-build",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case "agent.session.status":
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":          rpcString(req.Params, "session_id"),
				"workspace_id":        "ws",
				"agent_id":            "agent-alpha",
				"task_id":             "task-build",
				"status":              "ACTIVE",
				"summary":             rpcString(req.Params, "summary"),
				"keep_session_active": true,
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": "run-build"}})
		case "agent.state.set":
			stateValues = append(stateValues, rpcString(req.Params, "value"))
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "agent-alpha",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
			PlannerEvery:     45 * time.Second,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	task := projectCompletionGateTask()
	session := AgentSessionStateRecord{SessionID: "session-build", AgentID: "agent-alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{ToolCalls: []string{"shell"}, SuccessfulToolCalls: []string{"shell"}}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-build", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Edited product files from the shell",
		NextAction: "Run one more check before publishing.",
	}, trace)
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if len(stateValues) == 0 {
		t.Fatal("expected scratch state to be saved")
	}
	raw := stateValues[len(stateValues)-1]
	if !strings.Contains(raw, "UNCOMMITTED WORK") || !strings.Contains(raw, "project_branch_commit") {
		t.Fatalf("expected saved advisory commit directive, got %s", raw)
	}
	if !strings.Contains(raw, `"pending_trigger":"request_resume"`) {
		t.Fatalf("dirty shell checkout should queue immediate resume, got %s", raw)
	}
	if strings.Contains(raw, `"continuation_hold_task_id":"task-build"`) {
		t.Fatalf("immediate dirty-checkout resume should clear continuation hold, got %s", raw)
	}
}

// TestProjectImplementationUncommittedCommitDirectiveCleanCheckoutIsSilent: a clean
// (committed) checkout produces no directive — the nudge only fires on real dirty work.
func TestProjectImplementationUncommittedCommitDirectiveCleanCheckoutIsSilent(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	// No extra writes -> checkout is clean (committed by the harness).
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-alpha", Workdir: workdir},
		client: NewRhizomeClient(server.URL, "token"),
	}
	if d := runtime.projectImplementationUncommittedCommitDirective(context.Background(), projectCompletionGateTask()); d != "" {
		t.Fatalf("clean checkout must produce no commit directive, got %q", d)
	}
}

func TestProjectImplementationRecoverableFailureGateBlocksDirtyCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const dirty = true;\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "ACTIVE",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "failed", Summary: "browser smoke timed out"}

	gated, applied, err := runtime.enforceProjectImplementationRecoverableFailureGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("recoverable failure gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "blocked" {
		t.Fatalf("expected dirty failed implementation to become blocked recovery, applied=%v result=%+v", applied, gated)
	}
	for _, want := range []string{"recoverable_project_work", "uncommitted", "project_branch_commit", "project_branch_review_ready"} {
		if !strings.Contains(gated.Details+"\n"+gated.NextAction+blockedDetails(gated.BlockedOn), want) {
			t.Fatalf("expected recoverable failure guidance to contain %q, got %+v", want, gated)
		}
	}
}

func TestTrustFirstProjectImplementationCompletionGateStillRequiresBranchPublication(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckoutUnpublished(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "agent-alpha",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done without review-ready branch yet"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("trust-first must defer project completion until branch publication evidence is current, applied=%v result=%+v", applied, gated)
	}
	if !strings.Contains(gated.Details+"\n"+gated.NextAction, "not published") || !strings.Contains(gated.NextAction, "project_branch_commit") {
		t.Fatalf("expected hard publication guidance for unpublished branch evidence, got %+v", gated)
	}
}

func TestTrustFirstProjectImplementationCompletionGateStillDefersNegativePatchQueueDecision(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
		PatchQueueItems: []map[string]any{
			projectCompletionGatePatchQueueItem("BLOCKED", headSHA, "Setup failed during browser smoke", "2026-05-08T02:00:00Z"),
		},
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "agent-alpha",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done despite smoke failure"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("trust-first should still defer current-head negative product evidence, applied=%v result=%+v", applied, gated)
	}
	if !strings.Contains(gated.Details+"\n"+gated.NextAction, "Setup failed") {
		t.Fatalf("expected concrete patch queue failure to remain visible, got %+v", gated)
	}
}

func TestTrustFirstProjectImplementationCompletionGateUsesLatestTerminalPatchQueueDecision(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
		PatchQueueItems: []map[string]any{
			projectCompletionGatePatchQueueItem("BLOCKED", headSHA, "older browser smoke gap", "2026-05-08T02:00:00Z"),
			projectCompletionGatePatchQueueItem("ACCEPTED", headSHA, "newer evidence accepted", "2026-05-08T02:10:00Z"),
		},
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "agent-alpha",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done after accepted evidence"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if applied || normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("newer accepted same-head decision should supersede older negative decision, applied=%v result=%+v", applied, gated)
	}
}

func TestProjectImplementationCompletionGateDemotesStaleHeadEvidence(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, registeredHead := setupProjectCompletionGateCheckout(t)
	runGit(t, checkoutPath, "config", "user.name", "Rhizome Test")
	runGit(t, checkoutPath, "config", "user.email", "rhizome-test@example.invalid")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const revised = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}
	runGit(t, checkoutPath, "add", "web/app.js")
	runGit(t, checkoutPath, "commit", "-m", "Unpublished revision")
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got != "" {
		t.Fatalf("expected clean checkout after unpublished revision, got %q", got)
	}

	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  registeredHead,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected stale HEAD evidence to defer completion into continue, got applied=%v result=%+v", applied, gated)
	}
	if !strings.Contains(gated.Details, "does not match READY_FOR_REVIEW branch evidence") {
		t.Fatalf("expected stale-head detail, got %+v", gated)
	}
}

func TestProjectImplementationCompletionGateDemotesRemoteHeadMismatch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, registeredHead := setupProjectCompletionGateCheckout(t)
	other := filepath.Join(t.TempDir(), "other")
	runGitNoDir(t, "clone", remote, other)
	runGit(t, other, "checkout", "agent/agent-alpha/project-subpixel/task-build")
	runGit(t, other, "config", "user.name", "Rhizome Test")
	runGit(t, other, "config", "user.email", "rhizome-test@example.invalid")
	if err := os.MkdirAll(filepath.Join(other, "web"), 0o755); err != nil {
		t.Fatalf("mkdir other web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(other, "web", "remote.js"), []byte("export const remote = true;\n"), 0o644); err != nil {
		t.Fatalf("write remote revision: %v", err)
	}
	runGit(t, other, "add", "web/remote.js")
	runGit(t, other, "commit", "-m", "Remote branch advanced")
	runGit(t, other, "push", "origin", "agent/agent-alpha/project-subpixel/task-build")

	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  registeredHead,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceProjectImplementationCompletionEvidenceGate(context.Background(), task, result)
	if err != nil {
		t.Fatalf("completion evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected remote mismatch to defer completion, got applied=%v result=%+v", applied, gated)
	}
	if !strings.Contains(gated.Details, "remote head") || !strings.Contains(gated.Details, "does not match") {
		t.Fatalf("expected remote-head mismatch detail, got %+v", gated)
	}
}

func TestProjectImplementationGitEvidenceBlockIsSelfActionableContinue(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckout(t)
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const dirty = true;\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{
		Outcome:    "blocked",
		Summary:    "Evidence gap remains",
		Details:    "project_branch_commit and project_branch_review_ready are still missing",
		NextAction: "call project_branch_commit with push=true, then project_branch_review_ready",
		BlockedOn: []BlockedRef{{
			Kind:   "dependency",
			Detail: "git evidence is missing",
		}},
		Materialize: TaskMaterialization{
			DocKey:     "task.task-build.evidence_gap",
			DocTitle:   "Evidence Gap",
			DocContent: "Need git evidence before completion.",
		},
	}

	gated, applied, err := runtime.enforceProjectImplementationSelfActionableGitEvidenceBlock(context.Background(), task, result)
	if err != nil {
		t.Fatalf("self-actionable git evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected self-actionable git evidence block to become continue, got applied=%v result=%+v", applied, gated)
	}
	if len(gated.BlockedOn) != 0 || gated.Materialize.DocKey != "" {
		t.Fatalf("expected blocker/materialized evidence gap to be cleared, got %+v", gated)
	}
}

func TestTrustFirstProjectImplementationGitEvidenceBlockBecomesSelfActionableContinue(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "agent-alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
	}
	task := projectCompletionGateTask()
	result := StructuredTaskResult{
		Outcome:    "blocked",
		Summary:    "Evidence gap remains",
		Details:    "project_branch_commit and project_branch_review_ready are still missing",
		NextAction: "call project_branch_commit with push=true, then project_branch_review_ready",
		BlockedOn: []BlockedRef{{
			Kind:   "dependency",
			Detail: "git evidence is missing",
		}},
	}

	gated, applied, err := runtime.enforceProjectImplementationSelfActionableGitEvidenceBlock(context.Background(), task, result)
	if err != nil {
		t.Fatalf("self-actionable git evidence gate: %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("trust-first should turn self-actionable git blocker into continue, applied=%v result=%+v", applied, gated)
	}
	if len(gated.BlockedOn) != 0 {
		t.Fatalf("self-actionable blocker should be cleared, got %+v", gated.BlockedOn)
	}
	if !strings.Contains(gated.Details, "trust_first does not make code ownership/review evidence optional") {
		t.Fatalf("expected hard implementation evidence guidance for trust-first git evidence block, got %+v", gated)
	}
	if !strings.Contains(gated.NextAction, "project_branch_commit") || !strings.Contains(gated.NextAction, "project_branch_review_ready") {
		t.Fatalf("trust-first implementation ownership evidence should prescribe publication loop, got %+v", gated)
	}
}

func TestRecoverProjectImplementationExecutionErrorQueuesOwnerResumeForDirtyCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir, checkoutPath, remote, baseSHA := setupProjectCompletionGateCheckoutUnpublished(t)
	if err := os.WriteFile(filepath.Join(checkoutPath, "dirty.txt"), []byte("unfinished product work"), 0o644); err != nil {
		t.Fatalf("dirty checkout: %v", err)
	}

	var methods []string
	var stateValues []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:        "agent-alpha",
				RepoURL:        remote,
				BranchStatus:   "ACTIVE",
				BranchBaseSHA:  baseSHA,
				ActiveTaskID:   "task-build",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case "workspace.execution.step.write":
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-recover"}})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-recover"})
		case "agent.update.post":
			writeRPCResult(w, req, nil)
		case "agent.session.status":
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":   "session-build",
				"workspace_id": "ws",
				"agent_id":     "agent-alpha",
				"task_id":      "task-build",
				"status":       "ACTIVE",
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": "run-build"}})
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.ops.resolve":
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			stateValues = append(stateValues, rpcString(req.Params, "value"))
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "agent-alpha",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
			PlannerEvery:     45 * time.Second,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	task := projectCompletionGateTask()
	session := AgentSessionStateRecord{SessionID: "session-build", AgentID: "agent-alpha", TaskID: task.TaskID, Status: "ACTIVE"}

	recovered, err := runtime.recoverProjectImplementationExecutionError(context.Background(), task, session, "run-build", context.DeadlineExceeded, nil)
	if err != nil {
		t.Fatalf("recover execution error: %v", err)
	}
	if !recovered {
		t.Fatal("expected dirty project checkout to be recovered")
	}
	if !containsAll(methods, []string{"project.coordination.get", "workspace.execution.step.write", "agent.session.status", "workspace.execution.run.write", "agent.state.set"}) {
		t.Fatalf("expected recovery to write evidence and keep session active, got methods=%v", methods)
	}
	if len(stateValues) == 0 || !strings.Contains(stateValues[len(stateValues)-1], `"pending_trigger":"request_resume"`) {
		t.Fatalf("expected recovery to queue immediate request_resume, got state values=%v", stateValues)
	}
}

func setupProjectCompletionGateCheckout(t *testing.T) (string, string, string, string) {
	workdir, checkoutPath, remote, headSHA := setupProjectCompletionGateCheckoutUnpublished(t)
	runGit(t, checkoutPath, "push", "-u", "origin", "agent/agent-alpha/project-subpixel/task-build")
	return workdir, checkoutPath, remote, headSHA
}

func setupProjectCompletionGateCheckoutUnpublished(t *testing.T) (string, string, string, string) {
	t.Helper()
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	headSHA := gitOutput(t, checkoutPath, "rev-parse", "HEAD")
	return workdir, checkoutPath, remote, headSHA
}

func TestProjectImplementationPublicationDirectiveNudgesReviewReadyForCommittedBranch(t *testing.T) {
	// AG-01: a committed (ACTIVE) owned branch that is not yet READY_FOR_REVIEW gets a
	// deterministic review-ready nudge on the continue path. Pure coordination data, no git.
	head := strings.Repeat("a", 40)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:       "agent-alpha",
		BranchStatus:  "ACTIVE",
		BranchHeadSHA: head,
	}))
	defer server.Close()
	runtime := &Runtime{cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-alpha"}, client: NewRhizomeClient(server.URL, "token")}
	directive := runtime.projectImplementationPublicationDirective(context.Background(), projectCompletionGateTask())
	if !strings.Contains(directive, "project_branch_review_ready") {
		t.Fatalf("expected committed ACTIVE branch to nudge project_branch_review_ready, got %q", directive)
	}
}

func TestPostReceiptTerminalCompletionGateDemotesSideEffectSuccessorWithActiveBranch(t *testing.T) {
	head := strings.Repeat("d", 40)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:       "agent-alpha",
		BranchStatus:  "ACTIVE",
		BranchHeadSHA: head,
	}))
	defer server.Close()
	runtime := &Runtime{cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-alpha"}, client: NewRhizomeClient(server.URL, "token")}
	task := projectCompletionGateTask()
	task.TaskID = "task-side-effect-publish-branch"
	task.ProjectLane = "implementation"
	task.TaskRequirementsJSON = `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_cleanup","action_kind":"quarantine_bucket","successor_key":"abpc-resolution-successor:publish","decision":"quarantine"}`
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"quarantine","followup_task_id":"task-side-effect-publish-branch","followup_created":false,"successor_key":"abpc-resolution-successor:publish","next_transition":"quarantine_materialization"}`,
		}},
	}

	completed := completeSideEffectResolutionSuccessorReceipt(task, StructuredTaskResult{Outcome: "continue", Summary: "Side effect quarantined."}, trace)
	if normalizeOutcome(completed.Outcome) != "completed" {
		t.Fatalf("test setup expected receipt-induced completion, got %+v", completed)
	}
	gated, err := runtime.enforcePostReceiptTerminalCompletionEvidenceGate(context.Background(), task, AgentSessionStateRecord{}, "", completed, trace, nil)
	if err != nil {
		t.Fatalf("post-receipt completion gate: %v", err)
	}
	if normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("receipt-induced branch-backed completion should be demoted, got %+v", gated)
	}
	if !strings.Contains(gated.NextAction, "project_branch_review_ready") {
		t.Fatalf("expected review-ready next action after post-receipt demotion, got %+v", gated)
	}
}

func TestPostReceiptClassifierCompletionReRunsCoordinationGate(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-classify-beta",
		Title:                "Classify side effect before integration",
		Description:          "Review side-effect classification and coordinate peer review before completion.",
		Priority:             "high",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		ProjectLane:          "coordination",
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-beta", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{ToolReceipts: []TaskRunToolReceipt{{
		ToolName: "side_effect_resolve",
		Output:   `{"status":"decision_recorded","decision":"expand_boundary","classification_task_id":"task-side-effect-classify-beta","side_effect_refs":["side-effect:clearpress"],"boundary_transition_state":"authority_transition_applied","authority_transition_applied":true,"transition_executed":true,"existing_task_id":"task-role-scope-c3f628c747"}`,
	}}}

	completed := completeSideEffectClassifierTerminalDecision(task, StructuredTaskResult{Outcome: "continue", Summary: "Authority transition applied."}, trace)
	if normalizeOutcome(completed.Outcome) != "completed" {
		t.Fatalf("test setup expected receipt-induced completion, got %+v", completed)
	}

	var methods []string
	var gateSteps int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "workspace.agents.list":
			writeRPCResult(w, req, map[string]any{"agents": []any{}})
		case "workspace.execution.step.write":
			gateSteps++
			if got := rpcString(req.Params, "title"); got != "Defer completion pending peer coordination" {
				t.Fatalf("unexpected gate step title %q", got)
			}
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-post-receipt-coordination"}})
		case "agent.request":
			t.Fatalf("no peer is available; post-receipt gate must demote without queuing a request")
		default:
			t.Fatalf("unexpected method in post-receipt coordination gate path: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   server.URL,
		RhizomeToken: "token",
		WorkspaceID:  "ws-1",
		AgentID:      "agent-beta",
	}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	t.Cleanup(func() { _ = runtime.Close() })

	gated, err := runtime.enforcePostReceiptTerminalCompletionEvidenceGate(context.Background(), task, session, "run-beta", completed, trace, nil)
	if err != nil {
		t.Fatalf("post-receipt completion gate: %v", err)
	}
	if normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("receipt-induced classifier completion should be demoted by coordination gate, got %+v", gated)
	}
	if !strings.Contains(gated.Summary, "no peer available") {
		t.Fatalf("expected no-peer coordination demotion, got %+v", gated)
	}
	if gateSteps != 1 || !containsTrimmed(methods, "workspace.execution.step.write") {
		t.Fatalf("expected one durable post-receipt coordination step, steps=%d methods=%v", gateSteps, methods)
	}
}

func TestPostReceiptAuthorityDenialCompletionReRunsCoordinationGate(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:       "task-role-scope-gamma",
		Title:        "Review broad role scope conflict",
		Description:  "Coordinate review of a role-scope denial before terminal completion.",
		Priority:     "high",
		TaskKind:     "COORDINATION",
		TaskTemplate: "generic",
		ProjectLane:  "coordination",
		Tags:         []string{"project-role-scope"},
	}
	session := AgentSessionStateRecord{SessionID: "session-role-scope", TaskID: task.TaskID, Status: "ACTIVE"}
	packet := &AgentWorkPacket{PreferredTransition: "project_role_assign"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_role_assign"},
		FailedToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			IsError:  true,
			Output:   `{"boundary_transition_state":"ready_for_review_branch_rebind_blocked","transition_denied":true,"denial_recorded":true,"preferred_transition":"project_patch_queue_followup_or_revision_lane"}`,
		}},
	}

	completed := completeRecordedAuthorityTransitionDenial(task, StructuredTaskResult{Outcome: "continue", Summary: "Role-scope denial recorded."}, trace, packet)
	if normalizeOutcome(completed.Outcome) != "completed" {
		t.Fatalf("test setup expected authority denial receipt-induced completion, got %+v", completed)
	}

	var gateSteps int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.agents.list":
			writeRPCResult(w, req, map[string]any{"agents": []any{}})
		case "workspace.execution.step.write":
			gateSteps++
			if got := rpcString(req.Params, "title"); got != "Defer completion pending peer coordination" {
				t.Fatalf("unexpected gate step title %q", got)
			}
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-role-scope-post-receipt"}})
		case "agent.request":
			t.Fatalf("no peer is available; post-receipt authority denial gate must demote without queuing a request")
		default:
			t.Fatalf("unexpected method in post-receipt authority-denial gate path: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   server.URL,
		RhizomeToken: "token",
		WorkspaceID:  "ws-1",
		AgentID:      "agent-gamma",
	}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	t.Cleanup(func() { _ = runtime.Close() })

	gated, err := runtime.enforcePostReceiptTerminalCompletionEvidenceGate(context.Background(), task, session, "run-role-scope", completed, trace, packet)
	if err != nil {
		t.Fatalf("post-receipt authority-denial gate: %v", err)
	}
	if normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("receipt-induced authority denial completion should be demoted by coordination gate, got %+v", gated)
	}
	if !strings.Contains(gated.Summary, "no peer available") || gateSteps != 1 {
		t.Fatalf("expected no-peer coordination demotion with one step, steps=%d result=%+v", gateSteps, gated)
	}
}

func TestPatchQueueIntegrationMatcherExcludesRoleScopeAuthorityCarrier(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-epsilon",
		ProjectID:            "project-rq",
		Title:                "Adjust epsilon integration authority",
		Description:          "Fresh authority-transition phrasing without legacy role/scope literals.",
		TaskKind:             "COORDINATION",
		ProjectLane:          "coordination",
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign"}`,
	}
	if runtimeProjectTaskLooksPatchQueueIntegrationConvergence(task) {
		t.Fatalf("role-scope authority carrier must not be treated as patch queue integration convergence")
	}
}

func TestPatchQueueIntegrationMatcherRequiresStructuredIdentity(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-coordination-patchq-text-only",
		ProjectID:   "project-rq",
		Title:       "Coordinate accepted patch queue candidate",
		Description: "Accepted patch queue candidate needs canonical integration, but this is ordinary coordination prose without a typed integration identity.",
		TaskKind:    "COORDINATION",
		ProjectLane: "coordination",
		Tags:        []string{"coordination"},
	}
	if runtimeProjectTaskLooksPatchQueueIntegrationConvergence(task) {
		t.Fatalf("text-only patch queue/integration prose must not classify as convergence")
	}
}

func TestProjectImplementationPublicationDirectiveNudgesSubmitForUnsubmittedReadyBranch(t *testing.T) {
	// AG-02: a READY_FOR_REVIEW branch with no patch queue item for its head gets a submit nudge.
	head := strings.Repeat("b", 40)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		BranchStatus:   "READY_FOR_REVIEW",
		BranchHeadSHA:  head,
		ReviewDocKey:   "project.project-subpixel.branch.branch-1.review",
		WriteScopeJSON: `{"paths":["cmd/rq/**","README.md"]}`,
	}))
	defer server.Close()
	runtime := &Runtime{cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-alpha"}, client: NewRhizomeClient(server.URL, "token")}
	directive := runtime.projectImplementationPublicationDirective(context.Background(), projectCompletionGateTask())
	if !strings.Contains(directive, "project_patch_queue_submit") {
		t.Fatalf("expected unsubmitted READY_FOR_REVIEW branch to nudge project_patch_queue_submit, got %q", directive)
	}
	for _, want := range []string{"project_id: project-subpixel", "repo_id: projrepo-1", "branch_id: branch-1", "review_doc_key: project.project-subpixel.branch.branch-1.review", `pathset_json: {"paths":["cmd/rq/**","README.md"]}`, "controlled_queue: true"} {
		if !strings.Contains(directive, want) {
			t.Fatalf("expected submit directive to expose %q, got %q", want, directive)
		}
	}
}

func TestProjectImplementationPublicationDirectiveSilentWhenAlreadySubmitted(t *testing.T) {
	// AG-02 negative: a patch queue item already exists for the head -> no nudge.
	head := strings.Repeat("c", 40)
	server := newProjectCompletionGateServer(t, branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:       "agent-alpha",
		BranchStatus:  "READY_FOR_REVIEW",
		BranchHeadSHA: head,
		PatchQueueItems: []map[string]any{
			projectCompletionGatePatchQueueItem("PROPOSED", head, "candidate submitted", "2026-05-08T02:10:00Z"),
		},
	}))
	defer server.Close()
	runtime := &Runtime{cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-alpha"}, client: NewRhizomeClient(server.URL, "token")}
	directive := runtime.projectImplementationPublicationDirective(context.Background(), projectCompletionGateTask())
	if directive != "" {
		t.Fatalf("expected no nudge when a patch queue item already exists, got %q", directive)
	}
}

func newProjectCompletionGateServer(t *testing.T, coordination map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, coordination)
	}))
}

func projectCompletionGateTask() WorkspaceTaskRecord {
	return WorkspaceTaskRecord{
		TaskID:              "task-build",
		Title:               "Build project slice",
		Status:              "RUNNING",
		TaskKind:            "EXECUTION",
		ProjectID:           "project-subpixel",
		ProjectLane:         "implementation",
		ClaimBranchID:       stringPtr("branch-1"),
		ClaimCheckoutID:     stringPtr("checkout-1"),
		ClaimWriteScopeJSON: stringPtr(`{"paths":["web/**"]}`),
	}
}

func projectCompletionGatePatchQueueRevisionTask() WorkspaceTaskRecord {
	task := projectCompletionGateTask()
	task.TaskID = "task-revise-blocked-patch"
	task.Title = "Revise blocked patch queue candidate"
	task.Description = "Create a revision for patch queue item patchitem-branch-source in queue patchq-project-subpixel-projrepo-1 from branch branch-source at head " + strings.Repeat("1", 40) + "."
	task.Tags = []string{"project", "patch-queue", "revision"}
	return task
}

func projectCompletionGatePatchQueueItem(state, headSHA, decisionSummary, updatedAt string) map[string]any {
	return projectCompletionGatePatchQueueItemForBranch(state, "branch-1", headSHA, decisionSummary, updatedAt)
}

func projectCompletionGatePatchQueueItemForBranch(state, branchID, headSHA, decisionSummary, updatedAt string) map[string]any {
	return map[string]any{
		"queue_id":            "patchq-project-subpixel-projrepo-1",
		"item_id":             "patchitem-" + branchID,
		"workspace_id":        "ws",
		"project_id":          "project-subpixel",
		"repo_id":             "projrepo-1",
		"branch_id":           branchID,
		"review_doc_key":      "project.project-subpixel.branch." + branchID + ".review",
		"state":               state,
		"head_sha":            headSHA,
		"decision_summary":    decisionSummary,
		"decided_by":          "agent-reviewer",
		"decided_at":          updatedAt,
		"created_at":          updatedAt,
		"updated_at":          updatedAt,
		"pathset":             []string{"web/app.js"},
		"repo_authority_mode": "patch_only_temp_repo",
	}
}
