package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeNoopContinueGuardBlocksRepeatedAssistantOnlyContinue(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{TaskID: "task-stuck", Title: "Repair stale lane"}
	session := AgentSessionStateRecord{SessionID: "session-stuck", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{AssistantTurns: 1}

	first := StructuredTaskResult{Outcome: "continue", Summary: "Still thinking", NextAction: "Continue the active task cycle."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-stuck", &first, trace); err != nil {
		t.Fatalf("first applyNoopContinueGuard() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first assistant-only continue should get one grace cycle, got %+v", first)
	}
	if runtime.scratch.NoopContinueCount != 1 || runtime.scratch.NoopContinueTaskID != task.TaskID || runtime.scratch.NoopContinueSessionID != session.SessionID {
		t.Fatalf("expected persisted no-op counter, got %+v", runtime.scratch)
	}
	if taskCycleHasDurableProgress(first, trace, nil) {
		t.Fatalf("assistant-only continue should not count as durable progress")
	}

	second := StructuredTaskResult{Outcome: "continue", Summary: "Still thinking", NextAction: "Continue the active task cycle."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-stuck", &second, trace); err != nil {
		t.Fatalf("second applyNoopContinueGuard() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second assistant-only continue should be converted to blocked, got %+v", second)
	}
	if len(second.BlockedOn) != 1 || second.BlockedOn[0].Kind != "dependency" {
		t.Fatalf("expected routine dependency block, got %+v", second.BlockedOn)
	}
	if !strings.Contains(second.Summary, "No durable progress") {
		t.Fatalf("expected no-progress summary, got %q", second.Summary)
	}
	if !taskCycleHasDurableProgress(second, trace, nil) {
		t.Fatalf("blocked guard result should count as terminal progress")
	}
}

func TestRuntimeNoopContinueGuardResetsRevisionCounterWhenSignatureChanges(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-patchq-revision-project-signal01-queue-lexer-item-lexer",
		Title:                "Repair blocked lexer candidate",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","patch_queue_task_kind":"revision","required_transition":"project_patch_queue_revision_commit_review_submit"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-revision", AgentID: "beta", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{AssistantTurns: 1, ToolCalls: []string{"write_file"}, SuccessfulToolCalls: []string{"write_file"}}

	first := StructuredTaskResult{Outcome: "continue", Summary: "Wrote lexer token test", NextAction: "Run lexer tests."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-revision", &first, trace); err != nil {
		t.Fatalf("first applyNoopContinueGuard() error = %v", err)
	}
	second := StructuredTaskResult{Outcome: "continue", Summary: "Wrote lexer implementation", NextAction: "Run parser-facing checks."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-revision", &second, trace); err != nil {
		t.Fatalf("second applyNoopContinueGuard() error = %v", err)
	}
	if second.Outcome != "continue" || runtime.scratch.NoopContinueCount != 1 {
		t.Fatalf("changed revision-publication signature should reset counter, result=%+v scratch=%+v", second, runtime.scratch)
	}
}

func TestRuntimeNoopContinueGuardDoesNotTreatContinueBlockedOnAsDurableProgress(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{TaskID: "task-stuck", Title: "Repair stale lane"}
	session := AgentSessionStateRecord{SessionID: "session-stuck", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{AssistantTurns: 1}

	first := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Still waiting on the same thing",
		NextAction: "Continue waiting.",
		BlockedOn: []BlockedRef{{
			Kind:   "dependency",
			Detail: "waiting on the same stale validation answer",
		}},
	}
	if taskCycleHasDurableProgress(first, trace, nil) {
		t.Fatalf("continue + blocked_on should not count as terminal durable progress")
	}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-stuck", &first, trace); err != nil {
		t.Fatalf("first applyNoopContinueGuard() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first continue blocker-shaped result should get one grace cycle, got %+v", first)
	}

	second := first
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-stuck", &second, trace); err != nil {
		t.Fatalf("second applyNoopContinueGuard() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second blocker-shaped continue should be converted to blocked, got %+v", second)
	}
}

func TestRuntimeNoopContinueGuardBlocksRepeatedSelfActionablePublicationNarration(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{TaskID: "task-impl", ProjectID: "project-1", ProjectLane: "implementation"}
	session := AgentSessionStateRecord{SessionID: "session-impl", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		AssistantTurns:      1,
		ToolCalls:           []string{"workspace_doc_put"},
		SuccessfulToolCalls: []string{"workspace_doc_put"},
	}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Completion deferred: project implementation evidence still needs the owned git publication step",
		NextAction: "This is self-actionable, not an external blocker: call project_branch_commit with push=true for the owned checkout changes, then call project_branch_review_ready for the current HEAD.",
	}

	first := result
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-impl", &first, trace); err != nil {
		t.Fatalf("first applyNoopContinueGuard() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first self-action narration should get one grace cycle, got %+v", first)
	}
	if runtime.scratch.NoopContinueCount != 1 {
		t.Fatalf("expected self-action no-op counter to increment despite status doc publication, got %+v", runtime.scratch)
	}

	second := result
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-impl", &second, trace); err != nil {
		t.Fatalf("second applyNoopContinueGuard() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second self-action narration should be converted to blocked, got %+v", second)
	}
	if len(second.BlockedOn) != 1 || second.BlockedOn[0].Kind != "blocked_on_self_action_not_executed" {
		t.Fatalf("expected self-action block, got %+v", second.BlockedOn)
	}
	if !strings.Contains(second.NextAction, "project_branch_commit") || !strings.Contains(second.NextAction, "project_branch_review_ready") {
		t.Fatalf("expected tool-required next action, got %q", second.NextAction)
	}
}

func TestRuntimeNoopContinueGuardAllowsSelfActionablePublicationProgress(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			NoopContinueTaskID:    "task-impl",
			NoopContinueSessionID: "session-impl",
			NoopContinueRunID:     "run-impl",
			NoopContinueCount:     1,
		},
	}
	task := WorkspaceTaskRecord{TaskID: "task-impl", ProjectID: "project-1", ProjectLane: "implementation"}
	session := AgentSessionStateRecord{SessionID: "session-impl", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{ToolCalls: []string{"project_branch_commit"}, SuccessfulToolCalls: []string{"project_branch_commit"}}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Committed the owned branch",
		NextAction: "This is self-actionable: call project_branch_review_ready for the current HEAD after project_branch_commit.",
	}

	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-impl", &result, trace); err != nil {
		t.Fatalf("applyNoopContinueGuard() error = %v", err)
	}
	if result.Outcome != "continue" {
		t.Fatalf("successful publication tool should preserve continue outcome, got %+v", result)
	}
	if runtime.scratch.NoopContinueCount != 0 {
		t.Fatalf("expected no-op counter to clear after publication tool progress, got %+v", runtime.scratch)
	}
}

func TestRuntimeNoopContinueGuardBlocksRepeatedStatusDocOnlyWorldlessCycles(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{TaskID: "task-recovery", ProjectID: "project-1", ProjectLane: "coordination"}
	session := AgentSessionStateRecord{SessionID: "session-recovery", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"workspace_doc_put", "agent.update.post"},
		SuccessfulToolCalls: []string{"workspace_doc_put", "agent.update.post"},
	}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Recorded status and will inspect the blocker again.",
		NextAction: "Continue investigating the same blocker.",
	}

	first := result
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-recovery", &first, trace); err != nil {
		t.Fatalf("first applyNoopContinueGuard() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first status/doc-only worldless cycle should get one grace cycle, got %+v", first)
	}
	if runtime.scratch.NoopContinueCount != 1 {
		t.Fatalf("expected no-progress counter after first worldless cycle, got %+v", runtime.scratch)
	}

	second := result
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-recovery", &second, trace); err != nil {
		t.Fatalf("second applyNoopContinueGuard() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second status/doc-only worldless cycle should block, got %+v", second)
	}
	if len(second.BlockedOn) != 1 || second.BlockedOn[0].Kind != "blocked_on_no_world_state_progress" {
		t.Fatalf("expected no-world-state progress blocker, got %+v", second.BlockedOn)
	}
}

func TestRuntimeNoopContinueGuardAllowsConcreteProjectTransitionProgress(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			NoopContinueTaskID:    "task-recovery",
			NoopContinueSessionID: "session-recovery",
			NoopContinueCount:     1,
		},
	}
	task := WorkspaceTaskRecord{TaskID: "task-recovery", ProjectID: "project-1", ProjectLane: "coordination"}
	session := AgentSessionStateRecord{SessionID: "session-recovery", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_patch_queue_submit"},
		SuccessfulToolCalls: []string{"project_patch_queue_submit"},
	}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Submitted the ready branch to patch queue.",
		NextAction: "Wait for review decision.",
	}

	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-recovery", &result, trace); err != nil {
		t.Fatalf("applyNoopContinueGuard() error = %v", err)
	}
	if result.Outcome != "continue" {
		t.Fatalf("concrete project transition should preserve result, got %+v", result)
	}
	if runtime.scratch.NoopContinueCount != 0 {
		t.Fatalf("expected no-progress counter to clear after project transition, got %+v", runtime.scratch)
	}
}

func TestRuntimeRequiredTransitionGateBlocksRepeatedProjectRoleAssignNarration(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{TaskID: "task-role-scope-beta", ProjectID: "project-1", ProjectLane: "coordination"}
	session := AgentSessionStateRecord{SessionID: "session-role-scope", AgentID: "alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	packet := &AgentWorkPacket{PreferredTransition: "project_role_assign"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"workspace_doc_put"},
		SuccessfulToolCalls: []string{"workspace_doc_put"},
	}
	result := StructuredTaskResult{
		Outcome:    "completed",
		Summary:    "Scope valid; beta can continue.",
		NextAction: "Ask beta to publish branch evidence.",
	}

	first := result
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-role-scope", &first, trace, packet); err != nil {
		t.Fatalf("first applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first missing project_role_assign receipt should keep lane active, got %+v", first)
	}
	if runtime.scratch.RequiredTransitionCount != 1 || runtime.scratch.RequiredTransitionTool != "project_role_assign" {
		t.Fatalf("expected required-transition counter, got %+v", runtime.scratch)
	}
	if !strings.Contains(first.NextAction, "project_role_assign") {
		t.Fatalf("expected project_role_assign next action, got %q", first.NextAction)
	}

	second := result
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-role-scope", &second, trace, packet); err != nil {
		t.Fatalf("second applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second missing project_role_assign receipt should block status substitution, got %+v", second)
	}
	if len(second.BlockedOn) != 1 || second.BlockedOn[0].Kind != "required_transition_not_executed" {
		t.Fatalf("expected required transition blocker, got %+v", second.BlockedOn)
	}
}

func TestRuntimeRequiredTransitionGateBlocksRepeatedSideEffectVerificationNarration(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "zeta"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-verify-tooling",
		ProjectID:            "project-1",
		ProjectLane:          "review",
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_verification","action_kind":"verify_bucket"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-side-effect-verify", AgentID: "zeta", TaskID: task.TaskID, Status: "ACTIVE"}
	packet := &AgentWorkPacket{PreferredTransition: "side_effect_resolve"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"workspace_doc_put"},
		SuccessfulToolCalls: []string{"workspace_doc_put"},
	}
	result := StructuredTaskResult{
		Outcome:    "completed",
		Summary:    "Tooling bucket looks ambiguous; side effect verification is needed.",
		NextAction: "Record the final classification.",
	}

	first := result
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-side-effect-verify", &first, trace, packet); err != nil {
		t.Fatalf("first applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first missing side_effect_resolve receipt should keep recovery lane active, got %+v", first)
	}
	if runtime.scratch.RequiredTransitionCount != 1 || runtime.scratch.RequiredTransitionTool != "side_effect_resolve" {
		t.Fatalf("expected side_effect_resolve required-transition counter, got %+v", runtime.scratch)
	}
	if !strings.Contains(first.NextAction, "side_effect_resolve") {
		t.Fatalf("expected side_effect_resolve next action, got %q", first.NextAction)
	}

	second := result
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-side-effect-verify", &second, trace, packet); err != nil {
		t.Fatalf("second applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second missing side_effect_resolve receipt should block status substitution, got %+v", second)
	}
	if len(second.BlockedOn) != 1 || second.BlockedOn[0].Kind != "required_transition_not_executed" {
		t.Fatalf("expected required transition blocker, got %+v", second.BlockedOn)
	}
}

func TestRuntimeRequiredToolGateBlocksTaskBoundBrowserSessionNarration(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "iota"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-managed-visual-audit-canary",
		ProjectLane:          "review",
		TaskRequirementsJSON: `{"schema":"managed_visual_audit_canary.v1","required_tool":"browser_session","required_actions":["open","inspect","click","type","screenshot"]}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-visual-audit", AgentID: "iota", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"workspace_doc_put"},
		SuccessfulToolCalls: []string{"workspace_doc_put"},
	}
	result := StructuredTaskResult{
		Outcome:    "completed",
		Summary:    "Wrote visual-audit blocker doc from prior evidence.",
		NextAction: "Wait for a verified candidate.",
	}

	first := result
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-visual-audit", &first, trace, nil); err != nil {
		t.Fatalf("first applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first missing browser_session receipt should keep task active, got %+v", first)
	}
	if runtime.scratch.RequiredTransitionCount != 1 || runtime.scratch.RequiredTransitionTool != "browser_session" {
		t.Fatalf("expected required-tool counter for browser_session, got %+v", runtime.scratch)
	}
	if !strings.Contains(first.NextAction, "browser_session") {
		t.Fatalf("expected browser_session next action, got %q", first.NextAction)
	}

	second := result
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-visual-audit", &second, trace, nil); err != nil {
		t.Fatalf("second applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second missing browser_session receipt should block status substitution, got %+v", second)
	}
	if len(second.BlockedOn) != 1 || second.BlockedOn[0].Kind != "required_tool_not_executed_or_failed" {
		t.Fatalf("expected required tool blocker, got %+v", second.BlockedOn)
	}
}

func TestRuntimeRequiredToolGateRejectsFailedTaskBoundBrowserSessionWithoutTypedBlocker(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "iota"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-managed-visual-audit-canary",
			RequiredTransitionSessionID: "session-visual-audit",
			RequiredTransitionTool:      "browser_session",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-managed-visual-audit-canary",
		ProjectLane:          "review",
		TaskRequirementsJSON: `{"schema":"managed_visual_audit_canary.v1","required_tool":"browser_session"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-visual-audit", AgentID: "iota", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"browser_session"},
		FailedToolCalls: []string{"browser_session"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "browser_session",
			IsError:  true,
			Output:   `{"status":"block","reason":"browser session could not open"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:    "completed",
		Summary:    "browser_session could not open the candidate, so I wrote a status note.",
		NextAction: "Wait for another browser run.",
	}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-visual-audit", &result, trace, nil); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "blocked" {
		t.Fatalf("failed browser_session without typed blocker should trip required-tool gate, got %+v", result)
	}
	if len(result.BlockedOn) != 1 || result.BlockedOn[0].Kind != "required_tool_not_executed_or_failed" {
		t.Fatalf("expected required-tool failure blocker, got %+v", result.BlockedOn)
	}
	if runtime.scratch.RequiredTransitionCount != 2 || runtime.scratch.RequiredTransitionTool != "browser_session" {
		t.Fatalf("expected required-tool counter to remain after failed attempt without blocker, got %+v", runtime.scratch)
	}
}

func TestRuntimeEmptyFrontierRequiredOutcomeGate(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "epsilon"},
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-idle-reflection-project-project-rq-20260612-1530",
		ProjectID:            "project-rq",
		ProjectLane:          "qa",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","runtime_contract":"empty_product_frontier.v1","required_transition":"empty_product_frontier_outcome","allowed_terminal_receipts":["task_submit","project_phase_transition"],"project_id":"project-rq"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-empty-frontier", AgentID: "epsilon", TaskID: task.TaskID, Status: "ACTIVE"}
	recommendOnly := &TaskRunTrace{
		ToolCalls:           []string{"workspace_doc_put"},
		SuccessfulToolCalls: []string{"workspace_doc_put"},
	}
	result := StructuredTaskResult{
		Outcome:    "completed",
		Summary:    "Recommend marking the project DONE; no uncovered product gap remains.",
		NextAction: "Lead should advance the phase to DONE.",
	}

	first := result
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-empty-frontier", &first, recommendOnly, nil); err != nil {
		t.Fatalf("first applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("recommend-only empty frontier should remain active, got %+v", first)
	}
	if runtime.scratch.RequiredTransitionTool != "empty_product_frontier_outcome" || runtime.scratch.RequiredTransitionCount != 1 {
		t.Fatalf("expected empty-frontier counter, got %+v", runtime.scratch)
	}
	if !strings.Contains(first.NextAction, "project_phase_transition") || !strings.Contains(first.NextAction, "task_submit") {
		t.Fatalf("next action must name both valid receipts, got %q", first.NextAction)
	}

	second := result
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-empty-frontier", &second, &TaskRunTrace{SuccessfulToolCalls: []string{"project_phase_transition"}}, nil); err != nil {
		t.Fatalf("phase receipt applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if second.Outcome != "completed" {
		t.Fatalf("project_phase_transition receipt should satisfy empty frontier gate, got %+v", second)
	}

	third := result
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-empty-frontier", &third, &TaskRunTrace{SuccessfulToolCalls: []string{"task_submit"}}, nil); err != nil {
		t.Fatalf("task_submit receipt applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if third.Outcome != "completed" {
		t.Fatalf("task_submit receipt should satisfy empty frontier gate, got %+v", third)
	}
}

func TestRuntimeRequiredToolGateRejectsFailedTaskBoundBrowserSessionWithDetailOnlyBlocker(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "iota"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-managed-visual-audit-canary",
			RequiredTransitionSessionID: "session-visual-audit",
			RequiredTransitionTool:      "browser_session",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-managed-visual-audit-canary",
		ProjectLane:          "review",
		TaskRequirementsJSON: `{"schema":"managed_visual_audit_canary.v1","required_tool":"browser_session"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-visual-audit", AgentID: "iota", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"browser_session"},
		FailedToolCalls: []string{"browser_session"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "browser_session",
			IsError:  true,
			Output:   `{"status":"block","reason":"browser session could not open"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome: "blocked",
		Summary: "browser_session could not open the candidate.",
		BlockedOn: []BlockedRef{{
			Detail: "browser_session failed to open the candidate URL",
		}},
	}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-visual-audit", &result, trace, nil); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if runtime.scratch.RequiredTransitionCount != 2 || runtime.scratch.RequiredTransitionTool != "browser_session" {
		t.Fatalf("detail-only blocker must not clear required-tool counter, got %+v", runtime.scratch)
	}
	if len(result.BlockedOn) != 1 || result.BlockedOn[0].Kind != "required_tool_not_executed_or_failed" {
		t.Fatalf("expected typed required-tool blocker to replace detail-only blocker, got %+v", result.BlockedOn)
	}
}

func TestRuntimeRequiredToolGateAcceptsTaskBoundBrowserSessionTypedBlocker(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "iota"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-managed-visual-audit-canary",
			RequiredTransitionSessionID: "session-visual-audit",
			RequiredTransitionTool:      "browser_session",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-managed-visual-audit-canary",
		ProjectLane:          "review",
		TaskRequirementsJSON: `{"schema":"managed_visual_audit_canary.v1","required_tool":"browser_session"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-visual-audit", AgentID: "iota", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"browser_session"},
		FailedToolCalls: []string{"browser_session"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "browser_session",
			IsError:  true,
			Output:   `{"status":"block","reason":"browser session could not open"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:    "blocked",
		Summary:    "browser_session could not open the candidate.",
		NextAction: "Repair browser runtime before another audit.",
		BlockedOn: []BlockedRef{{
			Kind:   "browser_session_failed",
			Detail: "browser_session returned a runtime/tooling error before visual audit could proceed",
		}},
	}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-visual-audit", &result, trace, nil); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "blocked" || result.Summary != "browser_session could not open the candidate." {
		t.Fatalf("browser_session typed blocker should be preserved, got %+v", result)
	}
	if runtime.scratch.RequiredTransitionCount != 0 || runtime.scratch.RequiredTransitionTool != "" {
		t.Fatalf("expected required-tool counter to clear after browser_session typed blocker, got %+v", runtime.scratch)
	}
}

func TestRuntimeRequiredToolGateAcceptsSuccessfulTaskBoundBrowserSessionReceipt(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "iota"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-managed-visual-audit-canary",
			RequiredTransitionSessionID: "session-visual-audit",
			RequiredTransitionTool:      "browser_session",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-managed-visual-audit-canary",
		ProjectLane:          "review",
		TaskRequirementsJSON: `{"schema":"managed_visual_audit_canary.v1","required_tool":"browser_session"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-visual-audit", AgentID: "iota", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"browser_session"},
		SuccessfulToolCalls: []string{"browser_session"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "browser_session",
			Output:   `{"status":"pass","screenshots":["shot-1.png"]}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "browser_session inspected the candidate.",
		NextAction: "Publish the browser audit evidence.",
	}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-visual-audit", &result, trace, nil); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "continue" {
		t.Fatalf("successful browser_session receipt should preserve outcome, got %+v", result)
	}
	if runtime.scratch.RequiredTransitionCount != 0 || runtime.scratch.RequiredTransitionTool != "" {
		t.Fatalf("expected required-tool counter to clear after successful browser_session receipt, got %+v", runtime.scratch)
	}
}

func TestTaskBoundRequiredToolPreemptsBrowserSessionSubstitutes(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-managed-visual-audit-canary",
		TaskRequirementsJSON: `{"schema":"managed_visual_audit_canary.v1","required_tool":"browser_session","forbidden_substitutes":["browser_visual_probe_only","workspace_doc_get_only","status_only"]}`,
	}
	trace := &TaskRunTrace{ToolCalls: []string{"workspace_doc_get"}}

	required, reason, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, trace, "browser_visual_probe")
	if !ok || required != "browser_session" || !strings.Contains(reason, "probe-only") {
		t.Fatalf("expected browser_visual_probe to be blocked before browser_session, got required=%q reason=%q ok=%v", required, reason, ok)
	}

	required, reason, ok = taskBoundRequiredToolPreemptsSubstituteToolCall(task, trace, "workspace_doc_put")
	if !ok || required != "browser_session" || !strings.Contains(reason, "cannot publish") {
		t.Fatalf("expected workspace_doc_put to be blocked before browser_session, got required=%q reason=%q ok=%v", required, reason, ok)
	}

	afterFailedAttempt := &TaskRunTrace{ToolCalls: []string{"browser_session"}, FailedToolCalls: []string{"browser_session"}}
	required, reason, ok = taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterFailedAttempt, "browser_visual_probe")
	if !ok || required != "browser_session" || !strings.Contains(reason, "successful browser_session receipt") {
		t.Fatalf("browser_visual_probe should remain blocked after failed browser_session attempt, got required=%q reason=%q ok=%v", required, reason, ok)
	}
	if _, _, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterFailedAttempt, "workspace_doc_put"); ok {
		t.Fatalf("workspace_doc_put should be allowed after a browser_session attempt so the agent can publish a typed blocker")
	}

	afterReceipt := &TaskRunTrace{ToolCalls: []string{"browser_session"}, SuccessfulToolCalls: []string{"browser_session"}}
	if _, _, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterReceipt, "browser_visual_probe"); ok {
		t.Fatalf("substitute should be allowed after successful browser_session receipt")
	}
}

func TestRuntimeRequiredToolGateInfersPatchQueueIntegrationTransition(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-run32checkpoint-integration",
		ProjectID:   "project-signal01",
		ProjectLane: "integration",
		Title:       "Signal-01 checkpoint integration replay",
		Description: "Claim this task to replay patch queue integration against seeded accepted lane branches.",
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1",
			"patch_queue_task_kind":"integration",
			"required_transition":"project_patch_queue_integrate_then_full_product_verify"
		}`,
		Tags: []string{"project", "patch-queue", "integration"},
	}
	if got := taskBoundRequiredToolForTaskCycle(task); got != "project_patch_queue_integrate" {
		t.Fatalf("expected integration task to require project_patch_queue_integrate, got %q", got)
	}
}

func TestRuntimeRequiredToolGateInfersCanonicalIntegrationValidationTransition(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-1780787830378194400-bc1973cb",
		ProjectID:   "project-signal01-rq-root",
		TaskKind:    "COORDINATION",
		ProjectLane: "integration",
		Title:       "Validate canonical rq integration and full-product evidence",
		Description: "Follow-up from the claim-scope repair cycle. Verify exact branch/head provenance, run bounded build/test/smoke checks on the canonical target, and publish durable evidence of the assembled product state rather than another status note.",
		TaskRequirementsJSON: `{
			"evidence_needed":["exact branch/head SHA","checkout path","go build ./...","go test ./...","smoke or sample run if available"],
			"preferred_tools":["shell","browser_session"],
			"required_work_modes":["validation","review"],
			"schema":"task_requirements.v1"
		}`,
	}
	if got := taskBoundRequiredToolForTaskCycle(task); got != "project_patch_queue_integrate" {
		t.Fatalf("canonical integration validation must require project_patch_queue_integrate before shell validation, got %q", got)
	}
	required, reason, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, &TaskRunTrace{}, "shell")
	if !ok || required != "project_patch_queue_integrate" || !strings.Contains(reason, "must be attempted") {
		t.Fatalf("shell should be blocked before canonical integration materialization, required=%q reason=%q ok=%v", required, reason, ok)
	}
}

func TestTaskBoundRequiredToolPreemptsPatchQueueIntegrationBypassTools(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-run32checkpoint-integration",
		ProjectID:   "project-signal01",
		ProjectLane: "integration",
		Title:       "Merge accepted patch queue candidates",
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_kind":"integration",
			"required_tool":"project_patch_queue_integrate",
			"required_transition":"project_patch_queue_integrate_then_full_product_verify"
		}`,
		Tags: []string{"project", "patch-queue", "integration"},
	}
	for _, toolName := range []string{"shell", "project_checkout_materialize", "workspace_doc_put", "task_submit", "project_patch_queue_followup",
		"project_bootstrap", "project_repo_materialize", "project_repo_register", "project_phase_transition",
		// BP-01: terminalizing/re-routing substitutes must also be blocked pre-receipt.
		"project_patch_queue_lifecycle", "project_patch_queue_submit", "project_role_assign", "project_role_request"} {
		required, reason, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, &TaskRunTrace{ToolCalls: []string{"project_patch_queue_list"}}, toolName)
		if !ok || required != "project_patch_queue_integrate" || !strings.Contains(reason, "must be attempted") {
			t.Fatalf("expected %s to be blocked before project_patch_queue_integrate, got required=%q reason=%q ok=%v", toolName, required, reason, ok)
		}
	}
	if _, _, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, &TaskRunTrace{ToolCalls: []string{"project_patch_queue_list"}}, "workspace_doc_get"); ok {
		t.Fatalf("read-only workspace_doc_get should remain available before integration")
	}
	afterFailedAttempt := &TaskRunTrace{ToolCalls: []string{"project_patch_queue_integrate"}, FailedToolCalls: []string{"project_patch_queue_integrate"}}
	required, reason, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterFailedAttempt, "shell")
	if !ok || required != "project_patch_queue_integrate" || !strings.Contains(reason, "already failed") {
		t.Fatalf("shell should remain blocked after failed integration attempt, got required=%q reason=%q ok=%v", required, reason, ok)
	}
	if _, _, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterFailedAttempt, "project_patch_queue_followup"); !ok {
		t.Fatalf("project_patch_queue_followup should stay blocked until integrate emits a terminal repair/refusal receipt")
	}
	afterTerminalRepair := &TaskRunTrace{
		ToolCalls:       []string{"project_patch_queue_integrate"},
		FailedToolCalls: []string{"project_patch_queue_integrate"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_integrate",
			IsError:  true,
			Output:   `{"repair_receipt_recorded":true,"next_gate":"inspect_project_patch_queue_integration_repair_receipt_before_retrying_canonical_mutation"}`,
		}},
	}
	if _, _, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterTerminalRepair, "project_patch_queue_followup"); ok {
		t.Fatalf("project_patch_queue_followup should be available after terminal integration repair receipt")
	}
	required, reason, ok = taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterTerminalRepair, "shell")
	if !ok || required != "project_patch_queue_integrate" || !strings.Contains(reason, "terminal integration/refusal/repair receipt") {
		t.Fatalf("shell should remain blocked after terminal repair receipt, got required=%q reason=%q ok=%v", required, reason, ok)
	}
	afterAlreadyIntegrated := &TaskRunTrace{
		ToolCalls:       []string{"project_patch_queue_integrate"},
		FailedToolCalls: []string{"project_patch_queue_integrate"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_integrate",
			IsError:  true,
			Output:   `{"already_integrated":true,"integrated":false,"integration_recorded":false,"queue_id":"patchq-1","item_id":"patchitem-1","branch_id":"branch-1"}`,
		}},
	}
	if !traceHasRequiredTransitionReceipt(afterAlreadyIntegrated, "project_patch_queue_integrate") {
		t.Fatalf("already_integrated receipt should count as terminal required transition evidence")
	}
	if _, _, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterAlreadyIntegrated, "project_patch_queue_followup"); ok {
		t.Fatalf("project_patch_queue_followup should be available after already_integrated receipt")
	}
	if _, _, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterAlreadyIntegrated, "workspace_doc_put"); ok {
		t.Fatalf("workspace_doc_put should be available after already_integrated receipt so the agent can publish validation evidence")
	}
	if _, _, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterAlreadyIntegrated, "shell"); ok {
		t.Fatalf("shell should be available after already_integrated receipt so the agent can run full-product validation")
	}
	afterDedupRepair := &TaskRunTrace{
		ToolCalls:       []string{"project_patch_queue_integrate"},
		FailedToolCalls: []string{"project_patch_queue_integrate"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_integrate",
			IsError:  true,
			Output:   `project_patch_queue_integrate failed and could not record durable repair receipt: rpc project.patch_queue.integration_repair: dedup_key "project.patch_queue.integration:ws:queue:item:head" already exists; original repair reason: accepted head has unresolved same-head reviewer defect evidence`,
		}},
	}
	if !traceHasRequiredTransitionReceipt(afterDedupRepair, "project_patch_queue_integrate") {
		t.Fatalf("deduped integration_repair receipt should count as terminal required transition evidence")
	}
	if _, _, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterDedupRepair, "project_patch_queue_followup"); ok {
		t.Fatalf("project_patch_queue_followup should be available after deduped repair receipt")
	}
	afterReceipt := &TaskRunTrace{ToolCalls: []string{"project_patch_queue_integrate"}, SuccessfulToolCalls: []string{"project_patch_queue_integrate"}}
	if _, _, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, afterReceipt, "shell"); ok {
		t.Fatalf("shell should be available for bounded build/test after successful integration receipt")
	}
}

func TestRuntimeRequiredToolGateIgnoresBlockedPatchQueueValidationIntegrateLeak(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-validation-project-rq",
		ProjectID:   "project-signal01-rq-product-first",
		TaskKind:    "EXECUTION",
		ProjectLane: "validation",
		Title:       "Validate blocked integration candidate projbranch-1780945180974806609-111",
		Description: "Patch queue decision follow-up for a BLOCKED item; produce fresh exact branch/head validation evidence.",
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1",
			"patch_queue_task_kind":"validation",
			"queue_id":"patchq-project-signal01-rq-product-first-repo-signal01-rq-core",
			"item_id":"patchitem-projbranch-1780945180974806609-111",
			"branch_id":"projbranch-1780945180974806609-111",
			"head_sha":"186def09f4dd88cf6375166e1fe26884db681a34",
			"state":"BLOCKED",
			"required_tool":"project_patch_queue_integrate",
			"required_transition":"project_patch_queue_submit_or_revision_followup",
			"blocked_validation_recovery":"same_head_evidence_or_bounded_revision"
		}`,
		Tags: []string{"project", "patch-queue", "validation", "blocked"},
	}
	if got := taskBoundRequiredToolForTaskCycle(task); got != "" {
		t.Fatalf("blocked validation follow-up must not inherit leaked integration required_tool, got %q", got)
	}
	if required, reason, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, &TaskRunTrace{}, "project_patch_queue_followup"); ok {
		t.Fatalf("project_patch_queue_followup must remain available for blocked validation recovery, required=%q reason=%q", required, reason)
	}
}

func TestRuntimeRequiredToolGateIgnoresAcceptedPatchQueueValidationIntegrateLeak(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-validation-project-rq",
		ProjectID:   "project-signal01-rq-product-first",
		TaskKind:    "EXECUTION",
		ProjectLane: "validation",
		Title:       "Validate accepted integration candidate projbranch-1780924866614389862-15",
		Description: "Patch queue decision follow-up for an ACCEPTED item; publish post-integration build/test evidence.",
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1",
			"patch_queue_task_kind":"validation",
			"queue_id":"patchq-project-signal01-rq-product-first-repo-signal01-rq-core",
			"item_id":"patchitem-projbranch-1780924866614389862-15",
			"branch_id":"projbranch-1780924866614389862-15",
			"head_sha":"9415f3e2c0216738debfbdc620019fa4ef493328",
			"state":"ACCEPTED",
			"required_tool":"project_patch_queue_integrate",
			"required_transition":"project_patch_queue_integrate_then_full_product_verify",
			"integration_contract":"canonical_integration_before_full_product_validation.v1"
		}`,
		Tags: []string{"project", "patch-queue", "validation", "accepted"},
	}
	if got := taskBoundRequiredToolForTaskCycle(task); got != "" {
		t.Fatalf("accepted validation follow-up must not inherit leaked integration required_tool, got %q", got)
	}
	if required, reason, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, &TaskRunTrace{}, "shell"); ok {
		t.Fatalf("post-integration validation shell/build evidence must remain available, required=%q reason=%q", required, reason)
	}
}

func TestRuntimeRequiredToolGateIgnoresClaimStewardshipIntegrationLane(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-claim-stewardship-r58",
		ProjectID:   "project-signal01-rq-product-first",
		TaskKind:    "EXECUTION",
		ProjectLane: "integration",
		Title:       "Resolve claimed patch queue item lifecycle",
		Description: "Patch queue claim stewardship task created from agent.work.next frontier.",
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1",
			"patch_queue_task_kind":"claim_stewardship",
			"queue_id":"patchq-project-signal01-rq-p",
			"item_id":"patchitem-projbranch-1781142360396584010-28",
			"branch_id":"projbranch-1781142360396584010-28",
			"head_sha":"de68fb4c04db",
			"state":"CLAIMED",
			"required_transition":"project_patch_queue_lifecycle"
		}`,
		Tags: []string{"project", "patch-queue", "integration", "queue-stewardship", "claim-stewardship", "claimed-decision"},
	}
	if got := taskBoundRequiredToolForTaskCycle(task); got != "" {
		t.Fatalf("claim stewardship must not infer project_patch_queue_integrate from integration lane, got %q", got)
	}
	if required, reason, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, &TaskRunTrace{}, "project_patch_queue_lifecycle"); ok {
		t.Fatalf("lifecycle must remain callable for claim stewardship, required=%q reason=%q", required, reason)
	}
}

func TestRuntimeRequiredToolGateAcceptsPatchQueueIntegrateDefeasibleBlocker(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "zeta"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-run32checkpoint-integration",
			RequiredTransitionSessionID: "session-integration",
			RequiredTransitionTool:      "project_patch_queue_integrate",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-run32checkpoint-integration",
		ProjectID:   "project-signal01",
		ProjectLane: "integration",
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_kind":"integration",
			"required_tool":"project_patch_queue_integrate",
			"required_transition":"project_patch_queue_integrate_then_full_product_verify"
		}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-integration", AgentID: "zeta", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_patch_queue_integrate"},
		FailedToolCalls: []string{"project_patch_queue_integrate"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_integrate",
			IsError:  true,
			Output:   `{"pre_integration_gate":"defeasible_acceptance","no_canonical_mutation":true,"required_transition":"project_patch_queue_followup_revision_before_integration_retry"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome: "blocked",
		Summary: "Accepted candidate has unresolved same-head reviewer defect evidence.",
		BlockedOn: []BlockedRef{{
			Kind:   "defeasible_acceptance_gate",
			Detail: "project_patch_queue_integrate refused canonical mutation before integration",
		}},
	}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run32", &result, trace, nil); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "blocked" || result.Summary == "" {
		t.Fatalf("typed integrate blocker should be preserved, got %+v", result)
	}
	if runtime.scratch.RequiredTransitionCount != 0 || runtime.scratch.RequiredTransitionTool != "" {
		t.Fatalf("expected required-tool counter to clear after terminal integrate blocker, got %+v", runtime.scratch)
	}
}

func TestRuntimeRequiredToolGateAcceptsPatchQueueIntegrateTerminalReceiptBeforeTypedResult(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "zeta"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-patchq-integration",
			RequiredTransitionSessionID: "session-integration",
			RequiredTransitionTool:      "project_patch_queue_integrate",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-integration",
		ProjectID:   "project-signal01",
		ProjectLane: "integration",
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_kind":"integration",
			"required_tool":"project_patch_queue_integrate",
			"required_transition":"project_patch_queue_integrate_then_full_product_verify"
		}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-integration", AgentID: "zeta", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_patch_queue_integrate"},
		FailedToolCalls: []string{"project_patch_queue_integrate"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_integrate",
			IsError:  true,
			Output:   `{"repair_receipt_recorded":true,"required_transition":"project_patch_queue_followup_revision_before_integration_retry","queue_id":"queue-signal01","item_id":"item-accepted","branch_id":"branch-accepted","repair_reason":"source branch close failed after integrated receipt"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Integration tool emitted a repair receipt; waiting for follow-up routing.",
		NextAction: "Inspect the emitted repair receipt before retrying.",
	}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-r19", &result, trace, nil); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if strings.Contains(result.Summary, "Required task-bound tool") {
		t.Fatalf("terminal integrate receipt should not be rewritten as missing required tool: %+v", result)
	}
	if runtime.scratch.RequiredTransitionCount != 0 || runtime.scratch.RequiredTransitionTool != "" {
		t.Fatalf("expected required-tool counter to clear after terminal integrate receipt, got %+v", runtime.scratch)
	}

	completed := completeProjectPatchQueueIntegrationTerminalReceipt(task, result, trace)
	if completed.Outcome != "blocked" {
		t.Fatalf("expected terminal integrate receipt to become a blocker after gate clears, got %+v", completed)
	}
	if strings.Contains(completed.Summary, "Required task-bound tool") {
		t.Fatalf("terminal receipt blocker should not preserve false missing-tool summary: %+v", completed)
	}
	if !strings.Contains(completed.Details, "repair_receipt_recorded=true") {
		t.Fatalf("expected repair receipt evidence in blocker details, got %q", completed.Details)
	}
}

func TestCompleteProjectPatchQueueIntegrationTerminalReceiptBlocksCycle(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-run36checkpoint-integration",
		ProjectID:   "project-signal01",
		ProjectLane: "integration",
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_kind":"integration",
			"required_tool":"project_patch_queue_integrate",
			"required_transition":"project_patch_queue_integrate_then_full_product_verify"
		}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_patch_queue_integrate"},
		FailedToolCalls: []string{"project_patch_queue_integrate"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_integrate",
			IsError:  true,
			Output:   `{"pre_integration_gate":"defeasible_acceptance","no_canonical_mutation":true,"required_transition":"project_patch_queue_followup_revision_before_integration_retry","repair_receipt_recorded":true,"queue_id":"queue-signal01","item_id":"item-lexer","branch_id":"branch-lexer","defeating_item_id":"item-negative"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:       "continue",
		Summary:       "Integration hit a review blocker; retry next cycle.",
		NextAction:    "Retry project_patch_queue_integrate.",
		RequiresHuman: true,
		HumanReason:   "Need operator guidance.",
	}

	got := completeProjectPatchQueueIntegrationTerminalReceipt(task, result, trace)
	if got.Outcome != "blocked" {
		t.Fatalf("expected terminal integrate receipt to block the task cycle, got outcome=%q result=%+v", got.Outcome, got)
	}
	if got.RequiresHuman || got.HumanReason != "" || got.OwnerAction != "" || got.DecisionType != "" {
		t.Fatalf("terminal integrate receipt should clear human-routing fields, got %+v", got)
	}
	if len(got.BlockedOn) == 0 {
		t.Fatalf("expected typed blocked refs for terminal integrate receipt, got %+v", got)
	}
	joined := got.Details
	if !strings.Contains(joined, "defeasible_acceptance") || !strings.Contains(joined, "defeating_item_id=item-negative") || !strings.Contains(joined, "repair_receipt_recorded=true") {
		t.Fatalf("expected integration receipt evidence in details, got %q", joined)
	}
	if !strings.Contains(got.NextAction, "repair/revision follow-up") {
		t.Fatalf("expected next action to route to repair follow-up, got %q", got.NextAction)
	}
}

func TestCompleteProjectPatchQueueIntegrationEffectiveReceiptDoesNotBlockValidation(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-run24checkpoint-integration",
		ProjectID:   "project-signal01",
		ProjectLane: "integration",
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_kind":"integration",
			"required_tool":"project_patch_queue_integrate",
			"required_transition":"project_patch_queue_integrate_then_full_product_verify"
		}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_patch_queue_integrate"},
		FailedToolCalls: []string{"project_patch_queue_integrate"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_integrate",
			IsError:  true,
			Output:   `{"already_integrated":true,"integrated":true,"integration_recorded":false,"repair_receipt_recorded":true,"queue_id":"queue-signal01","item_id":"item-accepted","branch_id":"branch-accepted","target_head_after":"9ff5dd62f87905b5f17069f3d46ddd777f83dd82","repair_reason":"source_branch_close_failed_after_integrated_receipt"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Accepted candidate is already on canonical main; full-product validation still needs to run.",
		NextAction: "Run go build ./... && go test ./... on canonical main.",
	}

	got := completeProjectPatchQueueIntegrationTerminalReceipt(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("effective already-integrated receipt should not terminal-block validation, got %+v", got)
	}
	if got.Summary != result.Summary || got.NextAction != result.NextAction {
		t.Fatalf("effective integration receipt should preserve validation guidance, got %+v", got)
	}
}

func TestRuntimeRequiredTransitionGateBlocksPatchQueueRevisionCompletionWithoutSubmit(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-revision-project-signal01-queue-lexer-item-lexer",
		ProjectID:   "project-signal01",
		ProjectLane: "implementation",
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_kind":"revision",
			"required_transition":"project_patch_queue_revision_commit_review_submit",
			"required_tool_sequence":["project_branch_commit","project_branch_review_ready","project_patch_queue_submit"]
		}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-revision", AgentID: "beta", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_branch_commit", "project_branch_review_ready"},
		SuccessfulToolCalls: []string{"project_branch_commit", "project_branch_review_ready"},
	}
	result := StructuredTaskResult{Outcome: "completed", Summary: "Committed and marked the branch review-ready."}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-revision", &result, trace, nil); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "blocked" {
		t.Fatalf("completion without patch queue submit should block, got %+v", result)
	}
	if len(result.BlockedOn) != 1 || result.BlockedOn[0].Kind != "required_transition_not_executed" {
		t.Fatalf("expected required transition blocker, got %+v", result.BlockedOn)
	}
}

func TestRuntimeRequiredTransitionGateAcceptsPatchQueueRevisionTerminalSubmit(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-patchq-revision-project-signal01-queue-lexer-item-lexer",
			RequiredTransitionSessionID: "session-revision",
			RequiredTransitionTool:      "project_patch_queue_submit",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-patchq-revision-project-signal01-queue-lexer-item-lexer",
		ProjectID:            "project-signal01",
		ProjectLane:          "implementation",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","patch_queue_task_kind":"revision","required_transition":"project_patch_queue_revision_commit_review_submit"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-revision", AgentID: "beta", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_patch_queue_submit"},
		SuccessfulToolCalls: []string{"project_patch_queue_submit"},
	}
	result := StructuredTaskResult{Outcome: "completed", Summary: "Submitted fresh repair candidate."}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-revision", &result, trace, nil); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "completed" {
		t.Fatalf("terminal submit should satisfy revision completion gate, got %+v", result)
	}
	if runtime.scratch.RequiredTransitionCount != 0 || runtime.scratch.RequiredTransitionTool != "" {
		t.Fatalf("terminal submit should clear required-transition counter, got %+v", runtime.scratch)
	}
}

func TestActiveTaskBoundRequiredToolBlocksBrowserProbeOutsideTaskCycle(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-managed-visual-audit-canary",
		Status:               "RUNNING",
		ClaimAgentID:         stringPtr("iota"),
		TaskRequirementsJSON: `{"schema":"managed_visual_audit_canary.v1","required_tool":"browser_session","forbidden_substitutes":["browser_visual_probe_only"]}`,
	}
	runtime := &Runtime{
		cfg:        RuntimeConfig{WorkspaceID: "ws", AgentID: "iota"},
		activeTask: &task,
	}

	result, ok := runtime.activeTaskBoundRequiredToolSubstituteToolResult("browser_visual_probe")
	if !ok || !result.IsError || !strings.Contains(result.Output, "required_tool_substitute_blocked") {
		t.Fatalf("expected active task browser_visual_probe substitute to be blocked, ok=%v result=%+v", ok, result)
	}

	if _, ok := runtime.activeTaskBoundRequiredToolSubstituteToolResult("browser_session"); ok {
		t.Fatalf("browser_session itself must not be blocked by the substitute guard")
	}
}

func TestRuntimeRequiredTransitionGateInfersABPCRecoverySuccessorTransitions(t *testing.T) {
	for _, actionKind := range []string{"quarantine_bucket", "reassign_bucket", "revert_bucket"} {
		t.Run(actionKind, func(t *testing.T) {
			runtime := &Runtime{
				cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "zeta"},
				scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
			}
			task := WorkspaceTaskRecord{
				TaskID:               "task-side-effect-" + actionKind,
				ProjectID:            "project-1",
				ProjectLane:          "coordination",
				TaskRequirementsJSON: fmt.Sprintf(`{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_cleanup","action_kind":"%s","successor_key":"abpc-resolution-successor:%s"}`, actionKind, actionKind),
			}
			session := AgentSessionStateRecord{SessionID: "session-" + actionKind, AgentID: "zeta", TaskID: task.TaskID, Status: "ACTIVE"}
			trace := &TaskRunTrace{
				ToolCalls:           []string{"workspace_doc_put"},
				SuccessfulToolCalls: []string{"workspace_doc_put"},
			}
			result := StructuredTaskResult{
				Outcome:    "completed",
				Summary:    "Recovery successor needs a durable bucket decision.",
				NextAction: "Record the bucket decision.",
			}

			first := result
			if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-"+actionKind, &first, trace, nil); err != nil {
				t.Fatalf("first applyRequiredTransitionExecutionGate() error = %v", err)
			}
			if first.Outcome != "continue" {
				t.Fatalf("first missing side_effect_resolve receipt should keep %s active, got %+v", actionKind, first)
			}
			if runtime.scratch.RequiredTransitionTool != "side_effect_resolve" {
				t.Fatalf("expected inferred side_effect_resolve requirement for %s, got %+v", actionKind, runtime.scratch)
			}

			second := result
			if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-"+actionKind, &second, trace, nil); err != nil {
				t.Fatalf("second applyRequiredTransitionExecutionGate() error = %v", err)
			}
			if second.Outcome != "blocked" {
				t.Fatalf("second missing side_effect_resolve receipt should block %s narration, got %+v", actionKind, second)
			}
			if len(second.BlockedOn) != 1 || second.BlockedOn[0].Kind != "required_transition_not_executed" {
				t.Fatalf("expected required transition blocker for %s, got %+v", actionKind, second.BlockedOn)
			}
		})
	}
}

func TestRuntimeRequiredTransitionGateAcceptsProjectRoleAssignReceipt(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-role-scope-beta",
			RequiredTransitionSessionID: "session-role-scope",
			RequiredTransitionTool:      "project_role_assign",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{TaskID: "task-role-scope-beta", ProjectID: "project-1", ProjectLane: "coordination"}
	session := AgentSessionStateRecord{SessionID: "session-role-scope", AgentID: "alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	packet := &AgentWorkPacket{PreferredTransition: "project_role_assign"}
	trace := &TaskRunTrace{ToolCalls: []string{"project_role_assign"}, SuccessfulToolCalls: []string{"project_role_assign"}}
	result := StructuredTaskResult{Outcome: "completed", Summary: "Role/scope transition applied."}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-role-scope", &result, trace, packet); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "completed" {
		t.Fatalf("successful project_role_assign should preserve completed outcome, got %+v", result)
	}
	if runtime.scratch.RequiredTransitionCount != 0 || runtime.scratch.RequiredTransitionTool != "" {
		t.Fatalf("expected required-transition counter to clear after receipt, got %+v", runtime.scratch)
	}
}

func TestRuntimeRequiredTransitionGateInfersProjectRoleAssignForDedicatedRoleScopeCarrier(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-role-scope-cli-publication-ownership",
			RequiredTransitionSessionID: "session-role-scope",
			RequiredTransitionTool:      "project_role_assign",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-cli-publication-ownership",
		ProjectID:            "project-signal01-lua-capability",
		TaskKind:             "COORDINATION",
		ProjectLane:          "coordination",
		Title:                "Role-scope repair for overlapping CLI publication ownership",
		Tags:                 []string{"cli", "coordination", "ownership", "publication"},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","write_scope_hints":["README.md","internal/runner/**","scripts/**","testdata/smoke/**"]}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-role-scope", AgentID: "alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		ToolCalls: []string{"agent_request", "task_submit", "workspace_doc_put"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "agent_request",
			IsError:  true,
			Output:   "agent_request authority_transition blocked: task task-role-scope-cli-publication-ownership is already claimed by the current agent",
		}},
	}
	result := StructuredTaskResult{
		Outcome: "continue",
		Summary: "Keep the carrier task visible for frontier self-selection.",
		Materialize: TaskMaterialization{
			DocKey:     "task.task-role-scope-cli-publication-ownership.result",
			DocTitle:   "Task Result - Role-scope repair",
			DocContent: "No applied receipt exists; do not retry authority_transition on the claimed task.",
		},
	}

	if got := requiredTransitionToolForTaskCycle(task, nil); got != "project_role_assign" {
		t.Fatalf("dedicated role-scope carrier should require project_role_assign, got %q", got)
	}
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-role-scope", &result, trace, nil); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "blocked" {
		t.Fatalf("missing receipt on dedicated authority carrier should terminal-block, got %+v", result)
	}
	if len(result.BlockedOn) != 1 || result.BlockedOn[0].Kind != "authority_transition_terminal_blocker" {
		t.Fatalf("expected typed authority terminal blocker, got %+v", result.BlockedOn)
	}
	if !strings.Contains(result.Summary, "authority_transition_terminal_blocker.v1") ||
		!strings.Contains(result.Summary, "no_completion_without_project_role_assign_receipt") {
		t.Fatalf("expected schema-stamped blocker summary, got %q", result.Summary)
	}
}

func TestRuntimeDedicatedRoleScopeCarrierWithReceiptCompletesWithoutSchemaStamp(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-cli-publication-ownership",
		ProjectID:            "project-signal01-lua-capability",
		TaskKind:             "COORDINATION",
		ProjectLane:          "coordination",
		Title:                "Role-scope repair for overlapping CLI publication ownership",
		Tags:                 []string{"cli", "coordination", "ownership", "publication"},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_role_assign"},
		SuccessfulToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			Output:   `{"project_id":"project-signal01-lua-capability","agent_id":"beta","role_type":"IMPLEMENTER","role_id":"role-beta-impl","boundary_transition_state":"role_scope_assigned","authority_transition_applied":true,"transition_executed":true}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome: "continue",
		Summary: "Role transition recorded.",
		BlockedOn: []BlockedRef{{
			Kind:   "role_scope_pending",
			Detail: "beta needs implementation scope",
		}},
	}

	completed := completeRecordedAuthorityTransitionApplied(task, result, trace, nil)
	if completed.Outcome != "completed" {
		t.Fatalf("applied receipt should complete un-stamped dedicated carrier, got %+v", completed)
	}
	if len(completed.BlockedOn) != 0 || !strings.Contains(completed.Details, "role_scope_assigned") {
		t.Fatalf("expected completion with applied receipt details and no blocker, got %+v", completed)
	}
}

func TestRuntimeDedicatedAuthorityCarrierWithRoleScopeReceiptCompletesWithoutRoleScopeID(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-1781703325719662100-4df0c277",
		ProjectID:            "project-signal01-lua-capability",
		TaskKind:             "COORDINATION",
		ProjectLane:          "coordination",
		Title:                "Repair task-role-scope authority transition for CLI publication lane",
		Description:          "Dedicated authority-transition carrier for publication ownership repair.",
		Tags:                 []string{"cli", "publication", "claim", "ownership", "authority-transition", "coordination"},
		TaskRequirementsJSON: `{}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_role_assign"},
		SuccessfulToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			Output:   `{"project_id":"project-signal01-lua-capability","agent_id":"zeta","role_type":"IMPLEMENTER","role_id":"role-zeta-cli-publication","boundary_transition_state":"role_scope_assigned","authority_transition_applied":true,"transition_executed":true}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome: "continue",
		Summary: "Role transition recorded.",
		BlockedOn: []BlockedRef{{
			Kind:   "authority_transition_pending",
			Detail: "publication ownership repair needs durable role/scope evidence",
		}},
	}

	if got := requiredTransitionToolForTaskCycle(task, nil); got != projectRoleScopeAuthorityTransitionTool {
		t.Fatalf("generic role/scope authority carrier should infer project_role_assign, got %q", got)
	}
	completed := completeRecordedAuthorityTransitionApplied(task, result, trace, nil)
	if completed.Outcome != "completed" {
		t.Fatalf("applied receipt should complete generic dedicated authority carrier, got %+v", completed)
	}
	if len(completed.BlockedOn) != 0 || !strings.Contains(completed.Details, "role_scope_assigned") {
		t.Fatalf("expected completion with applied receipt details and no blocker, got %+v", completed)
	}
}

func TestRuntimeDedicatedRoleScopeCarrierWithoutReceiptDoesNotComplete(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-cli-publication-ownership",
		ProjectID:            "project-signal01-lua-capability",
		TaskKind:             "COORDINATION",
		ProjectLane:          "coordination",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1"}`,
	}
	trace := &TaskRunTrace{ToolCalls: []string{"workspace_doc_put"}}
	result := StructuredTaskResult{
		Outcome: "blocked",
		Summary: "No durable scope grant was recorded.",
		BlockedOn: []BlockedRef{{
			Kind:   "role_scope_pending",
			Detail: "still needs durable project_role_assign evidence",
		}},
	}

	if got := completeRecordedAuthorityTransitionApplied(task, result, trace, nil); got.Outcome != "blocked" {
		t.Fatalf("carrier without applied receipt must not complete, got %+v", got)
	}
	if got := completeRecordedAuthorityTransitionDenial(task, result, trace, nil); got.Outcome != "blocked" {
		t.Fatalf("carrier without terminal denial receipt must not complete, got %+v", got)
	}
}

func TestRuntimeRequiredTransitionGateAcceptsProjectRoleAssignDenialReceipt(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-role-scope-gamma",
			RequiredTransitionSessionID: "session-role-scope",
			RequiredTransitionTool:      "project_role_assign",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{TaskID: "task-role-scope-gamma", ProjectID: "project-1", ProjectLane: "coordination"}
	session := AgentSessionStateRecord{SessionID: "session-role-scope", AgentID: "alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	packet := &AgentWorkPacket{PreferredTransition: "project_role_assign"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_role_assign"},
		FailedToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			IsError:  true,
			Output:   `{"boundary_transition_state":"boundary_expansion_denied_overlap","transition_denied":true,"denial_recorded":true}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "completed", Summary: "Gamma expansion denied because beta owns the overlapping lane."}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-role-scope", &result, trace, packet); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "completed" {
		t.Fatalf("recorded project_role_assign denial should be accepted as terminal receipt, got %+v", result)
	}
	if runtime.scratch.RequiredTransitionCount != 0 || runtime.scratch.RequiredTransitionTool != "" {
		t.Fatalf("expected required-transition counter to clear after denial receipt, got %+v", runtime.scratch)
	}
	if !taskCycleHasDurableProgress(result, trace, nil) {
		t.Fatalf("recorded denial receipt should count as durable progress")
	}
}

func TestRuntimeRecordedProjectRoleAssignDenialCompletesBlockedAuthorityTask(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-role-scope-gamma",
		ProjectID:   "project-1",
		Title:       "Resolve project role/scope request for gamma",
		Description: "# Strategic Lead Role/Scope Request\n\n## Required Lead Action\nRun project_role_assign.",
		TaskKind:    "COORDINATION",
		ProjectLane: "coordination",
		Tags:        []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
	}
	packet := &AgentWorkPacket{PreferredTransition: "project_role_assign"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_role_assign"},
		FailedToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			IsError:  true,
			Output:   `{"boundary_transition_state":"boundary_expansion_denied_overlap","transition_denied":true,"denial_recorded":true,"denial_reason":"overlaps_live_owner_lane","preferred_transition":"wait_or_split_existing_owner_lane","allowed_next":["wait_for_conflicting_lane_publication","request_merge_or_adopt"]}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:       "blocked",
		Summary:       "Gamma expansion denied because beta owns the overlapping lane.",
		NextAction:    "Ask beta to release scope.",
		RequiresHuman: true,
		OwnerAction:   "operator_decision",
		HumanReason:   "overlap denial",
		DecisionType:  "scope_conflict",
		BlockedOn: []BlockedRef{{
			Kind:   "boundary_transition_denied",
			Detail: "beta owns the overlapping scope",
		}},
	}

	completed := completeRecordedAuthorityTransitionDenial(task, result, trace, packet)
	if completed.Outcome != "completed" {
		t.Fatalf("expected terminal denial to complete authority task, got %+v", completed)
	}
	if completed.RequiresHuman || completed.OwnerAction != "" || completed.HumanReason != "" || completed.DecisionType != "" {
		t.Fatalf("terminal denial should clear human/request fields, got %+v", completed)
	}
	if len(completed.BlockedOn) != 0 {
		t.Fatalf("authority task should not keep blocked lane after terminal denial, got %+v", completed.BlockedOn)
	}
	if !strings.Contains(completed.Details, "boundary_expansion_denied_overlap") {
		t.Fatalf("expected denial state in details, got %q", completed.Details)
	}
}

func TestRuntimeRecordedProjectRoleAssignDenialDoesNotCompleteOrdinaryTask(t *testing.T) {
	task := WorkspaceTaskRecord{TaskID: "task-ordinary", ProjectID: "project-1", ProjectLane: "coordination"}
	packet := &AgentWorkPacket{PreferredTransition: "project_role_assign"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_role_assign"},
		FailedToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			IsError:  true,
			Output:   `{"boundary_transition_state":"boundary_expansion_denied_overlap","transition_denied":true,"denial_recorded":true}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "blocked", Summary: "Ordinary task still blocked."}

	got := completeRecordedAuthorityTransitionDenial(task, result, trace, packet)
	if got.Outcome != "blocked" {
		t.Fatalf("ordinary task should not be terminalized by role-scope denial receipt, got %+v", got)
	}
}

func TestRuntimeRecordedProjectRoleAssignAppliedCompletesAuthorityTask(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-epsilon",
		ProjectID:            "project-rq",
		Title:                "Resolve project role/scope request for epsilon",
		Description:          "# Strategic Lead Role/Scope Request\n\n- requester_summary: Assign epsilon the INTEGRATOR role so the accepted patch queue item can proceed to canonical integration.",
		TaskKind:             "COORDINATION",
		ProjectLane:          "coordination",
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","project_id":"project-rq","target_agent_id":"epsilon","role_type":"INTEGRATOR","active_task_id":"task-integration","branch_id":"branch-accepted"}`,
	}
	packet := &AgentWorkPacket{PreferredTransition: "project_role_assign"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_role_assign"},
		SuccessfulToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			Output:   `{"project_id":"project-rq","agent_id":"epsilon","role_id":"role-epsilon-integrator","role_type":"INTEGRATOR","status":"ACTIVE","write_scope_json":"{}"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:       "continue",
		Summary:       "Durable role transition recorded: epsilon is now INTEGRATOR.",
		NextAction:    "Continue to assembly gate.",
		RequiresHuman: true,
		OwnerAction:   "operator_decision",
		HumanReason:   "waiting on role assignment",
		DecisionType:  "role_scope",
		BlockedOn: []BlockedRef{{
			Kind:   "role_scope_pending",
			Detail: "epsilon needs INTEGRATOR role",
		}},
	}

	completed := completeRecordedAuthorityTransitionApplied(task, result, trace, packet)
	if completed.Outcome != "completed" {
		t.Fatalf("expected applied role assignment to complete authority task, got %+v", completed)
	}
	if completed.RequiresHuman || completed.OwnerAction != "" || completed.HumanReason != "" || completed.DecisionType != "" {
		t.Fatalf("applied role assignment should clear human/request fields, got %+v", completed)
	}
	if len(completed.BlockedOn) != 0 {
		t.Fatalf("authority task should not keep blocker after applied receipt, got %+v", completed.BlockedOn)
	}
	if !strings.Contains(completed.Details, "role_scope_assigned") || !strings.Contains(completed.Details, "role-epsilon-integrator") {
		t.Fatalf("expected applied receipt details, got %q", completed.Details)
	}
}

func TestRuntimeRecordedProjectRoleAssignAppliedRejectsReceiptWithoutGrantedScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-epsilon",
		ProjectID:            "project-rq",
		Title:                "Resolve project role/scope request for epsilon",
		Description:          "# Strategic Lead Role/Scope Request\n\nRun project_role_assign.",
		TaskKind:             "COORDINATION",
		ProjectLane:          "coordination",
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","project_id":"project-rq","target_agent_id":"epsilon","role_type":"INTEGRATOR"}`,
	}
	packet := &AgentWorkPacket{PreferredTransition: "project_role_assign"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_role_assign"},
		SuccessfulToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			Output:   `{"project_id":"project-rq","target_agent_id":"epsilon","role_type":"INTEGRATOR","boundary_transition_state":"authority_transition_pending","status":"PENDING"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome: "blocked",
		Summary: "No durable scope grant was recorded.",
		BlockedOn: []BlockedRef{{
			Kind:   "role_scope_pending",
			Detail: "epsilon still needs INTEGRATOR scope",
		}},
	}

	got := completeRecordedAuthorityTransitionApplied(task, result, trace, packet)
	if got.Outcome != "blocked" {
		t.Fatalf("authority transition without granted scope must not complete, got %+v", got)
	}
	if len(got.BlockedOn) == 0 {
		t.Fatalf("rejected no-grant receipt should preserve blocker context, got %+v", got)
	}
}

func TestRuntimeRecordedProjectRoleAssignAppliedDoesNotCompleteMismatchedAuthorityTask(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-epsilon",
		ProjectID:            "project-rq",
		Title:                "Resolve project role/scope request for epsilon",
		Description:          "# Strategic Lead Role/Scope Request\n\nRun project_role_assign.",
		TaskKind:             "COORDINATION",
		ProjectLane:          "coordination",
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","project_id":"project-rq","target_agent_id":"epsilon","role_type":"INTEGRATOR"}`,
	}
	packet := &AgentWorkPacket{PreferredTransition: "project_role_assign"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_role_assign"},
		SuccessfulToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			Output:   `{"project_id":"project-rq","agent_id":"zeta","role_id":"role-zeta-integrator","role_type":"INTEGRATOR","status":"ACTIVE"}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Assigned a different agent."}

	got := completeRecordedAuthorityTransitionApplied(task, result, trace, packet)
	if got.Outcome != "continue" {
		t.Fatalf("mismatched role assignment must not complete authority task, got %+v", got)
	}
}

func TestRuntimeProjectClaimRepairDoesNotCompleteAfterProjectRoleAssignWithoutFollowThrough(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-project-claim-repair-abc123",
		ProjectID:            "project-clearpress",
		Title:                "Repair project claim scope conflict",
		Description:          "A project implementation lane is blocked by an overlapping write scope.",
		TaskKind:             "COORDINATION",
		ProjectLane:          "strategy",
		Tags:                 []string{"project-claim-repair", "strategic-lead", "coordination"},
		TaskRequirementsJSON: `{"schema":"project_claim_repair_task_v1"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_role_assign", "task_submit"},
		SuccessfulToolCalls: []string{"project_role_assign", "task_submit"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			Output: `{
				"schema":"project_role_assignment_result.v1",
				"role_id":"role-beta-impl",
				"agent_id":"beta",
				"role_type":"IMPLEMENTER",
				"active_claim_rebind":{"state":"updated","task_id":"task-beta","branch_id":"branch-beta"}
			}`,
		}, {
			ToolName: "task_submit",
			Output: `{
				"created":false,
				"task_submit_gate":"product_lane_liveness",
				"blocked_creation_kind":"coordination_split",
				"existing_task_id":"task-split-rq-claim-scopes",
				"existing_status":"PENDING",
				"existing_task_kind":"COORDINATION",
				"existing_project_lane":"coordination",
				"existing_product_task_ids":["task-1780696293925408200-c816b790"]
			}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:       "continue",
		Summary:       "Role matrix updated; now checking whether beta can publish.",
		NextAction:    "Ask epsilon to validate beta head.",
		RequiresHuman: true,
		OwnerAction:   "operator_decision",
		HumanReason:   "verify branch",
		DecisionType:  "scope_repair",
		BlockedOn: []BlockedRef{{
			Kind:   "project_claim_repair",
			Detail: "repair still open",
		}},
	}

	got := completeProjectClaimRepairReceipt(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("role assignment plus blocked coordination split must not complete project claim repair, got %+v", got)
	}
	if !got.RequiresHuman || got.OwnerAction != "operator_decision" || got.HumanReason != "verify branch" || got.DecisionType != "scope_repair" {
		t.Fatalf("non-terminal repair should preserve human/request fields, got %+v", got)
	}
	if len(got.BlockedOn) != 1 {
		t.Fatalf("non-terminal repair should keep blocker, got %+v", got.BlockedOn)
	}
	if traceHasRequiredTransitionReceipt(trace, "project_claim_repair_receipt") {
		t.Fatalf("role assignment plus blocked coordination split must not count as terminal project_claim_repair_receipt")
	}
	packet := &AgentWorkPacket{PreferredTransition: "project_claim_repair_receipt", WorkType: "project_claim_repair_authority_transition"}
	if requiredTool, ok := requiredTransitionReceiptPreemptsToolCall(task, packet, trace); ok {
		t.Fatalf("non-terminal project claim repair receipt must not preempt follow-through tools, got %q", requiredTool)
	}
}

func TestRuntimeProjectClaimRepairDoesNotCompleteAfterProjectRoleAssignDenialReceipt(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-denied",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_role_assign"},
		FailedToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			IsError:  true,
			Output:   `{"boundary_transition_state":"boundary_expansion_denied_overlap","transition_denied":true,"denial_recorded":true,"denial_reason":"overlaps_live_owner_lane"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "Repair denied because beta owns the overlapping lane.",
		BlockedOn: []BlockedRef{{Kind: "boundary_transition_denied", Detail: "overlap"}},
	}

	got := completeProjectClaimRepairReceipt(task, result, trace)
	if got.Outcome != "blocked" {
		t.Fatalf("project claim repair denial alone must keep the repair blocked, got %+v", got)
	}
	if len(got.BlockedOn) != 1 || got.BlockedOn[0].Kind != "boundary_transition_denied" {
		t.Fatalf("denial-only repair should preserve the typed blocker, got %+v", got.BlockedOn)
	}
	if traceHasRequiredTransitionReceipt(trace, "project_claim_repair_receipt") {
		t.Fatalf("project_role_assign denial alone must not count as terminal project_claim_repair_receipt")
	}
}

func TestRuntimeRequiredTransitionGateRejectsProjectClaimRepairDenialAsCompletion(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-denied",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	session := AgentSessionStateRecord{SessionID: "session-repair", AgentID: "alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	packet := &AgentWorkPacket{PreferredTransition: "project_claim_repair_receipt", WorkType: "project_claim_repair_authority_transition"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_role_assign"},
		FailedToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			IsError:  true,
			Output:   `{"boundary_transition_state":"boundary_expansion_denied_overlap","transition_denied":true,"denial_recorded":true,"denial_reason":"overlaps_live_owner_lane"}`,
		}},
	}

	completed := StructuredTaskResult{Outcome: "completed", Summary: "Role/scope repair denied; closing this repair."}
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-repair", &completed, trace, packet); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate(completed) error = %v", err)
	}
	if completed.Outcome != "continue" {
		t.Fatalf("denial-only completion should be rejected back to required transition, got %+v", completed)
	}
	if runtime.scratch.RequiredTransitionCount != 1 || runtime.scratch.RequiredTransitionTool != "project_claim_repair_receipt" {
		t.Fatalf("denial-only completion should keep repair receipt gate active, got %+v", runtime.scratch)
	}

	blocked := StructuredTaskResult{
		Outcome: "blocked",
		Summary: "No eligible claimant can exist while the overlapping lane remains active.",
		BlockedOn: []BlockedRef{{
			Kind:   "project_claim_repair_follow_through_blocker",
			Detail: "role/scope denial recorded; blocked task is still not claimable",
		}},
	}
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-repair", &blocked, trace, packet); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate(blocked) error = %v", err)
	}
	if blocked.Outcome != "blocked" || len(blocked.BlockedOn) != 1 {
		t.Fatalf("typed blocker should remain the terminal gate state, got %+v", blocked)
	}
	if runtime.scratch.RequiredTransitionCount != 0 || runtime.scratch.RequiredTransitionTool != "" {
		t.Fatalf("typed blocker should clear required-transition counter, got %+v", runtime.scratch)
	}
}

func TestRuntimeProjectClaimRepairDoesNotCompleteOnLeadRequestQueuedReceipt(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-queued",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_role_assign"},
		SuccessfulToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			Output:   `{"lead_task_id":"task-role-scope-beta","task_created":true,"do_not_retry":true,"next_action":"Stop retrying project_role_assign from this non-lead context."}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Queued a lead request."}

	got := completeProjectClaimRepairReceipt(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("lead-request queue receipt must not complete project claim repair, got %+v", got)
	}
}

func TestRuntimeProjectClaimRepairCompletesAfterPatchQueueFollowupHandoff(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-followup",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_patch_queue_followup"},
		SuccessfulToolCalls: []string{"project_patch_queue_followup"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_followup",
			Output: `{
				"created":false,
				"reason":"existing_followup_task_active",
				"project_id":"project-clearpress",
				"branch_id":"projbranch-1779577905424940037-151",
				"queue_id":"patchq-clearpress",
				"item_id":"patchitem-clearpress",
				"state":"BLOCKED",
				"followup_kind":"validation",
				"task_id":"task-1779579251068865600-af404f2a",
				"existing_task_id":"task-1779579251068865600-af404f2a",
				"existing_status":"PENDING",
				"terminal_existing":false
			}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:       "continue",
		Summary:       "Follow-up exists; now asking validators to take it.",
		NextAction:    "Delegate validation.",
		RequiresHuman: true,
		BlockedOn:     []BlockedRef{{Kind: "project_claim_repair", Detail: "handoff not closed"}},
	}

	got := completeProjectClaimRepairReceipt(task, result, trace)
	if got.Outcome != "completed" {
		t.Fatalf("project claim repair should complete after patch queue follow-up handoff, got %+v", got)
	}
	if len(got.BlockedOn) != 0 || got.RequiresHuman {
		t.Fatalf("handoff completion should clear blockers/human state, got %+v", got)
	}
	if !strings.Contains(got.Details, "task-1779579251068865600-af404f2a") {
		t.Fatalf("expected follow-up task in details, got %q", got.Details)
	}
}

func TestRuntimeProjectClaimRepairDoesNotCompleteOnTerminalPatchQueueFollowup(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-terminal-followup",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_patch_queue_followup"},
		SuccessfulToolCalls: []string{"project_patch_queue_followup"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_followup",
			Output: `{
				"created":false,
				"reason":"existing_followup_task_terminal",
				"state":"BLOCKED",
				"followup_kind":"validation",
				"task_id":"task-terminal",
				"existing_task_id":"task-terminal",
				"existing_status":"RESOLVED",
				"terminal_existing":true
			}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Terminal follow-up exists; inspect current evidence."}

	got := completeProjectClaimRepairReceipt(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("terminal existing follow-up should not be treated as executable handoff, got %+v", got)
	}
}

func TestRuntimeProjectClaimRepairCompletesAfterTaskSubmitHandoff(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-tasksubmit",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"task_submit"},
		SuccessfulToolCalls: []string{"task_submit"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "task_submit",
			Output: `{
				"branch_id":"projbranch-1779577905424940037-151",
				"created":false,
				"existing_status":"PENDING",
				"existing_task_id":"task-clearpress-visual-source-validation-20260524-0333",
				"existing_task_kind":"EXECUTION",
				"existing_project_lane":"validation"
			}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:   "continue",
		Summary:   "Reused the existing validation task and asked peers to handle it.",
		BlockedOn: []BlockedRef{{Kind: "project_claim_repair", Detail: "handoff still open"}},
	}

	got := completeProjectClaimRepairReceipt(task, result, trace)
	if got.Outcome != "completed" {
		t.Fatalf("project claim repair should complete after task_submit active handoff, got %+v", got)
	}
	if len(got.BlockedOn) != 0 {
		t.Fatalf("handoff completion should clear blockers, got %+v", got.BlockedOn)
	}
	if !strings.Contains(got.Details, "task-clearpress-visual-source-validation-20260524-0333") {
		t.Fatalf("expected submitted task in details, got %q", got.Details)
	}
}

func TestRuntimeProjectClaimRepairDoesNotCompleteOnCoordinationTaskSubmitGate(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-coordination-gate",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"task_submit"},
		SuccessfulToolCalls: []string{"task_submit"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "task_submit",
			Output: `{
				"created":false,
				"task_submit_gate":"product_lane_liveness",
				"blocked_creation_kind":"coordination_split",
				"existing_task_id":"task-split-rq-claim-scopes",
				"existing_status":"PENDING",
				"existing_task_kind":"COORDINATION",
				"existing_project_lane":"coordination",
				"existing_product_task_ids":["task-1780696293925408200-c816b790"]
			}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:   "continue",
		Summary:   "A coordination split was refused under product pressure.",
		BlockedOn: []BlockedRef{{Kind: "project_claim_repair", Detail: "product lane still not claimable"}},
	}

	got := completeProjectClaimRepairReceipt(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("blocked coordination split must not complete project claim repair, got %+v", got)
	}
	if len(got.BlockedOn) != 1 {
		t.Fatalf("repair blocker should remain when only coordination split was blocked, got %+v", got.BlockedOn)
	}
}

func TestRuntimeProjectClaimRepairDoesNotCompleteOnUnboundCoordinationIntegrationTask(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-unbound-integration",
		ProjectID:   "project-signal01-rq-root",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"task_submit"},
		SuccessfulToolCalls: []string{"task_submit"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "task_submit",
			Output: `{
				"task_id":"task-1780787830378194400-bc1973cb",
				"status":"PENDING",
				"task_kind":"COORDINATION",
				"project_lane":"integration",
				"task_requirements":{
					"schema":"task_requirements.v1",
					"evidence_needed":["exact branch/head SHA","go build ./...","go test ./..."]
				}
			}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:   "continue",
		Summary:   "Submitted an integration validation task without patch queue identity.",
		BlockedOn: []BlockedRef{{Kind: "project_claim_repair", Detail: "handoff still lacks integration materialization binding"}},
	}

	got := completeProjectClaimRepairReceipt(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("unbound coordination/integration task_submit must not complete project claim repair, got %+v", got)
	}
	if len(got.BlockedOn) != 1 {
		t.Fatalf("repair blocker should remain for unbound coordination/integration task, got %+v", got.BlockedOn)
	}
}

func TestRuntimeProjectClaimRepairCompletesAfterPatchQueueTaskSubmitHandoff(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-patchq-tasksubmit",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"task_submit"},
		SuccessfulToolCalls: []string{"task_submit"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "task_submit",
			Output: `{
				"created":false,
				"task_submit_gate":"patch_queue_identity_duplicate",
				"existing_task_id":"task-patchq-revision-clearpress",
				"existing_status":"PENDING",
				"queue_id":"patchq-clearpress",
				"item_id":"patchitem-clearpress",
				"required_transition":"project_patch_queue_followup"
			}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:   "continue",
		Summary:   "Patch queue follow-through task exists.",
		BlockedOn: []BlockedRef{{Kind: "project_claim_repair", Detail: "handoff still open"}},
	}

	got := completeProjectClaimRepairReceipt(task, result, trace)
	if got.Outcome != "completed" {
		t.Fatalf("patch-queue task_submit handoff should complete project claim repair, got %+v", got)
	}
	if len(got.BlockedOn) != 0 {
		t.Fatalf("handoff completion should clear blockers, got %+v", got.BlockedOn)
	}
}

func TestRuntimeProjectClaimRepairDoesNotCompleteOnTerminalTaskSubmitHandoff(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-terminal-tasksubmit",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"task_submit"},
		SuccessfulToolCalls: []string{"task_submit"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "task_submit",
			Output:   `{"existing_status":"RESOLVED","existing_task_id":"task-terminal"}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Terminal submitted task exists."}

	got := completeProjectClaimRepairReceipt(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("terminal submitted task should not complete claim repair, got %+v", got)
	}
}

func TestRuntimeProjectClaimRepairIntegratedPatchQueueConflictIsDurableReceipt(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-3fd9f2d0ae",
		ProjectID:   "project-signal01-rq-s1",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "blocker-unblock"},
		Description: strings.Join([]string{
			"A project implementation lane is blocked by an overlapping write scope.",
			"",
			"Conflict:",
			"- repo_id: repo-signal01-rq-core",
			"- conflict_branch_id: projbranch-1781296982949048677-119",
			"- conflict_branch_head_sha: 9cf9794b152f1a469f4e9b9cc23e0377c9b7a5a2",
			"- conflict_patch_item_id: patchitem-projbranch-1781296982949048677-119",
			"- conflict_patch_state: PROPOSED",
		}, "\n"),
	}
	conflict := projectClaimRepairPatchQueueConflictFromTask(task)
	item := ProjectPatchQueueItemRecord{
		QueueID:  "patchq-project-signal01-rq-s1-repo-signal01-rq-core",
		ItemID:   "patchitem-projbranch-1781296982949048677-119",
		RepoID:   "repo-signal01-rq-core",
		BranchID: "projbranch-1781296982949048677-119",
		HeadSHA:  "9cf9794b152f1a469f4e9b9cc23e0377c9b7a5a2",
		State:    "INTEGRATED",
	}
	if !conflict.Actionable() || !projectClaimRepairPatchQueueConflictMatchesItem(conflict, item) {
		t.Fatalf("expected R32 claim-repair conflict to match integrated patch queue item, conflict=%+v item=%+v", conflict, item)
	}
	result := StructuredTaskResult{
		Outcome: "continue",
		Summary: "Required durable transition was not executed",
		BlockedOn: []BlockedRef{{
			Kind:   "required_transition_not_executed",
			Detail: "project_claim_repair_receipt",
		}},
	}
	completeProjectClaimRepairResult(&result, "Project claim repair conflict is already resolved by durable patch queue integration; item=patchq/patchitem.")
	if result.Outcome != "completed" || len(result.BlockedOn) != 0 || result.RequiresHuman {
		t.Fatalf("integrated patch queue receipt should terminalize claim repair, got %+v", result)
	}
	if !strings.Contains(result.Details, "durable patch queue integration") {
		t.Fatalf("expected durable receipt in details, got %q", result.Details)
	}
}

func TestRuntimeProjectClaimRepairGateCompletesWhenConflictPatchQueueItemIntegrated(t *testing.T) {
	var methods []string
	var listParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)
		result := map[string]any{}
		switch method {
		case "project.patch_queue.list":
			listParams, _ = req["params"].(map[string]any)
			result = map[string]any{
				"patch_queue_items": []map[string]any{{
					"queue_id":     "patchq-project-signal01-rq-s1-repo-signal01-rq-core",
					"item_id":      "patchitem-projbranch-1781296982949048677-119",
					"workspace_id": "rhizome-main",
					"project_id":   "project-signal01-rq-s1",
					"repo_id":      "repo-signal01-rq-core",
					"branch_id":    "projbranch-1781296982949048677-119",
					"head_sha":     "9cf9794b152f1a469f4e9b9cc23e0377c9b7a5a2",
					"state":        "INTEGRATED",
				}},
			}
		case "agent.state.set":
			result = map[string]any{"ok": true}
		default:
			t.Fatalf("unexpected method %s", method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result":  result,
		})
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "rhizome-main", AgentID: "beta"},
		client: NewRhizomeClient(server.URL, ""),
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-project-claim-repair-3fd9f2d0ae",
			RequiredTransitionSessionID: "session-repair",
			RequiredTransitionTool:      "project_claim_repair_receipt",
			RequiredTransitionCount:     1,
			RequiredTransitionSummary:   "Required durable transition was not executed",
			RequiredTransitionAt:        "2026-06-12T20:57:57Z",
			RequiredTransitionRunID:     "run-repair",
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-3fd9f2d0ae",
		ProjectID:   "project-signal01-rq-s1",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "blocker-unblock"},
		Description: strings.Join([]string{
			"Conflict:",
			"- repo_id: repo-signal01-rq-core",
			"- conflict_branch_id: projbranch-1781296982949048677-119",
			"- conflict_branch_head_sha: 9cf9794b152f1a469f4e9b9cc23e0377c9b7a5a2",
			"- conflict_patch_item_id: patchitem-projbranch-1781296982949048677-119",
			"- conflict_patch_state: PROPOSED",
		}, "\n"),
	}
	session := AgentSessionStateRecord{SessionID: "session-repair", AgentID: "beta", TaskID: task.TaskID, Status: "ACTIVE"}
	result := StructuredTaskResult{
		Outcome: "continue",
		Summary: "Required durable transition was not executed",
		BlockedOn: []BlockedRef{{
			Kind:   "required_transition_not_executed",
			Detail: "project_claim_repair_receipt",
		}},
	}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-repair", &result, nil, &AgentWorkPacket{PreferredTransition: "project_claim_repair_receipt"}); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if strings.Join(methods, ",") != "project.patch_queue.list,agent.state.set" {
		t.Fatalf("expected list + scratch save, got %v", methods)
	}
	if listParams["repo_id"] != "repo-signal01-rq-core" || listParams["branch_id"] != "projbranch-1781296982949048677-119" {
		t.Fatalf("expected scoped patch queue lookup, got %+v", listParams)
	}
	if result.Outcome != "completed" || len(result.BlockedOn) != 0 || result.RequiresHuman {
		t.Fatalf("integrated patch queue item should complete claim repair gate, got %+v", result)
	}
	if runtime.scratch.RequiredTransitionTool != "" || runtime.scratch.RequiredTransitionCount != 0 {
		t.Fatalf("terminal durable receipt should clear required-transition counter, got %+v", runtime.scratch)
	}
}

func TestRuntimeSideEffectResolveSuccessorHandoffCompletesParentClassifier(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-classify-gamma",
		ProjectID:            "project-1",
		ProjectLane:          "coordination",
		Tags:                 []string{"side-effect-classification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"request_verification","parent_classification_state":"waiting_on_successors","parent_classification_blocked":true,"followup_task_id":"task-side-effect-verify-tooling","followup_action_kind":"verify_bucket","successor_key":"abpc-resolution-successor:abc123"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Verification successor created.",
		NextAction: "Continue classifying the parent.",
	}

	got := completeSideEffectClassifierSuccessorHandoff(task, result, trace)
	if got.Outcome != "completed" {
		t.Fatalf("parent classifier should terminalize after successor handoff receipt, got %+v", got)
	}
	if got.RequiresHuman || got.OwnerAction != "" || got.HumanReason != "" || got.DecisionType != "" || len(got.BlockedOn) != 0 {
		t.Fatalf("handoff completion should clear blocker/request fields, got %+v", got)
	}
	if !strings.Contains(got.NextAction, "task-side-effect-verify-tooling") {
		t.Fatalf("expected successor route in next action, got %q", got.NextAction)
	}
	if !strings.Contains(got.Details, "waiting") && !strings.Contains(got.Details, "recovery successor") {
		t.Fatalf("expected successor handoff note in details, got %q", got.Details)
	}
}

func TestRuntimeSideEffectResolveResolvedSuccessorHandoffCompletesParentClassifier(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-classify-gamma",
		ProjectID:            "project-1",
		ProjectLane:          "coordination",
		Tags:                 []string{"side-effect-classification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"request_verification","parent_classification_state":"resolved_waiting_on_successors","parent_classification_resolved_split":true,"followup_task_id":"task-side-effect-verify-tooling","followup_action_kind":"verify_bucket","successor_key":"abpc-resolution-successor:abc123"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome: "blocked",
		Summary: "Fresh verification needs a successor.",
		BlockedOn: []BlockedRef{{
			Kind:   "side_effect_classification",
			Detail: "waiting on successor",
		}},
	}

	got := completeSideEffectClassifierSuccessorHandoff(task, result, trace)
	if got.Outcome != "completed" {
		t.Fatalf("resolved successor handoff should terminalize parent classifier session, got %+v", got)
	}
	if len(got.BlockedOn) != 0 || got.RequiresHuman {
		t.Fatalf("resolved successor handoff should clear blocked fields, got %+v", got)
	}
}

func TestRuntimeSideEffectClassifierTerminalDecisionCompletesAppliedAuthorityReceipt(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-classify-beta",
		ProjectID:            "project-clearpress",
		ProjectLane:          "coordination",
		Tags:                 []string{"side-effect-classification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"expand_boundary","classification_task_id":"task-side-effect-classify-beta","side_effect_refs":["side-effect:clearpress","side-effect:test-setup"],"boundary_transition_state":"authority_transition_applied","authority_transition_applied":true,"transition_executed":true,"existing_task_id":"task-role-scope-c3f628c747"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Applied authority transition recognized.",
		NextAction: "Record the decision in another task.",
		BlockedOn:  []BlockedRef{{Kind: "side_effect_classification", Detail: "pending"}},
	}

	got := completeSideEffectClassifierTerminalDecision(task, result, trace)
	if got.Outcome != "completed" {
		t.Fatalf("classifier should terminalize after applied side_effect_resolve receipt, got %+v", got)
	}
	if got.RequiresHuman || got.OwnerAction != "" || got.HumanReason != "" || got.DecisionType != "" || len(got.BlockedOn) != 0 {
		t.Fatalf("terminal side-effect receipt should clear blocker/request fields, got %+v", got)
	}
	if !strings.Contains(got.Details, "authority_transition_applied") || !strings.Contains(got.Details, "side-effect:clearpress") {
		t.Fatalf("expected applied receipt details, got %q", got.Details)
	}
}

func TestRuntimeSideEffectClassifierTerminalDecisionCompletesOverlapDenialReceipt(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-classify-gamma",
		ProjectID:            "project-clearpress",
		ProjectLane:          "coordination",
		Tags:                 []string{"side-effect-classification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"expand_boundary","classification_task_id":"task-side-effect-classify-gamma","side_effect_refs":["side-effect:gamma-root"],"boundary_transition_state":"boundary_expansion_denied_overlap","transition_denied":true,"do_not_retry":true,"preferred_transition":"wait_or_split_existing_owner_lane","allowed_next":["wait_for_conflicting_lane_publication","request_merge_or_adopt"]}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Expansion was denied."}

	got := completeSideEffectClassifierTerminalDecision(task, result, trace)
	if got.Outcome != "completed" {
		t.Fatalf("classifier should terminalize after overlap denial receipt, got %+v", got)
	}
	if !strings.Contains(got.Details, "boundary_expansion_denied_overlap") {
		t.Fatalf("expected denial details, got %q", got.Details)
	}
}

func TestRuntimeSideEffectClassifierTerminalDecisionCompletesQuarantineReceipt(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-classify-readme",
		ProjectID:            "project-clearpress",
		ProjectLane:          "coordination",
		Tags:                 []string{"side-effect-classification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"quarantine","classification_task_id":"task-side-effect-classify-readme","side_effect_refs":["side-effect:readme.md"],"integration_status":"quarantined","next_transition":"quarantine_materialization"}`,
		}},
	}
	result := StructuredTaskResult{
		Outcome:   "continue",
		Summary:   "Quarantined the README side effect.",
		BlockedOn: []BlockedRef{{Kind: "side_effect_classification", Detail: "pending release"}},
	}

	got := completeSideEffectClassifierTerminalDecision(task, result, trace)
	if got.Outcome != "completed" {
		t.Fatalf("classifier should terminalize after quarantine side_effect_resolve receipt, got %+v", got)
	}
	if len(got.BlockedOn) != 0 || got.RequiresHuman {
		t.Fatalf("terminal quarantine receipt should clear blockers, got %+v", got)
	}
	if !strings.Contains(got.Details, "quarantine") || !strings.Contains(got.NextAction, "terminal") {
		t.Fatalf("expected quarantine terminal receipt details, got details=%q next=%q", got.Details, got.NextAction)
	}
}

func TestRuntimeSideEffectClassifierTerminalDecisionDoesNotCompletePendingAuthorityTransition(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-classify-beta",
		ProjectID:            "project-clearpress",
		ProjectLane:          "coordination",
		Tags:                 []string{"side-effect-classification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"expand_boundary","classification_task_id":"task-side-effect-classify-beta","side_effect_refs":["side-effect:clearpress"],"boundary_transition_state":"authority_transition_already_pending","transition_executed":false,"transition_already_pending":true,"lead_task_id":"task-role-scope-existing"}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Authority transition still pending."}

	got := completeSideEffectClassifierTerminalDecision(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("pending authority transition must not terminalize classifier, got %+v", got)
	}
}

func TestRuntimeSideEffectClassifierTerminalDecisionIgnoresMismatchedClassifierID(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-classify-beta",
		ProjectID:            "project-clearpress",
		ProjectLane:          "coordination",
		Tags:                 []string{"side-effect-classification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"expand_boundary","classification_task_id":"task-side-effect-classify-other","boundary_transition_state":"authority_transition_applied","authority_transition_applied":true,"transition_executed":true}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Different classifier."}

	got := completeSideEffectClassifierTerminalDecision(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("mismatched classifier receipt must not terminalize this classifier, got %+v", got)
	}
}

func TestRuntimeSideEffectClassifierBlockedOnSuccessorAllowsCompletion(t *testing.T) {
	agentID := "epsilon"
	claimAgent := agentID
	claimStatus := "BLOCKED"
	claimSummary := "waiting_on_side_effect_resolution_successors:task-side-effect-verify-tooling"
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-classify-gamma",
		Status:               "RUNNING",
		ProjectID:            "project-1",
		ProjectLane:          "coordination",
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification"}`,
		ClaimAgentID:         &claimAgent,
		ClaimStatus:          &claimStatus,
		ClaimSummary:         &claimSummary,
	}

	if !taskAllowsCompletionTransition(task, agentID) {
		t.Fatalf("blocked parent classifier waiting on successor should allow same-owner completion")
	}

	claimSummary = "blocked_on_unrelated_dependency"
	task.ClaimSummary = &claimSummary
	if taskAllowsCompletionTransition(task, agentID) {
		t.Fatalf("ordinary blocked classifier must not allow late completion")
	}
}

func TestRuntimeSideEffectResolveSuccessorHandoffDoesNotCompleteVerificationSuccessor(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-verify-tooling",
		ProjectID:            "project-1",
		ProjectLane:          "verification",
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_verification","action_kind":"verify_bucket"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"request_verification","parent_classification_state":"waiting_on_successors","followup_task_id":"task-side-effect-another"}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Verification successor still needs final verdict."}

	got := completeSideEffectClassifierSuccessorHandoff(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("verification successor must not be terminalized as a parent classifier, got %+v", got)
	}
}

func TestRuntimeSideEffectResolutionSuccessorReceiptCompletesRepeatedVerificationRequest(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-verify-tooling",
		ProjectID:            "project-1",
		ProjectLane:          "verification",
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_verification","action_kind":"verify_bucket","successor_key":"abpc-resolution-successor:verify","decision":"request_verification"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"request_verification","integration_status":"verification_requested","followup_task_id":"task-side-effect-verify-tooling","existing_task_id":"task-side-effect-verify-tooling","followup_created":false,"self_recursive_recovery_collapsed":true,"successor_key":"abpc-resolution-successor:verify","next_transition":"route_to_verifier"}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Verification successor reused itself."}

	got := completeSideEffectResolutionSuccessorReceipt(task, result, trace)
	if got.Outcome != "completed" {
		t.Fatalf("expected repeated verification request receipt to complete current successor, got %+v", got)
	}
	if got.RequiresHuman || got.OwnerAction != "" || got.HumanReason != "" || got.DecisionType != "" || len(got.BlockedOn) != 0 {
		t.Fatalf("successor completion should clear blocker/request fields, got %+v", got)
	}
	if !strings.Contains(got.Details, "decision=request_verification") {
		t.Fatalf("expected verification decision note in details, got %q", got.Details)
	}
}

func TestRuntimeSideEffectResolutionSuccessorReceiptCompletesCurrentSuccessor(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-quarantine-package-lock",
		ProjectID:            "project-1",
		ProjectLane:          "coordination",
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_cleanup","action_kind":"quarantine_bucket","successor_key":"abpc-resolution-successor:pkg","decision":"quarantine"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"quarantine","integration_status":"quarantined","followup_task_id":"task-side-effect-quarantine-package-lock","followup_created":false,"successor_key":"abpc-resolution-successor:pkg","next_transition":"quarantine_materialization"}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Quarantined package-lock side effect."}

	got := completeSideEffectResolutionSuccessorReceipt(task, result, trace)
	if got.Outcome != "completed" {
		t.Fatalf("expected recovery successor to complete from side_effect_resolve receipt, got %+v", got)
	}
	if strings.Contains(strings.ToLower(got.NextAction), "status") {
		t.Fatalf("successor receipt should route by transition, not status-only progress: %+v", got)
	}
}

func TestRuntimeSideEffectResolutionSuccessorReceiptDoesNotCompleteUnmatchedSuccessor(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-quarantine-package-lock",
		ProjectID:            "project-1",
		ProjectLane:          "coordination",
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_cleanup","action_kind":"quarantine_bucket","successor_key":"abpc-resolution-successor:pkg","decision":"quarantine"}`,
	}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"side_effect_resolve"},
		SuccessfulToolCalls: []string{"side_effect_resolve"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "side_effect_resolve",
			Output:   `{"status":"decision_recorded","decision":"quarantine","followup_task_id":"task-side-effect-other","successor_key":"abpc-resolution-successor:other"}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Other successor moved."}

	got := completeSideEffectResolutionSuccessorReceipt(task, result, trace)
	if got.Outcome != "continue" {
		t.Fatalf("unmatched side_effect_resolve receipt must not complete this successor, got %+v", got)
	}
}

func TestRuntimeRequiredTransitionGateRejectsUnrecordedProjectRoleAssignFailure(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-role-scope-gamma",
			RequiredTransitionSessionID: "session-role-scope",
			RequiredTransitionTool:      "project_role_assign",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{TaskID: "task-role-scope-gamma", ProjectID: "project-1", ProjectLane: "coordination"}
	session := AgentSessionStateRecord{SessionID: "session-role-scope", AgentID: "alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	packet := &AgentWorkPacket{PreferredTransition: "project_role_assign"}
	trace := &TaskRunTrace{
		ToolCalls:       []string{"project_role_assign"},
		FailedToolCalls: []string{"project_role_assign"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_role_assign",
			IsError:  true,
			Output:   `{"boundary_transition_state":"boundary_expansion_denied_overlap","transition_denied":true}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "completed", Summary: "Gamma expansion denied, but no durable denial update was recorded."}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-role-scope", &result, trace, packet); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "blocked" {
		t.Fatalf("unrecorded failed project_role_assign should still trip required-transition gate, got %+v", result)
	}
	if len(result.BlockedOn) != 1 || result.BlockedOn[0].Kind != "required_transition_not_executed" {
		t.Fatalf("expected required-transition blocker, got %+v", result.BlockedOn)
	}
}

func TestRuntimeRequiredTransitionGateAcceptsReadyForReviewRebindDenialReceipt(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{TaskID: "task-role-scope-beta", ProjectID: "project-1", ProjectLane: "coordination"}
	session := AgentSessionStateRecord{SessionID: "session-role-scope", AgentID: "alpha", TaskID: task.TaskID, Status: "ACTIVE"}
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
	result := StructuredTaskResult{Outcome: "continue", Summary: "Ready branch cannot be widened in place."}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-role-scope", &result, trace, packet); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "continue" || len(result.BlockedOn) != 0 {
		t.Fatalf("recorded ready-for-review denial should satisfy required transition gate, got %+v", result)
	}
}

func TestRuntimeRequiredTransitionGateInfersSideEffectResolveTask(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-classify-1",
		ProjectID:            "project-1",
		ProjectLane:          "coordination",
		Tags:                 []string{"side-effect-classification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification"}`,
	}
	if got := requiredTransitionToolForTaskCycle(task, nil); got != "side_effect_resolve" {
		t.Fatalf("expected side_effect_resolve required transition, got %q", got)
	}
}

func TestRuntimeRequiredTransitionGateInfersSideEffectVerificationSuccessor(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-side-effect-verify-1",
		ProjectID:            "project-1",
		ProjectLane:          "review",
		Tags:                 []string{"side-effect-resolution", "verification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_verification","action_kind":"verify_bucket"}`,
	}
	if got := requiredTransitionToolForTaskCycle(task, nil); got != "side_effect_resolve" {
		t.Fatalf("expected verification successor to require side_effect_resolve receipt, got %q", got)
	}
}

func TestRuntimeRequiredTransitionGateInfersGenericBoundaryDecisionTask(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-record-boundary-decision",
		ProjectID:            "project-1",
		ProjectLane:          "coordination",
		Title:                "Record durable boundary expansion decision",
		Tags:                 []string{"side-effects", "boundary", "authority-transition"},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","classification_decision":"expand_boundary","source_task_id":"task-side-effect-classify-1","side_effect_refs":["side-effect:one"]}`,
	}
	if got := requiredTransitionToolForTaskCycle(task, nil); got != "side_effect_resolve" {
		t.Fatalf("expected generic boundary decision task to require side_effect_resolve receipt, got %q", got)
	}
}

func TestRuntimeRequiredTransitionGateInfersProjectRoleAssignTask(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-beta",
		ProjectID:            "project-1",
		Title:                "Rebalance beta lane authority",
		Description:          "Fresh phrasing without the legacy role/scope header.",
		TaskKind:             "COORDINATION",
		ProjectLane:          "coordination",
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign"}`,
	}
	if got := requiredTransitionToolForTaskCycle(task, &AgentWorkPacket{PreferredTransition: "start_new"}); got != "project_role_assign" {
		t.Fatalf("expected project_role_assign required transition, got %q", got)
	}
}

func TestRuntimeRequiredTransitionGateInfersProjectClaimRepairReceipt(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-abc123",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	if got := requiredTransitionToolForTaskCycle(task, &AgentWorkPacket{PreferredTransition: "start_new"}); got != "project_claim_repair_receipt" {
		t.Fatalf("expected project_claim_repair_receipt required transition, got %q", got)
	}

	roleScope := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-gamma",
		ProjectID:            "project-clearpress",
		Title:                "Rebalance gamma lane authority",
		Description:          "Fresh phrasing without the legacy role/scope header.",
		TaskKind:             "COORDINATION",
		ProjectLane:          "coordination",
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign"}`,
	}
	if got := requiredTransitionToolForTaskCycle(roleScope, &AgentWorkPacket{PreferredTransition: "start_new"}); got != "project_role_assign" {
		t.Fatalf("role-scope authority task should still require project_role_assign, got %q", got)
	}
}

func TestRuntimeRequiredTransitionGateBlocksRepeatedProjectClaimRepairNarration(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-abc123",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	session := AgentSessionStateRecord{SessionID: "session-repair", AgentID: "alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	packet := &AgentWorkPacket{PreferredTransition: "project_claim_repair_receipt", WorkType: "project_claim_repair_authority_transition"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"agent_request", "workspace_doc_put"},
		SuccessfulToolCalls: []string{"agent_request", "workspace_doc_put"},
	}
	first := StructuredTaskResult{Outcome: "continue", Summary: "Asked validators and wrote a repair note."}
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-repair", &first, trace, packet); err != nil {
		t.Fatalf("first applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if first.Outcome != "continue" || runtime.scratch.RequiredTransitionTool != "project_claim_repair_receipt" {
		t.Fatalf("first missing repair receipt should keep lane active with counter, got result=%+v scratch=%+v", first, runtime.scratch)
	}
	if !strings.Contains(first.NextAction, "project_patch_queue_followup") {
		t.Fatalf("expected repair receipt next action, got %q", first.NextAction)
	}

	second := StructuredTaskResult{Outcome: "continue", Summary: "Published another status-only repair note."}
	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-repair", &second, trace, packet); err != nil {
		t.Fatalf("second applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second missing repair receipt should block narration, got %+v", second)
	}
	if len(second.BlockedOn) != 1 || second.BlockedOn[0].Kind != "required_transition_not_executed" {
		t.Fatalf("expected required transition blocker, got %+v", second.BlockedOn)
	}
}

func TestRuntimeRequiredTransitionGateAcceptsProjectClaimRepairFollowupReceipt(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{
			DocSHAs:                     map[string]string{},
			RequiredTransitionTaskID:    "task-project-claim-repair-abc123",
			RequiredTransitionSessionID: "session-repair",
			RequiredTransitionTool:      "project_claim_repair_receipt",
			RequiredTransitionCount:     1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-abc123",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	session := AgentSessionStateRecord{SessionID: "session-repair", AgentID: "alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	packet := &AgentWorkPacket{PreferredTransition: "project_claim_repair_receipt", WorkType: "project_claim_repair_authority_transition"}
	trace := &TaskRunTrace{
		ToolCalls:           []string{"project_patch_queue_followup"},
		SuccessfulToolCalls: []string{"project_patch_queue_followup"},
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_followup",
			Output:   `{"state":"BLOCKED","followup_kind":"validation","existing_task_id":"task-validation","existing_status":"PENDING","terminal_existing":false}`,
		}},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Follow-up route exists."}

	if err := runtime.applyRequiredTransitionExecutionGate(context.Background(), task, session, "run-repair", &result, trace, packet); err != nil {
		t.Fatalf("applyRequiredTransitionExecutionGate() error = %v", err)
	}
	if result.Outcome != "continue" {
		t.Fatalf("repair receipt should preserve outcome before terminalization transform, got %+v", result)
	}
	if runtime.scratch.RequiredTransitionCount != 0 || runtime.scratch.RequiredTransitionTool != "" {
		t.Fatalf("repair receipt should clear required-transition counter, got %+v", runtime.scratch)
	}
}

func TestRuntimeRequiredTransitionReceiptYieldStopsPostReceiptToolChatter(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-project-claim-repair-abc123",
		ProjectID:   "project-clearpress",
		Title:       "Repair project claim scope conflict",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
		Tags:        []string{"project-claim-repair", "strategic-lead", "coordination"},
	}
	packet := &AgentWorkPacket{PreferredTransition: "project_claim_repair_receipt", WorkType: "project_claim_repair_authority_transition"}
	trace := &TaskRunTrace{
		ToolReceipts: []TaskRunToolReceipt{{
			ToolName: "project_patch_queue_followup",
			Output:   `{"state":"BLOCKED","followup_kind":"validation","existing_task_id":"task-validation","existing_status":"PENDING","terminal_existing":false}`,
		}},
	}

	requiredTool, ok := requiredTransitionReceiptPreemptsToolCall(task, packet, trace)
	if !ok || requiredTool != "project_claim_repair_receipt" {
		t.Fatalf("expected post-receipt claim repair tool chatter to be preempted, tool=%q ok=%v", requiredTool, ok)
	}
	result := requiredTransitionReceiptYieldToolResult("agent_request", requiredTool)
	if !result.IsError || !strings.Contains(result.Output, requiredTransitionReceiptYieldMarker) || !strings.Contains(result.Output, "project_claim_repair_receipt") {
		t.Fatalf("expected required-transition receipt yield result, got %+v", result)
	}
}

func TestRuntimeNoopContinueGuardClearsAfterDurableProgress(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			NoopContinueTaskID:    "task-impl",
			NoopContinueSessionID: "session-impl",
			NoopContinueRunID:     "run-impl",
			NoopContinueCount:     1,
		},
	}
	task := WorkspaceTaskRecord{TaskID: "task-impl", ProjectID: "project-1", ProjectLane: "implementation"}
	session := AgentSessionStateRecord{SessionID: "session-impl", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Committed the owned branch", NextAction: "Publish review-ready evidence."}
	trace := &TaskRunTrace{ToolCalls: []string{"project_branch_commit"}, SuccessfulToolCalls: []string{"project_branch_commit"}}

	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-impl", &result, trace); err != nil {
		t.Fatalf("applyNoopContinueGuard() error = %v", err)
	}
	if result.Outcome != "continue" {
		t.Fatalf("durable progress should preserve continue outcome, got %+v", result)
	}
	if runtime.scratch.NoopContinueCount != 0 || runtime.scratch.NoopContinueTaskID != "" || runtime.scratch.NoopContinueSessionID != "" {
		t.Fatalf("expected no-op counter to clear after durable progress, got %+v", runtime.scratch)
	}
}

func TestRuntimeNoopContinueGuardDoesNotTreatFailedToolProbeAsProgress(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{TaskID: "task-stuck", Title: "Repair stale lane"}
	session := AgentSessionStateRecord{SessionID: "session-stuck", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{AssistantTurns: 1, ToolCalls: []string{"project_patch_queue_submit"}, FailedToolCalls: []string{"project_patch_queue_submit"}}

	first := StructuredTaskResult{Outcome: "continue", Summary: "Retrying the same owner-only call", NextAction: "Continue the active task cycle."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-stuck", &first, trace); err != nil {
		t.Fatalf("first applyNoopContinueGuard() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first failed-tool continue should get one grace cycle, got %+v", first)
	}
	if taskCycleHasDurableProgress(first, trace, nil) {
		t.Fatalf("failed tool calls alone should not count as durable progress")
	}

	second := StructuredTaskResult{Outcome: "continue", Summary: "Retrying the same owner-only call", NextAction: "Continue the active task cycle."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-stuck", &second, trace); err != nil {
		t.Fatalf("second applyNoopContinueGuard() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second failed-tool continue should be converted to blocked, got %+v", second)
	}
}

func TestRuntimeNoopContinueGuardDoesNotTreatAgentRequestOnlyAsDurableProgress(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{TaskID: "task-visual-qa", Title: "Browser visual QA"}
	session := AgentSessionStateRecord{SessionID: "session-visual-qa", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{AssistantTurns: 1, ToolCalls: []string{"agent_request"}, SuccessfulToolCalls: []string{"agent_request"}}

	first := StructuredTaskResult{Outcome: "continue", Summary: "Asked theta for browser QA", NextAction: "Wait for peer answer."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-visual-qa", &first, trace); err != nil {
		t.Fatalf("first applyNoopContinueGuard() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first agent-request-only continue should get one grace cycle, got %+v", first)
	}
	if taskCycleHasDurableProgress(first, trace, nil) {
		t.Fatalf("agent_request alone is coordination chatter, not durable task progress")
	}

	second := StructuredTaskResult{Outcome: "continue", Summary: "Asked theta again for browser QA", NextAction: "Wait for peer answer."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-visual-qa", &second, trace); err != nil {
		t.Fatalf("second applyNoopContinueGuard() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second agent-request-only continue should be converted to blocked, got %+v", second)
	}
	if len(second.BlockedOn) != 1 || second.BlockedOn[0].Kind != "dependency" {
		t.Fatalf("expected routine dependency block, got %+v", second.BlockedOn)
	}
}

func TestRuntimeNoopContinueGuardDoesNotTreatReadOnlyToolsAsDurableProgress(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{TaskID: "task-read-loop", Title: "Repair UI loop"}
	session := AgentSessionStateRecord{SessionID: "session-read-loop", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		AssistantTurns:      1,
		ToolCalls:           []string{"workspace_doc_get", "project_checkout_materialize", "read_file", "list_directory", "shell", "memory_read", "memory_search", "memory_coherence_read", "memory_promotion_read", "coalition_status", "reviewer_scarcity", "tool_bundle_registry", "some_dynamic_status_tool"},
		SuccessfulToolCalls: []string{"workspace_doc_get", "project_checkout_materialize", "read_file", "list_directory", "shell", "memory_read", "memory_search", "memory_coherence_read", "memory_promotion_read", "coalition_status", "reviewer_scarcity", "tool_bundle_registry:list", "tool_bundle_registry:status", "some_dynamic_status_tool"},
	}

	first := StructuredTaskResult{Outcome: "continue", Summary: "Read the same project again", NextAction: "Patch next cycle."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-read-loop", &first, trace); err != nil {
		t.Fatalf("first applyNoopContinueGuard() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first read-only continue should get one grace cycle, got %+v", first)
	}
	if taskCycleHasDurableProgress(first, trace, nil) {
		t.Fatalf("read-only exploration tools should not count as durable progress")
	}

	second := StructuredTaskResult{Outcome: "continue", Summary: "Read the same project again", NextAction: "Patch next cycle."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-read-loop", &second, trace); err != nil {
		t.Fatalf("second applyNoopContinueGuard() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second read-only continue should be converted to blocked, got %+v", second)
	}
}

func TestRuntimeNoopContinueGuardTreatsToolBundleRegistryMutationAsProgress(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			NoopContinueTaskID:    "task-tool-plane",
			NoopContinueSessionID: "session-tool-plane",
			NoopContinueRunID:     "run-tool-plane",
			NoopContinueCount:     1,
		},
	}
	task := WorkspaceTaskRecord{TaskID: "task-tool-plane", Title: "Repair tool plane"}
	session := AgentSessionStateRecord{SessionID: "session-tool-plane", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		AssistantTurns:      1,
		ToolCalls:           []string{"tool_bundle_registry"},
		SuccessfulToolCalls: []string{"tool_bundle_registry:mutated"},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Installed the required browser bundle.", NextAction: "Refresh tool readiness."}

	if !taskCycleHasDurableProgress(result, trace, nil) {
		t.Fatalf("tool_bundle_registry mutation marker should count as durable progress")
	}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-tool-plane", &result, trace); err != nil {
		t.Fatalf("applyNoopContinueGuard() error = %v", err)
	}
	if result.Outcome != "continue" {
		t.Fatalf("tool bundle mutation should preserve continue outcome, got %+v", result)
	}
	if runtime.scratch.NoopContinueCount != 0 {
		t.Fatalf("expected no-op counter to clear after tool bundle mutation, got %+v", runtime.scratch)
	}
}

func TestRuntimeNoopContinueGuardDoesNotTreatEphemeralWriteFileAsDurableProgress(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{TaskID: "task-temp-script-loop", Title: "Repair UI loop"}
	session := AgentSessionStateRecord{SessionID: "session-temp-script-loop", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		AssistantTurns:      1,
		ToolCalls:           []string{"write_file", "shell"},
		SuccessfulToolCalls: []string{"write_file:ephemeral", "shell"},
	}

	first := StructuredTaskResult{Outcome: "continue", Summary: "Wrote a temporary script", NextAction: "Retry the temporary script."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-temp-script-loop", &first, trace); err != nil {
		t.Fatalf("first applyNoopContinueGuard() error = %v", err)
	}
	if first.Outcome != "continue" {
		t.Fatalf("first ephemeral write continue should get one grace cycle, got %+v", first)
	}
	if taskCycleHasDurableProgress(first, trace, nil) {
		t.Fatalf("write_file to ephemeral paths should not count as durable progress")
	}

	second := StructuredTaskResult{Outcome: "continue", Summary: "Wrote a temporary script", NextAction: "Retry the temporary script."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-temp-script-loop", &second, trace); err != nil {
		t.Fatalf("second applyNoopContinueGuard() error = %v", err)
	}
	if second.Outcome != "blocked" {
		t.Fatalf("second ephemeral write continue should be converted to blocked, got %+v", second)
	}
}

func TestRuntimeNoopContinueGuardBlocksUnpublishedPatchQueueRevisionLoop(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-revision-project-signal01-queue-lexer-item-lexer",
		ProjectID:   "project-signal01",
		ProjectLane: "implementation",
		Title:       "Revise blocked lexer patch queue candidate",
		Tags:        []string{"project", "patch-queue", "revision", "owner-bound-kind:patch_queue_revision"},
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_kind":"revision",
			"required_transition":"project_patch_queue_revision_commit_review_submit",
			"required_tool_sequence":["project_branch_commit","project_branch_review_ready","project_patch_queue_submit"],
			"branch_id":"projbranch-lexer",
			"head_sha":"` + strings.Repeat("d", 40) + `"
		}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-revision", AgentID: "beta", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		AssistantTurns:      1,
		ToolCalls:           []string{"project_checkout_materialize", "write_file", "shell"},
		SuccessfulToolCalls: []string{"project_checkout_materialize", "write_file", "shell"},
	}

	first := StructuredTaskResult{Outcome: "continue", Summary: "Wrote lexer repair and ran go test.", NextAction: "Continue fixing lexer syntax."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-revision", &first, trace); err != nil {
		t.Fatalf("first applyNoopContinueGuard() error = %v", err)
	}
	if first.Outcome != "continue" || runtime.scratch.NoopContinueCount != 1 {
		t.Fatalf("first unpublished revision cycle should get one grace cycle, result=%+v scratch=%+v", first, runtime.scratch)
	}
	if !taskCycleHasDurableProgress(first, trace, nil) {
		t.Fatalf("ordinary source write remains durable progress outside the revision-publication guard")
	}

	for i := 2; i < patchQueueRevisionPublicationBlockThreshold; i++ {
		intermediate := StructuredTaskResult{Outcome: "continue", Summary: "Wrote lexer repair and ran go test.", NextAction: "Continue fixing lexer syntax."}
		if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-revision", &intermediate, trace); err != nil {
			t.Fatalf("cycle %d applyNoopContinueGuard() error = %v", i, err)
		}
		if intermediate.Outcome != "continue" {
			t.Fatalf("cycle %d should remain continue before revision publication threshold, got %+v", i, intermediate)
		}
	}

	blocked := StructuredTaskResult{Outcome: "continue", Summary: "Wrote lexer repair and ran go test.", NextAction: "Continue fixing lexer syntax."}
	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-revision", &blocked, trace); err != nil {
		t.Fatalf("threshold applyNoopContinueGuard() error = %v", err)
	}
	if blocked.Outcome != "blocked" {
		t.Fatalf("threshold unpublished revision cycle should block, got %+v", blocked)
	}
	if len(blocked.BlockedOn) != 1 || blocked.BlockedOn[0].Kind != "blocked_on_revision_publication_not_executed" {
		t.Fatalf("expected revision publication blocker, got %+v", blocked.BlockedOn)
	}
}

func TestTaskRequiresPatchQueueRevisionPublicationRequiresStrictContract(t *testing.T) {
	legacy := WorkspaceTaskRecord{
		TaskID:               "task-legacy-revision",
		Tags:                 []string{"project", "patch-queue", "revision"},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","patch_queue_task_kind":"revision"}`,
	}
	if taskRequiresPatchQueueRevisionPublication(legacy) {
		t.Fatalf("legacy revision task without publication contract must not trigger strict revision-publication guard")
	}

	strict := legacy
	strict.TaskRequirementsJSON = `{"schema":"task_requirements.v1","patch_queue_task_kind":"revision","required_terminal_tool":"project_patch_queue_submit"}`
	if !taskRequiresPatchQueueRevisionPublication(strict) {
		t.Fatalf("strict revision task with terminal submit requirement should trigger revision-publication guard")
	}
}

func TestRuntimeNoopContinueGuardDoesNotClearPatchQueueRevisionAfterCommitOnly(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			NoopContinueTaskID:    "task-patchq-revision-project-signal01-queue-lexer-item-lexer",
			NoopContinueSessionID: "session-revision",
			NoopContinueCount:     patchQueueRevisionPublicationBlockThreshold - 1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-patchq-revision-project-signal01-queue-lexer-item-lexer",
		ProjectID:            "project-signal01",
		ProjectLane:          "implementation",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","patch_queue_task_kind":"revision","required_transition":"project_patch_queue_revision_commit_review_submit"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-revision", AgentID: "beta", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		AssistantTurns:      1,
		ToolCalls:           []string{"project_branch_commit"},
		SuccessfulToolCalls: []string{"project_branch_commit"},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Committed repair head.", NextAction: "Publish review-ready packet."}

	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-revision", &result, trace); err != nil {
		t.Fatalf("applyNoopContinueGuard() error = %v", err)
	}
	if result.Outcome != "blocked" {
		t.Fatalf("commit-only cycle at threshold should still require terminal submit, got %+v", result)
	}
	if len(result.BlockedOn) != 1 || result.BlockedOn[0].Kind != "blocked_on_revision_publication_not_executed" {
		t.Fatalf("expected revision publication blocker, got %+v", result.BlockedOn)
	}
}

func TestRuntimeNoopContinueGuardClearsPatchQueueRevisionAfterTerminalSubmit(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			NoopContinueTaskID:    "task-patchq-revision-project-signal01-queue-lexer-item-lexer",
			NoopContinueSessionID: "session-revision",
			NoopContinueCount:     patchQueueRevisionPublicationBlockThreshold - 1,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-patchq-revision-project-signal01-queue-lexer-item-lexer",
		ProjectID:            "project-signal01",
		ProjectLane:          "implementation",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","patch_queue_task_kind":"revision","required_transition":"project_patch_queue_revision_commit_review_submit"}`,
	}
	session := AgentSessionStateRecord{SessionID: "session-revision", AgentID: "beta", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{
		AssistantTurns:      1,
		ToolCalls:           []string{"project_patch_queue_submit"},
		SuccessfulToolCalls: []string{"project_patch_queue_submit"},
	}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Submitted repair candidate.", NextAction: "Wait for review."}

	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-revision", &result, trace); err != nil {
		t.Fatalf("applyNoopContinueGuard() error = %v", err)
	}
	if result.Outcome != "continue" {
		t.Fatalf("terminal submit should preserve continue outcome, got %+v", result)
	}
	if runtime.scratch.NoopContinueCount != 0 || runtime.scratch.NoopContinueTaskID != "" {
		t.Fatalf("terminal submit should clear no-op counter, got %+v", runtime.scratch)
	}
}

func TestTaskRunTraceSuccessfulToolNameClassifiesEphemeralWriteFile(t *testing.T) {
	call := ToolCall{Function: FunctionCall{Name: "write_file", Arguments: `{"path":"project-checkouts/app/.tmp/fix-390-layout.mjs","content":"script"}`}}
	if got := taskRunTraceSuccessfulToolName(call); got != "write_file:ephemeral" {
		t.Fatalf("expected ephemeral write_file trace marker, got %q", got)
	}

	call = ToolCall{Function: FunctionCall{Name: "write_file", Arguments: `{"path":"project-checkouts/app/tmp/screenshot.png","content":"image"}`}}
	if got := taskRunTraceSuccessfulToolName(call); got != "write_file:ephemeral" {
		t.Fatalf("expected temp image write_file trace marker, got %q", got)
	}

	call = ToolCall{Function: FunctionCall{Name: "write_file", Arguments: `{"path":"project-checkouts/app/src/App.tsx","content":"source"}`}}
	if got := taskRunTraceSuccessfulToolName(call); got != "write_file" {
		t.Fatalf("expected source write_file to remain durable marker, got %q", got)
	}

	call = ToolCall{Function: FunctionCall{Name: "write_file", Arguments: `{"path":"project-checkouts/app/src/assets/logo.png","content":"image"}`}}
	if got := taskRunTraceSuccessfulToolName(call); got != "write_file" {
		t.Fatalf("expected source asset image write_file to remain durable marker, got %q", got)
	}
}

func TestTaskRunTraceSuccessfulToolNameClassifiesToolBundleRegistryReadOnlyActions(t *testing.T) {
	for _, action := range []string{"list", "status", "refresh"} {
		call := ToolCall{Function: FunctionCall{Name: "tool_bundle_registry", Arguments: fmt.Sprintf(`{"action":%q}`, action)}}
		got := taskRunTraceSuccessfulToolName(call, ToolResult{Output: fmt.Sprintf(`{"action":%q,"mutated":false}`, action)})
		if got != "tool_bundle_registry:"+action {
			t.Fatalf("expected tool_bundle_registry %s marker, got %q", action, got)
		}
		if successfulToolCallIsDurableProgress(got) {
			t.Fatalf("read-only tool_bundle_registry %s should not count as durable progress", action)
		}
	}

	for _, action := range []string{"register", "enable", "disable", "scaffold", "install", "download", "install_url", "migrate", "migrate_manifest"} {
		call := ToolCall{Function: FunctionCall{Name: "tool_bundle_registry", Arguments: fmt.Sprintf(`{"action":%q,"name":"browser_tools"}`, action)}}
		got := taskRunTraceSuccessfulToolName(call, ToolResult{Output: fmt.Sprintf(`{"action":%q,"mutated":true}`, action)})
		if got != "tool_bundle_registry:mutated" {
			t.Fatalf("expected mutating tool_bundle_registry %s marker, got %q", action, got)
		}
		if !successfulToolCallIsDurableProgress(got) {
			t.Fatalf("mutating tool_bundle_registry %s should count as durable progress", action)
		}
	}

	call := ToolCall{Function: FunctionCall{Name: "tool_bundle_registry", Arguments: `{"action":"migrate","name":"legacy_tools"}`}}
	got := taskRunTraceSuccessfulToolName(call, ToolResult{Output: `{"action":"migrate","mutated":false}`})
	if got != "tool_bundle_registry" {
		t.Fatalf("expected no-op tool_bundle_registry migrate to keep base marker, got %q", got)
	}
	if successfulToolCallIsDurableProgress(got) {
		t.Fatalf("no-op tool_bundle_registry migrate should not count as durable progress")
	}
}

func TestRuntimeNoopContinueGuardTreatsAgentRequestPlusMaterializationAsProgress(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-1"},
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			NoopContinueTaskID:    "task-visual-qa",
			NoopContinueSessionID: "session-visual-qa",
			NoopContinueRunID:     "run-visual-qa",
			NoopContinueCount:     1,
		},
	}
	task := WorkspaceTaskRecord{TaskID: "task-visual-qa", Title: "Browser visual QA"}
	session := AgentSessionStateRecord{SessionID: "session-visual-qa", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{AssistantTurns: 1, ToolCalls: []string{"agent_request", "workspace_doc_put"}, SuccessfulToolCalls: []string{"agent_request", "workspace_doc_put"}}
	result := StructuredTaskResult{Outcome: "continue", Summary: "Published peer blocker summary", NextAction: "Create a focused validation follow-up."}

	if err := runtime.applyNoopContinueGuard(context.Background(), task, session, "run-visual-qa", &result, trace); err != nil {
		t.Fatalf("applyNoopContinueGuard() error = %v", err)
	}
	if result.Outcome != "continue" {
		t.Fatalf("durable non-agent_request tool should preserve continue outcome, got %+v", result)
	}
	if runtime.scratch.NoopContinueCount != 0 {
		t.Fatalf("expected no-op counter to clear after durable tool progress, got %+v", runtime.scratch)
	}
}

// TestReviewRequiredToolGateRequiresDecisionNotClaim locks RPF-58A: a review task whose
// required_tool is project_patch_queue_lifecycle is satisfied ONLY by a durable accept/block/reject
// decision receipt, never by the CLAIM that precedes it. This is what stops the R58 failure where a
// reviewer claimed an item, delivered "accept" over the read-only model.ask channel, and left the
// item CLAIMED forever.
func TestReviewRequiredToolGateRequiresDecisionNotClaim(t *testing.T) {
	claimOnly := &TaskRunTrace{
		SuccessfulToolCalls: []string{"project_patch_queue_lifecycle"},
		ToolReceipts: []TaskRunToolReceipt{
			{ToolName: "project_patch_queue_lifecycle", IsError: false, Output: `{"action":"claim","state":"CLAIMED"}`},
		},
	}
	if traceHasRequiredTransitionReceipt(claimOnly, "project_patch_queue_lifecycle") {
		t.Fatal("claim-only lifecycle trace must NOT satisfy the review decision gate")
	}

	decided := &TaskRunTrace{
		SuccessfulToolCalls: []string{"project_patch_queue_lifecycle"},
		ToolReceipts: []TaskRunToolReceipt{
			{ToolName: "project_patch_queue_lifecycle", IsError: false, Output: `{"action":"claim","state":"CLAIMED"}`},
			{ToolName: "project_patch_queue_lifecycle", IsError: false, Output: `{"action":"accept","state":"ACCEPTED","integration_guidance":"` + projectPatchQueueLifecycleDecisionRecordedMarker + ` Acceptance is not a canonical baseline update."}`},
		},
	}
	if !traceHasRequiredTransitionReceipt(decided, "project_patch_queue_lifecycle") {
		t.Fatal("a recorded decision receipt must satisfy the review decision gate")
	}

	// A failed decision attempt does not satisfy the success path...
	failedDecision := &TaskRunTrace{
		ToolReceipts: []TaskRunToolReceipt{
			{ToolName: "project_patch_queue_lifecycle", IsError: true, Output: "decision failed"},
		},
	}
	if traceHasRequiredTransitionReceipt(failedDecision, "project_patch_queue_lifecycle") {
		t.Fatal("a failed lifecycle receipt must NOT satisfy the decision gate")
	}
	// ...but the outer gate still allows a typed blocker to terminate the review cycle.
	blockerResult := StructuredTaskResult{Outcome: "blocked", BlockedOn: []BlockedRef{{Kind: "required_transition", Detail: "cannot decide: missing evidence"}}}
	if !traceHasTaskBoundRequiredToolReceipt(failedDecision, "project_patch_queue_lifecycle", blockerResult) {
		t.Fatal("a typed blocker after a failed lifecycle attempt must satisfy the outer required-tool gate")
	}
}
