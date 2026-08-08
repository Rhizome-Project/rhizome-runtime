package main

import (
	"encoding/json"
	"testing"
)

func TestTaskFrontierTreatsPathlessABPCRecoveryCarrierAsBlockedLocally(t *testing.T) {
	pathlessRequirements, _ := json.Marshal(map[string]any{
		"schema":                          "artifact_bound_side_effect_resolution_followup.v1",
		"admission_kind":                  "abpc_recovery_action",
		"abpc_task_class":                 "side_effect_foundation",
		"action_kind":                     "split_foundation_bucket",
		"decision":                        "split_tension",
		"side_effect_refs":                []string{"side-effect:opaque-region"},
		"project_id":                      "",
		"branch_id":                       "",
		"branch_name":                     "",
		"active_task_id":                  "",
		"dirty_paths":                     []string{},
		"path_bucket":                     []string{},
		"write_scope_hints":               []string{},
		"write_scope_hints_authoritative": true,
	})
	pathBoundRequirements, _ := json.Marshal(map[string]any{
		"schema":                          "artifact_bound_side_effect_resolution_followup.v1",
		"admission_kind":                  "abpc_recovery_action",
		"abpc_task_class":                 "side_effect_foundation",
		"action_kind":                     "split_foundation_bucket",
		"decision":                        "split_tension",
		"side_effect_refs":                []string{"side-effect:cmd-glua-main"},
		"project_id":                      "project-lua",
		"branch_id":                       "projbranch-current",
		"active_task_id":                  "task-current",
		"dirty_paths":                     []string{"cmd/glua/main.go"},
		"path_bucket":                     []string{"cmd/glua/main.go"},
		"write_scope_hints":               []string{"cmd/glua/main.go"},
		"write_scope_hints_authoritative": true,
	})
	pathless := AgentWorkTaskFrontierCandidate{
		Task: WorkspaceTaskRecord{
			TaskID:               "task-side-effect-67be564540",
			Status:               "PENDING",
			TaskKind:             "EXECUTION",
			ProjectLane:          "implementation",
			TaskRequirementsJSON: string(pathlessRequirements),
		},
	}
	pathBound := AgentWorkTaskFrontierCandidate{
		Task: WorkspaceTaskRecord{
			TaskID:               "task-side-effect-path-bound",
			Status:               "PENDING",
			TaskKind:             "EXECUTION",
			ProjectID:            "project-lua",
			ProjectLane:          "implementation",
			TaskRequirementsJSON: string(pathBoundRequirements),
		},
	}

	if taskFrontierHasUnblockedCandidate([]AgentWorkTaskFrontierCandidate{pathless}) {
		t.Fatalf("pathless ABPC recovery carrier must not be counted as unblocked frontier work")
	}
	runtime := &Runtime{}
	if _, ok := runtime.selectableTaskFrontierCandidate([]AgentWorkTaskFrontierCandidate{pathless}, nil, "task-side-effect-67be564540"); ok {
		t.Fatalf("pathless ABPC recovery carrier must not be selectable")
	}
	if !taskFrontierHasUnblockedCandidate([]AgentWorkTaskFrontierCandidate{pathless, pathBound}) {
		t.Fatalf("path-bound ABPC recovery carrier should keep the frontier unblocked")
	}
	if _, ok := runtime.selectableTaskFrontierCandidate([]AgentWorkTaskFrontierCandidate{pathless, pathBound}, nil, "task-side-effect-path-bound"); !ok {
		t.Fatalf("path-bound ABPC recovery carrier should remain selectable")
	}
}
