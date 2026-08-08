package sqlite

import "testing"

func TestAgentWorkImplementationTaskClaimWriteScopePreservesAcceptanceMatrixHints(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-signal01-rqs1-acceptance-tests",
		Title:       "Add full acceptance test matrix",
		Description: "Add or extend Go tests so the product contract is executable: happy paths, edge cases, CLI file mode, and REPL non-crash behavior. Do not weaken existing keystone tests.",
		ProjectLane: "implementation",
		WriteScopeHints: []string{
			"internal/**",
			"cmd/**",
			"README.md",
		},
		TaskRequirementsJSON: `{"schema":"product_first_task_requirements.v1","product_slice":"acceptance_tests","must_add_tests":true}`,
	}

	got, ok := agentWorkImplementationTaskClaimWriteScope(task)
	want := `{"paths":["internal/**","cmd/**","README.md"]}`
	if !ok || got != want {
		t.Fatalf("acceptance matrix write scope = ok:%v %s, want %s", ok, got, want)
	}
}

func TestAgentWorkImplementationTaskClaimWriteScopePreserveFlagSkipsSemanticNarrowing(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:               "task-rq-cli-acceptance",
		Title:                "CLI acceptance sweep",
		Description:          "Exercise CLI file mode and REPL tests without narrowing the declared authority.",
		ProjectLane:          "implementation",
		WriteScopeHints:      []string{"internal/**", "cmd/**", "README.md"},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`,
	}

	got, ok := agentWorkImplementationTaskClaimWriteScope(task)
	want := `{"paths":["internal/**","cmd/**","README.md"]}`
	if !ok || got != want {
		t.Fatalf("preserved write scope = ok:%v %s, want %s", ok, got, want)
	}
}
