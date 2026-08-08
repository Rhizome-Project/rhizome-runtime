package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecutorCallDeniesRawSubprocessInProgramBGuard(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := filepath.Join(root, "owned.txt")
	executor := NewExecutor(root)
	denied := []ExecutorMutationDenyRecord{}
	executor.SetMutationPolicy(ExecutorMutationPolicy{
		RequireAuthority: true,
		DisableDirect:    true,
		RecordDeny: func(record ExecutorMutationDenyRecord) {
			denied = append(denied, record)
		},
	})

	if _, err := executor.Deploy(DeployInput{
		WorkspaceID: "ws-b2-3",
		ToolID:      "raw-writer",
		Runtime:     "python",
		EntryPoint:  "main.py",
		SourceCode:  "from pathlib import Path\nPath(r'" + strings.ReplaceAll(target, "\\", "\\\\") + "').write_text('bypass')\n",
		DeployedBy:  "test",
	}); err != nil {
		t.Fatalf("deploy writer tool: %v", err)
	}

	result, err := executor.Call(context.Background(), CallInput{
		WorkspaceID: "ws-b2-3",
		ToolID:      "raw-writer",
		Arguments:   map[string]any{},
	})
	if err != nil {
		t.Fatalf("executor policy deny should return a result, got error: %v", err)
	}
	if result == nil || result.ExitCode != 126 || !strings.Contains(result.Stderr, DirectExecutorMutationDeniedReason) {
		t.Fatalf("expected raw executor denial result, got %+v", result)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("denied executor call should not create target; stat err=%v", err)
	}
	if len(denied) != 1 {
		t.Fatalf("expected one executor deny record, got %+v", denied)
	}
	record := denied[0]
	if record.ToolID != "raw-writer" || record.WorkspaceID != "ws-b2-3" || record.Runtime != "python" || record.ReasonCode != DirectExecutorMutationDeniedReason {
		t.Fatalf("unexpected executor deny record: %+v", record)
	}
	for _, want := range []string{"repo_lease_id", "lease_term", "patch_queue_id", "patch_queue_item_id", "operation_id"} {
		if !containsString(record.MissingContext, want) {
			t.Fatalf("missing context %q not recorded in %+v", want, record.MissingContext)
		}
	}
}

func TestExecutorCallProgramBGuardDeniesRawRuntimeMatrixBeforeLaunch(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	executor := NewExecutor(root)
	denied := []ExecutorMutationDenyRecord{}
	executor.SetMutationPolicy(ExecutorMutationPolicy{
		RequireAuthority: true,
		DisableDirect:    true,
		RecordDeny: func(record ExecutorMutationDenyRecord) {
			denied = append(denied, record)
		},
	})

	cases := []struct {
		name       string
		runtime    string
		entryPoint string
		source     func(target string) string
	}{
		{
			name:       "python",
			runtime:    "python",
			entryPoint: "main.py",
			source: func(target string) string {
				return "from pathlib import Path\nPath(r'" + strings.ReplaceAll(target, "\\", "\\\\") + "').write_text('bypass')\n"
			},
		},
		{
			name:       "bash",
			runtime:    "bash",
			entryPoint: "main.sh",
			source: func(target string) string {
				return "#!/usr/bin/env bash\necho bypass > " + target + "\n"
			},
		},
		{
			name:       "node",
			runtime:    "node",
			entryPoint: "main.js",
			source: func(target string) string {
				return "require('fs').writeFileSync('" + strings.ReplaceAll(target, "\\", "\\\\") + "', 'bypass')\n"
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := filepath.Join(root, "owned-"+tc.name+".txt")
			workspaceID := "ws-b2-5-" + tc.name
			toolID := "raw-" + tc.name
			if _, err := executor.Deploy(DeployInput{
				WorkspaceID: workspaceID,
				ToolID:      toolID,
				Runtime:     tc.runtime,
				EntryPoint:  tc.entryPoint,
				SourceCode:  tc.source(target),
				DeployedBy:  "test",
			}); err != nil {
				t.Fatalf("deploy %s writer tool: %v", tc.name, err)
			}

			result, err := executor.Call(context.Background(), CallInput{
				WorkspaceID: workspaceID,
				ToolID:      toolID,
				Arguments:   map[string]any{},
			})
			if err != nil {
				t.Fatalf("executor policy deny should return a result, got error: %v", err)
			}
			if result == nil || result.ExitCode != 126 || !strings.Contains(result.Stderr, DirectExecutorMutationDeniedReason) {
				t.Fatalf("expected raw executor denial result for %s, got %+v", tc.name, result)
			}
			if _, err := os.Stat(target); !os.IsNotExist(err) {
				t.Fatalf("denied %s executor call should not create target; stat err=%v", tc.name, err)
			}
		})
	}

	if len(denied) != len(cases) {
		t.Fatalf("expected %d executor deny records, got %+v", len(cases), denied)
	}
	seen := map[string]bool{}
	for _, record := range denied {
		if record.ReasonCode != DirectExecutorMutationDeniedReason {
			t.Fatalf("unexpected executor deny record: %+v", record)
		}
		seen[record.Runtime] = true
	}
	for _, tc := range cases {
		if !seen[tc.runtime] {
			t.Fatalf("expected deny record for runtime %s, got %+v", tc.runtime, denied)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
