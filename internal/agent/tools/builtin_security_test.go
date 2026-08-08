package tools

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestSecurity_CapabilityTierRejection(t *testing.T) {
	t.Parallel()

	// 1. Create a registry with NO allowed tiers
	reg := NewRegistry()
	RegisterBuiltins(reg, BuiltinConfig{
		AllowedTiers: []string{}, // No access
	})

	// 2. Request bash (which is high_risk)
	tool, ok := reg.Get("bash")
	if !ok {
		t.Fatalf("bash tool not registered")
	}

	result, err := tool.Execute(context.Background(), mustInput(map[string]any{"command": "echo hacked"}))
	if err != nil {
		t.Fatalf("expected logic rejection via result string, not error: %v", err)
	}

	if !strings.Contains(result, "Permission Denied") || !strings.Contains(result, "high_risk") {
		t.Fatalf("expected Permission Denied for high_risk capability, got: %v", result)
	}
}

func TestSecurity_PathTraversalRejection(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	tool := &readTool{
		cfg: BuiltinConfig{
			WorkspaceDir: dir,
			AllowedTiers: []string{"autonomous"},
		},
	}

	// Try to read /etc/passwd or something outside the temp dir via traversal
	payload := filepath.Join(dir, "..", "..", "etc", "passwd")
	result, err := tool.Execute(context.Background(), mustInput(map[string]any{"file_path": payload}))

	if err != nil {
		t.Fatalf("expected logic rejection via result string, not error: %v", err)
	}

	if !strings.Contains(result, "Permission Denied: path") || !strings.Contains(result, "escapes workspace boundary") {
		t.Fatalf("expected boundary escape warning, got: %v", result)
	}
}
