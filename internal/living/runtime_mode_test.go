package living

import (
	"strings"
	"testing"
)

func TestLoadConfig_DefaultModeObserveOnly(t *testing.T) {
	cfg, err := LoadConfig(writeTemp(t, requiredOnlyYAML))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.RuntimeMode() != RuntimeModeObserveOnly {
		t.Fatalf("expected default mode %q, got %q", RuntimeModeObserveOnly, cfg.RuntimeMode())
	}
}

func TestLoadConfig_InvalidMode(t *testing.T) {
	path := writeTemp(t, `
id: a
role: b
mode: unsupported
workspace_id: ws
task_types: [x]
models: {primary: m1, worker: m2, cheap: m3}
rhizome_url: direct://sqlite
llm: {api_key: sk-key}
`)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected invalid mode error")
	}
	if !strings.Contains(err.Error(), "mode must be one of") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRunContract_RejectsRemoteRhizomeURL(t *testing.T) {
	cfg := Config{Mode: string(RuntimeModeObserveOnly), RhizomeURL: "http://localhost:8420"}
	err := ValidateRunContract(cfg)
	if err == nil {
		t.Fatal("expected unsupported remote topology error")
	}
	if !strings.Contains(err.Error(), "remote topology") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateDependencyContract_LocalExecutionRequiresExplicitDeps(t *testing.T) {
	err := ValidateDependencyContract(Config{Mode: string(RuntimeModeLocalExecution)}, &BrainDeps{})
	if err == nil {
		t.Fatal("expected dependency contract error")
	}
	if !strings.Contains(err.Error(), "task_runner") || !strings.Contains(err.Error(), "worker_runner") {
		t.Fatalf("unexpected error: %v", err)
	}
}
