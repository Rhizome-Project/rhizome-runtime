package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeLivingConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "living-config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestRunLiving_FailsFastOnUnsupportedRemoteTopology(t *testing.T) {
	configPath := writeLivingConfig(t, `
id: living-test
role: developer
mode: observe_only
workspace_id: ws-living
task_types: [coding]
models:
  primary: m1
  worker: m2
  cheap: m3
rhizome_url: http://localhost:8420
llm:
  api_key: sk-test
`)

	err := runLiving([]string{"run", configPath})
	if err == nil {
		t.Fatal("expected unsupported remote topology error")
	}
	if !strings.Contains(err.Error(), "remote topology") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunLiving_FailsFastOnUnsupportedExecutionMode(t *testing.T) {
	configPath := writeLivingConfig(t, `
id: living-test
role: developer
mode: local_execution
workspace_id: ws-living
task_types: [coding]
models:
  primary: m1
  worker: m2
  cheap: m3
rhizome_url: direct://sqlite
llm:
  api_key: sk-test
`)

	err := runLiving([]string{"run", configPath})
	if err == nil {
		t.Fatal("expected unsupported execution mode error")
	}
	if !strings.Contains(err.Error(), "not wired in current CLI") {
		t.Fatalf("unexpected error: %v", err)
	}
}
