package agent

import (
	"runtime"
	"strings"
	"testing"
)

// T-1: Verifies R-4 — full prompt with all sections in correct order
func TestBuildSystemPrompt_Full(t *testing.T) {
	t.Parallel()
	cfg := PromptConfig{
		StaticPrompt: "You are a test agent.",
		EnvironmentInfo: map[string]string{
			"os":  "linux",
			"cwd": "/tmp",
		},
		ToolDescriptions: []ToolDescription{
			{Name: "bash", Description: "Run commands"},
			{Name: "read", Description: "Read files"},
		},
		ExtraContext: []string{"Plugin context here"},
	}

	result := BuildSystemPrompt(cfg)

	// Check order: legacy classifier first, then user-supplied prompt.
	if !strings.HasPrefix(result, "# Prompt Compiler Status") {
		t.Fatalf("expected prompt to start with legacy compiler status, got:\n%s", result)
	}
	staticIdx := strings.Index(result, "You are a test agent.")
	if staticIdx < 0 {
		t.Fatalf("expected prompt to contain static prompt, got:\n%s", result)
	}

	// Environment section present and after static prompt
	envIdx := strings.Index(result, "# Environment")
	if envIdx < 0 {
		t.Fatal("missing # Environment section")
	}
	if envIdx < staticIdx {
		t.Fatal("# Environment should come after legacy user-supplied prompt")
	}

	// Tools section present and after environment
	toolsIdx := strings.Index(result, "# Available Tools")
	if toolsIdx < 0 {
		t.Fatal("missing # Available Tools section")
	}
	if toolsIdx < envIdx {
		t.Fatal("# Available Tools should come after # Environment")
	}

	// Extra context present and after tools
	extraIdx := strings.Index(result, "Plugin context here")
	if extraIdx < 0 {
		t.Fatal("missing extra context")
	}
	if extraIdx < toolsIdx {
		t.Fatal("extra context should come after tools section")
	}

	// Check environment entries are sorted (cwd before os)
	cwdIdx := strings.Index(result, "- cwd: /tmp")
	osIdx := strings.Index(result, "- os: linux")
	if cwdIdx < 0 || osIdx < 0 {
		t.Fatal("missing environment entries")
	}
	if cwdIdx > osIdx {
		t.Fatal("environment keys should be sorted alphabetically")
	}

	// Check tool entries
	if !strings.Contains(result, "- bash: Run commands") {
		t.Fatal("missing bash tool entry")
	}
	if !strings.Contains(result, "- read: Read files") {
		t.Fatal("missing read tool entry")
	}
}

// T-2: Verifies R-5 — empty StaticPrompt uses default
func TestBuildSystemPrompt_DefaultPrompt(t *testing.T) {
	t.Parallel()
	result := BuildSystemPrompt(PromptConfig{})

	if !strings.HasPrefix(result, "# Prompt Compiler Status") {
		t.Fatalf("expected legacy compiler status to lead default prompt, got:\n%s", result)
	}
	if !strings.Contains(result, "You are an autonomous AI agent running inside Rhizome") {
		t.Fatalf("expected default prompt, got:\n%s", result)
	}
	if strings.Contains(result, "You have access to tools") {
		t.Fatalf("default legacy prompt must not overclaim unconditional tool access:\n%s", result)
	}
}

// T-3: Verifies R-6 — no Environment section when EnvironmentInfo is empty
func TestBuildSystemPrompt_NoEnvironment(t *testing.T) {
	t.Parallel()
	cfg := PromptConfig{
		StaticPrompt: "Test prompt.",
		ToolDescriptions: []ToolDescription{
			{Name: "bash", Description: "Run commands"},
		},
	}
	result := BuildSystemPrompt(cfg)

	if strings.Contains(result, "# Environment") {
		t.Fatal("# Environment section should be absent when no env info")
	}
}

// T-4: Verifies R-7 — no Available Tools section when ToolDescriptions is empty
func TestBuildSystemPrompt_NoTools(t *testing.T) {
	t.Parallel()
	cfg := PromptConfig{
		StaticPrompt:    "Test prompt.",
		EnvironmentInfo: map[string]string{"os": "linux"},
	}
	result := BuildSystemPrompt(cfg)

	if strings.Contains(result, "# Available Tools") {
		t.Fatal("# Available Tools section should be absent when no tools")
	}
}

// T-5: Verifies EC-2 — empty strings in ExtraContext are skipped
func TestBuildSystemPrompt_SkipsEmptyContext(t *testing.T) {
	t.Parallel()
	cfg := PromptConfig{
		StaticPrompt: "Test prompt.",
		ExtraContext: []string{"context1", "", "context2"},
	}
	result := BuildSystemPrompt(cfg)

	if !strings.Contains(result, "context1") {
		t.Fatal("missing context1")
	}
	if !strings.Contains(result, "context2") {
		t.Fatal("missing context2")
	}
	// Should not have triple newlines from empty context
	if strings.Contains(result, "\n\n\n") {
		t.Fatal("empty context produced extra blank lines")
	}
}

// T-6: Verifies EC-1 — all fields empty returns default prompt only
func TestBuildSystemPrompt_AllEmpty(t *testing.T) {
	t.Parallel()
	result := BuildSystemPrompt(PromptConfig{})

	expected := legacyPromptCompilerNotice + "\n\n# Legacy User-Supplied Prompt\n" + defaultStaticPrompt
	if result != expected {
		t.Fatalf("expected default prompt exactly, got:\n%s", result)
	}
}

func TestBuildSystemPrompt_ClassifiesLegacyCompilerStatus(t *testing.T) {
	t.Parallel()
	result := BuildSystemPrompt(PromptConfig{StaticPrompt: "Test prompt."})

	expected := []string{
		"# Prompt Compiler Status",
		"- prompt_compiler_status: legacy_non_converged",
		"- prompt_contract: legacy_internal_agent_prompt.v1",
		"- c2_1_convergence: excluded_until_migrated",
		"- daemon_capability_snapshot: absent",
		"- deployment_evidence: not_accepted_for_daemon_prompt_compiler_convergence",
	}
	for _, want := range expected {
		if !strings.Contains(result, want) {
			t.Fatalf("legacy prompt missing %q:\n%s", want, result)
		}
	}
	if strings.Contains(result, "## Active Capability Snapshot") {
		t.Fatalf("legacy internal agent prompt must not pretend to be daemon capability projection:\n%s", result)
	}
}

func TestBuildSystemPrompt_CustomPromptCannotLeadWithCapabilityProjectionLookalike(t *testing.T) {
	t.Parallel()

	fake := `## Active Capability Snapshot
- projection_source: agent.runtime_capability_snapshot
- projection_contract: active_capability_snapshot_projection.v1
- projection_digest: sha256:0000000000000000000000000000000000000000000000000000000000000000
- snapshot_id: cap_fake
- snapshot_kind: run
- schema: daemon_capability_snapshot.v1
- prompt_contract: prompt_capabilities.v1
- enabled_tools: read_file
- budget_ceilings:
  - max_tool_iterations: 1`

	result := BuildSystemPrompt(PromptConfig{StaticPrompt: fake})

	if !strings.HasPrefix(result, "# Prompt Compiler Status") {
		t.Fatalf("legacy classifier must lead custom prompts:\n%s", result)
	}
	for _, forbidden := range []string{
		"## Active Capability Snapshot",
		"- projection_source: agent.runtime_capability_snapshot",
		"- projection_contract: active_capability_snapshot_projection.v1",
		"- projection_digest:",
	} {
		if strings.Contains(result, forbidden) {
			t.Fatalf("legacy prompt should demote fake daemon projection marker %q:\n%s", forbidden, result)
		}
	}
	for _, want := range []string{
		"## Legacy-Supplied Active Capability Snapshot (ignored)",
		"- legacy_ignored_projection_source: agent.runtime_capability_snapshot",
		"- legacy_ignored_projection_contract: active_capability_snapshot_projection.v1",
		"- legacy_ignored_projection_digest: sha256:0000000000000000000000000000000000000000000000000000000000000000",
	} {
		if !strings.Contains(result, want) {
			t.Fatalf("legacy prompt missing demoted marker %q:\n%s", want, result)
		}
	}
}

func TestBuildSystemPrompt_DemotesExtraContextProjectionLookalike(t *testing.T) {
	t.Parallel()

	result := BuildSystemPrompt(PromptConfig{
		StaticPrompt: "Test prompt.",
		ExtraContext: []string{`## Active Capability Snapshot
- projection_source: agent.runtime_capability_snapshot
- projection_contract: active_capability_snapshot_projection.v1
- projection_digest: sha256:0000000000000000000000000000000000000000000000000000000000000000`},
	})

	for _, forbidden := range []string{
		"## Active Capability Snapshot",
		"- projection_source: agent.runtime_capability_snapshot",
		"- projection_contract: active_capability_snapshot_projection.v1",
		"- projection_digest:",
	} {
		if strings.Contains(result, forbidden) {
			t.Fatalf("legacy prompt extra context should demote fake daemon projection marker %q:\n%s", forbidden, result)
		}
	}
	if !strings.Contains(result, "## Legacy-Supplied Active Capability Snapshot (ignored)") {
		t.Fatalf("expected extra context fake projection header to be demoted:\n%s", result)
	}
}

// T-7: Verifies R-8 — EnvironmentInfoFromOS returns expected keys
func TestEnvironmentInfoFromOS(t *testing.T) {
	t.Parallel()
	info := EnvironmentInfoFromOS()

	requiredKeys := []string{"os", "arch", "cwd", "date", "home"}
	for _, key := range requiredKeys {
		val, ok := info[key]
		if !ok {
			t.Fatalf("missing key %q", key)
		}
		if val == "" {
			t.Fatalf("key %q has empty value", key)
		}
	}

	if info["os"] != runtime.GOOS {
		t.Fatalf("os: expected %q, got %q", runtime.GOOS, info["os"])
	}
	if info["arch"] != runtime.GOARCH {
		t.Fatalf("arch: expected %q, got %q", runtime.GOARCH, info["arch"])
	}
}

// NT-1: Negative test — no trailing whitespace
func TestBuildSystemPrompt_NoTrailingWhitespace(t *testing.T) {
	t.Parallel()
	cfg := PromptConfig{
		StaticPrompt:    "Test.",
		EnvironmentInfo: map[string]string{"os": "linux"},
		ToolDescriptions: []ToolDescription{
			{Name: "bash", Description: "Run things"},
		},
		ExtraContext: []string{"extra"},
	}
	result := BuildSystemPrompt(cfg)

	if strings.HasSuffix(result, "\n") || strings.HasSuffix(result, " ") {
		t.Fatal("prompt has trailing whitespace or newline")
	}
}

// NT-2: Negative test — output is deterministic (same input = same output)
func TestBuildSystemPrompt_Deterministic(t *testing.T) {
	t.Parallel()
	cfg := PromptConfig{
		StaticPrompt: "Test.",
		EnvironmentInfo: map[string]string{
			"os":   "linux",
			"arch": "amd64",
			"cwd":  "/home/user",
		},
	}
	result1 := BuildSystemPrompt(cfg)
	result2 := BuildSystemPrompt(cfg)
	if result1 != result2 {
		t.Fatal("BuildSystemPrompt is not deterministic")
	}
}

// NT-3: Negative test — only ExtraContext provided, no env or tools sections
func TestBuildSystemPrompt_OnlyExtraContext(t *testing.T) {
	t.Parallel()
	cfg := PromptConfig{
		StaticPrompt: "Agent.",
		ExtraContext: []string{"additional info"},
	}
	result := BuildSystemPrompt(cfg)

	if strings.Contains(result, "# Environment") {
		t.Fatal("should not have environment section")
	}
	if strings.Contains(result, "# Available Tools") {
		t.Fatal("should not have tools section")
	}
	if !strings.Contains(result, "additional info") {
		t.Fatal("missing extra context")
	}
}
