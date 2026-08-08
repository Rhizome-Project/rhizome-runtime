package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentProfileDefaultsAndIdentityFiles(t *testing.T) {
	workdir := t.TempDir()
	profile := normalizeAgentProfile(AgentProfile{
		AgentID:               "beta-01",
		DisplayName:           "Beta 01",
		Role:                  "builder",
		PrimarySpecialization: "delivery and repair",
		SecondarySpecializations: []string{
			"review",
			"tooling",
		},
		DomainScope: []string{"tasks, docs", "reviews"},
	})

	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatalf("SaveAgentProfile() error: %v", err)
	}
	if err := WriteAgentIdentityFiles(workdir, profile); err != nil {
		t.Fatalf("WriteAgentIdentityFiles() error: %v", err)
	}

	loaded := LoadAgentProfile(workdir)
	if loaded.AgentID != "beta-01" || loaded.DisplayName != "Beta 01" {
		t.Fatalf("unexpected loaded agent profile: %+v", loaded)
	}
	if len(loaded.SuccessCriteria) == 0 || len(loaded.HardConstraints) == 0 || len(loaded.TypicalInputs) == 0 || len(loaded.TypicalOutputs) == 0 {
		t.Fatalf("expected default prompt fields to be populated: %+v", loaded)
	}
	if len(loaded.AutonomousObjectives) == 0 || len(loaded.InitiativeTriggers) == 0 || len(loaded.ServiceFactoryFocus) == 0 {
		t.Fatalf("expected autonomy prompt fields to be populated: %+v", loaded)
	}
	if loaded.ReflectionScope != "local" || loaded.IdleActionPolicy != "self_check" || loaded.CanOpenReflectionTasks == nil || *loaded.CanOpenReflectionTasks {
		t.Fatalf("expected builder profile to default to local self-check metacognition, got %+v", loaded)
	}

	for _, name := range []string{"AGENT.md", "SOUL.md", "TOOLS.md"} {
		path := filepath.Join(workdir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		text := string(data)
		if !strings.Contains(text, "beta-01") {
			t.Fatalf("expected %s to mention agent id, got:\n%s", name, text)
		}
		if name != "TOOLS.md" && !strings.Contains(strings.ToLower(text), "metacognition") {
			t.Fatalf("expected %s to mention metacognition profile, got:\n%s", name, text)
		}
		if name != "TOOLS.md" && !strings.Contains(text, "Autonomous Objectives") {
			t.Fatalf("expected %s to mention autonomous objectives, got:\n%s", name, text)
		}
	}

	toolsDoc, err := os.ReadFile(filepath.Join(workdir, "TOOLS.md"))
	if err != nil {
		t.Fatalf("read TOOLS.md: %v", err)
	}
	toolsText := string(toolsDoc)
	for _, needle := range []string{"shell", "read_file", "tension_attach", "coalition_offer", "reviewer_route", "memory_coherence_read", "agent_request", "Normal shell syntax is allowed", "Dynamic Routed Tools", "Runtime-Owned Capabilities", "scaffold/config ownership", "tsconfig*.json"} {
		if !strings.Contains(toolsText, needle) {
			t.Fatalf("expected TOOLS.md to mention %q, got:\n%s", needle, toolsText)
		}
	}
}

func TestWriteAgentIdentityFilesWithDisabledToolsHidesHarnessEscapes(t *testing.T) {
	workdir := t.TempDir()
	profile := normalizeAgentProfile(AgentProfile{
		AgentID:     "delta",
		DisplayName: "Delta",
		Role:        "lua eval implementer",
	})
	disabled := runtimeDisabledToolNameItems([]string{"agent_request", "project_checkout_materialize"})

	if err := WriteAgentIdentityFilesWithDisabledTools(workdir, profile, disabled); err != nil {
		t.Fatalf("WriteAgentIdentityFilesWithDisabledTools() error = %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(workdir, "TOOLS.md"))
	if err != nil {
		t.Fatalf("read TOOLS.md: %v", err)
	}
	text := string(raw)
	for _, forbidden := range []string{"### agent_request", "### project_checkout_materialize", "request_kind=delegate_task"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("expected disabled harness affordance %q to be absent, got:\n%s", forbidden, text)
		}
	}
	for _, kept := range []string{"### shell", "### project_branch_commit", "### project_checkout_register"} {
		if !strings.Contains(text, kept) {
			t.Fatalf("expected TOOLS.md to retain %q, got:\n%s", kept, text)
		}
	}
}

func TestAgentProfileMetacognitionDefaultsByRole(t *testing.T) {
	cases := []struct {
		role    string
		scope   string
		policy  string
		canOpen bool
	}{
		{role: "worker", scope: "local", policy: "self_check", canOpen: false},
		{role: "reviewer", scope: "artifact", policy: "review_artifact", canOpen: true},
		{role: "ui/ux critic", scope: "artifact", policy: "review_artifact", canOpen: true},
		{role: "ui ux implementer", scope: "artifact", policy: "review_artifact", canOpen: true},
		{role: "integrator", scope: "project", policy: "join_existing_reflection", canOpen: true},
		{role: "strategist", scope: "project", policy: "open_uncovered_direction", canOpen: true},
		{role: "market scout", scope: "global", policy: "open_uncovered_direction", canOpen: true},
		{role: "deploy operator", scope: "project", policy: "join_existing_reflection", canOpen: true},
		{role: "ad monetization compliance reviewer", scope: "global", policy: "open_uncovered_direction", canOpen: true},
	}

	for _, tc := range cases {
		t.Run(tc.role, func(t *testing.T) {
			profile := normalizeAgentProfile(AgentProfile{Role: tc.role})
			if profile.ReflectionScope != tc.scope || profile.IdleActionPolicy != tc.policy {
				t.Fatalf("unexpected metacognition defaults for %s: %+v", tc.role, profile)
			}
			if profile.CanOpenReflectionTasks == nil || *profile.CanOpenReflectionTasks != tc.canOpen {
				t.Fatalf("unexpected can_open_reflection_tasks for %s: %+v", tc.role, profile)
			}
		})
	}
}

func TestServiceFactoryRoleProfileMaterializesAutonomyMandate(t *testing.T) {
	workdir := t.TempDir()
	profile := normalizeAgentProfile(AgentProfile{
		AgentID:               "scout-1",
		DisplayName:           "Scout 1",
		Role:                  "market scout",
		PrimarySpecialization: "service factory opportunity discovery and portfolio learning",
	})
	if profile.ReflectionScope != "global" || profile.IdleActionPolicy != "open_uncovered_direction" || profile.CanOpenReflectionTasks == nil || !*profile.CanOpenReflectionTasks {
		t.Fatalf("expected market scout to default to global initiative, got %+v", profile)
	}
	for _, want := range []string{"distribution channel", "validation signal", "kill criteria"} {
		text := strings.Join(append(append([]string{}, profile.AutonomousObjectives...), profile.ServiceFactoryFocus...), "\n")
		if !strings.Contains(text, want) {
			t.Fatalf("expected service-factory profile defaults to contain %q, got:\n%s", want, text)
		}
	}

	if err := WriteAgentIdentityFiles(workdir, profile); err != nil {
		t.Fatalf("WriteAgentIdentityFiles() error = %v", err)
	}
	for _, name := range []string{"AGENT.md", "SOUL.md"} {
		data, err := os.ReadFile(filepath.Join(workdir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		text := string(data)
		for _, want := range []string{"Autonomous Objectives", "Initiative Triggers", "Service Factory Focus", "distribution channel"} {
			if !strings.Contains(text, want) {
				t.Fatalf("expected %s to contain %q, got:\n%s", name, want, text)
			}
		}
	}
}

func TestUXRealityCriticProfileMaterializesOperationalMandate(t *testing.T) {
	workdir := t.TempDir()
	profile := normalizeAgentProfile(AgentProfile{
		AgentID:               "iota",
		DisplayName:           "Iota",
		Role:                  "ui ux implementer",
		PrimarySpecialization: "interaction design, controls, visual polish, responsive ergonomics",
	})

	if profile.ReflectionScope != "artifact" || profile.IdleActionPolicy != "review_artifact" || profile.CanOpenReflectionTasks == nil || !*profile.CanOpenReflectionTasks {
		t.Fatalf("expected UI/UX profile to default to artifact review metacognition, got %+v", profile)
	}
	policy := normalizeMetacognitionPolicy(profile)
	for _, want := range []string{"harsh real-user critic", "real usage scenarios", "nonblank canvas", "viewport/device", "performance symptoms", "primary surface geometry", "mode/preset/difficulty-specific fit"} {
		if !strings.Contains(policy.RealityCheckDescription, want) {
			t.Fatalf("expected UX quality bar to contain %q, got %q", want, policy.RealityCheckDescription)
		}
	}

	if err := WriteAgentIdentityFiles(workdir, profile); err != nil {
		t.Fatalf("WriteAgentIdentityFiles() error = %v", err)
	}
	for _, name := range []string{"AGENT.md", "SOUL.md"} {
		data, err := os.ReadFile(filepath.Join(workdir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		text := string(data)
		for _, want := range []string{"UX Reality-Critic", "harsh real-user critic", "real user scenario tested", "artifact/version observed", "Missing canonical publication or patch queue evidence blocks acceptance", "primary-surface geometry/density"} {
			if !strings.Contains(text, want) {
				t.Fatalf("expected %s to contain %q, got:\n%s", name, want, text)
			}
		}
	}
}

func TestRuntimeConfigRoleOverrideRefreshesDefaultMetacognitionProfile(t *testing.T) {
	t.Run("worker file launched as reviewer", func(t *testing.T) {
		workdir := t.TempDir()
		if err := SaveAgentProfile(workdir, AgentProfile{
			AgentID: "agent-1",
			Role:    "worker",
		}); err != nil {
			t.Fatalf("SaveAgentProfile() error: %v", err)
		}

		policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Workdir: workdir, AgentID: "agent-1", Role: "reviewer"})
		if policy.ReflectionScope != "artifact" || policy.IdleActionPolicy != "review_artifact" || !policy.CanOpenReflectionTasks {
			t.Fatalf("runtime reviewer override should refresh worker-derived metacognition defaults, got %+v", policy)
		}
	})

	t.Run("reviewer file launched as worker", func(t *testing.T) {
		workdir := t.TempDir()
		if err := SaveAgentProfile(workdir, AgentProfile{
			AgentID: "agent-2",
			Role:    "reviewer",
		}); err != nil {
			t.Fatalf("SaveAgentProfile() error: %v", err)
		}

		policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Workdir: workdir, AgentID: "agent-2", Role: "worker"})
		if policy.ReflectionScope != "local" || policy.IdleActionPolicy != "self_check" || policy.CanOpenReflectionTasks {
			t.Fatalf("runtime worker override should refresh reviewer-derived metacognition defaults, got %+v", policy)
		}
	})
}

func TestRuntimeConfigRoleOverridePreservesExplicitMetacognitionProfile(t *testing.T) {
	workdir := t.TempDir()
	canOpen := true
	if err := SaveAgentProfile(workdir, AgentProfile{
		AgentID:                 "agent-3",
		Role:                    "worker",
		ReflectionScope:         "artifact",
		IdleActionPolicy:        "review_artifact",
		CanOpenReflectionTasks:  &canOpen,
		MaxNewTasksPerIdleCycle: 1,
	}); err != nil {
		t.Fatalf("SaveAgentProfile() error: %v", err)
	}

	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Workdir: workdir, AgentID: "agent-3", Role: "reviewer"})
	if policy.ReflectionScope != "artifact" || policy.IdleActionPolicy != "review_artifact" || !policy.CanOpenReflectionTasks || policy.MaxNewTasksPerIdleCycle != 1 {
		t.Fatalf("runtime override should preserve explicit metacognition fields, got %+v", policy)
	}
}

func TestMetacognitionPolicyLiftsStaleStrategistScope(t *testing.T) {
	canOpen := false
	policy := normalizeMetacognitionPolicy(AgentProfile{
		AgentID:                 "alpha",
		Role:                    "strategist",
		PrimarySpecialization:   "global strategy and project architecture",
		Mission:                 "High global meta-cognition steward for shared project coordination",
		ReflectionScope:         "artifact",
		IdleActionPolicy:        "review_artifact",
		CanOpenReflectionTasks:  &canOpen,
		MaxNewTasksPerIdleCycle: 1,
	})
	if policy.ReflectionScope != "global" || policy.IdleActionPolicy != "open_uncovered_direction" || !policy.CanOpenReflectionTasks || policy.MaxNewTasksPerIdleCycle != 3 {
		t.Fatalf("stale strategist artifact fields should be lifted to global initiative, got %+v", policy)
	}
}

func TestMetacognitionPolicyKeepsUXCriticArtifactScoped(t *testing.T) {
	policy := normalizeMetacognitionPolicy(AgentProfile{
		AgentID:               "iota",
		Role:                  "ui/ux critic",
		PrimarySpecialization: "harsh real-user usability review and visual polish",
		Mission:               "Mentions project strategy in feedback but remains grounded in artifact reality checks",
		ReflectionScope:       "artifact",
		IdleActionPolicy:      "review_artifact",
	})
	if policy.ReflectionScope != "artifact" || policy.IdleActionPolicy != "review_artifact" {
		t.Fatalf("UX reality critic should remain artifact-scoped, got %+v", policy)
	}
}

func TestUniqueTrimmedCSVStringsDeduplicates(t *testing.T) {
	got := uniqueTrimmedCSVStrings([]string{"alpha, beta", "beta", " gamma "})
	if strings.Join(got, "|") != "alpha|beta|gamma" {
		t.Fatalf("unexpected normalized csv values: %+v", got)
	}
}
