package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// fullHarnessAnatomy builds a strategist anatomy with ALL FOUR layers explicitly
// enabled: Layer A (constitution + seed rule), Layer B (reflection_channel_cap),
// Layer C (system_sensor enabled), Layer D (strategy_synthesis — already on for the
// strategist preset). This is the "flags fully ON" configuration.
func fullHarnessAnatomy(profile AgentProfile) AgentAnatomyConfig {
	anatomy := defaultAgentAnatomyBaseForPreset(profile, "strategist")
	enabled := true
	anatomy.Memory.Constitution = AgentAnatomyConstitution{
		Enabled:     &enabled,
		MaxRules:    7,
		MinEvidence: 2,
		Promotion:   "evidence_or_seed",
		SeedRules: []AgentAnatomyConstitutionRule{
			{ID: "seed-no-secrets", Rule: "never write secret values into notes or logs", Kind: "invariant", Priority: 100},
		},
	}
	anatomy.Memory.ReflectionChannelCap = 8
	sensor := defaultSystemSensorHeartbeat()
	sensor.Enabled = &enabled
	anatomy.Heartbeats = append(anatomy.Heartbeats, sensor)
	return anatomy
}

// TestHarnessEndToEndFilePipelineAllLayersOn proves a real on-disk anatomy with all
// four layers enabled survives the full production load path:
// ReadAgentAnatomyConfigFile -> validateAgentAnatomyRaw -> NormalizeAgentAnatomyConfig
// -> Validate, and that each layer's marker is still present afterward.
func TestHarnessEndToEndFilePipelineAllLayersOn(t *testing.T) {
	profile := DefaultAgentProfile("zeta", "Zeta", "strategic planner")
	anatomy := fullHarnessAnatomy(profile)

	raw, err := json.MarshalIndent(anatomy, "", "  ")
	if err != nil {
		t.Fatalf("marshal anatomy: %v", err)
	}
	dir := t.TempDir()
	path := agentAnatomyPath(dir)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write anatomy file: %v", err)
	}

	loaded, err := ReadAgentAnatomyConfig(dir, profile)
	if err != nil {
		t.Fatalf("end-to-end load of all-layers-on anatomy failed: %v", err)
	}

	// Layer A survived.
	if loaded.Memory.Constitution.Enabled == nil || !*loaded.Memory.Constitution.Enabled {
		t.Fatalf("Layer A: constitution must remain enabled after load, got %+v", loaded.Memory.Constitution)
	}
	if len(loaded.Memory.Constitution.SeedRules) == 0 {
		t.Fatalf("Layer A: seed rule must survive normalization")
	}
	// Layer B survived.
	if loaded.Memory.ReflectionChannelCap != 8 {
		t.Fatalf("Layer B: reflection_channel_cap must remain 8, got %d", loaded.Memory.ReflectionChannelCap)
	}
	// Layers C and D present and enabled.
	var sawSensor, sawSynth bool
	for _, hb := range loaded.Heartbeats {
		switch hb.Kind {
		case heartbeatKindSystemSensing:
			sawSensor = true
			if hb.Enabled == nil || !*hb.Enabled {
				t.Fatalf("Layer C: system_sensor must remain enabled, got %+v", hb.Enabled)
			}
		case heartbeatKindStrategySynthesis:
			sawSynth = true
			if hb.Enabled == nil || !*hb.Enabled {
				t.Fatalf("Layer D: strategy_synthesis must remain enabled, got %+v", hb.Enabled)
			}
		}
	}
	if !sawSensor {
		t.Fatalf("Layer C: system_sensor missing after load")
	}
	if !sawSynth {
		t.Fatalf("Layer D: strategy_synthesis missing after load")
	}

	// Digest must be stable: re-loading the same bytes yields the same digest.
	loaded2, err := ReadAgentAnatomyConfig(dir, profile)
	if err != nil {
		t.Fatalf("second load failed: %v", err)
	}
	if AgentAnatomyDigest(loaded) != AgentAnatomyDigest(loaded2) {
		t.Fatalf("digest unstable across identical loads")
	}
}

// TestHarnessSensorAndSynthesizerCannotLaunchOrPreempt is the core safety assertion:
// the executor policy for both new read-only kinds must be LocalOnly with no task
// loop, no task submit, no public-doc authority, and no agent-request authority — so
// neither can launch a live run, mutate workspace state, or preempt active work.
func TestHarnessSensorAndSynthesizerCannotLaunchOrPreempt(t *testing.T) {
	for _, spec := range []AgentHeartbeatSpec{
		defaultSystemSensorHeartbeat(),
		defaultStrategySynthesisHeartbeat(),
	} {
		policy := internalHeartbeatExecutionPolicy(spec)
		if !policy.LocalOnly {
			t.Fatalf("%s: must be LocalOnly (read-only, cannot reach public authority)", spec.Kind)
		}
		if policy.RequiresTaskLoop {
			t.Fatalf("%s: must NOT require the task loop (cannot drive task execution)", spec.Kind)
		}
		if policy.AllowTaskSubmit || policy.MaxTaskSubmits != 0 {
			t.Fatalf("%s: must NOT be able to submit tasks (cannot launch work)", spec.Kind)
		}
		if policy.AllowPublicDocs {
			t.Fatalf("%s: must NOT hold public-doc authority (cannot mutate shared state)", spec.Kind)
		}
		if policy.AllowAgentRequest {
			t.Fatalf("%s: must NOT be able to issue agent requests (cannot preempt peers)", spec.Kind)
		}
		// Will directives are allowed, but only advisory/publish — never resume/switch/replan.
		for _, action := range policy.WillActions {
			switch normalizeAgentHeartbeatWillAction(action) {
			case "request_resume", "runtime_switch_task", "replan_active_work":
				t.Fatalf("%s: preemptive will action %q leaked into the execution policy", spec.Kind, action)
			}
		}
	}
}

// TestHarnessFileLoadRejectsMisconfiguredSensor proves the read-only invariant is
// enforced at file-load time (not merely soft-clamped): an on-disk anatomy whose
// system_sensing heartbeat carries an authority suite must fail to load.
func TestHarnessFileLoadRejectsMisconfiguredSensor(t *testing.T) {
	profile := DefaultAgentProfile("zeta", "Zeta", "strategic planner")
	anatomy := defaultAgentAnatomyBaseForPreset(profile, "generalist")
	enabled := true
	badSensor := defaultSystemSensorHeartbeat()
	badSensor.Enabled = &enabled
	badSensor.ToolSuites = []string{"rhizome_read", "task_authority"} // illegal write suite
	anatomy.Heartbeats = append(anatomy.Heartbeats, badSensor)

	raw, err := json.Marshal(anatomy)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(agentAnatomyPath(dir), raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := ReadAgentAnatomyConfig(dir, profile); err == nil {
		t.Fatalf("a system_sensing heartbeat with task_authority must be rejected at file load, not accepted")
	}
}

// TestHarnessFlagsOffFileIsByteIdenticalDigest proves the rollout guarantee at the
// file level: a generalist anatomy with NO harness flags loads to the same digest as
// the in-memory default, so an existing on-disk anatomy is unaffected by the harness.
func TestHarnessFlagsOffFileIsByteIdenticalDigest(t *testing.T) {
	profile := DefaultAgentProfile("gamma", "Gamma", "generalist implementer")
	def := DefaultAgentAnatomyConfig(profile)

	raw, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(agentAnatomyPath(dir), raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	loaded, err := ReadAgentAnatomyConfig(dir, profile)
	if err != nil {
		t.Fatalf("load default failed: %v", err)
	}
	if got, want := AgentAnatomyDigest(loaded), AgentAnatomyDigest(def); got != want {
		t.Fatalf("flags-off file must round-trip byte-identical (digest %s != %s)", got, want)
	}
	// And no harness markers leaked into the generalist default.
	if loaded.Memory.Constitution.Enabled != nil && *loaded.Memory.Constitution.Enabled {
		t.Fatalf("generalist default must not enable the constitution")
	}
	if loaded.Memory.ReflectionChannelCap != 0 {
		t.Fatalf("generalist default reflection cap must be 0, got %d", loaded.Memory.ReflectionChannelCap)
	}
	for _, hb := range loaded.Heartbeats {
		if hb.Kind == heartbeatKindSystemSensing || hb.Kind == heartbeatKindStrategySynthesis {
			t.Fatalf("generalist default must not carry harness heartbeat %q", hb.Kind)
		}
	}
}

// TestHarnessReflectionAndAdvisoryRenderDistinctlyInSystemPrompt proves the Layer B
// render reaches the actual agent system prompt and that the reflection channel and
// the watchdog advisory ring are rendered as two distinct, non-contaminating
// sections (a reflection insight never appears as a watchdog intervention).
func TestHarnessReflectionAndAdvisoryRenderDistinctlyInSystemPrompt(t *testing.T) {
	agent := &Agent{Workdir: t.TempDir()}
	agent.Init()

	const advisoryText = "BUDGET INTERVENTION: you are over the tool budget"
	const reflectionText = "queue is starving downstream QA"
	taskCtx := AgentTaskContext{
		Mode:            "daemon",
		AdvisorySignals: []string{advisoryText},
		ReflectionSignals: []ReflectionSignal{
			{Source: "system_sensor", Kind: "risk", Text: reflectionText, Count: 2},
		},
	}
	prompt := agent.buildSystemPrompt(taskCtx)

	if !strings.Contains(prompt, "🚨 Watchdog Advisory Signals 🚨") {
		t.Fatalf("advisory section header missing from system prompt")
	}
	if !strings.Contains(prompt, "## Reflection Channel (self-authored insight)") {
		t.Fatalf("reflection section header missing from system prompt")
	}
	if !strings.Contains(prompt, advisoryText) || !strings.Contains(prompt, reflectionText) {
		t.Fatalf("both signal texts must appear in the prompt")
	}
	// No cross-contamination: the watchdog block (everything before the reflection
	// header) must not contain the reflection text, and vice versa.
	idx := strings.Index(prompt, "## Reflection Channel")
	advisoryBlock := prompt[:idx]
	reflectionBlock := prompt[idx:]
	if strings.Contains(advisoryBlock, reflectionText) {
		t.Fatalf("reflection insight leaked into the watchdog advisory block")
	}
	if strings.Contains(reflectionBlock, advisoryText) {
		t.Fatalf("watchdog intervention leaked into the reflection block")
	}
}

// TestHarnessReflectionAbsentWhenChannelEmpty proves the flags-off render guarantee:
// with no reflection signals the section is wholly absent from the system prompt.
func TestHarnessReflectionAbsentWhenChannelEmpty(t *testing.T) {
	agent := &Agent{Workdir: t.TempDir()}
	agent.Init()
	prompt := agent.buildSystemPrompt(AgentTaskContext{Mode: "daemon", AdvisorySignals: []string{"some watchdog note"}})
	if strings.Contains(prompt, "Reflection Channel") {
		t.Fatalf("reflection section must be absent when the channel is empty")
	}
}
