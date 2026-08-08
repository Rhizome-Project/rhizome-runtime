package main

import (
	"testing"
)

func strategySynthesisSpec() AgentHeartbeatSpec {
	enabled := true
	willEnabled := true
	return AgentHeartbeatSpec{
		ID:                "strategy_synthesis",
		Kind:              heartbeatKindStrategySynthesis,
		Cadence:           "every_2h",
		Priority:          30,
		Enabled:           &enabled,
		ToolSuites:        []string{"rhizome_read", "memory_and_docs_read"},
		MaxParallel:       1,
		MaxToolIterations: 8,
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			Enabled:        &willEnabled,
			AllowedActions: []string{"advisory_signal", "publish_rhizome_update"},
			MaxDirectives:  3,
		},
	}
}

func TestDefaultStrategySynthesisHeartbeatIsOptInAndValid(t *testing.T) {
	spec := defaultStrategySynthesisHeartbeat()
	if spec.Kind != heartbeatKindStrategySynthesis {
		t.Fatalf("kind = %q, want %q", spec.Kind, heartbeatKindStrategySynthesis)
	}
	if spec.Enabled == nil || *spec.Enabled {
		t.Fatalf("strategy_synthesis builder must default DISABLED (opt-in), got %+v", spec.Enabled)
	}
	if spec.MaxToolIterations != 8 {
		t.Fatalf("strategy_synthesis must carry the elevated iteration budget (8), got %d", spec.MaxToolIterations)
	}
	if spec.ActiveMemory == nil || spec.ActiveMemory.MaxEntries != 20 {
		t.Fatalf("strategy_synthesis must read wide (active_memory.max_entries=20, the system ceiling), got %+v", spec.ActiveMemory)
	}
	if err := validateAgentHeartbeatEnums(spec); err != nil {
		t.Fatalf("default strategy_synthesis should validate enums: %v", err)
	}
	if err := validateStrategySynthesisHeartbeat(spec); err != nil {
		t.Fatalf("default strategy_synthesis must satisfy Layer D invariants: %v", err)
	}
	if err := validateAgentHeartbeatActiveMemorySpec(spec.ID, spec.ActiveMemory); err != nil {
		t.Fatalf("active_memory budget must be within clamps: %v", err)
	}
}

func TestStrategySynthesisDefaultOnOnlyForStewardPresets(t *testing.T) {
	stewards := map[string]bool{"strategist": true, "service_factory_operator": true}
	for _, role := range []string{
		"strategic planner",        // → strategist
		"service factory operator", // → service_factory_operator
		"generalist implementer",
		"UI/UX evil reality critic",
	} {
		profile := DefaultAgentProfile("x", "X", role)
		anatomy := DefaultAgentAnatomyConfig(profile)
		var found *AgentHeartbeatSpec
		for i := range anatomy.Heartbeats {
			if anatomy.Heartbeats[i].Kind == heartbeatKindStrategySynthesis {
				found = &anatomy.Heartbeats[i]
			}
		}
		if stewards[anatomy.Preset] {
			if found == nil {
				t.Fatalf("steward preset %q (role %q) must include strategy_synthesis", anatomy.Preset, role)
			}
			if found.Enabled == nil || !*found.Enabled {
				t.Fatalf("steward preset %q must have strategy_synthesis ENABLED, got %+v", anatomy.Preset, found.Enabled)
			}
		} else if found != nil {
			t.Fatalf("non-steward preset %q (role %q) must NOT include strategy_synthesis", anatomy.Preset, role)
		}
		if err := anatomy.Validate(); err != nil {
			t.Fatalf("preset %q (role %q) default anatomy should validate: %v", anatomy.Preset, role, err)
		}
	}
}

func TestStrategySynthesisRejectsWriteSuites(t *testing.T) {
	for _, badSuite := range []string{"task_authority", "workspace_tools", "local_execution", "bounded_task_submit", "browser_unrestricted"} {
		spec := strategySynthesisSpec()
		spec.ToolSuites = []string{"rhizome_read", badSuite}
		if err := validateStrategySynthesisHeartbeat(spec); err == nil {
			t.Fatalf("strategy_synthesis with write suite %q must be rejected", badSuite)
		}
		if err := validateAgentHeartbeatEnums(spec); err == nil {
			t.Fatalf("validateAgentHeartbeatEnums must reject strategy_synthesis + %q", badSuite)
		}
	}
}

func TestStrategySynthesisAcceptsReadOnlySuites(t *testing.T) {
	for _, okSuite := range []string{"rhizome_read", "memory_and_docs_read", "workspace_docs_read", "local_log_read", "patch_queue_read", "local_tests_read"} {
		spec := strategySynthesisSpec()
		spec.ToolSuites = []string{okSuite}
		if err := validateStrategySynthesisHeartbeat(spec); err != nil {
			t.Fatalf("strategy_synthesis with read-only suite %q must be accepted: %v", okSuite, err)
		}
	}
}

func TestStrategySynthesisRejectsPreemptiveWillActions(t *testing.T) {
	for _, badAction := range []string{"request_resume", "runtime_switch_task", "replan_active_work", "switch_task", "resume"} {
		spec := strategySynthesisSpec()
		spec.WillPolicy.AllowedActions = []string{"advisory_signal", badAction}
		if err := validateStrategySynthesisHeartbeat(spec); err == nil {
			t.Fatalf("strategy_synthesis with preemptive will action %q must be rejected", badAction)
		}
	}
}

func TestStrategySynthesisRoutesToReflectionChannel(t *testing.T) {
	// Layer B routing: a strategy_synthesis heartbeat's advisory output is reflection,
	// so it must drain into the reflection channel, not the watchdog advisory ring.
	if !heartbeatKindUsesReflectionChannel(heartbeatKindStrategySynthesis) {
		t.Fatalf("strategy_synthesis must route to the reflection channel")
	}
}

func TestStrategySynthesisHeartbeatValidatesEndToEnd(t *testing.T) {
	profile := DefaultAgentProfile("sigma", "Sigma", "generalist implementer")
	anatomy := DefaultAgentAnatomyConfig(profile)
	synth := defaultStrategySynthesisHeartbeat()
	enabled := true
	synth.Enabled = &enabled
	anatomy.Heartbeats = append(anatomy.Heartbeats, synth)
	if err := validateAgentAnatomyRaw(anatomy); err != nil {
		t.Fatalf("raw validation of anatomy with enabled synthesizer failed: %v", err)
	}
	normalized := NormalizeAgentAnatomyConfig(anatomy, profile)
	if err := normalized.Validate(); err != nil {
		t.Fatalf("normalized validation of anatomy with enabled synthesizer failed: %v", err)
	}
}
