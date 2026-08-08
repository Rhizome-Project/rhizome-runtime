package main

import (
	"testing"
	"time"
)

func systemSensingSpec() AgentHeartbeatSpec {
	enabled := false
	willEnabled := true
	return AgentHeartbeatSpec{
		ID:                "system_sensor",
		Kind:              heartbeatKindSystemSensing,
		Cadence:           "every_10m",
		Priority:          40,
		Enabled:           &enabled,
		ToolSuites:        []string{"rhizome_read"},
		MaxParallel:       1,
		MaxToolIterations: 3,
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			Enabled:        &willEnabled,
			AllowedActions: []string{"advisory_signal", "publish_rhizome_update"},
			MaxDirectives:  2,
		},
	}
}

func TestDefaultSystemSensorHeartbeatIsOptInAndValid(t *testing.T) {
	spec := defaultSystemSensorHeartbeat()
	if spec.Kind != heartbeatKindSystemSensing {
		t.Fatalf("kind = %q, want %q", spec.Kind, heartbeatKindSystemSensing)
	}
	if spec.Enabled == nil || *spec.Enabled {
		t.Fatalf("system sensor must default DISABLED (opt-in), got %+v", spec.Enabled)
	}
	if err := validateAgentHeartbeatEnums(spec); err != nil {
		t.Fatalf("default system sensor should validate: %v", err)
	}
	if err := validateSystemSensingHeartbeat(spec); err != nil {
		t.Fatalf("default system sensor must satisfy Layer C invariants: %v", err)
	}
}

func TestDefaultSystemSensorNotInjectedIntoPresets(t *testing.T) {
	// Byte-identical-when-off guarantee: the sensor is never auto-added to a
	// preset, so a default anatomy has no system_sensing heartbeat.
	for _, role := range []string{
		"generalist implementer",
		"UI/UX evil reality critic",
		"strategic planner",
	} {
		profile := DefaultAgentProfile("x", "X", role)
		anatomy := DefaultAgentAnatomyConfig(profile)
		for _, hb := range anatomy.Heartbeats {
			if hb.Kind == heartbeatKindSystemSensing {
				t.Fatalf("role %q default anatomy must not include a system_sensing heartbeat, got %q", role, hb.ID)
			}
		}
		if err := anatomy.Validate(); err != nil {
			t.Fatalf("role %q default anatomy should validate: %v", role, err)
		}
	}
}

func TestSystemSensingRejectsWriteSuites(t *testing.T) {
	for _, badSuite := range []string{"task_authority", "workspace_tools", "local_execution", "bounded_task_submit", "browser_unrestricted"} {
		spec := systemSensingSpec()
		spec.ToolSuites = []string{"rhizome_read", badSuite}
		if err := validateSystemSensingHeartbeat(spec); err == nil {
			t.Fatalf("system_sensing with write suite %q must be rejected", badSuite)
		}
		// And the full enum validator (called from both Validate paths) must also reject.
		if err := validateAgentHeartbeatEnums(spec); err == nil {
			t.Fatalf("validateAgentHeartbeatEnums must reject system_sensing + %q", badSuite)
		}
	}
}

func TestSystemSensingAcceptsReadOnlySuites(t *testing.T) {
	for _, okSuite := range []string{"rhizome_read", "memory_and_docs_read", "workspace_docs_read", "local_log_read", "patch_queue_read", "local_tests_read"} {
		spec := systemSensingSpec()
		spec.ToolSuites = []string{okSuite}
		if err := validateSystemSensingHeartbeat(spec); err != nil {
			t.Fatalf("system_sensing with read-only suite %q must be accepted: %v", okSuite, err)
		}
	}
}

func TestSystemSensingRejectsPreemptiveWillActions(t *testing.T) {
	for _, badAction := range []string{"request_resume", "runtime_switch_task", "replan_active_work", "switch_task", "resume"} {
		spec := systemSensingSpec()
		spec.WillPolicy.AllowedActions = []string{"advisory_signal", badAction}
		if err := validateSystemSensingHeartbeat(spec); err == nil {
			t.Fatalf("system_sensing with preemptive will action %q must be rejected", badAction)
		}
	}
}

func TestSystemSensingAllowsAdvisoryAndPublish(t *testing.T) {
	spec := systemSensingSpec()
	spec.WillPolicy.AllowedActions = []string{"advisory_signal", "publish_rhizome_update"}
	if err := validateSystemSensingHeartbeat(spec); err != nil {
		t.Fatalf("advisory+publish must be allowed for system_sensing: %v", err)
	}
}

func TestNonSystemSensingKindUnconstrainedByLayerC(t *testing.T) {
	// A normal task_execution heartbeat with a write suite + resume action must
	// NOT be touched by the Layer C invariant.
	enabled := true
	spec := AgentHeartbeatSpec{
		ID:                "active_task_execution",
		Kind:              "task_execution",
		Cadence:           heartbeatCadenceWhileClaimed,
		Priority:          100,
		Enabled:           &enabled,
		ToolSuites:        []string{"task_authority", "workspace_tools"},
		MaxParallel:       1,
		MaxToolIterations: 0,
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			AllowedActions: []string{"request_resume", "runtime_switch_task"},
		},
	}
	if err := validateSystemSensingHeartbeat(spec); err != nil {
		t.Fatalf("Layer C invariant must not constrain non-sensor kinds: %v", err)
	}
}

func TestSystemSensorSelectorsHydrateRealWorkspaceData(t *testing.T) {
	// The sensor's three selectors must resolve to TYPED hydration (not the
	// prompt-only "not_hydrated" default), so it senses real workspace state
	// through the existing read-only path with no new RPC surface.
	snapshot := WorkspaceSnapshot{
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "t1", Title: "Open high", Status: "OPEN", Priority: "HIGH"},
			{TaskID: "t2", Title: "Blocked", Status: "BLOCKED"},
		},
		Agents: []AgentRecord{
			{AgentID: "peer-1", DisplayName: "Peer One"},
			{AgentID: "peer-2", DisplayName: "Peer Two"},
		},
		RecentUpdates: []AgentUpdateRecord{
			{UpdateID: "u1", UpdateType: "task_completed", Summary: "closed t0"},
		},
	}
	sensor := defaultSystemSensorHeartbeat()
	packets := internalHeartbeatSelectorPayloads(
		sensor.ContextSelectors, snapshot, nil,
		InternalHeartbeatTrustedScope{Source: "test"},
		ProjectCoordinationRecord{}, false, "self", "", time.Now(),
	)
	byName := map[string]InternalHeartbeatSelectorPacket{}
	for _, p := range packets {
		byName[p.Selector] = p
	}
	for _, sel := range []string{"workspace_queue", "peer_roster", "throughput_window"} {
		p, ok := byName[sel]
		if !ok {
			t.Fatalf("selector %q missing from payloads", sel)
		}
		if p.Status == "not_hydrated" {
			t.Fatalf("selector %q must be typed-hydrated, got status=%q", sel, p.Status)
		}
	}
	if byName["workspace_queue"].Workspace.TaskCount != 2 {
		t.Fatalf("workspace_queue should carry task counts, got %+v", byName["workspace_queue"].Workspace)
	}
	if len(byName["peer_roster"].Agents) != 2 {
		t.Fatalf("peer_roster should carry peer agents, got %d", len(byName["peer_roster"].Agents))
	}
	if byName["throughput_window"].Workspace.TaskCount != 2 {
		t.Fatalf("throughput_window should carry rolling workspace counts, got %+v", byName["throughput_window"].Workspace)
	}
}

func TestSystemSensingHeartbeatValidatesEndToEnd(t *testing.T) {
	// A full anatomy carrying an explicitly-configured (enabled) sensor must pass
	// both the raw and normalized validation paths.
	profile := DefaultAgentProfile("iota", "Iota", "generalist implementer")
	anatomy := DefaultAgentAnatomyConfig(profile)
	sensor := defaultSystemSensorHeartbeat()
	enabled := true
	sensor.Enabled = &enabled
	anatomy.Heartbeats = append(anatomy.Heartbeats, sensor)
	if err := validateAgentAnatomyRaw(anatomy); err != nil {
		t.Fatalf("raw validation of anatomy with enabled sensor failed: %v", err)
	}
	normalized := NormalizeAgentAnatomyConfig(anatomy, profile)
	if err := normalized.Validate(); err != nil {
		t.Fatalf("normalized validation of anatomy with enabled sensor failed: %v", err)
	}
}
