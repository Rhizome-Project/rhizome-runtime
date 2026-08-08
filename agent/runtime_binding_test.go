package main

import "testing"

func TestNewRuntimeBindsAgentBeforeInit(t *testing.T) {
	cfg := RuntimeConfig{
		Workdir:      t.TempDir(),
		RhizomeRPC:   "https://rhizome.test/rpc",
		WorkspaceID:  "ws-1",
		AgentID:      "agent-1",
		OwnerUserID:  "owner-1",
		DisplayName:  "Agent One",
		RhizomeToken: "token-1",
	}

	runtime := NewRuntime(cfg, nil)
	if runtime == nil || runtime.agent == nil {
		t.Fatal("expected runtime and agent to be initialized")
	}
	if runtime.agent.Client != runtime.client {
		t.Fatalf("expected agent client to be bound before init, got %+v", runtime.agent.Client)
	}
	if runtime.agent.WorkspaceID != cfg.WorkspaceID {
		t.Fatalf("expected agent workspace id to be bound before init, got %q", runtime.agent.WorkspaceID)
	}
	if runtime.agent.AgentID != cfg.AgentID {
		t.Fatalf("expected agent id to be bound before init, got %q", runtime.agent.AgentID)
	}
	if runtime.agent.registry == nil {
		t.Fatal("expected agent registry to be initialized")
	}
	for _, name := range []string{"tension_attach", "tension_detach", "tension_lifecycle_update"} {
		if _, ok := runtime.agent.registry.Get(name); !ok {
			t.Fatalf("expected %s to be registered after early binding", name)
		}
	}
}
