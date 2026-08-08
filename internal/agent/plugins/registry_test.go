package plugins

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/hooks"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
)

// --- Mock types ---

type mockTool struct {
	name string
}

func (m *mockTool) Name() string        { return m.name }
func (m *mockTool) Description() string { return "mock tool: " + m.name }
func (m *mockTool) Schema() tools.Schema {
	return tools.Schema{Type: "object"}
}
func (m *mockTool) Execute(_ context.Context, _ json.RawMessage) (string, error) {
	return "ok", nil
}

type mockHook struct {
	name string
}

func (m *mockHook) Name() string          { return m.name }
func (m *mockHook) Points() []hooks.Point { return []hooks.Point{hooks.BeforeTool} }
func (m *mockHook) Run(_ context.Context, _ hooks.Context) (hooks.Result, error) {
	return hooks.Result{}, nil
}

type mockPlugin struct {
	name            string
	description     string
	tools           []tools.Tool
	hooks           []hooks.Hook
	promptFragments []string
}

func (m *mockPlugin) Name() string              { return m.name }
func (m *mockPlugin) Description() string       { return m.description }
func (m *mockPlugin) Tools() []tools.Tool       { return m.tools }
func (m *mockPlugin) Hooks() []hooks.Hook       { return m.hooks }
func (m *mockPlugin) PromptFragments() []string { return m.promptFragments }

// --- Tests ---

// T-1: Verifies R-3 — register a plugin, get it by name.
func TestPluginRegistryRegisterAndGet(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	plugin := &mockPlugin{name: "test-plugin", description: "A test plugin"}

	if err := reg.Register(plugin); err != nil {
		t.Fatalf("register: %v", err)
	}

	got, ok := reg.Get("test-plugin")
	if !ok {
		t.Fatal("expected Get to return true")
	}
	if got.Name() != "test-plugin" {
		t.Fatalf("expected name %q, got %q", "test-plugin", got.Name())
	}
	if got.Description() != "A test plugin" {
		t.Fatalf("expected description %q, got %q", "A test plugin", got.Description())
	}
}

// T-2: Verifies EC-1 — register same name twice, error.
func TestPluginRegistryDuplicate(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	p1 := &mockPlugin{name: "dup"}
	p2 := &mockPlugin{name: "dup"}

	if err := reg.Register(p1); err != nil {
		t.Fatalf("first register: %v", err)
	}
	err := reg.Register(p2)
	if err == nil {
		t.Fatal("expected error on duplicate registration")
	}
}

// T-3: Verifies R-3 — register 2 plugins with 2 tools each, AllTools returns 4 tools in order.
func TestPluginRegistryAllTools(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	reg.Register(&mockPlugin{
		name:  "plugin-a",
		tools: []tools.Tool{&mockTool{name: "tool-a1"}, &mockTool{name: "tool-a2"}},
	})
	reg.Register(&mockPlugin{
		name:  "plugin-b",
		tools: []tools.Tool{&mockTool{name: "tool-b1"}, &mockTool{name: "tool-b2"}},
	})

	allTools := reg.AllTools()
	if len(allTools) != 4 {
		t.Fatalf("expected 4 tools, got %d", len(allTools))
	}

	expected := []string{"tool-a1", "tool-a2", "tool-b1", "tool-b2"}
	for i, tool := range allTools {
		if tool.Name() != expected[i] {
			t.Fatalf("tool[%d]: expected %q, got %q", i, expected[i], tool.Name())
		}
	}
}

// T-4: Verifies R-3 — register plugin with hooks, AllHooks returns them.
func TestPluginRegistryAllHooks(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	reg.Register(&mockPlugin{
		name:  "hook-plugin",
		hooks: []hooks.Hook{&mockHook{name: "hook-1"}, &mockHook{name: "hook-2"}},
	})

	allHooks := reg.AllHooks()
	if len(allHooks) != 2 {
		t.Fatalf("expected 2 hooks, got %d", len(allHooks))
	}
	if allHooks[0].Name() != "hook-1" || allHooks[1].Name() != "hook-2" {
		t.Fatalf("hooks not in expected order")
	}
}

// T-5: Verifies EC-3 — empty registry returns empty slices.
func TestPluginRegistryEmpty(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()

	allTools := reg.AllTools()
	if allTools == nil {
		t.Fatal("AllTools should return non-nil empty slice")
	}
	if len(allTools) != 0 {
		t.Fatalf("expected 0 tools, got %d", len(allTools))
	}

	allHooks := reg.AllHooks()
	if allHooks == nil {
		t.Fatal("AllHooks should return non-nil empty slice")
	}
	if len(allHooks) != 0 {
		t.Fatalf("expected 0 hooks, got %d", len(allHooks))
	}

	allFragments := reg.AllPromptFragments()
	if allFragments == nil {
		t.Fatal("AllPromptFragments should return non-nil empty slice")
	}
	if len(allFragments) != 0 {
		t.Fatalf("expected 0 fragments, got %d", len(allFragments))
	}
}

// T-6: Verifies EC-2 — register plugin with no tools/hooks/fragments, no error.
func TestPluginRegistryEmptyPlugin(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	empty := &mockPlugin{name: "empty-plugin", description: "no capabilities"}

	if err := reg.Register(empty); err != nil {
		t.Fatalf("register empty plugin: %v", err)
	}

	list := reg.List()
	if len(list) != 1 {
		t.Fatalf("expected 1 plugin, got %d", len(list))
	}
	if list[0].Name() != "empty-plugin" {
		t.Fatalf("expected %q, got %q", "empty-plugin", list[0].Name())
	}
}

// Verifies AllPromptFragments aggregates from multiple plugins.
func TestPluginRegistryAllPromptFragments(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	reg.Register(&mockPlugin{
		name:            "frag-a",
		promptFragments: []string{"Fragment A1", "Fragment A2"},
	})
	reg.Register(&mockPlugin{
		name:            "frag-b",
		promptFragments: []string{"Fragment B1"},
	})

	fragments := reg.AllPromptFragments()
	if len(fragments) != 3 {
		t.Fatalf("expected 3 fragments, got %d", len(fragments))
	}
	if fragments[0] != "Fragment A1" || fragments[2] != "Fragment B1" {
		t.Fatalf("fragments not in expected order: %v", fragments)
	}
}

// Verifies List returns copy in registration order.
func TestPluginRegistryList_Order(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	reg.Register(&mockPlugin{name: "alpha"})
	reg.Register(&mockPlugin{name: "beta"})
	reg.Register(&mockPlugin{name: "gamma"})

	list := reg.List()
	if len(list) != 3 {
		t.Fatalf("expected 3 plugins, got %d", len(list))
	}
	expected := []string{"alpha", "beta", "gamma"}
	for i, p := range list {
		if p.Name() != expected[i] {
			t.Fatalf("list[%d]: expected %q, got %q", i, expected[i], p.Name())
		}
	}
}

// Verifies Get returns false for non-existent plugin.
func TestPluginRegistryGet_NotFound(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	_, ok := reg.Get("nonexistent")
	if ok {
		t.Fatal("expected Get to return false for non-existent plugin")
	}
}

// NT-1: Plugin with tools but no hooks — AllHooks still works.
func TestPluginRegistryMixedCapabilities(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	reg.Register(&mockPlugin{
		name:  "tools-only",
		tools: []tools.Tool{&mockTool{name: "t1"}},
	})
	reg.Register(&mockPlugin{
		name:  "hooks-only",
		hooks: []hooks.Hook{&mockHook{name: "h1"}},
	})

	allTools := reg.AllTools()
	if len(allTools) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(allTools))
	}
	allHooks := reg.AllHooks()
	if len(allHooks) != 1 {
		t.Fatalf("expected 1 hook, got %d", len(allHooks))
	}
}
