package tools

import (
	"context"
	"encoding/json"
	"testing"
)

// mockTool implements Tool for testing.
type mockTool struct {
	name        string
	description string
	schema      Schema
}

func (m *mockTool) Name() string                                                 { return m.name }
func (m *mockTool) Description() string                                          { return m.description }
func (m *mockTool) Schema() Schema                                               { return m.schema }
func (m *mockTool) Execute(_ context.Context, _ json.RawMessage) (string, error) { return "ok", nil }

func newMock(name, desc string) *mockTool {
	return &mockTool{name: name, description: desc, schema: Schema{Type: "object"}}
}

// T-1: Verifies R-6 — Register and Get round-trip
func TestRegistryRegisterAndGet(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	tool := newMock("bash", "Execute commands")

	if err := reg.Register(tool); err != nil {
		t.Fatalf("register: %v", err)
	}

	got, ok := reg.Get("bash")
	if !ok {
		t.Fatal("expected Get to return true")
	}
	if got.Name() != "bash" {
		t.Fatalf("expected name %q, got %q", "bash", got.Name())
	}
}

// T-2: Verifies EC-1 — duplicate registration returns error with name
func TestRegistryDuplicateError(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	tool1 := newMock("bash", "v1")
	tool2 := newMock("bash", "v2")

	if err := reg.Register(tool1); err != nil {
		t.Fatalf("first register: %v", err)
	}
	err := reg.Register(tool2)
	if err == nil {
		t.Fatal("expected error on duplicate registration")
	}
	if got := err.Error(); got != "tool already registered: bash" {
		t.Fatalf("expected descriptive error, got %q", got)
	}
}

// T-3: Verifies R-6 — List returns tools in registration order
func TestRegistryList_Order(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if err := reg.Register(newMock(name, "")); err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
	}

	list := reg.List()
	if len(list) != 3 {
		t.Fatalf("expected 3 tools, got %d", len(list))
	}
	expected := []string{"alpha", "beta", "gamma"}
	for i, tool := range list {
		if tool.Name() != expected[i] {
			t.Fatalf("list[%d]: expected %q, got %q", i, expected[i], tool.Name())
		}
	}
}

// T-4: Verifies EC-2 — Get with unknown name returns false
func TestRegistryGetMissing(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	got, ok := reg.Get("nonexistent")
	if ok {
		t.Fatal("expected ok to be false")
	}
	if got != nil {
		t.Fatalf("expected nil tool, got %v", got)
	}
}

// T-5: Verifies EC-3 — List and Names on empty registry return empty non-nil slices
func TestRegistryEmptyList(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()

	list := reg.List()
	if list == nil {
		t.Fatal("List() returned nil, expected empty non-nil slice")
	}
	if len(list) != 0 {
		t.Fatalf("expected 0 tools, got %d", len(list))
	}

	names := reg.Names()
	if names == nil {
		t.Fatal("Names() returned nil, expected empty non-nil slice")
	}
	if len(names) != 0 {
		t.Fatalf("expected 0 names, got %d", len(names))
	}
}

// T-6: Verifies R-9 — SchemaToClaudeFormat produces correct structure
func TestSchemaToClaudeFormat(t *testing.T) {
	t.Parallel()
	schema := Schema{
		Type: "object",
		Properties: map[string]Property{
			"command": {Type: "string", Description: "The command to run"},
		},
		Required: []string{"command"},
	}

	result := SchemaToClaudeFormat("bash", "Execute bash commands", schema)

	if result["name"] != "bash" {
		t.Fatalf("expected name %q, got %v", "bash", result["name"])
	}
	if result["description"] != "Execute bash commands" {
		t.Fatalf("expected description mismatch")
	}
	inputSchema, ok := result["input_schema"].(map[string]any)
	if !ok {
		t.Fatal("input_schema is not a map")
	}
	if inputSchema["type"] != "object" {
		t.Fatalf("expected input_schema.type %q, got %v", "object", inputSchema["type"])
	}

	// Verify it marshals to valid JSON
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if parsed["name"] != "bash" {
		t.Fatal("round-trip name mismatch")
	}
}

// T-7: Verifies R-5 — Schema JSON serialization
func TestSchemaJSONSerialization(t *testing.T) {
	t.Parallel()
	schema := Schema{
		Type: "object",
		Properties: map[string]Property{
			"path": {Type: "string", Description: "File path"},
			"mode": {Type: "string", Enum: []string{"read", "write"}},
		},
		Required: []string{"path"},
	}

	data, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed["type"] != "object" {
		t.Fatal("expected type object")
	}
	props, ok := parsed["properties"].(map[string]any)
	if !ok {
		t.Fatal("properties not a map")
	}
	if _, ok := props["path"]; !ok {
		t.Fatal("missing path property")
	}
}

// Verifies R-6 — Names returns names in registration order
func TestRegistryNames_Order(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	for _, name := range []string{"read", "edit", "bash"} {
		if err := reg.Register(newMock(name, "")); err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
	}

	names := reg.Names()
	expected := []string{"read", "edit", "bash"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}
	for i, n := range names {
		if n != expected[i] {
			t.Fatalf("names[%d]: expected %q, got %q", i, expected[i], n)
		}
	}
}

// NT-1: Negative test — List returns a copy, not internal slice
func TestRegistryList_ReturnsCopy(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	if err := reg.Register(newMock("bash", "")); err != nil {
		t.Fatalf("register: %v", err)
	}

	list1 := reg.List()
	list2 := reg.List()
	if len(list1) != 1 || len(list2) != 1 {
		t.Fatalf("expected 1 tool in each list")
	}
	// Modifying the returned slice should not affect the registry
	list1[0] = nil
	list3 := reg.List()
	if list3[0] == nil {
		t.Fatal("modifying returned slice affected internal state")
	}
}
