package sqlite_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAttachToolCallPromptContextEnvelopeBindsRuntimePayload(t *testing.T) {
	t.Parallel()

	const (
		workspaceID = "ws-tool-call-prompt-context"
		toolID      = "tool-call-prompt-context"
		agentID     = "agent-tool-call-prompt-context"
		operationID = "toolcall-operation-prompt-context"
	)
	payload := map[string]any{
		"exit_code": 0,
		"timed_out": false,
	}
	fields := map[string]string{
		"workspace_id":          workspaceID,
		"tool_id":               toolID,
		"event_type":            "tool.call.executed",
		"entity_type":           "tool",
		"entity_id":             toolID,
		"actor_type":            "agent",
		"actor_id":              agentID,
		"requested_capability":  "tool.call",
		"operation_id":          operationID,
		"authority_event_scope": "tool.call",
	}
	out, err := sqlite.AttachToolCallPromptContextEnvelope(
		payload,
		sqlite.BuildToolCallPromptContextEnvelope("tool.call", "server_rpc", workspaceID, "agent", agentID),
		fields,
	)
	if err != nil {
		t.Fatalf("attach tool call prompt context: %v", err)
	}
	envelope := requireToolCallPromptContextEnvelope(t, out)
	assertToolCallPromptContextField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertToolCallPromptContextField(t, envelope, "context_kind", "authority_bearing_tool_call")
	assertToolCallPromptContextField(t, envelope, "surface", "tool.call")
	assertToolCallPromptContextField(t, envelope, "origin", "server_rpc")
	assertToolCallPromptContextField(t, envelope, "workspace_id", workspaceID)
	assertToolCallPromptContextField(t, envelope, "principal_type", "agent")
	assertToolCallPromptContextField(t, envelope, "principal_id", agentID)
	for key, want := range fields {
		assertToolCallPromptContextField(t, envelope, key, want)
	}
}

func TestAttachToolCallPromptContextEnvelopeRejectsForgedBinding(t *testing.T) {
	t.Parallel()

	const (
		workspaceID = "ws-tool-call-prompt-context-forged"
		toolID      = "tool-call-prompt-context-forged"
		agentID     = "agent-tool-call-prompt-context-forged"
	)
	fields := map[string]string{
		"workspace_id":          workspaceID,
		"tool_id":               toolID,
		"event_type":            "tool.call.executed",
		"entity_type":           "tool",
		"entity_id":             toolID,
		"actor_type":            "agent",
		"actor_id":              agentID,
		"requested_capability":  "tool.call",
		"authority_event_scope": "tool.call",
	}
	tests := []struct {
		name    string
		payload map[string]any
		envelop map[string]any
	}{
		{
			name:    "wrong surface",
			payload: map[string]any{},
			envelop: sqlite.BuildToolCallPromptContextEnvelope("tool.deploy", "server_rpc", workspaceID, "agent", agentID),
		},
		{
			name:    "wrong workspace",
			payload: map[string]any{},
			envelop: sqlite.BuildToolCallPromptContextEnvelope("tool.call", "server_rpc", workspaceID+"-other", "agent", agentID),
		},
		{
			name:    "wrong actor",
			payload: map[string]any{},
			envelop: func() map[string]any {
				envelope := sqlite.BuildToolCallPromptContextEnvelope("tool.call", "server_rpc", workspaceID, "agent", agentID)
				envelope["actor_id"] = "agent-other"
				return envelope
			}(),
		},
		{
			name: "nested wrong tool",
			payload: map[string]any{
				"nested": map[string]any{
					"prompt_context_envelope": func() map[string]any {
						envelope := sqlite.BuildToolCallPromptContextEnvelope("tool.call", "server_rpc", workspaceID, "agent", agentID)
						envelope["tool_id"] = "tool-other"
						return envelope
					}(),
				},
			},
			envelop: sqlite.BuildToolCallPromptContextEnvelope("tool.call", "server_rpc", workspaceID, "agent", agentID),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := sqlite.AttachToolCallPromptContextEnvelope(tt.payload, tt.envelop, fields); err == nil {
				t.Fatal("expected forged tool call prompt context to fail")
			}
		})
	}
}

func requireToolCallPromptContextEnvelope(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	raw, ok := payload["prompt_context_envelope"]
	if !ok {
		t.Fatalf("missing prompt_context_envelope in %+v", payload)
	}
	envelope, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("prompt_context_envelope has type %T, want map[string]any", raw)
	}
	return envelope
}

func assertToolCallPromptContextField(t *testing.T, envelope map[string]any, key, want string) {
	t.Helper()
	got, ok := envelope[key].(string)
	if !ok {
		t.Fatalf("prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
	}
	if got != want {
		t.Fatalf("prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
	}
}

func toolCallRuntimeEventPayloadJSONForTest(t *testing.T, workspaceID, toolID, eventType, actorType, actorID, requestedCapability, operationID string, payload map[string]any) string {
	t.Helper()
	if payload == nil {
		payload = map[string]any{}
	}
	fields := map[string]string{
		"workspace_id":          workspaceID,
		"tool_id":               toolID,
		"event_type":            eventType,
		"entity_type":           "tool",
		"entity_id":             toolID,
		"actor_type":            actorType,
		"actor_id":              actorID,
		"requested_capability":  requestedCapability,
		"authority_event_scope": "tool.call",
	}
	if strings.TrimSpace(operationID) != "" {
		fields["operation_id"] = operationID
	}
	for key, value := range fields {
		if strings.TrimSpace(value) != "" {
			payload[key] = value
		}
	}
	principalType := strings.TrimSpace(actorType)
	principalID := strings.TrimSpace(actorID)
	if principalType == "" {
		principalType = "system"
	}
	if principalID == "" {
		principalID = "tests"
	}
	out, err := sqlite.AttachToolCallPromptContextEnvelope(
		payload,
		sqlite.BuildToolCallPromptContextEnvelope("tool.call", "server_rpc", workspaceID, principalType, principalID),
		fields,
	)
	if err != nil {
		t.Fatalf("attach tool call prompt context: %v", err)
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal tool call runtime event payload: %v", err)
	}
	return string(encoded)
}
