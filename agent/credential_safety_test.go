package main

import (
	"strings"
	"testing"
)

func TestCredentialReadSurfacesAreRedacted(t *testing.T) {
	const secret = "workspace-password-must-not-appear"
	if got := configuredSecretLabel(secret); strings.Contains(got, secret) || got != "[set]" {
		t.Fatalf("configuredSecretLabel() = %q, want [set] without the secret", got)
	}
	redacted := redactBotManagerDefaults(BotManagerDefaults{WorkspacePassword: secret})
	if redacted.WorkspacePassword != "" {
		t.Fatal("redactBotManagerDefaults retained the workspace password")
	}
}

func TestManagedAgentArgumentsNeverContainCredentials(t *testing.T) {
	const secret = "managed-process-secret"
	args := appendManagedAgentRuntimeConfigArgs(nil, RuntimeConfig{
		WorkspacePassword: secret,
		RhizomeToken:      secret,
		OpenAIKey:         secret,
		WorkspaceID:       "workspace",
		AgentID:           "agent",
	})
	if strings.Contains(strings.Join(args, "\x00"), secret) {
		t.Fatalf("managed process arguments exposed a credential: %v", args)
	}
}
