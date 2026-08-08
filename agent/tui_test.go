package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestTerminalChatUIPrintToolEventsDeepRedactsJournalSecrets(t *testing.T) {
	const (
		claimSentinel      = "CLAIM_SENTINEL_MUST_NOT_RENDER"
		leaseSentinel      = "LEASE_SENTINEL_MUST_NOT_RENDER"
		apiKeySentinel     = "API_KEY_SENTINEL_MUST_NOT_RENDER"
		accessSentinel     = "ACCESS_SENTINEL_MUST_NOT_RENDER"
		passwordSentinel   = "PASSWORD_SENTINEL_MUST_NOT_RENDER"
		plainLeaseSentinel = "PLAIN_LEASE_SENTINEL_MUST_NOT_RENDER"
	)

	var output bytes.Buffer
	ui := &TerminalChatUI{out: &output}
	ui.printToolEvents([]ChatToolEvent{
		{
			Name:      "project_patch_queue_review",
			Arguments: `{"visible":"argument-kept","nested":[{"claim_token":"` + claimSentinel + `"},{"OPENAI_API_KEY":"` + apiKeySentinel + `"}]}`,
			Output:    `{"visible":"output-kept","nested":{"lease_token":"` + leaseSentinel + `","access_token":"` + accessSentinel + `","workspace_password":"` + passwordSentinel + `"}}`,
		},
		{
			Name:    "plain_text_failure",
			Output:  `request failed: lease_token=` + plainLeaseSentinel + `; retry is safe`,
			IsError: true,
		},
	})

	rendered := output.String()
	for _, sentinel := range []string{
		claimSentinel,
		leaseSentinel,
		apiKeySentinel,
		accessSentinel,
		passwordSentinel,
		plainLeaseSentinel,
	} {
		if strings.Contains(rendered, sentinel) {
			t.Fatalf("TUI journal leaked sentinel %q:\n%s", sentinel, rendered)
		}
	}
	for _, visible := range []string{"argument-kept", "output-kept", "retry is safe"} {
		if !strings.Contains(rendered, visible) {
			t.Fatalf("TUI journal lost non-secret value %q:\n%s", visible, rendered)
		}
	}
	if count := strings.Count(rendered, tuiJournalRedacted); count < 6 {
		t.Fatalf("expected every sentinel field to be visibly redacted, got %d markers:\n%s", count, rendered)
	}
}
