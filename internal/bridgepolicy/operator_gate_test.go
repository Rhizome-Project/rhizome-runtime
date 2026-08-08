package bridgepolicy

import "testing"

func TestRequiresOperatorGateFallsBackToCapabilities(t *testing.T) {
	t.Parallel()

	if !RequiresOperatorGate(nil, []string{"bridge.high_risk", "bridge.operator_control_required"}) {
		t.Fatal("expected capability fallback to require operator gate")
	}
	if RequiresOperatorGate(nil, []string{"bridge.high_risk"}) {
		t.Fatal("expected incomplete capability tags to stay ungated")
	}
}

func TestApprovalRequestKeyIsStableAndSanitized(t *testing.T) {
	t.Parallel()

	got := ApprovalRequestKey("dangerous-provider", "Agent", "worker/1", "tool.call")
	want := "high_risk_bridge:dangerous_provider:agent:worker_1:tool_call"
	if got != want {
		t.Fatalf("ApprovalRequestKey() = %q, want %q", got, want)
	}
}
