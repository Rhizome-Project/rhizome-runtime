package bridgepolicy

import (
	"encoding/json"
	"testing"
)

func TestBuildEnvelopeMarksHighRiskOperatorControl(t *testing.T) {
	envelope := BuildEnvelope(
		"external-code-provider",
		TierCodeExec,
		PostureLegacyUnsupported,
		[]SurfacePolicy{
			NewLegacyPolicy("external-code-provider/local-provider", TierCodeExec, "local CLI"),
			NewLegacyPolicy("external-code-provider/signed-session-shortcut", TierHighRisk, "signed-session shortcut"),
		},
		"legacy bridge",
	)

	if envelope.PrimaryTier != TierCodeExec {
		t.Fatalf("expected primary tier code_exec, got %q", envelope.PrimaryTier)
	}
	if envelope.HighestTier != TierHighRisk {
		t.Fatalf("expected highest tier high_risk, got %q", envelope.HighestTier)
	}
	if !envelope.HighRisk || !envelope.OperatorControlRequired || !envelope.AuditVisibilityRequired {
		t.Fatalf("expected high-risk envelope to require operator control and audit visibility, got %+v", envelope)
	}
	if envelope.SupportedArchitecture {
		t.Fatalf("legacy envelope should not be marked supported")
	}
}

func TestParsePolicyEnvelopeFromManifest(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"provider": "external-provider",
		"policy_envelope": BuildEnvelope(
			"external-provider",
			TierNetworked,
			PostureLegacyUnsupported,
			[]SurfacePolicy{
				NewLegacyPolicy("external-provider/http-provider", TierNetworked, "remote transport"),
			},
		),
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	envelope := ParsePolicyEnvelopeFromManifest(string(raw))
	if envelope == nil {
		t.Fatal("expected policy envelope")
	}
	if envelope.Surface != "external-provider" || envelope.PrimaryTier != TierNetworked {
		t.Fatalf("unexpected parsed envelope %+v", envelope)
	}
}
