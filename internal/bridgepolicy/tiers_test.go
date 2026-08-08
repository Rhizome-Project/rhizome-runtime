package bridgepolicy

import "testing"

func TestCatalogHasNoRemovedLegacyBridgeSurfaces(t *testing.T) {
	catalog := Catalog()

	for _, policy := range catalog {
		if policy.Surface == "antigravity/http-provider" ||
			policy.Surface == "antigravity/local-provider" ||
			policy.Surface == "antigravity/signed-session-shortcut" {
			t.Fatalf("removed legacy bridge surface still present in catalog: %+v", policy)
		}
	}
}

func TestCatalogDescribesCurrentBridgeSurfaces(t *testing.T) {
	catalog := Catalog()

	want := map[string]struct {
		tier    CapabilityTier
		posture SupportPosture
	}{
		"workspace-local-tooling": {tier: TierSafeLocal, posture: PostureSupported},
	}

	for surface, expected := range want {
		found := false
		for _, policy := range catalog {
			if policy.Surface == surface {
				found = true
				if policy.Tier != expected.tier {
					t.Fatalf("surface %q tier = %q, want %q", surface, policy.Tier, expected.tier)
				}
				if policy.Posture != expected.posture {
					t.Fatalf("surface %q posture = %q, want %q", surface, policy.Posture, expected.posture)
				}
				if len(policy.Notes) == 0 {
					t.Fatalf("surface %q should carry explanatory notes", surface)
				}
				break
			}
		}
		if !found {
			t.Fatalf("missing policy for surface %q", surface)
		}
	}
}

func TestNewPolicyTrimsBlankNotes(t *testing.T) {
	policy := NewPolicy("example", TierSafeLocal, "  first note  ", "", "\t", "second note")
	if len(policy.Notes) != 2 {
		t.Fatalf("expected 2 notes, got %d", len(policy.Notes))
	}
	if policy.Notes[0] != "first note" || policy.Notes[1] != "second note" {
		t.Fatalf("notes not normalized: %+v", policy.Notes)
	}
	if policy.Posture != PostureSupported {
		t.Fatalf("default posture should be supported, got %q", policy.Posture)
	}
}

func TestNewLegacyPolicyMarksUnsupported(t *testing.T) {
	policy := NewLegacyPolicy("legacy", TierHighRisk, "legacy path")
	if policy.Posture != PostureLegacyUnsupported {
		t.Fatalf("expected legacy posture, got %q", policy.Posture)
	}
	if policy.IsSupported() {
		t.Fatalf("legacy policy should not be marked supported")
	}
}
