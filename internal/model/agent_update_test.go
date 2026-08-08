package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseAndNormalizeAgentUpdatePayloadV1PreservesSideEffects(t *testing.T) {
	raw := `{
		"side_effects": [
			{
				"schema": " artifact_bound_side_effect.v1 ",
				"side_effect_ref": " side-effect:1 ",
				"actor": " agent-alpha ",
				"lane_ref": " abpc:lane:v1;workspace=w;tension=t;artifact=a;regions=r;owner=o ",
				"tension_ref": " tension:clearpress ",
				"artifact_ref": " artifact:repo ",
				"region_ref": " region:editor ",
				"operation": " change ",
				"source_kind": " adapter:git ",
				"boundary_ref": " boundary:editor ",
				"boundary_relation": " out_of_boundary ",
				"materialization_state": " present_unintegrated ",
				"integration_intent": " foundation_effect ",
				"integration_status": " pending_classification ",
				"decision": " none_pending ",
				"justification": " created adjacent foundation files ",
				"derived_region_refs": [" path:package.json ", "path:package.json", " path:src/App.tsx "]
			}
		]
	}`

	normalized, err := ParseAndNormalizeAgentUpdatePayloadV1(raw)
	if err != nil {
		t.Fatalf("ParseAndNormalizeAgentUpdatePayloadV1 returned error: %v", err)
	}

	var payload AgentUpdatePayloadV1
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		t.Fatalf("decode normalized payload: %v", err)
	}
	if len(payload.SideEffects) != 1 {
		t.Fatalf("expected one side effect, got %d", len(payload.SideEffects))
	}
	effect := payload.SideEffects[0]
	if effect.Schema != "artifact_bound_side_effect.v1" {
		t.Fatalf("schema was not normalized, got %q", effect.Schema)
	}
	if effect.SourceKind != "adapter:git" {
		t.Fatalf("source_kind was not normalized, got %q", effect.SourceKind)
	}
	if got, want := effect.DerivedRegionRefs, []string{"path:package.json", "path:src/App.tsx"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("derived refs = %#v, want %#v", got, want)
	}

	effects, err := ParseAgentUpdateSideEffectsV1(normalized)
	if err != nil {
		t.Fatalf("ParseAgentUpdateSideEffectsV1 returned error: %v", err)
	}
	if len(effects) != 1 || effects[0].SideEffectRef != "side-effect:1" {
		t.Fatalf("unexpected side effect reader result: %#v", effects)
	}
}

func TestParseAndNormalizeAgentUpdatePayloadV1RejectsInvalidSideEffectEnum(t *testing.T) {
	raw := validSideEffectPayloadForTest()
	raw = strings.Replace(raw, `"boundary_relation":"out_of_boundary"`, `"boundary_relation":"generated_or_whatever"`, 1)

	_, err := ParseAndNormalizeAgentUpdatePayloadV1(raw)
	if err == nil {
		t.Fatal("expected invalid boundary_relation to be rejected")
	}
	if !strings.Contains(err.Error(), "side_effects.boundary_relation is invalid") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseAndNormalizeAgentUpdatePayloadV1RejectsPartialSideEffectInsteadOfDropping(t *testing.T) {
	raw := `{"status":"blocked","side_effects":[{"actor":"agent-alpha","operation":"change"}]}`

	_, err := ParseAndNormalizeAgentUpdatePayloadV1(raw)
	if err == nil {
		t.Fatal("expected partial side effect to be rejected")
	}
	if !strings.Contains(err.Error(), "side_effects.schema must be artifact_bound_side_effect.v1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseAgentUpdateSideEffectsV1IgnoresLegacyNonJSONPayloads(t *testing.T) {
	effects, err := ParseAgentUpdateSideEffectsV1("plain update text for task-123")
	if err != nil {
		t.Fatalf("legacy non-json payload should not fail side-effect extraction: %v", err)
	}
	if len(effects) != 0 {
		t.Fatalf("expected no side effects from legacy non-json payload, got %+v", effects)
	}
}

func TestParseAndNormalizeAgentUpdatePayloadV1RejectsUnnamespacedSideEffectSourceKind(t *testing.T) {
	for _, sourceKind := range []string{"git", "adapter:", "adapter:git dirty"} {
		t.Run(sourceKind, func(t *testing.T) {
			raw := validSideEffectPayloadForTest()
			raw = strings.Replace(raw, `"source_kind":"adapter:git"`, `"source_kind":"`+sourceKind+`"`, 1)

			_, err := ParseAndNormalizeAgentUpdatePayloadV1(raw)
			if err == nil {
				t.Fatal("expected invalid source_kind to be rejected")
			}
			if !strings.Contains(err.Error(), "side_effects.source_kind must start with adapter: or core:") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func validSideEffectPayloadForTest() string {
	return `{"side_effects":[{"schema":"artifact_bound_side_effect.v1","side_effect_ref":"side-effect:1","actor":"agent-alpha","lane_ref":"lane:1","tension_ref":"tension:1","artifact_ref":"artifact:1","region_ref":"region:1","operation":"change","source_kind":"adapter:git","boundary_ref":"boundary:1","boundary_relation":"out_of_boundary","materialization_state":"present_unintegrated","integration_intent":"foundation_effect","integration_status":"pending_classification","decision":"none_pending","justification":"classification required","derived_region_refs":["region:1"]}]}`
}

// --- TE-51 protocol guard: fast-path-emitted transitions stay byte-compatible ---
//
// The deterministic side-effect fast paths in agent record their decision
// through side_effect_resolve, whose decision-effect emitter
// (sideEffectResolveDecisionEffects) produces artifact_bound_side_effect.v1
// entries with operation=classify, source_kind=core:side_effect_resolution, and a
// (boundary_relation, integration_status, decision) triple derived from the
// decision. This guard pins that EVERY value triple the fast paths and the
// resolve tool can emit still validates through the model's schema validators,
// so a fast-path resolution update round-trips like the LLM-path resolution
// update. If a new decision is added without a matching validator enum value,
// this table fails closed here rather than at runtime.

// fastPathResolutionSideEffectPayload mirrors the JSON shape the agent
// side_effect_resolve decision-effect emitter writes into a
// side_effect_resolution update payload for a single classified region.
func fastPathResolutionSideEffectPayload(decision, integrationStatus, boundaryRelation string) string {
	return `{"schema":"artifact_bound_side_effect_resolution.v1","status":"resolved","decision":"` + decision +
		`","integration_status":"` + integrationStatus +
		`","side_effects":[{"schema":"artifact_bound_side_effect.v1","side_effect_ref":"side-effect:ws:project:branch:src-ui-widget-tsx","actor":"agent-beta","lane_ref":"lane:ws:project:task-ui","tension_ref":"task-ui","artifact_ref":"artifact:project:project:branch:branch-beta","region_ref":"region:adapter:git:path:src/ui/widget.tsx","operation":"classify","source_kind":"core:side_effect_resolution","boundary_ref":"boundary:side_effect_resolution:branch-beta","boundary_relation":"` + boundaryRelation +
		`","materialization_state":"present_unintegrated","integration_intent":"repair","integration_status":"` + integrationStatus +
		`","decision":"` + decision +
		`","justification":"deterministic side-effect fast path","derived_region_refs":["region:adapter:git:path:src/ui/widget.tsx"]}]}`
}

func TestAgentUpdateFastPathResolutionSideEffectsStayByteCompatible(t *testing.T) {
	// (decision, integration_status, boundary_relation) triples exactly as the
	// agent resolve tool derives them (sideEffectResolveIntegrationStatus /
	// sideEffectResolveBoundaryRelation), including the fast-path reroute decision.
	cases := []struct {
		decision          string
		integrationStatus string
		boundaryRelation  string
	}{
		{"reroute_to_active_lane", "verification_requested", "ambiguous"},
		{"accept", "accepted", "in_boundary"},
		{"expand_boundary", "boundary_expansion_requested", "in_boundary"},
		{"split_tension", "split_requested", "adjacent_foundation_candidate"},
		{"reassign", "split_requested", "out_of_boundary"},
		{"request_verification", "verification_requested", "ambiguous"},
		{"quarantine", "quarantined", "out_of_boundary"},
		{"revert", "rejected", "out_of_boundary"},
	}
	for _, tc := range cases {
		t.Run(tc.decision, func(t *testing.T) {
			raw := fastPathResolutionSideEffectPayload(tc.decision, tc.integrationStatus, tc.boundaryRelation)

			effects, err := ParseAgentUpdateSideEffectsV1(raw)
			if err != nil {
				t.Fatalf("fast-path %s resolution side effect must validate, got error: %v", tc.decision, err)
			}
			if len(effects) != 1 {
				t.Fatalf("expected exactly one fast-path resolution side effect, got %d", len(effects))
			}
			effect := effects[0]
			if effect.Operation != "classify" {
				t.Fatalf("fast-path resolution must record operation=classify, got %q", effect.Operation)
			}
			if effect.SourceKind != "core:side_effect_resolution" {
				t.Fatalf("fast-path resolution must record source_kind=core:side_effect_resolution, got %q", effect.SourceKind)
			}
			if effect.Decision != tc.decision || effect.IntegrationStatus != tc.integrationStatus || effect.BoundaryRelation != tc.boundaryRelation {
				t.Fatalf("decision triple drifted: got (%q,%q,%q) want (%q,%q,%q)", effect.Decision, effect.IntegrationStatus, effect.BoundaryRelation, tc.decision, tc.integrationStatus, tc.boundaryRelation)
			}

			// The same payload must also normalize/validate through the full payload
			// validator, proving the fast-path update is byte-compatible end to end.
			if _, err := ParseAndNormalizeAgentUpdatePayloadV1(raw); err != nil {
				t.Fatalf("fast-path %s resolution payload must normalize cleanly, got error: %v", tc.decision, err)
			}
		})
	}
}

// TestAgentUpdateFastPathMalformedResolutionSideEffectsFailClosed proves the
// fast-path-shaped side effects still hard-reject when a required ref is empty or
// an enum is corrupted -- the deterministic emitter cannot smuggle an unvalidated
// transition through the schema by routing through the resolve update path.
func TestAgentUpdateFastPathMalformedResolutionSideEffectsFailClosed(t *testing.T) {
	base := fastPathResolutionSideEffectPayload("reroute_to_active_lane", "verification_requested", "ambiguous")

	cases := []struct {
		name    string
		mutate  func(string) string
		wantErr string
	}{
		{
			name: "empty_side_effect_ref",
			mutate: func(s string) string {
				return strings.Replace(s, `"side_effect_ref":"side-effect:ws:project:branch:src-ui-widget-tsx"`, `"side_effect_ref":""`, 1)
			},
			wantErr: "side_effects.side_effect_ref is required",
		},
		{
			name: "missing_derived_region_refs",
			mutate: func(s string) string {
				return strings.Replace(s, `"derived_region_refs":["region:adapter:git:path:src/ui/widget.tsx"]`, `"derived_region_refs":[]`, 1)
			},
			wantErr: "side_effects.derived_region_refs requires at least one ref",
		},
		{
			name: "invalid_decision_enum",
			mutate: func(s string) string {
				return strings.Replace(s, `"decision":"reroute_to_active_lane","justification"`, `"decision":"resolve_however","justification"`, 1)
			},
			wantErr: "side_effects.decision is invalid",
		},
		{
			name: "invalid_integration_status_enum",
			mutate: func(s string) string {
				return strings.Replace(s, `"integration_status":"verification_requested","decision"`, `"integration_status":"totally_resolved","decision"`, 1)
			},
			wantErr: "side_effects.integration_status is invalid",
		},
		{
			name: "unnamespaced_source_kind",
			mutate: func(s string) string {
				return strings.Replace(s, `"source_kind":"core:side_effect_resolution"`, `"source_kind":"side_effect_resolution"`, 1)
			},
			wantErr: "side_effects.source_kind must start with adapter: or core:",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.mutate(base)
			if raw == base {
				t.Fatalf("mutation %q did not change the payload", tc.name)
			}

			if _, err := ParseAgentUpdateSideEffectsV1(raw); err == nil {
				t.Fatalf("expected reader to reject %s", tc.name)
			} else if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("reader error %q does not contain %q", err.Error(), tc.wantErr)
			}
			if _, err := ParseAndNormalizeAgentUpdatePayloadV1(raw); err == nil {
				t.Fatalf("expected full payload validator to reject %s", tc.name)
			} else if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validator error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}
