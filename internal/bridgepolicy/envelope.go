package bridgepolicy

import "encoding/json"

// PolicyEnvelope is a machine-readable summary of the bridge/provider-adapter
// posture that can be attached to tool manifests and audit events.
type PolicyEnvelope struct {
	Surface                 string          `json:"surface"`
	PrimaryTier             CapabilityTier  `json:"primary_tier"`
	HighestTier             CapabilityTier  `json:"highest_tier"`
	SupportPosture          SupportPosture  `json:"support_posture"`
	SupportedArchitecture   bool            `json:"supported_architecture"`
	HighRisk                bool            `json:"high_risk,omitempty"`
	OperatorControlRequired bool            `json:"operator_control_required,omitempty"`
	AuditVisibilityRequired bool            `json:"audit_visibility_required,omitempty"`
	Policies                []SurfacePolicy `json:"policies,omitempty"`
	Notes                   []string        `json:"notes,omitempty"`
}

type manifestEnvelopeCarrier struct {
	PolicyEnvelope *PolicyEnvelope `json:"policy_envelope,omitempty"`
}

// BuildEnvelope derives a policy envelope from the active surface policies for one bridge surface.
func BuildEnvelope(surface string, primaryTier CapabilityTier, posture SupportPosture, policies []SurfacePolicy, notes ...string) PolicyEnvelope {
	if posture == "" {
		posture = PostureSupported
	}
	envelope := PolicyEnvelope{
		Surface:               trimSpace(surface),
		PrimaryTier:           primaryTier,
		HighestTier:           highestCapabilityTier(policies, primaryTier),
		SupportPosture:        posture,
		SupportedArchitecture: posture == PostureSupported,
	}
	if len(policies) > 0 {
		envelope.Policies = make([]SurfacePolicy, len(policies))
		copy(envelope.Policies, policies)
	}
	for _, policy := range policies {
		if policy.Tier == TierHighRisk {
			envelope.HighRisk = true
			envelope.OperatorControlRequired = true
			envelope.AuditVisibilityRequired = true
			break
		}
	}
	if len(notes) > 0 {
		envelope.Notes = make([]string, 0, len(notes))
		for _, note := range notes {
			if trimmed := trimSpace(note); trimmed != "" {
				envelope.Notes = append(envelope.Notes, trimmed)
			}
		}
	}
	return envelope
}

// ParsePolicyEnvelopeFromManifest extracts a policy envelope from tool manifest JSON.
func ParsePolicyEnvelopeFromManifest(raw string) *PolicyEnvelope {
	raw = trimSpace(raw)
	if raw == "" || raw == "{}" {
		return nil
	}
	var carrier manifestEnvelopeCarrier
	if err := json.Unmarshal([]byte(raw), &carrier); err != nil || carrier.PolicyEnvelope == nil {
		return nil
	}
	out := *carrier.PolicyEnvelope
	if out.Surface == "" {
		return nil
	}
	return &out
}

func highestCapabilityTier(policies []SurfacePolicy, fallback CapabilityTier) CapabilityTier {
	best := fallback
	bestWeight := capabilityTierWeight(best)
	for _, policy := range policies {
		if weight := capabilityTierWeight(policy.Tier); weight > bestWeight {
			best = policy.Tier
			bestWeight = weight
		}
	}
	return best
}

func capabilityTierWeight(t CapabilityTier) int {
	switch t {
	case TierSafeLocal:
		return 1
	case TierNetworked:
		return 2
	case TierCodeExec:
		return 3
	case TierHighRisk:
		return 4
	default:
		return 0
	}
}
