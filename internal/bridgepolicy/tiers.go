package bridgepolicy

// CapabilityTier names the trust/authority tier for a bridge or provider-adapter surface.
type CapabilityTier string

// SupportPosture describes whether a surface remains part of the supported architecture.
type SupportPosture string

const (
	// TierSafeLocal is reserved for workspace-bounded local helpers that do not leave the process boundary.
	TierSafeLocal CapabilityTier = "safe_local"

	// TierNetworked is for explicit remote API bridges that use network transport but do not execute local code.
	TierNetworked CapabilityTier = "networked"

	// TierCodeExec is for local provider-adapter surfaces that can invoke local code or binaries under confinement.
	TierCodeExec CapabilityTier = "code_exec"

	// TierHighRisk is for legacy shortcuts that combine transport, local execution, or implicit authority escalation.
	TierHighRisk CapabilityTier = "high_risk"
)

const (
	// PostureSupported is reserved for surfaces that remain in the supported architecture.
	PostureSupported SupportPosture = "supported"

	// PostureLegacyUnsupported marks surfaces that are still present for transition/audit purposes
	// but are no longer considered part of the supported architecture.
	PostureLegacyUnsupported SupportPosture = "legacy_unsupported"
)

// SurfacePolicy records the current policy classification for one bridge/provider-adapter surface.
type SurfacePolicy struct {
	Surface string         `json:"surface"`
	Tier    CapabilityTier `json:"tier"`
	Posture SupportPosture `json:"posture"`
	Notes   []string       `json:"notes,omitempty"`
}

// NewPolicy constructs a policy entry with normalized note trimming.
func NewPolicy(surface string, tier CapabilityTier, notes ...string) SurfacePolicy {
	return NewPolicyWithPosture(surface, tier, PostureSupported, notes...)
}

// NewPolicyWithPosture constructs a policy entry with an explicit support posture.
func NewPolicyWithPosture(surface string, tier CapabilityTier, posture SupportPosture, notes ...string) SurfacePolicy {
	out := SurfacePolicy{
		Surface: surface,
		Tier:    tier,
		Posture: posture,
	}
	if len(notes) > 0 {
		out.Notes = make([]string, 0, len(notes))
		for _, note := range notes {
			if trimmed := trimSpace(note); trimmed != "" {
				out.Notes = append(out.Notes, trimmed)
			}
		}
	}
	return out
}

// NewLegacyPolicy constructs a legacy/unsupported policy entry.
func NewLegacyPolicy(surface string, tier CapabilityTier, notes ...string) SurfacePolicy {
	return NewPolicyWithPosture(surface, tier, PostureLegacyUnsupported, notes...)
}

// IsSupported reports whether the policy remains part of the supported architecture.
func (p SurfacePolicy) IsSupported() bool {
	return p.Posture == "" || p.Posture == PostureSupported
}

// Catalog returns the current policy catalog for the remaining bridge/provider-adapter surfaces.
// The catalog is descriptive only; enforcement is intentionally deferred to later phases.
func Catalog() []SurfacePolicy {
	return []SurfacePolicy{
		NewPolicy(
			"workspace-local-tooling",
			TierSafeLocal,
			"reserved reference tier for workspace-bounded local helpers",
			"no network transport and no arbitrary code execution",
		),
	}
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}
