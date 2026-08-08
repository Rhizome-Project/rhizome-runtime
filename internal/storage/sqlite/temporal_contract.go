package sqlite

import "strings"

const (
	temporalContractSchemaVersion = "1.0"

	temporalBasisWallClock    = "wall_clock"
	temporalBasisControlEpoch = "control_epoch"

	temporalMappingExactWallClock = "exact_wall_clock"
	temporalMappingExplicitPhi    = "explicit_phi_required"

	temporalStateLive    = "LIVE"
	temporalStateDue     = "DUE"
	temporalStateExpired = "EXPIRED"
	temporalStatePending = "PENDING"
	temporalStateUnknown = "UNKNOWN"
)

type TemporalHorizonContract struct {
	SchemaVersion       string   `json:"schema_version"`
	Domain              string   `json:"domain"`
	HorizonKind         string   `json:"horizon_kind"`
	Basis               string   `json:"basis"`
	Mapping             string   `json:"mapping"`
	WallClockComparable bool     `json:"wall_clock_comparable"`
	State               string   `json:"state"`
	ReferenceAt         string   `json:"reference_at,omitempty"`
	AnchorAt            string   `json:"anchor_at,omitempty"`
	TargetAt            string   `json:"target_at,omitempty"`
	CurrentEpoch        int      `json:"current_epoch,omitempty"`
	TargetEpoch         int      `json:"target_epoch,omitempty"`
	EpochAnchorAt       string   `json:"epoch_anchor_at,omitempty"`
	ScopeSource         string   `json:"scope_source,omitempty"`
	Notes               []string `json:"notes,omitempty"`
}

func newTemporalHorizonContract(domain, horizonKind, basis, mapping string) TemporalHorizonContract {
	return TemporalHorizonContract{
		SchemaVersion:       temporalContractSchemaVersion,
		Domain:              strings.TrimSpace(domain),
		HorizonKind:         strings.TrimSpace(horizonKind),
		Basis:               strings.TrimSpace(basis),
		Mapping:             strings.TrimSpace(mapping),
		WallClockComparable: basis == temporalBasisWallClock,
		State:               temporalStateUnknown,
	}
}

func temporalWallClockState(targetAt, referenceAt, elapsedState string) string {
	targetAt = strings.TrimSpace(targetAt)
	referenceAt = strings.TrimSpace(referenceAt)
	if targetAt == "" || referenceAt == "" {
		return temporalStateUnknown
	}
	targetTS, targetOK := controlParseTimestamp(targetAt)
	referenceTS, referenceOK := controlParseTimestamp(referenceAt)
	if !targetOK || !referenceOK {
		return temporalStateUnknown
	}
	if targetTS.After(referenceTS) {
		return temporalStateLive
	}
	switch strings.TrimSpace(elapsedState) {
	case temporalStateExpired:
		return temporalStateExpired
	case temporalStatePending:
		return temporalStatePending
	default:
		return temporalStateDue
	}
}

func memoryGraphRetentionTemporalContracts(record MemoryGraphNodeRecord, referenceAt string) []TemporalHorizonContract {
	contracts := make([]TemporalHorizonContract, 0, 3)
	anchorAt := strings.TrimSpace(firstNonEmpty(derefString(record.LastTrustedAccess), derefString(record.LastAnyAccess)))
	if anchorAt == "" {
		anchorAt = strings.TrimSpace(record.UpdatedAt)
	}
	appendContract := func(horizonKind, targetAt, elapsedState string) {
		targetAt = strings.TrimSpace(targetAt)
		if targetAt == "" {
			return
		}
		contract := newTemporalHorizonContract("retention", horizonKind, temporalBasisWallClock, temporalMappingExactWallClock)
		contract.ReferenceAt = strings.TrimSpace(referenceAt)
		contract.AnchorAt = anchorAt
		contract.TargetAt = targetAt
		contract.State = temporalWallClockState(targetAt, referenceAt, elapsedState)
		contract.Notes = []string{
			"retention thresholds live on shared wall-clock time",
		}
		contracts = append(contracts, contract)
	}
	appendContract("retention_hot_until", derefString(record.RetentionHotUntil), temporalStateDue)
	appendContract("retention_warm_until", derefString(record.RetentionWarmUntil), temporalStateDue)
	appendContract("retention_expires_at", derefString(record.RetentionExpiresAt), temporalStateExpired)
	if len(contracts) == 0 {
		return nil
	}
	return contracts
}

func applyEffectiveControlsTemporalContract(record *EffectiveControlsRecord, referenceAt, scopeSource string) {
	if record == nil {
		return
	}
	contract := newTemporalHorizonContract("effective_controls", "ttl_window", temporalBasisWallClock, temporalMappingExactWallClock)
	contract.ReferenceAt = strings.TrimSpace(referenceAt)
	contract.AnchorAt = strings.TrimSpace(record.GeneratedAt)
	contract.TargetAt = strings.TrimSpace(record.ExpiresAt)
	contract.ScopeSource = strings.TrimSpace(scopeSource)
	switch {
	case record.Pending:
		contract.State = temporalStatePending
	case record.Expired:
		contract.State = temporalStateExpired
	default:
		contract.State = temporalWallClockState(record.ExpiresAt, referenceAt, temporalStateExpired)
	}
	notes := []string{
		"effective controls stay on wall-clock ttl semantics",
	}
	if strings.TrimSpace(scopeSource) == "workspace_fallback" {
		notes = append(notes, "scope resolution fell back from proto-cluster to workspace controls")
	}
	contract.Notes = notes
	record.TemporalContract = &contract
}

func applyUnifiedEffectiveControlsTemporalContract(audit *UnifiedControlEffectiveControlsAudit, referenceAt string) {
	if audit == nil {
		return
	}
	contract := newTemporalHorizonContract("effective_controls", "ttl_window", temporalBasisWallClock, temporalMappingExactWallClock)
	contract.ReferenceAt = strings.TrimSpace(referenceAt)
	contract.AnchorAt = strings.TrimSpace(audit.GeneratedAt)
	contract.TargetAt = strings.TrimSpace(audit.ExpiresAt)
	contract.ScopeSource = strings.TrimSpace(audit.ScopeSource)
	switch {
	case audit.Pending:
		contract.State = temporalStatePending
	case audit.Expired:
		contract.State = temporalStateExpired
	default:
		contract.State = temporalWallClockState(audit.ExpiresAt, referenceAt, temporalStateExpired)
	}
	notes := []string{
		"effective controls stay on wall-clock ttl semantics",
	}
	if strings.TrimSpace(audit.ScopeSource) == "workspace_fallback" {
		notes = append(notes, "scope resolution fell back from proto-cluster to workspace controls")
	}
	contract.Notes = notes
	audit.TemporalContract = &contract
}

func applyMemoryInvalidationTemporalContracts(record *MemoryInvalidationRecord) {
	if record == nil {
		return
	}
	referenceAt := strings.TrimSpace(record.TimeAuthority.ReferenceAt)
	contracts := make([]TemporalHorizonContract, 0, 2)
	if leaseAt := strings.TrimSpace(record.LeaseExpiresAt); leaseAt != "" {
		contract := newTemporalHorizonContract("memory_invalidation", "lease_expiry", temporalBasisWallClock, temporalMappingExactWallClock)
		contract.ReferenceAt = referenceAt
		contract.AnchorAt = strings.TrimSpace(firstNonEmpty(record.DeliveredAt, record.LastDeliveryAttemptAt, record.CreatedAt))
		contract.TargetAt = leaseAt
		contract.State = temporalWallClockState(leaseAt, referenceAt, temporalStateExpired)
		contract.Notes = []string{
			"invalidation lease validity stays on shared wall-clock time",
		}
		contracts = append(contracts, contract)
	}
	if nextAt := strings.TrimSpace(record.NextDeliveryAt); nextAt != "" {
		contract := newTemporalHorizonContract("memory_invalidation", "next_delivery_at", temporalBasisWallClock, temporalMappingExactWallClock)
		contract.ReferenceAt = referenceAt
		contract.AnchorAt = strings.TrimSpace(firstNonEmpty(record.LastFailureAt, record.UpdatedAt, record.CreatedAt))
		contract.TargetAt = nextAt
		contract.State = temporalWallClockState(nextAt, referenceAt, temporalStateDue)
		if contract.State == temporalStateLive {
			contract.State = temporalStatePending
		}
		contract.Notes = []string{
			"retry scheduling stays on shared wall-clock time",
		}
		contracts = append(contracts, contract)
	}
	if len(contracts) == 0 {
		record.TemporalContracts = nil
		return
	}
	record.TemporalContracts = contracts
}

func rspForecastTemporalContract(authority WorkspaceTimeAuthority, horizonEpochs int) *TemporalHorizonContract {
	contract := newTemporalHorizonContract("forecast", "projection_horizon", temporalBasisControlEpoch, temporalMappingExplicitPhi)
	contract.ReferenceAt = strings.TrimSpace(authority.ReferenceAt)
	contract.CurrentEpoch = authority.CurrentEpoch
	contract.TargetEpoch = authority.CurrentEpoch + maxInt(horizonEpochs, 0)
	contract.EpochAnchorAt = strings.TrimSpace(firstNonEmpty(authority.EpochAnchorAt, authority.RuntimeEventAnchorAt))
	contract.State = temporalStatePending
	contract.Notes = []string{
		"forecast horizons stay epoch-relative until explicit k->t_k mapping is applied",
		"wall-clock comparison is unsafe without explicit phi mapping",
	}
	return &contract
}

func workspaceTimeAuthorityTemporalContract(authority WorkspaceTimeAuthority) *TemporalHorizonContract {
	contract := newTemporalHorizonContract("control_epoch", "current_epoch", temporalBasisControlEpoch, temporalMappingExplicitPhi)
	contract.ReferenceAt = strings.TrimSpace(authority.ReferenceAt)
	contract.CurrentEpoch = authority.CurrentEpoch
	contract.TargetEpoch = authority.CurrentEpoch
	contract.EpochAnchorAt = strings.TrimSpace(firstNonEmpty(authority.EpochAnchorAt, authority.RuntimeEventAnchorAt))
	contract.State = temporalStateLive
	contract.Notes = []string{
		"workspace time authority anchors the current control epoch",
		"wall-clock comparison is unsafe without explicit phi mapping",
	}
	return &contract
}
