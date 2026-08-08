package sqlite

import (
	"context"
	"log"
	"time"
)

// RSPLocalActuator handles Phase S2 local autonomics.
// It explicitly avoids modifying global RRP state (no Tensions, no routing).
// It restricts itself to local agent reflex commands.
type RSPLocalActuator struct {
	store *Store
}

type rspLocalGateResult struct {
	EffectiveThreshold float64
	GateOpen           bool
}

func NewRSPLocalActuator(store *Store) *RSPLocalActuator {
	return &RSPLocalActuator{store: store}
}

// Gate_local assesses whether an anomaly is persistent enough to warrant local action.
// severity: 0.0-1.0 (e.g., thrashing_risk)
// persistence: number of consecutive epochs the anomaly has been raised
// uncertainty: 0.0-1.0 (e.g., lack of data or evidence_mass). Higher uncertainty lowers action confidence.
func (a *RSPLocalActuator) Gate_local(severity, persistence, uncertainty float64) bool {
	return rspLocalActuationGate(severity, persistence, uncertainty).GateOpen
}

func rspLocalActuationGate(severity, persistence, uncertainty float64) rspLocalGateResult {
	// Baseline threshold \theta_s = 0.75 for local actions
	thetaS := 0.75

	// Action threshold is reduced by persistence but increased by uncertainty
	// Example: Persistence of 3 drops the threshold, meaning we act sooner.
	effectiveThreshold := thetaS - (persistence * 0.1) + (uncertainty * 0.2)

	if effectiveThreshold < 0.2 {
		effectiveThreshold = 0.2 // Floor
	}

	return rspLocalGateResult{
		EffectiveThreshold: effectiveThreshold,
		GateOpen:           severity >= effectiveThreshold,
	}
}

// EvaluateAndActuate is kept as a compatibility shim while local control commands remain preview-only.
func (a *RSPLocalActuator) EvaluateAndActuate(ctx context.Context, workspaceID, agentID string, thrashingRisk, ungroundedRisk, thrashingPersistence, ungroundedPersistence float64) {
	flags := a.store.GetRSPCapabilityFlags(ctx, workspaceID)
	if !flags.SafeLocalAutonomicsLive {
		log.Printf("[RSP-S2] observe-only: skipped local actuation for agent %s", agentID)
		return
	}

	if a.Gate_local(thrashingRisk, thrashingPersistence, 0.2) {
		log.Printf("[RSP-S2] observe-only: local flush-cache candidate pending canonical command path for agent %s", agentID)
	}
	if a.Gate_local(ungroundedRisk, ungroundedPersistence, 0.2) {
		log.Printf("[RSP-S2] observe-only: local refresh-kernel candidate pending canonical command path for agent %s", agentID)
	}
}

// emitLocalControlEvent is retained for future canonical command-path work and is not used in the current shipped path.
func (a *RSPLocalActuator) emitLocalControlEvent(ctx context.Context, workspaceID, agentID, command, reason string) {
	evt := RuntimeEventInput{
		EventID:     nextID("evt"),
		WorkspaceID: workspaceID,
		EventType:   command,
		EntityType:  "AGENT",
		EntityID:    agentID,
		ActorType:   "SYSTEM",
		ActorID:     "rsp_local_actuator",
		AgentID:     agentID,
		PayloadJSON: `{"reason":"` + reason + `"}`,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	record, err := a.store.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, evt)
	if err != nil {
		log.Printf("[RSP-S2] Failed to append actuator event: %v", err)
		return
	}
	log.Printf("[RSP-S2] LOCAL ACTUATOR fired: %s targeting agent %s via %s", command, agentID, record.EventID)
}
