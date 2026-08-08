package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

const (
	motifThrashEphemeralEventType      = "ephemeral.rsp.motif.thrash"
	motifBounceEphemeralEventType      = "ephemeral.rsp.motif.bounce"
	motifInstructionEphemeralEventType = "ephemeral.system.instruction"
	motifMetaTensionEphemeralEventType = "ephemeral.system.meta_tension.request"
)

// MotifDetectors acts as event bus middleware to prevent agentic loops.
// Canonical/persisted anomaly protocol signals come from storage-side ANOMALY_ALERT.
// These middleware publishes are internal ephemeral assists only.
type MotifDetectors struct {
	store    *sqlite.Store
	eventBus *EventBus

	mu           sync.Mutex
	thrashEvents map[string][]time.Time          // key: workspace_id|tension_id|agent_id -> slice of fail times
	bounceEvents map[string]map[string]time.Time // key: workspace_id|artifact_ref -> map[agent_id]time
}

func NewMotifDetectors(store *sqlite.Store, eventBus *EventBus) *MotifDetectors {
	md := &MotifDetectors{
		store:        store,
		eventBus:     eventBus,
		thrashEvents: make(map[string][]time.Time),
		bounceEvents: make(map[string]map[string]time.Time),
	}
	// Stagnation detector loop could be started here
	go md.runStagnationLoop()
	return md
}

// Middleware is intended to be called by EventBus.Publish
func (md *MotifDetectors) Middleware(msg EventMessage) {
	switch msg.Type {
	case "tension.lifecycle.update":
		// Thrash detection: check for repeated FAILED updates
		var payload struct {
			LifecycleState string `json:"lifecycle_state"`
		}
		if err := json.Unmarshal([]byte(msg.PayloadJSON), &payload); err == nil {
			if strings.ToUpper(payload.LifecycleState) == "FAILED" {
				md.handleThrashEvent(msg)
			}
		}

	case "workspace.artifact.write":
		// Bounce detection: check for close updates by different agents
		var payload struct {
			ArtifactRef string `json:"artifact_ref"`
		}
		if err := json.Unmarshal([]byte(msg.PayloadJSON), &payload); err == nil && payload.ArtifactRef != "" {
			md.handleBounceEvent(msg, payload.ArtifactRef)
		}
	}
}

func (md *MotifDetectors) handleThrashEvent(msg EventMessage) {
	var payload struct {
		TensionID string `json:"tension_id"`
	}
	if err := json.Unmarshal([]byte(msg.PayloadJSON), &payload); err != nil || payload.TensionID == "" {
		return
	}

	workspaceID := msg.WorkspaceID
	agentID := msg.AgentID
	if agentID == "" {
		return
	}

	key := fmt.Sprintf("%s|%s|%s", workspaceID, payload.TensionID, agentID)
	now := time.Now()

	md.mu.Lock()
	times := md.thrashEvents[key]
	// Clean up older than 10 minutes
	valid := make([]time.Time, 0, len(times)+1)
	for _, t := range times {
		if now.Sub(t) < 10*time.Minute {
			valid = append(valid, t)
		}
	}
	valid = append(valid, now)
	md.thrashEvents[key] = valid
	count := len(valid)
	md.mu.Unlock()

	if count >= 3 {
		log.Printf("[MOTIF] Thrash detected for Agent %s on Tension %s", agentID, payload.TensionID)

		// Reset tracking to avoid spamming
		md.mu.Lock()
		delete(md.thrashEvents, key)
		md.mu.Unlock()

		// 1. Block agent locally via DB Exclusion for 2 hours
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, err := md.store.RequestControlCommandWithEvent(ctx, sqlite.ControlCommandInput{
			WorkspaceID: workspaceID,
			CommandType: sqlite.ControlCommandExcludeAgentTension,
			TensionID:   payload.TensionID,
			AgentID:     agentID,
			TTLSeconds:  int((2 * time.Hour) / time.Second),
			Reason:      "motif_detector_ephemeral.rsp.motif.thrash",
			RequestedBy: "rsp.motif_detector",
			ActorType:   "system",
		})
		if err != nil {
			log.Printf("[MOTIF] Error excluding agent %s from tension %s: %v", agentID, payload.TensionID, err)
		}

		// 2. Emit internal-ephemeral thrash signal.
		md.eventBus.Publish(EventMessage{
			Type:        motifThrashEphemeralEventType,
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			Summary:     "Motif detector observed thrash loop",
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
			PayloadJSON: fmt.Sprintf(`{"tension_id":"%s","reason":"N>=3 verifier fails in 10m"}`, payload.TensionID),
		})

		// 3. Internal-ephemeral FLUSH_P1_CACHE instruction.
		md.eventBus.Publish(EventMessage{
			Type:        motifInstructionEphemeralEventType,
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			Summary:     "Motif detector requested cache flush",
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
			PayloadJSON: `{"instruction":"FLUSH_P1_CACHE"}`,
		})
	}
}

func (md *MotifDetectors) handleBounceEvent(msg EventMessage, artifactRef string) {
	workspaceID := msg.WorkspaceID
	agentID := msg.AgentID
	if agentID == "" {
		return
	}

	now := time.Now()
	key := fmt.Sprintf("%s|%s", workspaceID, artifactRef)

	md.mu.Lock()
	if md.bounceEvents[key] == nil {
		md.bounceEvents[key] = make(map[string]time.Time)
	}

	agents := md.bounceEvents[key]
	agents[agentID] = now

	var conflictingAgent string
	for otherAgent, t := range agents {
		if otherAgent != agentID && now.Sub(t) < 2*time.Minute {
			conflictingAgent = otherAgent
			break
		}
	}

	// Clean up old
	for a, t := range agents {
		if now.Sub(t) > 10*time.Minute {
			delete(agents, a)
		}
	}
	md.mu.Unlock()

	if conflictingAgent != "" {
		log.Printf("[MOTIF] Bounce detected on %s between %s and %s", artifactRef, agentID, conflictingAgent)

		// Reset to prevent spam
		md.mu.Lock()
		delete(md.bounceEvents, key)
		md.mu.Unlock()

		// Emit internal-ephemeral bounce signal.
		md.eventBus.Publish(EventMessage{
			Type:        motifBounceEphemeralEventType,
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			Summary:     "Motif detector observed bounce loop",
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
			PayloadJSON: fmt.Sprintf(`{"artifact_ref":"%s","conflicting_agent":"%s"}`, artifactRef, conflictingAgent),
		})

		// Internal-ephemeral request to spawn a DISSENT meta-tension.
		md.eventBus.Publish(EventMessage{
			Type:        motifMetaTensionEphemeralEventType,
			WorkspaceID: workspaceID,
			AgentID:     "system:motif",
			Summary:     "Motif detector requested DISSENT meta-tension",
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
			PayloadJSON: fmt.Sprintf(`{"type":"DISSENT","target_artifact":"%s","agents":["%s","%s"],"reason":"Bounce anomaly"}`, artifactRef, agentID, conflictingAgent),
		})
	}
}

func (md *MotifDetectors) runStagnationLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		// Mock read of open tensions vs merges
		// We emit stagnation if needed
		// For Deployment Trials, we leave this skeletal, as requested.
	}
}
