package sqlite

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// firehoseRunning sentinel: 0 = stopped, 1 = running.
const (
	firehoseStateIdle    int32 = 0
	firehoseStateRunning int32 = 1
)

// RSPFirehose manages the shadow-mode telemetry event stream for RSP-1.2.
// It uses a non-blocking channel to ensure that observation never stalls the
// primary RMP/RRP transactions.
type RSPFirehose struct {
	eventCh    chan RuntimeEventRecord
	store      *Store
	mu         sync.Mutex
	cancel     context.CancelFunc
	done       chan struct{}
	liveMirror atomic.Value // func(RuntimeEventRecord)

	// Readiness tracking (P2D-001): honest counters, not synthetic bools.
	droppedEvents atomic.Int64
	running       atomic.Int32

	// Temporal Motif Tracking (S1)
	agentContext map[string]*agentTelemetryContext
}

type agentTelemetryContext struct {
	LastEntityEvents map[string][]RuntimeEventRecord
	CacheReads       int
	CacheMisses      int
	StaleHits        int
	WindowStart      time.Time

	// Phase S2: Persistence Tracking
	ConsecutiveThrashing  float64
	ConsecutiveUngrounded float64
}

// NewRSPFirehose creates a new firehose with a large channel buffer to absorb
// bursty cluster events without dropping them.
func NewRSPFirehose(store *Store) *RSPFirehose {
	return &RSPFirehose{
		eventCh:      make(chan RuntimeEventRecord, 10000),
		store:        store,
		agentContext: make(map[string]*agentTelemetryContext),
	}
}

// Start begins the asynchronous processing loop.
func (fh *RSPFirehose) Start(ctx context.Context) {
	if fh == nil {
		return
	}
	if err := fh.Stop(); err != nil {
		log.Printf("[RSP-1.2 Shadow] WARN: previous firehose stop failed before restart: %v", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	fh.mu.Lock()
	fh.cancel = cancel
	fh.done = done
	fh.running.Store(firehoseStateRunning)
	fh.mu.Unlock()
	go func() {
		defer func() {
			fh.running.Store(firehoseStateIdle)
			close(done)
		}()
		fh.loop(ctx)
	}()
}

// Stop halts processing and reports if the worker did not exit before the
// store can be closed safely.
func (fh *RSPFirehose) Stop() error {
	if fh == nil {
		return nil
	}
	fh.mu.Lock()
	cancel := fh.cancel
	done := fh.done
	fh.cancel = nil
	fh.done = nil
	fh.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("rsp firehose stop timed out")
	}
}

func (fh *RSPFirehose) SetLiveMirror(fn func(RuntimeEventRecord)) {
	if fh == nil {
		return
	}
	fh.liveMirror.Store(fn)
}

func (fh *RSPFirehose) publishLiveMirror(event RuntimeEventRecord) {
	if fh == nil {
		return
	}
	value := fh.liveMirror.Load()
	if value == nil {
		return
	}
	if fn, ok := value.(func(RuntimeEventRecord)); ok && fn != nil {
		fn(event)
	}
}

// Dispatch attempts to send the event to the firehose asynchronously.
// If the buffer is full, the event is immediately dropped with a warning to
// satisfy the Zero Blast Radius constraint.
func (fh *RSPFirehose) Dispatch(event RuntimeEventRecord) {
	if fh == nil {
		return
	}
	// Do not process synthetic shadow/overlay events to prevent feedback loops.
	if isSyntheticOperationalEvent(event) {
		return
	}

	select {
	case fh.eventCh <- event:
		// Sent successfully
	default:
		fh.droppedEvents.Add(1)
		log.Printf("[RSP-1.2 Shadow] WARN: Firehose dropped event %s (buffer full). RRP protected.", event.EventID)
	}
}

func (fh *RSPFirehose) loop(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-fh.eventCh:
			fh.processEvent(ctx, event)
		case <-ticker.C:
			fh.drainCommittedOutbox(ctx)
		}
	}
}

// DroppedEvents returns the total number of events dropped due to full buffer.
// This is an honest readiness signal: non-zero means the firehose is degraded.
func (fh *RSPFirehose) DroppedEvents() int64 {
	if fh == nil {
		return 0
	}
	return fh.droppedEvents.Load()
}

// IsRunning returns true if the firehose processing loop goroutine is alive.
// This reflects actual goroutine ownership, not a config flag.
func (fh *RSPFirehose) IsRunning() bool {
	if fh == nil {
		return false
	}
	return fh.running.Load() == firehoseStateRunning
}

func (fh *RSPFirehose) processEvent(ctx context.Context, event RuntimeEventRecord) {
	if event.WorkspaceID == "" {
		return
	}
	// Fire belief calibration updates
	fh.store.ProcessRSPBeliefTelemetry(ctx, event)

	// Fire sequential anomaly detection
	fh.store.ProcessRSPAnomalyTelemetry(ctx, event)

	// Fire motif detectors and update local agent state estimators
	fh.processMotifsAndAgentState(ctx, event)
}

func (fh *RSPFirehose) drainCommittedOutbox(ctx context.Context) {
	if fh == nil || fh.store == nil || fh.store.db == nil {
		return
	}
	events, err := fh.store.listRuntimeEventFirehoseOutboxBatch(ctx, 100)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table") {
			return
		}
		log.Printf("[RSP-1.2 Shadow] WARN: firehose outbox drain failed: %v", err)
		return
	}
	for _, event := range events {
		if !isSyntheticOperationalEvent(event) {
			fh.processEvent(ctx, event)
		}
		if err := fh.store.markRuntimeEventFirehoseOutboxDispatched(ctx, event.WorkspaceID, event.EventID); err != nil {
			log.Printf("[RSP-1.2 Shadow] WARN: firehose outbox mark dispatched failed workspace=%s event=%s err=%v", event.WorkspaceID, event.EventID, err)
			return
		}
	}
}

func (s *Store) listRuntimeEventFirehoseOutboxBatch(ctx context.Context, limit int) ([]RuntimeEventRecord, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT re.event_id, COALESCE(re.dedup_key,''), re.workspace_id, re.event_type, re.entity_type, re.entity_id,
       re.actor_type, re.actor_id, COALESCE(re.agent_id,''), COALESCE(re.session_id,''), COALESCE(re.task_id,''),
       re.payload_json, COALESCE(re.root_cause_id,''), COALESCE(re.provenance_group_id,''), COALESCE(re.parent_refs_json,'[]'), re.created_at,
       COALESCE(re.authority_holder_node_id,''), COALESCE(re.authority_term,0), COALESCE(re.authority_lease_token_fingerprint,''), COALESCE(re.ingest_seq,0)
  FROM runtime_event_firehose_outbox o
  JOIN runtime_events re
    ON re.workspace_id = o.workspace_id
   AND re.event_id = o.event_id
 WHERE o.dispatched_at IS NULL
 ORDER BY o.ingest_seq ASC, o.created_at ASC, o.workspace_id ASC, o.event_id ASC
 LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("query runtime event firehose outbox: %w", err)
	}
	defer rows.Close()
	out := make([]RuntimeEventRecord, 0, limit)
	for rows.Next() {
		var event RuntimeEventRecord
		if err := scanRuntimeEvent(rows, &event); err != nil {
			return nil, fmt.Errorf("scan runtime event firehose outbox: %w", err)
		}
		out = append(out, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtime event firehose outbox: %w", err)
	}
	return out, nil
}

func (s *Store) markRuntimeEventFirehoseOutboxDispatched(ctx context.Context, workspaceID, eventID string) error {
	if s == nil || s.writeDB == nil {
		return nil
	}
	_, err := s.writeDB.ExecContext(ctx,
		`UPDATE runtime_event_firehose_outbox
		    SET dispatched_at = ?
		  WHERE workspace_id = ? AND event_id = ? AND dispatched_at IS NULL`,
		time.Now().UTC().Format(time.RFC3339Nano),
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(eventID),
	)
	if err != nil {
		return fmt.Errorf("mark runtime event firehose outbox dispatched: %w", err)
	}
	return nil
}

func (fh *RSPFirehose) processMotifsAndAgentState(ctx context.Context, event RuntimeEventRecord) {
	if event.AgentID == "" {
		// Only track agent-driven motifs
		return
	}
	flags := fh.store.GetRSPCapabilityFlags(ctx, event.WorkspaceID)
	if !flags.StateShadow && !flags.AnomalyShadow && !flags.GovernedHintsLive && !flags.StrongConsequencesLive {
		return
	}

	agentKey := rspFirehoseAgentContextKey(event.WorkspaceID, event.AgentID)
	agentCtx, ok := fh.agentContext[agentKey]
	if !ok {
		agentCtx = &agentTelemetryContext{
			LastEntityEvents: make(map[string][]RuntimeEventRecord),
			WindowStart:      time.Now(),
		}
		fh.agentContext[agentKey] = agentCtx
	}

	// 1. Check for Behavioral Motifs over the target Entity
	if event.EntityID != "" && (flags.AnomalyShadow || flags.GovernedHintsLive || flags.StrongConsequencesLive) {
		entityKey := rspFirehoseEntityContextKey(event.WorkspaceID, event.EntityID)
		history := agentCtx.LastEntityEvents[entityKey]
		history = append(history, event)
		if len(history) > 3 {
			history = history[len(history)-3:] // Keep last 3
		}
		agentCtx.LastEntityEvents[entityKey] = history

		// Look for Thrash_e (PATCH -> VERIFIER_FAIL -> PATCH)
		// Look for Bounce_e (PATCH -> CRITIQUE -> PATCH)
		if len(history) == 3 {
			ev1 := strings.ToLower(history[0].EventType)
			ev2 := strings.ToLower(history[1].EventType)
			ev3 := strings.ToLower(history[2].EventType)

			isPatch1 := strings.Contains(ev1, "patch") || strings.Contains(ev1, "update")
			isPatch3 := strings.Contains(ev3, "patch") || strings.Contains(ev3, "update")

			if isPatch1 && isPatch3 {
				if strings.Contains(ev2, "verifier") && strings.Contains(ev2, "fail") {
					// Thrash_e detected
					contract := rspAnomalyTelemetryCalibrationContract()
					fh.store.appendAnomalyTelemetryLog(ctx, event.WorkspaceID, rspAnomalyDefaultMode, "Thrash_e", rspAnomalyDefaultTaskClass, rspAnomalyDefaultPhase, "", contract.Basis, contract.CalibrationVersion, 0, "", 0.0, 0.0, 1.0, 1.0, 1.0, "MOTIF_THRASH", time.Now().UTC().Format(time.RFC3339Nano))
					fh.emitGlobalAnomalyAlert(ctx, event.WorkspaceID, event.AgentID, event.EntityID, "MOTIF_THRASH", "Thrash_e sequence detected")
					agentCtx.LastEntityEvents[entityKey] = nil // Reset window
				} else if strings.Contains(ev2, "critique") {
					// Bounce_e detected
					contract := rspAnomalyTelemetryCalibrationContract()
					fh.store.appendAnomalyTelemetryLog(ctx, event.WorkspaceID, rspAnomalyDefaultMode, "Bounce_e", rspAnomalyDefaultTaskClass, rspAnomalyDefaultPhase, "", contract.Basis, contract.CalibrationVersion, 0, "", 0.0, 0.0, 1.0, 1.0, 1.0, "MOTIF_BOUNCE", time.Now().UTC().Format(time.RFC3339Nano))
					fh.emitGlobalAnomalyAlert(ctx, event.WorkspaceID, event.AgentID, event.EntityID, "MOTIF_BOUNCE", "Bounce_e sequence detected")
					agentCtx.LastEntityEvents[entityKey] = nil // Reset window
				}
			}
		}
	}

	if !flags.StateShadow {
		return
	}

	// 2. Track general Agent State statistics (CachePressure, StaleHits)
	evType := strings.ToLower(event.EventType)
	if strings.Contains(evType, "cache.hit") {
		agentCtx.CacheReads++
	} else if strings.Contains(evType, "cache.miss") || strings.Contains(evType, "search") {
		agentCtx.CacheMisses++
	}

	if strings.Contains(evType, "stale_hit") {
		agentCtx.StaleHits++
	}

	// Dump Agent State Telemetry every 20 actions or every minute
	if agentCtx.CacheReads+agentCtx.CacheMisses+agentCtx.StaleHits >= 10 || time.Since(agentCtx.WindowStart) > time.Minute {
		fh.flushAgentStateTelemetry(ctx, event.WorkspaceID, event.AgentID, agentCtx)
	}
}

func (fh *RSPFirehose) flushAgentStateTelemetry(ctx context.Context, workspaceID, agentID string, agentCtx *agentTelemetryContext) {
	totalOps := agentCtx.CacheReads + agentCtx.CacheMisses
	cachePressure := 0.0
	if totalOps > 0 {
		cachePressure = float64(agentCtx.CacheReads) / float64(totalOps)
	}

	staleHitRate := 0.0
	if totalOps > 0 {
		staleHitRate = float64(agentCtx.StaleHits) / float64(totalOps)
	}

	thrashingRisk := 0.0
	if staleHitRate > 0.2 {
		thrashingRisk = 0.5
		agentCtx.ConsecutiveThrashing++
	} else {
		agentCtx.ConsecutiveThrashing = 0
	}

	ungroundedRisk := 0.0
	if cachePressure > 0.8 {
		// Relying heavily on local cache might indicate losing grounding
		ungroundedRisk = 0.6
		agentCtx.ConsecutiveUngrounded++
	} else {
		agentCtx.ConsecutiveUngrounded = 0
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	id := nextID("rspstate")

	_, err := fh.store.writeDB.ExecContext(ctx,
		`INSERT INTO rsp_agent_state_telemetry(id, workspace_id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id, workspaceID, agentID, cachePressure, staleHitRate, thrashingRisk, ungroundedRisk, now,
	)
	if err != nil {
		log.Printf("[RSP-1.2] Failed to flush agent state telemetry: %v", err)
	}

	// Local autonomics stay inspectable-only until canonical command-path actuation exists.

	// Reset counters
	agentCtx.CacheReads = 0
	agentCtx.CacheMisses = 0
	agentCtx.StaleHits = 0
	agentCtx.WindowStart = time.Now()
}

func rspFirehoseAgentContextKey(workspaceID, agentID string) string {
	return strings.TrimSpace(workspaceID) + "::" + strings.TrimSpace(agentID)
}

func rspFirehoseEntityContextKey(workspaceID, entityID string) string {
	return strings.TrimSpace(workspaceID) + "::" + strings.TrimSpace(entityID)
}

func (fh *RSPFirehose) emitGlobalAnomalyAlert(ctx context.Context, workspaceID, agentID, entityID, alertType, reason string) {
	flags := fh.store.GetRSPCapabilityFlags(ctx, workspaceID)
	if !flags.AnomalyShadow && !flags.GovernedHintsLive && !flags.StrongConsequencesLive {
		log.Printf("[RSP-S3] anomaly signal disabled: suppressed ANOMALY_ALERT for %s on %s", alertType, agentID)
		return
	}
	actuationClass := "shadow_only"
	if flags.StrongConsequencesLive {
		actuationClass = "strong_consequence"
	} else if flags.GovernedHintsLive {
		actuationClass = "governed_hint"
	}
	payload := mustJSON(map[string]any{
		"entity_id":        entityID,
		"alert_type":       alertType,
		"reason":           reason,
		"actuation_class":  actuationClass,
		"shadow_only":      actuationClass == "shadow_only",
		"capability_flags": flags,
	})

	evt := RuntimeEventInput{
		EventID:     nextID("evt"),
		WorkspaceID: workspaceID,
		EventType:   "ANOMALY_ALERT",
		EntityType:  "FACT",
		EntityID:    entityID,
		ActorType:   "SYSTEM",
		ActorID:     "rsp_firehose",
		AgentID:     agentID,
		PayloadJSON: payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	record, err := fh.store.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, evt)
	if err == nil {
		fh.publishLiveMirror(record)
		log.Printf("[RSP-S3] Emitted Global ANOMALY_ALERT for %s on %s", alertType, agentID)
	} else {
		log.Printf("[RSP-S3] Failed to emit Global ANOMALY_ALERT for %s: %v", alertType, err)
	}
}
