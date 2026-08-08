package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	memoryPromotionCandidateKindWorkspaceMemory = "WORKSPACE_MEMORY"

	memoryPromotionStatePending    = "PENDING"
	memoryPromotionStateAccepted   = "ACCEPTED"
	memoryPromotionStateRejected   = "REJECTED"
	memoryPromotionStateSuperseded = "SUPERSEDED"
	memoryPromotionStateCancelled  = "CANCELLED"
)

type MemoryPromotionCandidate struct {
	MemoryType string   `json:"memory_type,omitempty"`
	Title      string   `json:"title,omitempty"`
	Body       string   `json:"body"`
	Summary    string   `json:"summary,omitempty"`
	AgentID    string   `json:"agent_id,omitempty"`
	SessionID  string   `json:"session_id,omitempty"`
	TaskID     string   `json:"task_id,omitempty"`
	SourceKind string   `json:"source_kind"`
	SourceID   string   `json:"source_id"`
	Tags       []string `json:"tags,omitempty"`
	Importance float64  `json:"importance,omitempty"`
	Confidence float64  `json:"confidence,omitempty"`
}

type MemoryPromotionEnqueueInput struct {
	PromotionID   string
	WorkspaceID   string
	CandidateKind string
	Candidate     MemoryPromotionCandidate
	BasisDigest   string
	BasisRefs     []string
	ProposedBy    string
}

type MemoryPromotionResolveInput struct {
	WorkspaceID    string
	PromotionID    string
	QueueKey       string
	Resolution     string
	ResolutionNote string
	ResolvedBy     string
}

type MemoryPromotionFilter struct {
	WorkspaceID   string
	State         string
	CandidateKind string
	CandidateType string
	Limit         int
}

type MemoryPromotionRecord struct {
	PromotionID    string                        `json:"promotion_id"`
	WorkspaceID    string                        `json:"workspace_id"`
	QueueKey       string                        `json:"queue_key"`
	State          string                        `json:"state"`
	CandidateKind  string                        `json:"candidate_kind"`
	CandidateType  string                        `json:"candidate_type"`
	TargetMemoryID string                        `json:"target_memory_id"`
	Candidate      MemoryPromotionCandidate      `json:"candidate"`
	BasisDigest    string                        `json:"basis_digest"`
	BasisRefs      []string                      `json:"basis_refs,omitempty"`
	ProposedBy     string                        `json:"proposed_by"`
	ResolutionNote string                        `json:"resolution_note,omitempty"`
	AppliedKind    string                        `json:"applied_kind,omitempty"`
	AppliedID      string                        `json:"applied_id,omitempty"`
	ResolvedAt     *string                       `json:"resolved_at,omitempty"`
	ResolvedBy     *string                       `json:"resolved_by,omitempty"`
	CreatedAt      string                        `json:"created_at"`
	UpdatedAt      string                        `json:"updated_at"`
	CoherenceGate  *MemoryPromotionCoherenceGate `json:"coherence_gate,omitempty"`
}

type MemoryPromotionResolveResult struct {
	Promotion     MemoryPromotionRecord  `json:"promotion"`
	AppliedMemory *MemoryNodeWriteResult `json:"applied_memory,omitempty"`
	Event         *RuntimeEventRecord    `json:"event,omitempty"`
}

type MemoryPromotionDeferredAcceptError struct {
	Promotion  MemoryPromotionRecord `json:"promotion"`
	Queue      *OperatorQueueRecord  `json:"queue,omitempty"`
	QueueEvent *RuntimeEventRecord   `json:"queue_event,omitempty"`
	message    string
}

func (e *MemoryPromotionDeferredAcceptError) Error() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.message)
}

type MemoryPromotionCoherenceGate struct {
	ReportScope             string   `json:"report_scope"`
	SessionID               string   `json:"session_id,omitempty"`
	MetricsReportID         string   `json:"metrics_report_id,omitempty"`
	ResidencyReportID       string   `json:"residency_report_id,omitempty"`
	CoherenceBand           string   `json:"coherence_band"`
	AdvisoryAction          string   `json:"advisory_action"`
	NeedsAttention          bool     `json:"needs_attention"`
	AttentionReasons        []string `json:"attention_reasons,omitempty"`
	StaleHitRate            float64  `json:"stale_hit_rate"`
	PromotionPrecision      float64  `json:"promotion_precision"`
	StaleReadRate           float64  `json:"stale_read_rate"`
	InvalidatedReplicaCount int      `json:"invalidated_replica_count"`
	OpenInvalidationCount   int      `json:"open_invalidation_count"`
	ReadyInvalidationCount  int      `json:"ready_invalidation_count"`
	DeadLetterCount         int      `json:"dead_letter_count"`
	Summary                 string   `json:"summary,omitempty"`
}

func (s *Store) EnqueueMemoryPromotion(ctx context.Context, input MemoryPromotionEnqueueInput) (MemoryPromotionRecord, error) {
	record, _, err := s.EnqueueMemoryPromotionWithEvent(ctx, input)
	return record, err
}

func (s *Store) EnqueueMemoryPromotionWithEvent(ctx context.Context, input MemoryPromotionEnqueueInput) (MemoryPromotionRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return MemoryPromotionRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	proposedBy := strings.TrimSpace(input.ProposedBy)
	if proposedBy == "" {
		return MemoryPromotionRecord{}, RuntimeEventRecord{}, errors.New("proposed_by is required")
	}
	basisDigest := strings.TrimSpace(input.BasisDigest)
	if basisDigest == "" {
		return MemoryPromotionRecord{}, RuntimeEventRecord{}, errors.New("basis_digest is required")
	}
	candidate, candidateKind, candidateType, err := s.normalizeMemoryPromotionCandidate(ctx, workspaceID, input.CandidateKind, input.Candidate)
	if err != nil {
		return MemoryPromotionRecord{}, RuntimeEventRecord{}, err
	}
	basisRefs := uniqueTrimmedStrings(input.BasisRefs)

	layer := memoryGraphLayerForType(candidateType)

	queueKey := memoryPromotionQueueKey(candidateKind, candidate, basisDigest, basisRefs)

	existing, err := s.lookupMemoryPromotion(ctx, workspaceID, strings.TrimSpace(input.PromotionID), queueKey)
	if err != nil {
		return MemoryPromotionRecord{}, RuntimeEventRecord{}, err
	}
	if existing != nil {
		if strings.TrimSpace(input.PromotionID) != "" && existing.QueueKey != queueKey {
			return MemoryPromotionRecord{}, RuntimeEventRecord{}, fmt.Errorf("promotion_id replay payload does not match existing candidate")
		}
		return *existing, RuntimeEventRecord{}, nil
	}

	candidateJSON, err := json.Marshal(candidate)
	if err != nil {
		return MemoryPromotionRecord{}, RuntimeEventRecord{}, fmt.Errorf("encode promotion candidate: %w", err)
	}
	basisRefsJSON, err := json.Marshal(basisRefs)
	if err != nil {
		return MemoryPromotionRecord{}, RuntimeEventRecord{}, fmt.Errorf("encode promotion basis refs: %w", err)
	}

	record := MemoryPromotionRecord{
		PromotionID:    firstNonEmpty(strings.TrimSpace(input.PromotionID), nextID("mpromo")),
		WorkspaceID:    workspaceID,
		QueueKey:       queueKey,
		State:          memoryPromotionStatePending,
		CandidateKind:  candidateKind,
		CandidateType:  candidateType,
		TargetMemoryID: nextID("memory"),
		Candidate:      candidate,
		BasisDigest:    basisDigest,
		BasisRefs:      basisRefs,
		ProposedBy:     proposedBy,
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	record.CreatedAt = now
	record.UpdatedAt = now
	if gate := s.enrichMemoryPromotionCoherenceGate(ctx, record.WorkspaceID, record.Candidate); gate != nil {
		record.CoherenceGate = gate
	}

	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return MemoryPromotionRecord{}, RuntimeEventRecord{}, err
	}
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		if layer == "PROCEDURAL" {
			if err := validateProceduralPromotionEvidenceTx(ctx, tx, workspaceID, basisRefs); err != nil {
				return err
			}
		} else if layer == "IDENTITY" {
			if err := validateIdentityPromotionEvidenceTx(ctx, tx, workspaceID, basisRefs); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO memory_promotion_queue(
			    promotion_id, workspace_id, queue_key, state, candidate_kind, candidate_type,
			    target_memory_id, candidate_json, basis_digest, basis_refs_json, proposed_by,
			    resolution_note, applied_kind, applied_id, resolved_at, resolved_by, created_at, updated_at
			  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', '', NULL, NULL, ?, ?)`,
			record.PromotionID,
			record.WorkspaceID,
			record.QueueKey,
			record.State,
			record.CandidateKind,
			record.CandidateType,
			record.TargetMemoryID,
			string(candidateJSON),
			record.BasisDigest,
			string(basisRefsJSON),
			record.ProposedBy,
			record.CreatedAt,
			record.UpdatedAt,
		); err != nil {
			return fmt.Errorf("insert memory promotion queue item: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after memory promotion enqueue: %w", err)
		}
		if _, _, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(memoryPromotionOperatorQueueInput(record)), now); err != nil {
			return err
		}
		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "memory_promotion_enqueued",
			EntityType: "memory_promotion",
			EntityID:   record.PromotionID,
			ActorID:    record.ProposedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":     record.WorkspaceID,
				"promotion_id":     record.PromotionID,
				"queue_key":        record.QueueKey,
				"candidate_kind":   record.CandidateKind,
				"candidate_type":   record.CandidateType,
				"target_memory_id": record.TargetMemoryID,
				"basis_digest":     record.BasisDigest,
				"basis_refs":       record.BasisRefs,
				"proposed_by":      record.ProposedBy,
				"coherence_gate":   record.CoherenceGate,
			}),
		}); err != nil {
			return err
		}
		var innerErr error
		event, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: record.WorkspaceID,
			EventType:   "memory.promotion_enqueued",
			EntityType:  "memory_promotion",
			EntityID:    record.PromotionID,
			ActorType:   memoryPromotionActorType(record),
			ActorID:     record.ProposedBy,
			AgentID:     record.Candidate.AgentID,
			SessionID:   record.Candidate.SessionID,
			TaskID:      record.Candidate.TaskID,
			PayloadJSON: mustJSON(memoryPromotionEventPayload(record)),
			CreatedAt:   now,
		})
		return innerErr
	}); err != nil {
		return MemoryPromotionRecord{}, RuntimeEventRecord{}, err
	}
	resolved, err := s.GetMemoryPromotion(ctx, workspaceID, record.PromotionID)
	return resolved, event, err
}

func (s *Store) ResolveMemoryPromotion(ctx context.Context, input MemoryPromotionResolveInput) (MemoryPromotionResolveResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return MemoryPromotionResolveResult{}, errors.New("workspace_id is required")
	}
	promotionID := strings.TrimSpace(input.PromotionID)
	queueKey := strings.TrimSpace(input.QueueKey)
	if promotionID == "" && queueKey == "" {
		return MemoryPromotionResolveResult{}, errors.New("promotion_id or queue_key is required")
	}
	resolvedBy := strings.TrimSpace(input.ResolvedBy)
	if resolvedBy == "" {
		return MemoryPromotionResolveResult{}, errors.New("resolved_by is required")
	}
	resolution := normalizeMemoryPromotionResolution(input.Resolution)
	if resolution == memoryPromotionStatePending {
		return MemoryPromotionResolveResult{}, errors.New("resolution must be one of ACCEPTED, REJECTED, SUPERSEDED, CANCELLED")
	}

	record, err := s.getMemoryPromotion(ctx, workspaceID, promotionID, queueKey)
	if err != nil {
		return MemoryPromotionResolveResult{}, err
	}
	record = s.enrichMemoryPromotionRecord(ctx, record)
	if record.State != memoryPromotionStatePending {
		if record.State == resolution {
			applied, err := s.loadMemoryPromotionAppliedResult(ctx, record)
			if err != nil {
				return MemoryPromotionResolveResult{}, err
			}
			return MemoryPromotionResolveResult{Promotion: record, AppliedMemory: applied}, nil
		}
		return MemoryPromotionResolveResult{}, fmt.Errorf("memory promotion is already resolved: %s", record.State)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return MemoryPromotionResolveResult{}, err
	}
	var (
		current         MemoryPromotionRecord
		applied         *MemoryNodeWriteResult
		event           RuntimeEventRecord
		deferredAccept  *MemoryPromotionDeferredAcceptError
		alreadyResolved bool
	)
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		current, err = s.getMemoryPromotionTx(ctx, tx, workspaceID, promotionID, queueKey)
		if err != nil {
			return err
		}
		if current.State != memoryPromotionStatePending {
			if current.State == resolution {
				alreadyResolved = true
				return nil
			}
			return fmt.Errorf("memory promotion is already resolved: %s", current.State)
		}
		if resolution == memoryPromotionStateAccepted {
			if err := validateMemoryPromotionAcceptanceEvidenceTx(ctx, tx, current); err != nil {
				return err
			}
			gate, deferAccept, err := s.validateMemoryPromotionAcceptanceCoherenceGateTx(ctx, tx, current)
			if err != nil {
				return err
			}
			current.CoherenceGate = gate
			if deferAccept {
				queueRecord, queueEvent, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(memoryPromotionOperatorQueueInput(current)), now)
				if err != nil {
					return err
				}
				summary := strings.TrimSpace(current.CoherenceGate.Summary)
				if summary == "" {
					summary = "deferred accept required by current memory coherence state"
				}
				deferredAccept = &MemoryPromotionDeferredAcceptError{
					Promotion:  current,
					Queue:      &queueRecord,
					QueueEvent: &queueEvent,
					message:    fmt.Sprintf("memory promotion coherence gate requires deferred accept: %s", summary),
				}
				return nil
			}
			result, err := s.applyMemoryPromotionCandidateTx(ctx, tx, current, authority, now)
			if err != nil {
				return err
			}
			applied = &result
		}
		current.State = resolution
		current.ResolutionNote = strings.TrimSpace(input.ResolutionNote)
		current.ResolvedAt = stringPtr(now)
		current.ResolvedBy = stringPtr(resolvedBy)
		current.UpdatedAt = now
		current = s.enrichMemoryPromotionRecord(ctx, current)
		if applied != nil {
			current.AppliedKind = "workspace_memory"
			current.AppliedID = applied.MemoryID
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE memory_promotion_queue
			    SET state = ?, resolution_note = ?, applied_kind = ?, applied_id = ?, resolved_at = ?, resolved_by = ?, updated_at = ?
			  WHERE workspace_id = ? AND promotion_id = ?`,
			current.State,
			current.ResolutionNote,
			current.AppliedKind,
			current.AppliedID,
			blankStringOrNil(derefString(current.ResolvedAt)),
			blankStringOrNil(derefString(current.ResolvedBy)),
			current.UpdatedAt,
			workspaceID,
			current.PromotionID,
		); err != nil {
			return fmt.Errorf("update memory promotion resolution: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after memory promotion resolve: %w", err)
		}
		operatorQueueStatus := "RESOLVED"
		if current.State == memoryPromotionStateCancelled {
			operatorQueueStatus = "CANCELLED"
		}
		if _, _, err := s.resolveOperatorQueueItemWithAuthorityTx(ctx, tx, authority, workspaceID, "", memoryPromotionOperatorQueueKey(current.PromotionID), operatorQueueStatus, resolvedBy, firstNonEmpty(current.ResolutionNote, strings.ToLower(current.State)), "", "", "", "", 0, "", nil, now); err != nil {
			if !errors.Is(err, ErrOperatorQueueItemNotFound) {
				return err
			}
		}
		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "memory_promotion_resolved",
			EntityType: "memory_promotion",
			EntityID:   current.PromotionID,
			ActorID:    resolvedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":    current.WorkspaceID,
				"promotion_id":    current.PromotionID,
				"queue_key":       current.QueueKey,
				"state":           current.State,
				"resolution_note": current.ResolutionNote,
				"applied_kind":    current.AppliedKind,
				"applied_id":      current.AppliedID,
				"coherence_gate":  current.CoherenceGate,
			}),
		}); err != nil {
			return err
		}
		var innerErr error
		event, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: current.WorkspaceID,
			EventType:   "memory.promotion_resolved",
			EntityType:  "memory_promotion",
			EntityID:    current.PromotionID,
			ActorType:   memoryPromotionActorType(current),
			ActorID:     resolvedBy,
			AgentID:     current.Candidate.AgentID,
			SessionID:   current.Candidate.SessionID,
			TaskID:      current.Candidate.TaskID,
			PayloadJSON: mustJSON(memoryPromotionEventPayload(current)),
			CreatedAt:   now,
		})
		return innerErr
	}); err != nil {
		return MemoryPromotionResolveResult{}, err
	}
	if alreadyResolved {
		current = s.enrichMemoryPromotionRecord(ctx, current)
		applied, err = s.loadMemoryPromotionAppliedResult(ctx, current)
		if err != nil {
			return MemoryPromotionResolveResult{}, err
		}
		return MemoryPromotionResolveResult{Promotion: current, AppliedMemory: applied}, nil
	}
	if deferredAccept != nil {
		return MemoryPromotionResolveResult{}, deferredAccept
	}
	return MemoryPromotionResolveResult{Promotion: current, AppliedMemory: applied, Event: &event}, nil
}

func (s *Store) GetMemoryPromotion(ctx context.Context, workspaceID, promotionID string) (MemoryPromotionRecord, error) {
	record, err := s.getMemoryPromotion(ctx, workspaceID, promotionID, "")
	if err != nil {
		return MemoryPromotionRecord{}, err
	}
	return s.enrichMemoryPromotionRecord(ctx, record), nil
}

func (s *Store) ListMemoryPromotions(ctx context.Context, filter MemoryPromotionFilter) ([]MemoryPromotionRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if raw := strings.TrimSpace(filter.State); raw != "" && normalizeMemoryPromotionState(raw, false) == "" {
		return nil, errors.New("state must be one of PENDING, ACCEPTED, REJECTED, SUPERSEDED, CANCELLED")
	}
	if raw := strings.TrimSpace(filter.CandidateKind); raw != "" && normalizeMemoryPromotionCandidateKind(raw) == "" {
		return nil, fmt.Errorf("candidate_kind must be %s", memoryPromotionCandidateKindWorkspaceMemory)
	}
	query := strings.Builder{}
	query.WriteString(`SELECT promotion_id, workspace_id, queue_key, state, candidate_kind, candidate_type,
	        target_memory_id, candidate_json, basis_digest, basis_refs_json, proposed_by, resolution_note,
	        COALESCE(applied_kind,''), COALESCE(applied_id,''), resolved_at, resolved_by, created_at, updated_at
	   FROM memory_promotion_queue
	  WHERE workspace_id = ?`)
	args := []any{workspaceID}
	if trimmed := normalizeMemoryPromotionState(filter.State, true); trimmed != "" {
		query.WriteString(` AND state = ?`)
		args = append(args, trimmed)
	}
	if trimmed := normalizeMemoryPromotionCandidateKind(filter.CandidateKind); trimmed != "" {
		query.WriteString(` AND candidate_kind = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.CandidateType); trimmed != "" {
		query.WriteString(` AND candidate_type = ?`)
		args = append(args, strings.ToUpper(trimmed))
	}
	query.WriteString(` ORDER BY updated_at DESC, promotion_id DESC LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list memory promotions: %w", err)
	}
	defer rows.Close()
	items, err := collectMemoryPromotionRows(rows)
	if err != nil {
		return nil, err
	}
	return s.enrichMemoryPromotionRecords(ctx, items), nil
}

func (s *Store) normalizeMemoryPromotionCandidate(ctx context.Context, workspaceID, rawKind string, candidate MemoryPromotionCandidate) (MemoryPromotionCandidate, string, string, error) {
	candidateKind := normalizeMemoryPromotionCandidateKind(rawKind)
	if candidateKind == "" {
		candidateKind = memoryPromotionCandidateKindWorkspaceMemory
	}
	if candidateKind != memoryPromotionCandidateKindWorkspaceMemory {
		return MemoryPromotionCandidate{}, "", "", fmt.Errorf("candidate_kind must be %s", memoryPromotionCandidateKindWorkspaceMemory)
	}
	if strings.TrimSpace(candidate.Body) == "" {
		return MemoryPromotionCandidate{}, "", "", errors.New("body is required")
	}
	if strings.TrimSpace(candidate.SourceKind) == "" {
		return MemoryPromotionCandidate{}, "", "", errors.New("source_kind is required")
	}
	if strings.TrimSpace(candidate.SourceID) == "" {
		return MemoryPromotionCandidate{}, "", "", errors.New("source_id is required")
	}
	writeInput := MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		MemoryType:  strings.TrimSpace(candidate.MemoryType),
		Title:       strings.TrimSpace(candidate.Title),
		Body:        strings.TrimSpace(candidate.Body),
		Summary:     strings.TrimSpace(candidate.Summary),
		AgentID:     strings.TrimSpace(candidate.AgentID),
		SessionID:   strings.TrimSpace(candidate.SessionID),
		TaskID:      strings.TrimSpace(candidate.TaskID),
		SourceKind:  normalizeWorkspaceMemorySourceKind(candidate.SourceKind),
		SourceID:    strings.TrimSpace(candidate.SourceID),
		Tags:        normalizeStringSlice(candidate.Tags),
		Importance:  candidate.Importance,
		Confidence:  candidate.Confidence,
	}
	if err := validateMemoryNodeWriteType(writeInput.MemoryType); err != nil {
		return MemoryPromotionCandidate{}, "", "", err
	}
	if err := s.normalizeMemoryNodeWriteAnchors(ctx, &writeInput); err != nil {
		return MemoryPromotionCandidate{}, "", "", err
	}
	if strings.TrimSpace(writeInput.MemoryType) == "" {
		writeInput.MemoryType = "NOTE"
	}
	normalized := MemoryPromotionCandidate{
		MemoryType: strings.ToUpper(strings.TrimSpace(writeInput.MemoryType)),
		Title:      strings.TrimSpace(writeInput.Title),
		Body:       strings.TrimSpace(writeInput.Body),
		Summary:    strings.TrimSpace(writeInput.Summary),
		AgentID:    strings.TrimSpace(writeInput.AgentID),
		SessionID:  strings.TrimSpace(writeInput.SessionID),
		TaskID:     strings.TrimSpace(writeInput.TaskID),
		SourceKind: normalizeWorkspaceMemorySourceKind(writeInput.SourceKind),
		SourceID:   strings.TrimSpace(writeInput.SourceID),
		Tags:       normalizeStringSlice(writeInput.Tags),
		Importance: clampUnitInterval(writeInput.Importance),
		Confidence: clampUnitInterval(writeInput.Confidence),
	}
	return normalized, candidateKind, normalized.MemoryType, nil
}

func (s *Store) applyMemoryPromotionCandidate(ctx context.Context, record MemoryPromotionRecord) (MemoryNodeWriteResult, error) {
	if record.CandidateKind != memoryPromotionCandidateKindWorkspaceMemory {
		return MemoryNodeWriteResult{}, fmt.Errorf("unsupported memory promotion candidate_kind: %s", record.CandidateKind)
	}
	return s.WriteMemoryNode(ctx, MemoryNodeWriteInput{
		WorkspaceID: record.WorkspaceID,
		MemoryID:    record.TargetMemoryID,
		MemoryType:  record.Candidate.MemoryType,
		Title:       record.Candidate.Title,
		Body:        record.Candidate.Body,
		Summary:     record.Candidate.Summary,
		AgentID:     record.Candidate.AgentID,
		SessionID:   record.Candidate.SessionID,
		TaskID:      record.Candidate.TaskID,
		SourceKind:  record.Candidate.SourceKind,
		SourceID:    record.Candidate.SourceID,
		Tags:        append([]string(nil), record.Candidate.Tags...),
		Importance:  record.Candidate.Importance,
		Confidence:  record.Candidate.Confidence,
	})
}

func (s *Store) applyMemoryPromotionCandidateTx(ctx context.Context, tx *sql.Tx, record MemoryPromotionRecord, authority WorkspaceAuthorityRecord, now string) (MemoryNodeWriteResult, error) {
	if record.CandidateKind != memoryPromotionCandidateKindWorkspaceMemory {
		return MemoryNodeWriteResult{}, fmt.Errorf("unsupported memory promotion candidate_kind: %s", record.CandidateKind)
	}
	appliedRecord, event, syncEffects, err := s.recordWorkspaceMemoryWithAuthorityTx(ctx, tx, WorkspaceMemoryInput{
		MemoryID:    record.TargetMemoryID,
		WorkspaceID: record.WorkspaceID,
		MemoryType:  record.Candidate.MemoryType,
		Title:       record.Candidate.Title,
		Body:        record.Candidate.Body,
		Summary:     record.Candidate.Summary,
		AgentID:     record.Candidate.AgentID,
		SessionID:   record.Candidate.SessionID,
		TaskID:      record.Candidate.TaskID,
		SourceKind:  record.Candidate.SourceKind,
		SourceID:    record.Candidate.SourceID,
		Tags:        append([]string(nil), record.Candidate.Tags...),
		Importance:  record.Candidate.Importance,
		Confidence:  record.Candidate.Confidence,
	}, authority, now)
	if err != nil {
		return MemoryNodeWriteResult{}, err
	}
	nodeID := memoryGraphNodeID("workspace_memory", appliedRecord.MemoryID)
	node, err := s.loadMemoryGraphNodeTx(ctx, tx, appliedRecord.WorkspaceID, nodeID)
	if err != nil {
		return MemoryNodeWriteResult{}, err
	}
	return MemoryNodeWriteResult{
		WorkspaceID:          appliedRecord.WorkspaceID,
		NodeID:               nodeID,
		MemoryID:             appliedRecord.MemoryID,
		OriginKind:           "workspace_memory",
		OriginID:             appliedRecord.MemoryID,
		Status:               "RECORDED",
		Record:               appliedRecord,
		Event:                event,
		Node:                 node,
		PromotedClaimEffects: syncEffects,
	}, nil
}

func (s *Store) loadMemoryPromotionAppliedResult(ctx context.Context, record MemoryPromotionRecord) (*MemoryNodeWriteResult, error) {
	if strings.TrimSpace(record.AppliedKind) == "" || strings.TrimSpace(record.AppliedID) == "" {
		return nil, nil
	}
	if strings.TrimSpace(record.AppliedKind) != "workspace_memory" {
		return nil, nil
	}
	memoryRecord, err := s.GetWorkspaceMemory(ctx, record.WorkspaceID, record.AppliedID)
	if err != nil {
		return nil, err
	}
	nodeID := memoryGraphNodeID("workspace_memory", memoryRecord.MemoryID)
	detail, err := s.GetMemoryGraphNode(ctx, record.WorkspaceID, nodeID)
	if err != nil {
		return nil, err
	}
	result := MemoryNodeWriteResult{
		WorkspaceID: memoryRecord.WorkspaceID,
		NodeID:      nodeID,
		MemoryID:    memoryRecord.MemoryID,
		OriginKind:  "workspace_memory",
		OriginID:    memoryRecord.MemoryID,
		Status:      "RECORDED",
		Record:      memoryRecord,
		Node:        detail.Node,
	}
	return &result, nil
}

func (s *Store) lookupMemoryPromotion(ctx context.Context, workspaceID, promotionID, queueKey string) (*MemoryPromotionRecord, error) {
	if trimmed := strings.TrimSpace(promotionID); trimmed != "" {
		record, err := s.getMemoryPromotion(ctx, workspaceID, trimmed, "")
		switch {
		case err == nil:
			return &record, nil
		case strings.Contains(err.Error(), "memory promotion not found"):
		default:
			return nil, err
		}
	}
	if trimmed := strings.TrimSpace(queueKey); trimmed != "" {
		record, err := s.getMemoryPromotion(ctx, workspaceID, "", trimmed)
		switch {
		case err == nil:
			return &record, nil
		case strings.Contains(err.Error(), "memory promotion not found"):
		default:
			return nil, err
		}
	}
	return nil, nil
}

func (s *Store) getMemoryPromotion(ctx context.Context, workspaceID, promotionID, queueKey string) (MemoryPromotionRecord, error) {
	return s.getMemoryPromotionTx(ctx, nil, workspaceID, promotionID, queueKey)
}

func (s *Store) getMemoryPromotionTx(ctx context.Context, tx *sql.Tx, workspaceID, promotionID, queueKey string) (MemoryPromotionRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return MemoryPromotionRecord{}, errors.New("workspace_id is required")
	}
	column := "promotion_id"
	value := strings.TrimSpace(promotionID)
	if value == "" {
		column = "queue_key"
		value = strings.TrimSpace(queueKey)
	}
	if value == "" {
		return MemoryPromotionRecord{}, errors.New("promotion_id or queue_key is required")
	}
	query := `SELECT promotion_id, workspace_id, queue_key, state, candidate_kind, candidate_type,
	        target_memory_id, candidate_json, basis_digest, basis_refs_json, proposed_by, resolution_note,
	        COALESCE(applied_kind,''), COALESCE(applied_id,''), resolved_at, resolved_by, created_at, updated_at
	   FROM memory_promotion_queue
	  WHERE workspace_id = ? AND ` + column + ` = ?
	  LIMIT 1`
	var row interface{ Scan(dest ...any) error }
	if tx != nil {
		row = tx.QueryRowContext(ctx, query, workspaceID, value)
	} else {
		row = s.db.QueryRowContext(ctx, query, workspaceID, value)
	}
	record, err := scanMemoryPromotionRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MemoryPromotionRecord{}, fmt.Errorf("memory promotion not found: %s/%s", workspaceID, value)
		}
		return MemoryPromotionRecord{}, err
	}
	return record, nil
}

func scanMemoryPromotionRecord(scanner interface{ Scan(dest ...any) error }) (MemoryPromotionRecord, error) {
	var record MemoryPromotionRecord
	var candidateJSON string
	var basisRefsJSON string
	var resolvedAt sql.NullString
	var resolvedBy sql.NullString
	if err := scanner.Scan(
		&record.PromotionID,
		&record.WorkspaceID,
		&record.QueueKey,
		&record.State,
		&record.CandidateKind,
		&record.CandidateType,
		&record.TargetMemoryID,
		&candidateJSON,
		&record.BasisDigest,
		&basisRefsJSON,
		&record.ProposedBy,
		&record.ResolutionNote,
		&record.AppliedKind,
		&record.AppliedID,
		&resolvedAt,
		&resolvedBy,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return MemoryPromotionRecord{}, err
	}
	if strings.TrimSpace(candidateJSON) == "" {
		candidateJSON = "{}"
	}
	if err := json.Unmarshal([]byte(candidateJSON), &record.Candidate); err != nil {
		return MemoryPromotionRecord{}, fmt.Errorf("decode memory promotion candidate: %w", err)
	}
	if strings.TrimSpace(basisRefsJSON) == "" {
		basisRefsJSON = "[]"
	}
	if err := json.Unmarshal([]byte(basisRefsJSON), &record.BasisRefs); err != nil {
		return MemoryPromotionRecord{}, fmt.Errorf("decode memory promotion basis refs: %w", err)
	}
	record.ResolvedAt = nullStringPtr(resolvedAt)
	record.ResolvedBy = nullStringPtr(resolvedBy)
	if record.BasisRefs == nil {
		record.BasisRefs = []string{}
	}
	if record.Candidate.Tags == nil {
		record.Candidate.Tags = []string{}
	}
	return record, nil
}

func collectMemoryPromotionRows(rows *sql.Rows) ([]MemoryPromotionRecord, error) {
	items := []MemoryPromotionRecord{}
	for rows.Next() {
		record, err := scanMemoryPromotionRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("scan memory promotion row: %w", err)
		}
		items = append(items, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory promotions: %w", err)
	}
	return items, nil
}

func (s *Store) enrichMemoryPromotionRecord(ctx context.Context, record MemoryPromotionRecord) MemoryPromotionRecord {
	record.CoherenceGate = s.enrichMemoryPromotionCoherenceGate(ctx, record.WorkspaceID, record.Candidate)
	return record
}

func (s *Store) enrichMemoryPromotionRecords(ctx context.Context, items []MemoryPromotionRecord) []MemoryPromotionRecord {
	cache := make(map[string]*MemoryPromotionCoherenceGate, len(items))
	for idx := range items {
		key := memoryPromotionCoherenceGateKey(items[idx].WorkspaceID, items[idx].Candidate)
		if key == "" {
			items[idx].CoherenceGate = nil
			continue
		}
		if gate, ok := cache[key]; ok {
			items[idx].CoherenceGate = gate
			continue
		}
		gate := s.enrichMemoryPromotionCoherenceGate(ctx, items[idx].WorkspaceID, items[idx].Candidate)
		cache[key] = gate
		items[idx].CoherenceGate = gate
	}
	return items
}

func (s *Store) enrichMemoryPromotionCoherenceGate(ctx context.Context, workspaceID string, candidate MemoryPromotionCandidate) *MemoryPromotionCoherenceGate {
	agentID := strings.TrimSpace(candidate.AgentID)
	if agentID == "" {
		return nil
	}
	sessionID := strings.TrimSpace(candidate.SessionID)
	reportScope := "AGENT"
	if sessionID != "" {
		reportScope = "SESSION"
	}
	report, err := s.buildMemoryCoherenceScopeReport(ctx, memoryCoherenceScopeKey{
		WorkspaceID: strings.TrimSpace(workspaceID),
		AgentID:     agentID,
		SessionID:   sessionID,
		ReportScope: reportScope,
	})
	if err != nil || !memoryPromotionHasCoherenceSignal(report) {
		return nil
	}
	return &MemoryPromotionCoherenceGate{
		ReportScope:             report.ReportScope,
		SessionID:               report.SessionID,
		MetricsReportID:         report.MetricsReportID,
		ResidencyReportID:       report.ResidencyReportID,
		CoherenceBand:           report.CoherenceBandHint,
		AdvisoryAction:          memoryPromotionCoherenceAction(report),
		NeedsAttention:          report.NeedsAttention,
		AttentionReasons:        append([]string(nil), report.AttentionReasons...),
		StaleHitRate:            report.StaleHitRate,
		PromotionPrecision:      report.PromotionPrecision,
		StaleReadRate:           report.StaleReadRate,
		InvalidatedReplicaCount: report.InvalidatedReplicaCount,
		OpenInvalidationCount:   report.OpenInvalidationCount,
		ReadyInvalidationCount:  report.ReadyInvalidationCount,
		DeadLetterCount:         report.DeadLetterCount,
		Summary:                 strings.TrimSpace(report.Summary),
	}
}

func memoryPromotionCoherenceGateKey(workspaceID string, candidate MemoryPromotionCandidate) string {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID := strings.TrimSpace(candidate.AgentID)
	sessionID := strings.TrimSpace(candidate.SessionID)
	if workspaceID == "" || agentID == "" {
		return ""
	}
	return workspaceID + "|" + agentID + "|" + sessionID
}

func memoryPromotionHasCoherenceSignal(report MemoryCoherenceScopeReport) bool {
	return strings.TrimSpace(report.MetricsReportID) != "" ||
		strings.TrimSpace(report.ResidencyReportID) != "" ||
		strings.TrimSpace(report.InvalidationUpdatedAt) != "" ||
		report.OpenInvalidationCount > 0 ||
		report.ReadyInvalidationCount > 0 ||
		report.LeasedInvalidationCount > 0 ||
		report.BackoffInvalidationCount > 0 ||
		report.AckedInvalidationCount > 0 ||
		report.DeadLetterCount > 0 ||
		report.InvalidatedReplicaCount > 0
}

func memoryPromotionCoherenceAction(report MemoryCoherenceScopeReport) string {
	switch {
	case report.DeadLetterCount > 0 || report.ReadyInvalidationCount > 0 || strings.EqualFold(strings.TrimSpace(report.CoherenceBandHint), "CRITICAL"):
		return "DEFER_ACCEPT"
	case report.NeedsAttention || report.OpenInvalidationCount > 0 || report.InvalidatedReplicaCount > 0:
		return "REVIEW"
	default:
		return "ALLOW"
	}
}

func memoryPromotionQueueKey(candidateKind string, candidate MemoryPromotionCandidate, basisDigest string, basisRefs []string) string {
	refs := uniqueTrimmedStrings(basisRefs)
	sort.Strings(refs)
	payload := mustJSON(map[string]any{
		"basis_digest":   strings.TrimSpace(basisDigest),
		"basis_refs":     refs,
		"candidate":      candidate,
		"candidate_kind": strings.TrimSpace(candidateKind),
	})
	sum := sha256.Sum256([]byte(payload))
	return "memory-promotion:" + hex.EncodeToString(sum[:])
}

func memoryPromotionEventPayload(record MemoryPromotionRecord) map[string]any {
	payload := map[string]any{
		"promotion_id":     record.PromotionID,
		"queue_key":        record.QueueKey,
		"state":            record.State,
		"candidate_kind":   record.CandidateKind,
		"candidate_type":   record.CandidateType,
		"target_memory_id": record.TargetMemoryID,
		"basis_digest":     record.BasisDigest,
		"basis_refs":       record.BasisRefs,
		"proposed_by":      record.ProposedBy,
		"resolution_note":  record.ResolutionNote,
		"applied_kind":     record.AppliedKind,
		"applied_id":       record.AppliedID,
		"candidate":        record.Candidate,
	}
	if record.CoherenceGate != nil {
		payload["coherence_gate"] = record.CoherenceGate
	}
	return payload
}

func memoryPromotionActorType(record MemoryPromotionRecord) string {
	if strings.TrimSpace(record.Candidate.AgentID) != "" && strings.TrimSpace(record.ProposedBy) == strings.TrimSpace(record.Candidate.AgentID) {
		return "agent"
	}
	return "operator"
}

func memoryPromotionOperatorQueueKey(promotionID string) string {
	promotionID = strings.TrimSpace(promotionID)
	if promotionID == "" {
		return ""
	}
	return "memory_promotion:" + promotionID + ":review"
}

func memoryPromotionOperatorQueueInput(record MemoryPromotionRecord) OperatorQueueUpsertInput {
	lines := []string{
		fmt.Sprintf("Promotion ID: %s", strings.TrimSpace(record.PromotionID)),
		fmt.Sprintf("Candidate kind: %s", strings.TrimSpace(record.CandidateKind)),
		fmt.Sprintf("Candidate type: %s", strings.TrimSpace(record.CandidateType)),
		fmt.Sprintf("Source: %s/%s", strings.TrimSpace(record.Candidate.SourceKind), strings.TrimSpace(record.Candidate.SourceID)),
		fmt.Sprintf("Basis digest: %s", strings.TrimSpace(record.BasisDigest)),
	}
	if len(record.BasisRefs) > 0 {
		lines = append(lines, "Basis refs:")
		for _, ref := range record.BasisRefs {
			lines = append(lines, "- "+ref)
		}
	}
	if summary := strings.TrimSpace(record.Candidate.Summary); summary != "" {
		lines = append(lines, "Summary: "+summary)
	}
	if body := strings.TrimSpace(record.Candidate.Body); body != "" {
		lines = append(lines, "Body: "+body)
	}
	layer := memoryGraphLayerForType(record.CandidateType)
	queueType := "FOLLOW_UP"
	urgency := "NORMAL"
	if layer == "IDENTITY" {
		queueType = "IDENTITY_REVIEW"
		urgency = "LOW"
	} else if layer == "PROCEDURAL" {
		queueType = "PROCEDURAL_REVIEW"
		urgency = "LOW"
	}
	if record.CoherenceGate != nil {
		lines = append(lines, fmt.Sprintf("Coherence gate: %s (%s)", strings.TrimSpace(record.CoherenceGate.AdvisoryAction), firstNonEmpty(strings.TrimSpace(record.CoherenceGate.CoherenceBand), "STABLE")))
		if summary := strings.TrimSpace(record.CoherenceGate.Summary); summary != "" {
			lines = append(lines, "Coherence summary: "+summary)
		}
		if len(record.CoherenceGate.AttentionReasons) > 0 {
			lines = append(lines, "Coherence attention reasons: "+strings.Join(record.CoherenceGate.AttentionReasons, ", "))
		}
		switch strings.TrimSpace(record.CoherenceGate.AdvisoryAction) {
		case "DEFER_ACCEPT":
			if urgency == "LOW" || urgency == "NORMAL" {
				urgency = "HIGH"
			}
		case "REVIEW":
			if urgency == "LOW" {
				urgency = "NORMAL"
			}
		}
	}

	return OperatorQueueUpsertInput{
		WorkspaceID: record.WorkspaceID,
		QueueKey:    memoryPromotionOperatorQueueKey(record.PromotionID),
		QueueType:   queueType,
		Title:       "Review memory promotion",
		Summary:     firstNonEmpty(strings.TrimSpace(record.Candidate.Title), clipSummary(strings.TrimSpace(record.Candidate.Body), 160)),
		Details:     strings.Join(lines, "\n"),
		PayloadJSON: mustJSON(memoryPromotionEventPayload(record)),
		SourceKind:  "memory_promotion",
		SourceID:    strings.TrimSpace(record.PromotionID),
		Urgency:     urgency,
	}
}

func validateProceduralPromotionEvidenceTx(ctx context.Context, tx *sql.Tx, workspaceID string, basisRefs []string) error {
	sessions := extractedDistinctBasisRefIDs(basisRefs, "session:")
	packs := extractedDistinctBasisRefIDs(basisRefs, "episode_pack:")
	tasks := extractedDistinctBasisRefIDs(basisRefs, "task:")
	if len(sessions)+len(packs)+len(tasks) < 2 {
		return errors.New("procedural memory promotion requires cross-episodic evidence (at least 2 distinct session, task, or episode_pack refs)")
	}
	return verifyBasisRefIDsTx(ctx, tx, workspaceID, sessions, packs, tasks)
}

func validateMemoryPromotionAcceptanceEvidenceTx(ctx context.Context, tx *sql.Tx, record MemoryPromotionRecord) error {
	switch memoryGraphLayerForType(record.CandidateType) {
	case "PROCEDURAL":
		if err := validateProceduralPromotionEvidenceTx(ctx, tx, strings.TrimSpace(record.WorkspaceID), record.BasisRefs); err != nil {
			return fmt.Errorf("memory promotion evidence is stale: %w", err)
		}
	case "IDENTITY":
		if err := validateIdentityPromotionEvidenceTx(ctx, tx, strings.TrimSpace(record.WorkspaceID), record.BasisRefs); err != nil {
			return fmt.Errorf("memory promotion evidence is stale: %w", err)
		}
	}
	return nil
}

func (s *Store) validateMemoryPromotionAcceptanceCoherenceGateTx(ctx context.Context, tx *sql.Tx, record MemoryPromotionRecord) (*MemoryPromotionCoherenceGate, bool, error) {
	gate, err := s.buildMemoryPromotionCoherenceGateTx(ctx, tx, record.WorkspaceID, record.Candidate)
	if err != nil {
		return nil, false, fmt.Errorf("memory promotion coherence gate evaluation failed: %w", err)
	}
	if gate == nil {
		return nil, false, nil
	}
	return gate, strings.EqualFold(strings.TrimSpace(gate.AdvisoryAction), "DEFER_ACCEPT"), nil
}

func (s *Store) buildMemoryPromotionCoherenceGateTx(ctx context.Context, tx *sql.Tx, workspaceID string, candidate MemoryPromotionCandidate) (*MemoryPromotionCoherenceGate, error) {
	agentID := strings.TrimSpace(candidate.AgentID)
	if agentID == "" {
		return nil, nil
	}
	sessionID := strings.TrimSpace(candidate.SessionID)
	reportScope := "AGENT"
	if sessionID != "" {
		reportScope = "SESSION"
	}
	report, err := s.buildMemoryCoherenceScopeReportTx(ctx, tx, memoryCoherenceScopeKey{
		WorkspaceID: strings.TrimSpace(workspaceID),
		AgentID:     agentID,
		SessionID:   sessionID,
		ReportScope: reportScope,
	})
	if err != nil {
		return nil, err
	}
	if !memoryPromotionHasCoherenceSignal(report) {
		return nil, nil
	}
	return &MemoryPromotionCoherenceGate{
		ReportScope:             report.ReportScope,
		SessionID:               report.SessionID,
		MetricsReportID:         report.MetricsReportID,
		ResidencyReportID:       report.ResidencyReportID,
		CoherenceBand:           report.CoherenceBandHint,
		AdvisoryAction:          memoryPromotionCoherenceAction(report),
		NeedsAttention:          report.NeedsAttention,
		AttentionReasons:        append([]string(nil), report.AttentionReasons...),
		StaleHitRate:            report.StaleHitRate,
		PromotionPrecision:      report.PromotionPrecision,
		StaleReadRate:           report.StaleReadRate,
		InvalidatedReplicaCount: report.InvalidatedReplicaCount,
		OpenInvalidationCount:   report.OpenInvalidationCount,
		ReadyInvalidationCount:  report.ReadyInvalidationCount,
		DeadLetterCount:         report.DeadLetterCount,
		Summary:                 strings.TrimSpace(report.Summary),
	}, nil
}

func validateIdentityPromotionEvidenceTx(ctx context.Context, tx *sql.Tx, workspaceID string, basisRefs []string) error {
	sessions := extractedDistinctBasisRefIDs(basisRefs, "session:")
	packs := extractedDistinctBasisRefIDs(basisRefs, "episode_pack:")
	tasks := extractedDistinctBasisRefIDs(basisRefs, "task:")
	if len(sessions)+len(packs)+len(tasks) < 3 {
		return errors.New("identity memory promotion requires substantial cross-episodic evidence (at least 3 distinct session, task, or episode_pack refs)")
	}
	return verifyBasisRefIDsTx(ctx, tx, workspaceID, sessions, packs, tasks)
}

func verifyBasisRefIDsTx(ctx context.Context, tx *sql.Tx, workspaceID string, sessions, packs, tasks []string) error {
	for _, id := range sessions {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM agent_sessions WHERE workspace_id = ? AND session_id = ?`, workspaceID, id).Scan(&count); err != nil || count == 0 {
			return fmt.Errorf("invalid evidence: session %s not found in workspace", id)
		}
	}
	for _, id := range packs {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM episode_packs WHERE workspace_id = ? AND episode_pack_id = ?`, workspaceID, id).Scan(&count); err != nil || count == 0 {
			return fmt.Errorf("invalid evidence: episode_pack %s not found in workspace", id)
		}
	}
	for _, id := range tasks {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM workspace_tasks WHERE workspace_id = ? AND task_id = ?`, workspaceID, id).Scan(&count); err != nil || count == 0 {
			return fmt.Errorf("invalid evidence: task %s not found in workspace", id)
		}
	}
	return nil
}

func extractedDistinctBasisRefIDs(refs []string, prefix string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, ref := range refs {
		if strings.HasPrefix(ref, prefix) {
			id := strings.TrimSpace(strings.TrimPrefix(ref, prefix))
			if id != "" {
				if _, ok := seen[id]; !ok {
					seen[id] = struct{}{}
					out = append(out, id)
				}
			}
		}
	}
	return out
}

func normalizeMemoryPromotionCandidateKind(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", memoryPromotionCandidateKindWorkspaceMemory:
		return memoryPromotionCandidateKindWorkspaceMemory
	default:
		return ""
	}
}

func normalizeMemoryPromotionResolution(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case memoryPromotionStateAccepted:
		return memoryPromotionStateAccepted
	case memoryPromotionStateRejected:
		return memoryPromotionStateRejected
	case memoryPromotionStateSuperseded:
		return memoryPromotionStateSuperseded
	case memoryPromotionStateCancelled:
		return memoryPromotionStateCancelled
	default:
		return memoryPromotionStatePending
	}
}

func normalizeMemoryPromotionState(raw string, allowEmpty bool) string {
	if strings.TrimSpace(raw) == "" && allowEmpty {
		return ""
	}
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case memoryPromotionStatePending:
		return memoryPromotionStatePending
	case memoryPromotionStateAccepted:
		return memoryPromotionStateAccepted
	case memoryPromotionStateRejected:
		return memoryPromotionStateRejected
	case memoryPromotionStateSuperseded:
		return memoryPromotionStateSuperseded
	case memoryPromotionStateCancelled:
		return memoryPromotionStateCancelled
	default:
		return ""
	}
}
