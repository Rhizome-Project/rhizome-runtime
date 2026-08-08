package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrStaleEffectiveControlsEpoch = errors.New("effective controls epoch is stale")

const (
	effectiveControlsEventType      = "effective_controls.persisted"
	effectiveControlsEntityType     = "effective_controls"
	effectiveControlsTypedEventType = "EFFECTIVE_CONTROLS_PERSISTED"
)

type EffectiveControlsInput struct {
	WorkspaceID       string
	ProtoClusterID    string
	Epoch             int
	TTLSeconds        int
	ControlMode       string
	CandidateMode     string
	CandidateControls ControlSuggestedControls
	AdvisoryControls  ControlSuggestedControls
	EffectiveControls ControlSuggestedControls
	ResolvedFrom      string
	MatchScore        int
	BasisSummary      string
	GeneratedAt       string
	ActorID           string
}

type EffectiveControlsRecord struct {
	WorkspaceID       string                   `json:"workspace_id"`
	ProtoClusterID    string                   `json:"proto_cluster_id,omitempty"`
	Epoch             int                      `json:"epoch"`
	TTLSeconds        int                      `json:"ttl_seconds"`
	ExpiresAt         string                   `json:"expires_at"`
	ControlMode       string                   `json:"control_mode,omitempty"`
	CandidateMode     string                   `json:"candidate_mode,omitempty"`
	CandidateControls ControlSuggestedControls `json:"candidate_controls"`
	AdvisoryControls  ControlSuggestedControls `json:"advisory_controls"`
	EffectiveControls ControlSuggestedControls `json:"effective_controls"`
	ResolvedFrom      string                   `json:"resolved_from,omitempty"`
	MatchScore        int                      `json:"match_score,omitempty"`
	BasisSummary      string                   `json:"basis_summary,omitempty"`
	GeneratedAt       string                   `json:"generated_at"`
	ActorID           string                   `json:"actor_id,omitempty"`
	CreatedAt         string                   `json:"created_at"`
	UpdatedAt         string                   `json:"updated_at"`
	Expired           bool                     `json:"expired,omitempty"`
	Pending           bool                     `json:"pending,omitempty"`
	TemporalContract  *TemporalHorizonContract `json:"temporal_contract,omitempty"`
}

type EffectiveControlsScopeResolution struct {
	Record      EffectiveControlsRecord `json:"record"`
	Found       bool                    `json:"found"`
	Live        bool                    `json:"live"`
	ScopeSource string                  `json:"scope_source,omitempty"`
}

type effectiveControlsQueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) PersistEffectiveControls(ctx context.Context, input EffectiveControlsInput) (EffectiveControlsRecord, error) {
	record, err := normalizeEffectiveControlsInput(input)
	if err != nil {
		return EffectiveControlsRecord{}, err
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, record.UpdatedAt)
	if err != nil {
		return EffectiveControlsRecord{}, err
	}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
			return err
		}
		existing, err := loadEffectiveControlsRecordWithQueryer(ctx, tx, record.WorkspaceID, record.ProtoClusterID)
		switch {
		case err == nil:
			if effectiveControlsRecordIsStale(existing, record) {
				return fmt.Errorf(
					"%w: existing epoch=%d generated_at=%s incoming epoch=%d generated_at=%s",
					ErrStaleEffectiveControlsEpoch,
					existing.Epoch,
					existing.GeneratedAt,
					record.Epoch,
					record.GeneratedAt,
				)
			}
			record.CreatedAt = existing.CreatedAt
		case errors.Is(err, sql.ErrNoRows):
			// New scope record; allow insert below.
		default:
			return fmt.Errorf("load existing effective controls: %w", err)
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO workspace_effective_controls(
			    workspace_id, proto_cluster_id, epoch, ttl_seconds, expires_at,
			    control_mode, candidate_mode,
			    candidate_controls_json, advisory_controls_json, effective_controls_json,
			    resolved_from, match_score, basis_summary, generated_at, actor_id, created_at, updated_at
			  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			  ON CONFLICT(workspace_id, proto_cluster_id) DO UPDATE SET
			    epoch = excluded.epoch,
			    ttl_seconds = excluded.ttl_seconds,
			    expires_at = excluded.expires_at,
			    control_mode = excluded.control_mode,
			    candidate_mode = excluded.candidate_mode,
			    candidate_controls_json = excluded.candidate_controls_json,
			    advisory_controls_json = excluded.advisory_controls_json,
			    effective_controls_json = excluded.effective_controls_json,
			    resolved_from = excluded.resolved_from,
			    match_score = excluded.match_score,
			    basis_summary = excluded.basis_summary,
			    generated_at = excluded.generated_at,
			    actor_id = excluded.actor_id,
			    updated_at = excluded.updated_at`,
			record.WorkspaceID,
			record.ProtoClusterID,
			record.Epoch,
			record.TTLSeconds,
			record.ExpiresAt,
			record.ControlMode,
			record.CandidateMode,
			mustJSON(record.CandidateControls),
			mustJSON(record.AdvisoryControls),
			mustJSON(record.EffectiveControls),
			record.ResolvedFrom,
			record.MatchScore,
			record.BasisSummary,
			record.GeneratedAt,
			record.ActorID,
			record.CreatedAt,
			record.UpdatedAt,
		); err != nil {
			return fmt.Errorf("upsert effective controls: %w", err)
		}
		if _, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: record.WorkspaceID,
			EventType:   effectiveControlsEventType,
			EntityType:  effectiveControlsEntityType,
			EntityID:    effectiveControlsRuntimeEntityID(record),
			ActorType:   "operator",
			ActorID:     record.ActorID,
			PayloadJSON: mustJSON(effectiveControlsRuntimeEventPayload(record)),
			CreatedAt:   record.UpdatedAt,
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return EffectiveControlsRecord{}, err
	}
	return record, nil
}

func (s *Store) LoadEffectiveControls(ctx context.Context, workspaceID, protoClusterID, referenceAt string) (EffectiveControlsRecord, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	protoClusterID = strings.TrimSpace(protoClusterID)
	referenceAt = strings.TrimSpace(referenceAt)
	if workspaceID == "" {
		return EffectiveControlsRecord{}, false, errors.New("workspace_id is required")
	}
	if referenceAt == "" {
		referenceAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	record, err := s.loadEffectiveControlsRecord(ctx, workspaceID, protoClusterID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EffectiveControlsRecord{}, false, nil
		}
		return EffectiveControlsRecord{}, false, err
	}
	record.Expired = effectiveControlsExpired(record.ExpiresAt, referenceAt)
	record.Pending = effectiveControlsPending(record.GeneratedAt, referenceAt)
	applyEffectiveControlsTemporalContract(&record, referenceAt, "")
	return record, !record.Expired && !record.Pending, nil
}

func (s *Store) LoadEffectiveControlsForScope(ctx context.Context, workspaceID, protoClusterID, referenceAt string) (EffectiveControlsRecord, bool, error) {
	resolution, err := s.ResolveEffectiveControlsScope(ctx, workspaceID, protoClusterID, referenceAt)
	if err != nil {
		return EffectiveControlsRecord{}, false, err
	}
	return resolution.Record, resolution.Live, nil
}

func (s *Store) ResolveEffectiveControlsScope(ctx context.Context, workspaceID, protoClusterID, referenceAt string) (EffectiveControlsScopeResolution, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	protoClusterID = strings.TrimSpace(protoClusterID)
	if workspaceID == "" {
		return EffectiveControlsScopeResolution{}, errors.New("workspace_id is required")
	}
	if protoClusterID != "" {
		record, live, err := s.LoadEffectiveControls(ctx, workspaceID, protoClusterID, referenceAt)
		if err != nil {
			return EffectiveControlsScopeResolution{}, err
		}
		if record.WorkspaceID != "" {
			applyEffectiveControlsTemporalContract(&record, referenceAt, "proto_cluster")
			return EffectiveControlsScopeResolution{
				Record:      record,
				Found:       true,
				Live:        live,
				ScopeSource: "proto_cluster",
			}, nil
		}
	}
	record, live, err := s.LoadEffectiveControls(ctx, workspaceID, "", referenceAt)
	if err != nil {
		return EffectiveControlsScopeResolution{}, err
	}
	if record.WorkspaceID == "" {
		return EffectiveControlsScopeResolution{}, nil
	}
	scopeSource := "workspace"
	if protoClusterID != "" {
		scopeSource = "workspace_fallback"
	}
	applyEffectiveControlsTemporalContract(&record, referenceAt, scopeSource)
	return EffectiveControlsScopeResolution{
		Record:      record,
		Found:       true,
		Live:        live,
		ScopeSource: scopeSource,
	}, nil
}

func (s *Store) loadEffectiveControlsRecord(ctx context.Context, workspaceID, protoClusterID string) (EffectiveControlsRecord, error) {
	return loadEffectiveControlsRecordWithQueryer(ctx, s.db, workspaceID, protoClusterID)
}

func loadEffectiveControlsRecordWithQueryer(ctx context.Context, q effectiveControlsQueryRower, workspaceID, protoClusterID string) (EffectiveControlsRecord, error) {
	var record EffectiveControlsRecord
	var candidateJSON, advisoryJSON, effectiveJSON string
	err := q.QueryRowContext(
		ctx,
		`SELECT workspace_id, proto_cluster_id, epoch, ttl_seconds, expires_at,
		        control_mode, candidate_mode,
		        candidate_controls_json, advisory_controls_json, effective_controls_json,
		        resolved_from, match_score, basis_summary, generated_at, actor_id, created_at, updated_at
		   FROM workspace_effective_controls
		  WHERE workspace_id = ? AND proto_cluster_id = ?`,
		workspaceID,
		protoClusterID,
	).Scan(
		&record.WorkspaceID,
		&record.ProtoClusterID,
		&record.Epoch,
		&record.TTLSeconds,
		&record.ExpiresAt,
		&record.ControlMode,
		&record.CandidateMode,
		&candidateJSON,
		&advisoryJSON,
		&effectiveJSON,
		&record.ResolvedFrom,
		&record.MatchScore,
		&record.BasisSummary,
		&record.GeneratedAt,
		&record.ActorID,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return EffectiveControlsRecord{}, err
	}
	if err := decodeControlSuggestedControls(candidateJSON, &record.CandidateControls); err != nil {
		return EffectiveControlsRecord{}, fmt.Errorf("decode candidate controls: %w", err)
	}
	if err := decodeControlSuggestedControls(advisoryJSON, &record.AdvisoryControls); err != nil {
		return EffectiveControlsRecord{}, fmt.Errorf("decode advisory controls: %w", err)
	}
	if err := decodeControlSuggestedControls(effectiveJSON, &record.EffectiveControls); err != nil {
		return EffectiveControlsRecord{}, fmt.Errorf("decode effective controls: %w", err)
	}
	return record, nil
}

func normalizeEffectiveControlsInput(input EffectiveControlsInput) (EffectiveControlsRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return EffectiveControlsRecord{}, errors.New("workspace_id is required")
	}
	if input.TTLSeconds <= 0 {
		return EffectiveControlsRecord{}, errors.New("ttl_seconds must be > 0")
	}
	generatedAt := strings.TrimSpace(input.GeneratedAt)
	if generatedAt == "" {
		generatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	generatedTS, err := time.Parse(time.RFC3339Nano, generatedAt)
	if err != nil {
		return EffectiveControlsRecord{}, fmt.Errorf("generated_at must be RFC3339Nano: %w", err)
	}
	expiresAt := generatedTS.Add(time.Duration(input.TTLSeconds) * time.Second).UTC().Format(time.RFC3339Nano)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return EffectiveControlsRecord{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    strings.TrimSpace(input.ProtoClusterID),
		Epoch:             maxInt(input.Epoch, 0),
		TTLSeconds:        input.TTLSeconds,
		ExpiresAt:         expiresAt,
		ControlMode:       strings.TrimSpace(input.ControlMode),
		CandidateMode:     strings.TrimSpace(input.CandidateMode),
		CandidateControls: input.CandidateControls,
		AdvisoryControls:  input.AdvisoryControls,
		EffectiveControls: input.EffectiveControls,
		ResolvedFrom:      strings.TrimSpace(input.ResolvedFrom),
		MatchScore:        input.MatchScore,
		BasisSummary:      strings.TrimSpace(input.BasisSummary),
		GeneratedAt:       generatedAt,
		ActorID:           firstNonEmpty(strings.TrimSpace(input.ActorID), "effective.controls"),
		CreatedAt:         now,
		UpdatedAt:         now,
	}, nil
}

func effectiveControlsExpired(expiresAt, referenceAt string) bool {
	expiresAt = strings.TrimSpace(expiresAt)
	referenceAt = strings.TrimSpace(referenceAt)
	if expiresAt == "" {
		return true
	}
	expiryTS, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return true
	}
	referenceTS, err := time.Parse(time.RFC3339Nano, referenceAt)
	if err != nil {
		referenceTS = time.Now().UTC()
	}
	return !expiryTS.After(referenceTS)
}

func effectiveControlsPending(generatedAt, referenceAt string) bool {
	generatedAt = strings.TrimSpace(generatedAt)
	referenceAt = strings.TrimSpace(referenceAt)
	if generatedAt == "" {
		return true
	}
	generatedTS, err := time.Parse(time.RFC3339Nano, generatedAt)
	if err != nil {
		return true
	}
	referenceTS, err := time.Parse(time.RFC3339Nano, referenceAt)
	if err != nil {
		referenceTS = time.Now().UTC()
	}
	return generatedTS.After(referenceTS)
}

func effectiveControlsRecordIsStale(existing, candidate EffectiveControlsRecord) bool {
	if candidate.Epoch < existing.Epoch {
		return true
	}
	if candidate.Epoch > existing.Epoch {
		return false
	}
	existingTS, existingErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(existing.GeneratedAt))
	candidateTS, candidateErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(candidate.GeneratedAt))
	if existingErr != nil || candidateErr != nil {
		return false
	}
	return candidateTS.Before(existingTS)
}

func decodeControlSuggestedControls(raw string, target *ControlSuggestedControls) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "{}"
	}
	return json.Unmarshal([]byte(raw), target)
}

func effectiveControlsRuntimeEntityID(record EffectiveControlsRecord) string {
	if clusterID := strings.TrimSpace(record.ProtoClusterID); clusterID != "" {
		return "proto_cluster:" + clusterID
	}
	return "workspace:" + strings.TrimSpace(record.WorkspaceID)
}

func effectiveControlsRuntimeEventPayload(record EffectiveControlsRecord) map[string]any {
	payload := map[string]any{
		"workspace_id":       record.WorkspaceID,
		"proto_cluster_id":   record.ProtoClusterID,
		"epoch":              record.Epoch,
		"ttl_seconds":        record.TTLSeconds,
		"expires_at":         record.ExpiresAt,
		"control_mode":       record.ControlMode,
		"candidate_mode":     record.CandidateMode,
		"candidate_controls": record.CandidateControls,
		"advisory_controls":  record.AdvisoryControls,
		"effective_controls": record.EffectiveControls,
		"resolved_from":      record.ResolvedFrom,
		"match_score":        record.MatchScore,
		"basis_summary":      record.BasisSummary,
		"generated_at":       record.GeneratedAt,
		"actor_id":           record.ActorID,
		"created_at":         record.CreatedAt,
		"updated_at":         record.UpdatedAt,
		"typed_event_type":   effectiveControlsTypedEventType,
		"event_kind":         effectiveControlsEventType,
	}
	return payload
}
