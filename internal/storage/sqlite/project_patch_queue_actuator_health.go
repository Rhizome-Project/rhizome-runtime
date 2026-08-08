package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	ProjectPatchQueueActuatorHealthContract     = "project_patch_queue_actuator_health.v1"
	ProjectPatchQueueActuatorStartedStaleAfter  = 2 * time.Minute
	projectPatchQueueActuatorHealthExampleLimit = 5
)

type ProjectPatchQueueActuatorHealthSnapshot struct {
	Contract                     string                                   `json:"contract,omitempty"`
	State                        string                                   `json:"state,omitempty"`
	Message                      string                                   `json:"message,omitempty"`
	ReferenceAt                  string                                   `json:"reference_at,omitempty"`
	StartedStaleAfterMillis      int64                                    `json:"started_stale_after_millis"`
	StartedEventCount            int                                      `json:"started_event_count"`
	AppliedEventCount            int                                      `json:"applied_event_count"`
	OpenStartedCount             int                                      `json:"open_started_count"`
	StaleOpenStartedCount        int                                      `json:"stale_open_started_count"`
	MalformedStartedPayloadCount int                                      `json:"malformed_started_payload_count"`
	MalformedStartedAtCount      int                                      `json:"malformed_started_at_count"`
	OldestOpenStartedAt          string                                   `json:"oldest_open_started_at,omitempty"`
	OldestStaleOpenStartedAt     string                                   `json:"oldest_stale_open_started_at,omitempty"`
	OpenStartedExamples          []ProjectPatchQueueActuatorHealthExample `json:"open_started_examples,omitempty"`
	StaleOpenStartedExamples     []ProjectPatchQueueActuatorHealthExample `json:"stale_open_started_examples,omitempty"`
	Error                        string                                   `json:"error,omitempty"`
}

type ProjectPatchQueueActuatorHealthExample struct {
	WorkspaceID                         string `json:"workspace_id,omitempty"`
	ProjectID                           string `json:"project_id,omitempty"`
	RepoID                              string `json:"repo_id,omitempty"`
	QueueID                             string `json:"queue_id,omitempty"`
	ItemID                              string `json:"item_id,omitempty"`
	EntityID                            string `json:"entity_id,omitempty"`
	TargetCheckoutID                    string `json:"target_checkout_id,omitempty"`
	TargetBranchName                    string `json:"target_branch_name,omitempty"`
	ActivationDigest                    string `json:"activation_digest,omitempty"`
	MaterializationDigest               string `json:"materialization_digest,omitempty"`
	MaterializationAuthorityProofDigest string `json:"materialization_authority_proof_digest,omitempty"`
	StartedAt                           string `json:"started_at,omitempty"`
	AgeMillis                           int64  `json:"age_millis,omitempty"`
}

type projectPatchQueueActuatorStartedPayload struct {
	WorkspaceID                         string `json:"workspace_id"`
	ProjectID                           string `json:"project_id"`
	RepoID                              string `json:"repo_id"`
	QueueID                             string `json:"queue_id"`
	ItemID                              string `json:"item_id"`
	TargetCheckoutID                    string `json:"target_checkout_id"`
	TargetBranchName                    string `json:"target_branch_name"`
	ActivationDigest                    string `json:"activation_digest"`
	MaterializationDigest               string `json:"materialization_digest"`
	MaterializationAuthorityProofDigest string `json:"materialization_authority_proof_digest"`
}

func (s *Store) CurrentProjectPatchQueueActuatorHealthSnapshot(ctx context.Context) ProjectPatchQueueActuatorHealthSnapshot {
	referenceAt := time.Now().UTC()
	snapshot := ProjectPatchQueueActuatorHealthSnapshot{
		Contract:                ProjectPatchQueueActuatorHealthContract,
		State:                   "ok",
		Message:                 "repo mutation actuator journal has no stale started events",
		ReferenceAt:             referenceAt.Format(time.RFC3339Nano),
		StartedStaleAfterMillis: ProjectPatchQueueActuatorStartedStaleAfter.Milliseconds(),
	}
	if s == nil {
		snapshot.State = "unsupported"
		snapshot.Message = "sqlite store unavailable"
		return snapshot
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if err := s.populateProjectPatchQueueActuatorHealthCounts(ctx, &snapshot); err != nil {
		snapshot.State = "error"
		snapshot.Message = "repo mutation actuator journal health query failed"
		snapshot.Error = err.Error()
		return snapshot
	}
	if err := s.populateProjectPatchQueueActuatorOpenStarts(ctx, &snapshot, referenceAt); err != nil {
		snapshot.State = "error"
		snapshot.Message = "repo mutation actuator open-start query failed"
		snapshot.Error = err.Error()
		return snapshot
	}

	if snapshot.MalformedStartedPayloadCount > 0 ||
		snapshot.MalformedStartedAtCount > 0 ||
		snapshot.StaleOpenStartedCount > 0 {
		snapshot.State = "degraded"
		parts := make([]string, 0, 5)
		if snapshot.StaleOpenStartedCount > 0 {
			parts = append(parts, fmt.Sprintf("stale_started=%d", snapshot.StaleOpenStartedCount))
		}
		if snapshot.OpenStartedCount > 0 {
			parts = append(parts, fmt.Sprintf("open_started=%d", snapshot.OpenStartedCount))
		}
		if snapshot.MalformedStartedPayloadCount > 0 {
			parts = append(parts, fmt.Sprintf("malformed_started_payload=%d", snapshot.MalformedStartedPayloadCount))
		}
		if snapshot.MalformedStartedAtCount > 0 {
			parts = append(parts, fmt.Sprintf("malformed_started_at=%d", snapshot.MalformedStartedAtCount))
		}
		if snapshot.OldestStaleOpenStartedAt != "" {
			parts = append(parts, "oldest_stale_started_at="+snapshot.OldestStaleOpenStartedAt)
		}
		snapshot.Message = "repo mutation actuator journal degraded: " + strings.Join(parts, ", ")
		return snapshot
	}
	if snapshot.OpenStartedCount > 0 {
		snapshot.Message = fmt.Sprintf("repo mutation actuator has %d in-flight started event(s) within the recovery window", snapshot.OpenStartedCount)
	}
	return snapshot
}

func (s *Store) populateProjectPatchQueueActuatorHealthCounts(ctx context.Context, snapshot *ProjectPatchQueueActuatorHealthSnapshot) error {
	if snapshot == nil {
		return nil
	}
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM runtime_events
 WHERE event_type = ?
   AND entity_type = 'project_patch_queue_item'`,
		ProjectPatchQueueActuatorStartedEventType,
	).Scan(&snapshot.StartedEventCount); err != nil {
		return fmt.Errorf("count actuator started events: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM runtime_events
 WHERE event_type = ?
   AND entity_type = 'project_patch_queue_item'`,
		ProjectPatchQueueActuatorAppliedEventType,
	).Scan(&snapshot.AppliedEventCount); err != nil {
		return fmt.Errorf("count actuator applied events: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM runtime_events
 WHERE event_type = ?
   AND entity_type = 'project_patch_queue_item'
   AND CASE
         WHEN json_valid(payload_json) = 0 THEN 1
         WHEN TRIM(COALESCE(json_extract(payload_json, '$.materialization_digest'), '')) = '' THEN 1
         ELSE 0
       END = 1`,
		ProjectPatchQueueActuatorStartedEventType,
	).Scan(&snapshot.MalformedStartedPayloadCount); err != nil {
		return fmt.Errorf("count malformed actuator started payloads: %w", err)
	}
	return nil
}

func (s *Store) populateProjectPatchQueueActuatorOpenStarts(ctx context.Context, snapshot *ProjectPatchQueueActuatorHealthSnapshot, referenceAt time.Time) error {
	if snapshot == nil {
		return nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT s.workspace_id, s.entity_id, s.created_at, s.payload_json
  FROM runtime_events s
 WHERE s.event_type = ?
   AND s.entity_type = 'project_patch_queue_item'
   AND json_valid(s.payload_json) = 1
   AND TRIM(COALESCE(json_extract(s.payload_json, '$.materialization_digest'), '')) <> ''
   AND NOT EXISTS (
       SELECT 1
         FROM runtime_events a
        WHERE a.event_type = ?
          AND a.entity_type = s.entity_type
          AND a.workspace_id = s.workspace_id
          AND a.entity_id = s.entity_id
          AND json_valid(a.payload_json) = 1
          AND TRIM(COALESCE(json_extract(a.payload_json, '$.materialization_digest'), '')) =
              TRIM(COALESCE(json_extract(s.payload_json, '$.materialization_digest'), ''))
   )
 ORDER BY s.created_at ASC, s.event_id ASC
 LIMIT 64`,
		ProjectPatchQueueActuatorStartedEventType,
		ProjectPatchQueueActuatorAppliedEventType,
	)
	if err != nil {
		return fmt.Errorf("query open actuator started events: %w", err)
	}
	defer rows.Close()

	var oldestOpen time.Time
	var oldestStale time.Time
	for rows.Next() {
		var workspaceID string
		var entityID string
		var createdAtRaw string
		var payloadJSON sql.NullString
		if err := rows.Scan(&workspaceID, &entityID, &createdAtRaw, &payloadJSON); err != nil {
			return fmt.Errorf("scan open actuator started event: %w", err)
		}
		snapshot.OpenStartedCount++
		startedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(createdAtRaw))
		if err != nil {
			snapshot.MalformedStartedAtCount++
			continue
		}
		if oldestOpen.IsZero() || startedAt.Before(oldestOpen) {
			oldestOpen = startedAt
		}
		example := projectPatchQueueActuatorHealthExampleFromPayload(workspaceID, entityID, startedAt, referenceAt, payloadJSON.String)
		snapshot.OpenStartedExamples = appendProjectPatchQueueActuatorHealthExample(snapshot.OpenStartedExamples, example)
		if referenceAt.Sub(startedAt) <= ProjectPatchQueueActuatorStartedStaleAfter {
			continue
		}
		snapshot.StaleOpenStartedCount++
		if oldestStale.IsZero() || startedAt.Before(oldestStale) {
			oldestStale = startedAt
		}
		snapshot.StaleOpenStartedExamples = appendProjectPatchQueueActuatorHealthExample(snapshot.StaleOpenStartedExamples, example)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate open actuator started events: %w", err)
	}
	if !oldestOpen.IsZero() {
		snapshot.OldestOpenStartedAt = oldestOpen.UTC().Format(time.RFC3339Nano)
	}
	if !oldestStale.IsZero() {
		snapshot.OldestStaleOpenStartedAt = oldestStale.UTC().Format(time.RFC3339Nano)
	}
	return nil
}

func appendProjectPatchQueueActuatorHealthExample(examples []ProjectPatchQueueActuatorHealthExample, example ProjectPatchQueueActuatorHealthExample) []ProjectPatchQueueActuatorHealthExample {
	if len(examples) >= projectPatchQueueActuatorHealthExampleLimit {
		return examples
	}
	return append(examples, example)
}

func projectPatchQueueActuatorHealthExampleFromPayload(workspaceID, entityID string, startedAt, referenceAt time.Time, payloadJSON string) ProjectPatchQueueActuatorHealthExample {
	var payload projectPatchQueueActuatorStartedPayload
	_ = json.Unmarshal([]byte(payloadJSON), &payload)
	ageMillis := referenceAt.Sub(startedAt).Milliseconds()
	if ageMillis < 0 {
		ageMillis = 0
	}
	return ProjectPatchQueueActuatorHealthExample{
		WorkspaceID:                         firstNonEmptyString(strings.TrimSpace(payload.WorkspaceID), strings.TrimSpace(workspaceID)),
		ProjectID:                           strings.TrimSpace(payload.ProjectID),
		RepoID:                              strings.TrimSpace(payload.RepoID),
		QueueID:                             strings.TrimSpace(payload.QueueID),
		ItemID:                              strings.TrimSpace(payload.ItemID),
		EntityID:                            strings.TrimSpace(entityID),
		TargetCheckoutID:                    strings.TrimSpace(payload.TargetCheckoutID),
		TargetBranchName:                    strings.TrimSpace(payload.TargetBranchName),
		ActivationDigest:                    strings.TrimSpace(payload.ActivationDigest),
		MaterializationDigest:               strings.TrimSpace(payload.MaterializationDigest),
		MaterializationAuthorityProofDigest: strings.TrimSpace(payload.MaterializationAuthorityProofDigest),
		StartedAt:                           startedAt.UTC().Format(time.RFC3339Nano),
		AgeMillis:                           ageMillis,
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
