package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type KnowledgeClaimRelationFilter struct {
	WorkspaceID  string
	ClaimID      string
	FromClaimID  string
	ToClaimID    string
	RelationType string
	Limit        int
}

type KnowledgeClaimRelationRecord struct {
	RelationID   string  `json:"relation_id"`
	WorkspaceID  string  `json:"workspace_id"`
	FromClaimID  string  `json:"from_claim_id"`
	ToClaimID    string  `json:"to_claim_id"`
	RelationType string  `json:"relation_type"`
	Weight       float64 `json:"weight"`
	SourceKind   string  `json:"source_kind"`
	SourceID     string  `json:"source_id,omitempty"`
	CreatedAt    string  `json:"created_at"`
	UpdatedAt    string  `json:"updated_at"`
}

func (s *Store) ListKnowledgeClaimRelations(ctx context.Context, filter KnowledgeClaimRelationFilter) ([]KnowledgeClaimRelationRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if err := validateOptionalKnowledgeClaimRelationType(filter.RelationType); err != nil {
		return nil, err
	}
	query := strings.Builder{}
	query.WriteString(`SELECT relation_id, workspace_id, from_claim_id, to_claim_id, relation_type, weight, source_kind, source_id, created_at, updated_at
	   FROM knowledge_claim_relations
	  WHERE workspace_id = ?`)
	args := []any{filter.WorkspaceID}
	if trimmed := strings.TrimSpace(filter.ClaimID); trimmed != "" {
		query.WriteString(` AND (from_claim_id = ? OR to_claim_id = ?)`)
		args = append(args, trimmed, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.FromClaimID); trimmed != "" {
		query.WriteString(` AND from_claim_id = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.ToClaimID); trimmed != "" {
		query.WriteString(` AND to_claim_id = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.RelationType); trimmed != "" {
		query.WriteString(` AND relation_type = ?`)
		args = append(args, normalizeKnowledgeClaimRelationType(trimmed))
	}
	query.WriteString(` ORDER BY updated_at DESC, relation_id DESC LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list knowledge claim relations: %w", err)
	}
	defer rows.Close()
	return collectKnowledgeClaimRelationRows(rows)
}

func (s *Store) syncKnowledgeClaimRelationsTx(ctx context.Context, tx *sql.Tx, record KnowledgeClaimRecord, now string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM knowledge_claim_relations WHERE workspace_id = ? AND from_claim_id = ?`, record.WorkspaceID, record.ClaimID); err != nil {
		return fmt.Errorf("clear knowledge claim relations: %w", err)
	}
	for _, relation := range derivedKnowledgeClaimRelations(record, now) {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO knowledge_claim_relations(
			    relation_id, workspace_id, from_claim_id, to_claim_id, relation_type, weight, source_kind, source_id, created_at, updated_at
			  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			  ON CONFLICT(workspace_id, from_claim_id, to_claim_id, relation_type) DO UPDATE SET
			    weight = excluded.weight,
			    source_kind = excluded.source_kind,
			    source_id = excluded.source_id,
			    updated_at = excluded.updated_at`,
			relation.RelationID,
			relation.WorkspaceID,
			relation.FromClaimID,
			relation.ToClaimID,
			relation.RelationType,
			relation.Weight,
			relation.SourceKind,
			relation.SourceID,
			relation.CreatedAt,
			relation.UpdatedAt,
		); err != nil {
			return fmt.Errorf("upsert knowledge claim relation: %w", err)
		}
	}
	return nil
}

func (s *Store) listKnowledgeClaimRelationsTx(ctx context.Context, tx *sql.Tx, workspaceID, fromClaimID string) ([]KnowledgeClaimRelationRecord, error) {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT relation_id, workspace_id, from_claim_id, to_claim_id, relation_type, weight, source_kind, source_id, created_at, updated_at
		   FROM knowledge_claim_relations
		  WHERE workspace_id = ? AND from_claim_id = ?
		  ORDER BY updated_at ASC, relation_id ASC`,
		workspaceID,
		fromClaimID,
	)
	if err != nil {
		return nil, fmt.Errorf("list knowledge claim relations tx: %w", err)
	}
	defer rows.Close()
	return collectKnowledgeClaimRelationRows(rows)
}

func collectKnowledgeClaimRelationRows(rows *sql.Rows) ([]KnowledgeClaimRelationRecord, error) {
	out := make([]KnowledgeClaimRelationRecord, 0)
	for rows.Next() {
		var record KnowledgeClaimRelationRecord
		if err := rows.Scan(
			&record.RelationID,
			&record.WorkspaceID,
			&record.FromClaimID,
			&record.ToClaimID,
			&record.RelationType,
			&record.Weight,
			&record.SourceKind,
			&record.SourceID,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan knowledge claim relation: %w", err)
		}
		record.RelationType = normalizeKnowledgeClaimRelationType(record.RelationType)
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate knowledge claim relations: %w", err)
	}
	return out, nil
}

func derivedKnowledgeClaimRelations(record KnowledgeClaimRecord, now string) []KnowledgeClaimRelationRecord {
	type relationKey struct {
		relationType string
		toClaimID    string
	}
	relations := make(map[relationKey]KnowledgeClaimRelationRecord)
	add := func(relationType, toClaimID, sourceKind string, weight float64) {
		relationType = normalizeKnowledgeClaimRelationType(relationType)
		toClaimID = strings.TrimSpace(toClaimID)
		if relationType == "" || toClaimID == "" || toClaimID == strings.TrimSpace(record.ClaimID) {
			return
		}
		key := relationKey{relationType: relationType, toClaimID: toClaimID}
		if existing, ok := relations[key]; ok {
			if weight > existing.Weight {
				existing.Weight = weight
			}
			relations[key] = existing
			return
		}
		relations[key] = KnowledgeClaimRelationRecord{
			RelationID:   stableKnowledgeClaimRelationID(record.WorkspaceID, record.ClaimID, relationType, toClaimID),
			WorkspaceID:  strings.TrimSpace(record.WorkspaceID),
			FromClaimID:  strings.TrimSpace(record.ClaimID),
			ToClaimID:    toClaimID,
			RelationType: relationType,
			Weight:       weight,
			SourceKind:   sourceKind,
			SourceID:     strings.TrimSpace(record.ClaimID),
			CreatedAt:    now,
			UpdatedAt:    now,
		}
	}
	if trimmed := strings.TrimSpace(record.SupersedesClaimID); trimmed != "" {
		add("SUPERSEDES", trimmed, "knowledge_claim_field", 1)
	}
	if trimmed := strings.TrimSpace(record.ConflictsClaimID); trimmed != "" {
		add("CONTRADICTS", trimmed, "knowledge_claim_field", 1)
	}
	for _, evidence := range record.Evidence {
		relationType, targetClaimID, weight, ok := parseKnowledgeClaimRelationEvidence(evidence)
		if !ok {
			continue
		}
		add(relationType, targetClaimID, "knowledge_claim_evidence", weight)
	}
	out := make([]KnowledgeClaimRelationRecord, 0, len(relations))
	for _, relation := range relations {
		out = append(out, relation)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RelationType != out[j].RelationType {
			return out[i].RelationType < out[j].RelationType
		}
		if out[i].ToClaimID != out[j].ToClaimID {
			return out[i].ToClaimID < out[j].ToClaimID
		}
		return out[i].RelationID < out[j].RelationID
	})
	return out
}

func knowledgeClaimRelationTargets(record KnowledgeClaimRecord) []string {
	targets := make([]string, 0, 4)
	if trimmed := strings.TrimSpace(record.SupersedesClaimID); trimmed != "" {
		targets = append(targets, trimmed)
	}
	if trimmed := strings.TrimSpace(record.ConflictsClaimID); trimmed != "" {
		targets = append(targets, trimmed)
	}
	for _, evidence := range record.Evidence {
		_, targetClaimID, _, ok := parseKnowledgeClaimRelationEvidence(evidence)
		if !ok {
			continue
		}
		targetClaimID = strings.TrimSpace(targetClaimID)
		if targetClaimID == "" || targetClaimID == strings.TrimSpace(record.ClaimID) {
			continue
		}
		targets = append(targets, targetClaimID)
	}
	return uniqueTrimmedStrings(targets)
}

func parseKnowledgeClaimRelationEvidence(raw string) (relationType, targetClaimID string, weight float64, ok bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", 0, false
	}
	prefix, remainder, found := strings.Cut(raw, ":")
	if !found {
		return "", "", 0, false
	}
	remainder = strings.TrimSpace(remainder)
	if remainder == "" {
		return "", "", 0, false
	}
	switch strings.ToLower(strings.TrimSpace(prefix)) {
	case "supports":
		return "SUPPORTS", remainder, 0.9, true
	case "validated_by":
		return "VALIDATED_BY", remainder, 0.95, true
	case "blocks":
		return "BLOCKS", remainder, 0.9, true
	case "resolves":
		return "RESOLVES", remainder, 0.9, true
	case "contradicts":
		return "CONTRADICTS", remainder, 0.9, true
	case "supersedes":
		return "SUPERSEDES", remainder, 0.95, true
	default:
		return "", "", 0, false
	}
}

func normalizeKnowledgeClaimRelationType(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "SUPPORTS", "CONTRADICTS", "SUPERSEDES", "VALIDATED_BY", "BLOCKS", "RESOLVES":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return ""
	}
}

func validateOptionalKnowledgeClaimRelationType(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	if normalizeKnowledgeClaimRelationType(raw) == "" {
		return errors.New("relation_type has invalid value")
	}
	return nil
}

func stableKnowledgeClaimRelationID(workspaceID, fromClaimID, relationType, toClaimID string) string {
	payload := strings.Join([]string{
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(fromClaimID),
		normalizeKnowledgeClaimRelationType(relationType),
		strings.TrimSpace(toClaimID),
	}, "\n")
	sum := sha256.Sum256([]byte(payload))
	return "claimrel:" + hex.EncodeToString(sum[:16])
}
