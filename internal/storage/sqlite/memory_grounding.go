package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type MemoryGraphVersionStatus struct {
	RefKind             string  `json:"ref_kind"`
	RefID               string  `json:"ref_id"`
	StoredVersionToken  string  `json:"stored_version_token,omitempty"`
	CurrentVersionToken string  `json:"current_version_token,omitempty"`
	Weight              float64 `json:"weight"`
	State               string  `json:"state"`
}

type MemoryGraphDriftReport struct {
	TimeAuthority      WorkspaceTimeAuthority     `json:"time_authority"`
	Status             string                     `json:"status"`
	Drift              float64                    `json:"drift"`
	ComparedRefCount   int                        `json:"compared_ref_count"`
	DriftedRefCount    int                        `json:"drifted_ref_count"`
	MissingRefCount    int                        `json:"missing_ref_count"`
	UnresolvedRefCount int                        `json:"unresolved_ref_count"`
	EvaluatedAt        string                     `json:"evaluated_at"`
	Items              []MemoryGraphVersionStatus `json:"items,omitempty"`
}

type memoryGraphGrounding struct {
	refs     []MemoryGraphNodeRefInput
	versions []MemoryGraphNodeVersionInput
	edges    []MemoryGraphEdgeInput
}

func workspaceArtifactVersionToken(record WorkspaceArtifactRecord) string {
	return firstNonEmpty(
		strings.TrimSpace(record.ArtifactID)+"@"+strings.TrimSpace(record.CreatedAt),
		strings.TrimSpace(record.CreatedAt),
		strings.TrimSpace(record.ArtifactID),
	)
}

func memoryGraphSegmentVersionToken(segmentRef, sourceVersion string) string {
	return strings.TrimSpace(sourceVersion) + "::" + strings.TrimSpace(segmentRef)
}

func memoryGraphSyntheticSegmentNodeID(segmentRef string) string {
	return "memnode:workspace_segment:" + strings.TrimSpace(segmentRef)
}

func normalizeMemoryGraphGroundingSourceKind(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "workspace_doc", "doc", "document":
		return "workspace_doc"
	case "artifact_ref", "artifact", "workspace_artifact":
		return "artifact_ref"
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

func extractMemoryGroundingDocKeys(sourceKind, sourceID string) []string {
	if normalizeMemoryGraphGroundingSourceKind(sourceKind) != "workspace_doc" {
		return nil
	}
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return nil
	}
	if strings.HasPrefix(sourceID, "workspace_doc:") {
		if idx := strings.Index(sourceID, "/"); idx >= 0 && idx+1 < len(sourceID) {
			rest := sourceID[idx+1:]
			parts := strings.SplitN(rest, "#", 2)
			if len(parts) > 0 {
				return uniqueSortedStrings([]string{strings.TrimSpace(parts[0])})
			}
		}
	}
	return uniqueSortedStrings([]string{sourceID})
}

func extractMemoryGroundingArtifactRefs(sourceKind, sourceID string) []string {
	if normalizeMemoryGraphGroundingSourceKind(sourceKind) != "artifact_ref" {
		return nil
	}
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return nil
	}
	if strings.HasPrefix(sourceID, "artifact:") {
		if idx := strings.Index(sourceID, "/"); idx >= 0 && idx+1 < len(sourceID) {
			rest := sourceID[idx+1:]
			parts := strings.SplitN(rest, "#", 2)
			if len(parts) > 0 {
				return uniqueSortedStrings([]string{strings.TrimSpace(parts[0])})
			}
		}
	}
	return uniqueSortedStrings([]string{sourceID})
}

func (s *Store) loadWorkspaceDocTx(ctx context.Context, tx *sql.Tx, workspaceID, docKey string) (WorkspaceDocRecord, error) {
	var record WorkspaceDocRecord
	var archivedAt sql.NullString
	var archivedBy sql.NullString
	if err := tx.QueryRowContext(
		ctx,
		`SELECT doc_key, title, content, updated_by, created_at, updated_at, archived_at, archived_by
		   FROM workspace_docs
		  WHERE workspace_id = ? AND doc_key = ?`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(docKey),
	).Scan(
		&record.DocKey,
		&record.Title,
		&record.Content,
		&record.UpdatedBy,
		&record.CreatedAt,
		&record.UpdatedAt,
		&archivedAt,
		&archivedBy,
	); err != nil {
		return WorkspaceDocRecord{}, err
	}
	record.ArchivedAt = nullStringPtr(archivedAt)
	record.ArchivedBy = nullStringPtr(archivedBy)
	record.SHA = contentSHA256(record.Content)
	return record, nil
}

func (s *Store) loadWorkspaceMemoryTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string) (WorkspaceMemoryRecord, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT memory_id, workspace_id, memory_type, title, body, summary,
		        COALESCE(agent_id,''), COALESCE(session_id,''), COALESCE(task_id,''),
		        source_kind, COALESCE(source_id,''), tags_json, importance, confidence,
		        created_at, updated_at, archived_at, archived_by, COALESCE(archived_reason,''), COALESCE(recovery_reason,'')
		   FROM workspace_memory
		  WHERE workspace_id = ? AND memory_id = ?`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(memoryID),
	)
	return scanWorkspaceMemoryRecord(row)
}

func (s *Store) loadKnowledgeClaimRelationByIDTx(ctx context.Context, tx *sql.Tx, workspaceID, relationID string) (KnowledgeClaimRelationRecord, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT relation_id, workspace_id, from_claim_id, to_claim_id, relation_type, weight, source_kind, source_id, created_at, updated_at
		   FROM knowledge_claim_relations
		  WHERE workspace_id = ? AND relation_id = ?`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(relationID),
	)
	return scanKnowledgeClaimRelationRecord(row)
}

func (s *Store) loadKnowledgeClaimRelationByID(ctx context.Context, workspaceID, relationID string) (KnowledgeClaimRelationRecord, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT relation_id, workspace_id, from_claim_id, to_claim_id, relation_type, weight, source_kind, source_id, created_at, updated_at
		   FROM knowledge_claim_relations
		  WHERE workspace_id = ? AND relation_id = ?`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(relationID),
	)
	return scanKnowledgeClaimRelationRecord(row)
}

func (s *Store) currentWorkspaceDocVersionTokenTx(ctx context.Context, tx *sql.Tx, workspaceID, docKey string) (string, bool, error) {
	record, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, docKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("load workspace doc token: %w", err)
	}
	return strings.TrimSpace(record.SHA), true, nil
}

func (s *Store) currentWorkspaceArtifactVersionTokenTx(ctx context.Context, tx *sql.Tx, workspaceID, artifactRef string) (string, bool, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT artifact_id, workspace_id, task_id, update_id, title, artifact_ref, kind, content_type, created_by, metadata_json, created_at
		   FROM workspace_artifacts
		  WHERE workspace_id = ? AND artifact_ref = ?
		  ORDER BY created_at DESC, artifact_id DESC
		  LIMIT 1`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(artifactRef),
	)
	var record WorkspaceArtifactRecord
	var taskID, updateID sql.NullString
	if err := row.Scan(
		&record.ArtifactID,
		&record.WorkspaceID,
		&taskID,
		&updateID,
		&record.Title,
		&record.ArtifactRef,
		&record.Kind,
		&record.ContentType,
		&record.CreatedBy,
		&record.MetadataJSON,
		&record.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("load workspace artifact token: %w", err)
	}
	record.TaskID = nullStringPtr(taskID)
	record.UpdateID = nullStringPtr(updateID)
	return workspaceArtifactVersionToken(record), true, nil
}

func (s *Store) currentWorkspaceMemoryVersionTokenTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string) (string, bool, error) {
	record, err := s.loadWorkspaceMemoryTx(ctx, tx, workspaceID, memoryID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("load workspace memory token: %w", err)
	}
	return firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt)), true, nil
}

func (s *Store) currentKnowledgeClaimVersionTokenTx(ctx context.Context, tx *sql.Tx, workspaceID, claimID string) (string, bool, error) {
	record, err := s.loadKnowledgeClaimRecordTx(ctx, tx, workspaceID, claimID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("load knowledge claim token: %w", err)
	}
	return firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt)), true, nil
}

func (s *Store) currentKnowledgeClaimRelationVersionTokenTx(ctx context.Context, tx *sql.Tx, workspaceID, relationID string) (string, bool, error) {
	record, err := s.loadKnowledgeClaimRelationByIDTx(ctx, tx, workspaceID, relationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("load knowledge claim relation token: %w", err)
	}
	return firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt)), true, nil
}

func (s *Store) currentWorkspaceDocVersionToken(ctx context.Context, workspaceID, docKey string) (string, bool, error) {
	record, err := s.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			return "", false, nil
		}
		return "", false, err
	}
	return strings.TrimSpace(record.SHA), true, nil
}

func (s *Store) currentWorkspaceArtifactVersionToken(ctx context.Context, workspaceID, artifactRef string) (string, bool, error) {
	record, err := s.loadWorkspaceArtifactByRef(ctx, workspaceID, artifactRef)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			return "", false, nil
		}
		return "", false, err
	}
	return workspaceArtifactVersionToken(record), true, nil
}

func (s *Store) currentWorkspaceMemoryVersionToken(ctx context.Context, workspaceID, memoryID string) (string, bool, error) {
	record, err := s.GetWorkspaceMemory(ctx, workspaceID, memoryID)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			return "", false, nil
		}
		return "", false, err
	}
	return firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt)), true, nil
}

func (s *Store) currentKnowledgeClaimVersionToken(ctx context.Context, workspaceID, claimID string) (string, bool, error) {
	record, err := s.GetKnowledgeClaim(ctx, workspaceID, claimID)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			return "", false, nil
		}
		return "", false, err
	}
	return firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt)), true, nil
}

func (s *Store) currentKnowledgeClaimRelationVersionToken(ctx context.Context, workspaceID, relationID string) (string, bool, error) {
	record, err := s.loadKnowledgeClaimRelationByID(ctx, workspaceID, relationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	return firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt)), true, nil
}

func (s *Store) currentSegmentVersionToken(ctx context.Context, workspaceID, segmentRef string) (string, bool, error) {
	sourceKind, sourceRef, err := parseWorkspaceSegmentRef(workspaceID, segmentRef)
	if err != nil {
		return "", false, nil
	}
	switch sourceKind {
	case "workspace_doc":
		token, ok, err := s.currentWorkspaceDocVersionToken(ctx, workspaceID, sourceRef)
		if err != nil || !ok {
			return "", ok, err
		}
		return memoryGraphSegmentVersionToken(segmentRef, token), true, nil
	case "workspace_artifact":
		token, ok, err := s.currentWorkspaceArtifactVersionToken(ctx, workspaceID, sourceRef)
		if err != nil || !ok {
			return "", ok, err
		}
		return memoryGraphSegmentVersionToken(segmentRef, token), true, nil
	default:
		return "", false, nil
	}
}

func (s *Store) currentSegmentVersionTokenTx(ctx context.Context, tx *sql.Tx, workspaceID, segmentRef string) (string, bool, error) {
	sourceKind, sourceRef, err := parseWorkspaceSegmentRef(workspaceID, segmentRef)
	if err != nil {
		return "", false, nil
	}
	switch sourceKind {
	case "workspace_doc":
		token, ok, err := s.currentWorkspaceDocVersionTokenTx(ctx, tx, workspaceID, sourceRef)
		if err != nil || !ok {
			return "", ok, err
		}
		return memoryGraphSegmentVersionToken(segmentRef, token), true, nil
	case "workspace_artifact":
		token, ok, err := s.currentWorkspaceArtifactVersionTokenTx(ctx, tx, workspaceID, sourceRef)
		if err != nil || !ok {
			return "", ok, err
		}
		return memoryGraphSegmentVersionToken(segmentRef, token), true, nil
	default:
		return "", false, nil
	}
}

func (s *Store) appendMemoryGraphGroundingFromSourceTx(ctx context.Context, tx *sql.Tx, grounding *memoryGraphGrounding, workspaceID, nodeID, originKind, originID, sourceKind, sourceID, role string, weight float64) error {
	switch normalizeMemoryGraphGroundingSourceKind(sourceKind) {
	case "workspace_doc":
		for _, docKey := range extractMemoryGroundingDocKeys(sourceKind, sourceID) {
			if err := s.appendMemoryGraphGroundedDocTx(ctx, tx, grounding, workspaceID, nodeID, originKind, originID, docKey, role, weight); err != nil {
				return err
			}
		}
	case "artifact_ref":
		for _, artifactRef := range extractMemoryGroundingArtifactRefs(sourceKind, sourceID) {
			if err := s.appendMemoryGraphGroundedArtifactTx(ctx, tx, grounding, workspaceID, nodeID, originKind, originID, artifactRef, role, weight); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) appendMemoryGraphGroundedDocTx(ctx context.Context, tx *sql.Tx, grounding *memoryGraphGrounding, workspaceID, nodeID, originKind, originID, docKey, role string, weight float64) error {
	docKey = strings.TrimSpace(docKey)
	if docKey == "" {
		return nil
	}
	versionToken, exists, err := s.currentWorkspaceDocVersionTokenTx(ctx, tx, workspaceID, docKey)
	if err != nil {
		return err
	}
	grounding.refs = append(grounding.refs, MemoryGraphNodeRefInput{
		MemoryID:     nodeID,
		WorkspaceID:  workspaceID,
		RefKind:      "workspace_doc",
		RefID:        docKey,
		RefRole:      role,
		Weight:       weight,
		MetadataJSON: "{}",
	})
	if exists {
		grounding.versions = append(grounding.versions, MemoryGraphNodeVersionInput{
			MemoryID:     nodeID,
			WorkspaceID:  workspaceID,
			RefKind:      "workspace_doc",
			RefID:        docKey,
			VersionToken: versionToken,
			Weight:       weight,
		})
	}
	segmentRef := buildWorkspaceDocSegmentRef(workspaceID, docKey, "root")
	grounding.refs = append(grounding.refs, MemoryGraphNodeRefInput{
		MemoryID:    nodeID,
		WorkspaceID: workspaceID,
		RefKind:     "segment_ref",
		RefID:       segmentRef,
		RefRole:     "about_root",
		RefValue:    docKey,
		Weight:      weight,
		MetadataJSON: encodeStringMapJSON(map[string]any{
			"source_kind":     "workspace_doc",
			"segment_kind":    "root",
			"grounding_scope": "coarse_root",
		}),
	})
	return nil
}

func (s *Store) appendMemoryGraphGroundedArtifactTx(ctx context.Context, tx *sql.Tx, grounding *memoryGraphGrounding, workspaceID, nodeID, originKind, originID, artifactRef, role string, weight float64) error {
	artifactRef = strings.TrimSpace(artifactRef)
	if artifactRef == "" {
		return nil
	}
	versionToken, exists, err := s.currentWorkspaceArtifactVersionTokenTx(ctx, tx, workspaceID, artifactRef)
	if err != nil {
		return err
	}
	grounding.refs = append(grounding.refs, MemoryGraphNodeRefInput{
		MemoryID:     nodeID,
		WorkspaceID:  workspaceID,
		RefKind:      "artifact_ref",
		RefID:        artifactRef,
		RefRole:      role,
		Weight:       weight,
		MetadataJSON: "{}",
	})
	if exists {
		grounding.versions = append(grounding.versions, MemoryGraphNodeVersionInput{
			MemoryID:     nodeID,
			WorkspaceID:  workspaceID,
			RefKind:      "artifact_ref",
			RefID:        artifactRef,
			VersionToken: versionToken,
			Weight:       weight,
		})
	}
	segmentRef := buildWorkspaceArtifactSegmentRef(workspaceID, artifactRef, "root")
	grounding.refs = append(grounding.refs, MemoryGraphNodeRefInput{
		MemoryID:    nodeID,
		WorkspaceID: workspaceID,
		RefKind:     "segment_ref",
		RefID:       segmentRef,
		RefRole:     "about_root",
		RefValue:    artifactRef,
		Weight:      weight,
		MetadataJSON: encodeStringMapJSON(map[string]any{
			"source_kind":     "artifact_ref",
			"segment_kind":    "root",
			"grounding_scope": "coarse_root",
		}),
	})
	return nil
}

func (s *Store) memoryGraphGroundingForWorkspaceMemoryTx(ctx context.Context, tx *sql.Tx, nodeID string, record WorkspaceMemoryRecord) (memoryGraphGrounding, error) {
	grounding := memoryGraphGrounding{}
	if err := s.appendMemoryGraphGroundingFromSourceTx(ctx, tx, &grounding, record.WorkspaceID, nodeID, "workspace_memory", record.MemoryID, strings.TrimSpace(record.SourceKind), strings.TrimSpace(record.SourceID), "source_grounding", 1); err != nil {
		return memoryGraphGrounding{}, err
	}
	return grounding, nil
}

func (s *Store) memoryGraphAnchorVersionsForWorkspaceMemoryTx(ctx context.Context, tx *sql.Tx, nodeID string, record WorkspaceMemoryRecord) ([]MemoryGraphNodeVersionInput, error) {
	versions := make([]MemoryGraphNodeVersionInput, 0, 2)
	if versionToken, exists, err := s.currentWorkspaceMemoryVersionTokenTx(ctx, tx, record.WorkspaceID, record.MemoryID); err != nil {
		return nil, err
	} else if exists {
		versions = append(versions, MemoryGraphNodeVersionInput{
			MemoryID:     nodeID,
			WorkspaceID:  record.WorkspaceID,
			RefKind:      "workspace_memory",
			RefID:        strings.TrimSpace(record.MemoryID),
			VersionToken: versionToken,
			Weight:       1,
		})
	}
	switch normalizeMemoryGraphGroundingSourceKind(record.SourceKind) {
	case "workspace_doc":
		for _, docKey := range extractMemoryGroundingDocKeys(record.SourceKind, record.SourceID) {
			versionToken, exists, err := s.currentWorkspaceDocVersionTokenTx(ctx, tx, record.WorkspaceID, docKey)
			if err != nil {
				return nil, err
			}
			if !exists {
				continue
			}
			versions = append(versions, MemoryGraphNodeVersionInput{
				MemoryID:     nodeID,
				WorkspaceID:  record.WorkspaceID,
				RefKind:      "workspace_doc",
				RefID:        docKey,
				VersionToken: versionToken,
				Weight:       1,
			})
		}
	case "artifact_ref":
		for _, artifactRef := range extractMemoryGroundingArtifactRefs(record.SourceKind, record.SourceID) {
			versionToken, exists, err := s.currentWorkspaceArtifactVersionTokenTx(ctx, tx, record.WorkspaceID, artifactRef)
			if err != nil {
				return nil, err
			}
			if !exists {
				continue
			}
			versions = append(versions, MemoryGraphNodeVersionInput{
				MemoryID:     nodeID,
				WorkspaceID:  record.WorkspaceID,
				RefKind:      "artifact_ref",
				RefID:        artifactRef,
				VersionToken: versionToken,
				Weight:       1,
			})
		}
	}
	return uniqueMemoryGraphNodeVersions(versions), nil
}

func (s *Store) memoryGraphGroundingForKnowledgeClaimTx(ctx context.Context, tx *sql.Tx, nodeID string, record KnowledgeClaimRecord, relations []KnowledgeClaimRelationRecord) (memoryGraphGrounding, error) {
	grounding := memoryGraphGrounding{}
	if err := s.appendMemoryGraphGroundingFromSourceTx(ctx, tx, &grounding, record.WorkspaceID, nodeID, "knowledge_claim", record.ClaimID, strings.TrimSpace(record.SourceKind), strings.TrimSpace(record.SourceID), "source_grounding", 1); err != nil {
		return memoryGraphGrounding{}, err
	}
	if strings.TrimSpace(record.MemoryID) != "" {
		backing, err := s.loadWorkspaceMemoryTx(ctx, tx, record.WorkspaceID, record.MemoryID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return memoryGraphGrounding{}, err
		}
		if err == nil {
			if err := s.appendMemoryGraphGroundingFromSourceTx(ctx, tx, &grounding, record.WorkspaceID, nodeID, "knowledge_claim", record.ClaimID, strings.TrimSpace(backing.SourceKind), strings.TrimSpace(backing.SourceID), "backing_grounding", 0.9); err != nil {
				return memoryGraphGrounding{}, err
			}
		}
	}
	for _, relation := range relations {
		if trimmed := strings.TrimSpace(relation.ToClaimID); trimmed != "" {
			versionToken, exists, err := s.currentKnowledgeClaimVersionTokenTx(ctx, tx, record.WorkspaceID, trimmed)
			if err != nil {
				return memoryGraphGrounding{}, err
			}
			if exists {
				grounding.versions = append(grounding.versions, MemoryGraphNodeVersionInput{
					MemoryID:     nodeID,
					WorkspaceID:  record.WorkspaceID,
					RefKind:      "knowledge_claim",
					RefID:        trimmed,
					VersionToken: versionToken,
					Weight:       relation.Weight,
				})
			}
		}
	}
	return grounding, nil
}

func (s *Store) memoryGraphGroundingForEpisodePackTx(ctx context.Context, tx *sql.Tx, nodeID string, record EpisodePackRecord) (memoryGraphGrounding, error) {
	grounding := memoryGraphGrounding{}
	for _, value := range record.ProvenanceRefs {
		prefix, rest, ok := strings.Cut(strings.TrimSpace(value), ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(prefix) {
		case "workspace_doc":
			docKey := strings.TrimSpace(rest)
			if docKey == "" {
				continue
			}
			if err := s.appendMemoryGraphGroundedDocTx(ctx, tx, &grounding, record.WorkspaceID, nodeID, "episode_pack", record.PackID, docKey, "related", 0.75); err != nil {
				return memoryGraphGrounding{}, err
			}
		case "artifact_ref":
			artifactRef := strings.TrimSpace(rest)
			if artifactRef == "" {
				continue
			}
			if err := s.appendMemoryGraphGroundedArtifactTx(ctx, tx, &grounding, record.WorkspaceID, nodeID, "episode_pack", record.PackID, artifactRef, "related", 0.75); err != nil {
				return memoryGraphGrounding{}, err
			}
		}
	}
	return grounding, nil
}

func (s *Store) resolveCurrentMemoryGraphVersion(ctx context.Context, workspaceID string, version MemoryGraphNodeVersionRecord) (string, string, error) {
	refKind := strings.TrimSpace(version.RefKind)
	refID := strings.TrimSpace(version.RefID)
	if refKind == "" || refID == "" {
		return "", "UNRESOLVED", nil
	}
	switch refKind {
	case "workspace_doc":
		token, ok, err := s.currentWorkspaceDocVersionToken(ctx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "MISSING_SOURCE", nil
		}
		if token != strings.TrimSpace(version.VersionToken) {
			return token, "STALE", nil
		}
		return token, "CURRENT", nil
	case "artifact_ref":
		token, ok, err := s.currentWorkspaceArtifactVersionToken(ctx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "MISSING_SOURCE", nil
		}
		if token != strings.TrimSpace(version.VersionToken) {
			return token, "STALE", nil
		}
		return token, "CURRENT", nil
	case "workspace_memory":
		token, ok, err := s.currentWorkspaceMemoryVersionToken(ctx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "MISSING_SOURCE", nil
		}
		if token != strings.TrimSpace(version.VersionToken) {
			return token, "STALE", nil
		}
		return token, "CURRENT", nil
	case "knowledge_claim":
		token, ok, err := s.currentKnowledgeClaimVersionToken(ctx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "MISSING_SOURCE", nil
		}
		if token != strings.TrimSpace(version.VersionToken) {
			return token, "STALE", nil
		}
		return token, "CURRENT", nil
	case "knowledge_claim_relation":
		token, ok, err := s.currentKnowledgeClaimRelationVersionToken(ctx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "MISSING_SOURCE", nil
		}
		if token != strings.TrimSpace(version.VersionToken) {
			return token, "STALE", nil
		}
		return token, "CURRENT", nil
	case "segment_ref":
		token, ok, err := s.currentSegmentVersionToken(ctx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "MISSING_SOURCE", nil
		}
		if token != strings.TrimSpace(version.VersionToken) {
			return token, "STALE", nil
		}
		return token, "CURRENT", nil
	case "episode_pack", "session_compaction_snapshot", "runtime_event":
		return strings.TrimSpace(version.VersionToken), "CURRENT", nil
	default:
		return "", "UNRESOLVED", nil
	}
}

func (s *Store) buildMemoryGraphDriftReport(ctx context.Context, workspaceID string, versions []MemoryGraphNodeVersionRecord) (MemoryGraphDriftReport, error) {
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return MemoryGraphDriftReport{}, err
	}
	report := MemoryGraphDriftReport{
		TimeAuthority: authority,
		Status:        "CURRENT",
		Drift:         0,
		EvaluatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Items:         make([]MemoryGraphVersionStatus, 0, len(versions)),
	}
	aliasIndex := memoryGraphRootSegmentAliasIndex(workspaceID, versions)
	product := 1.0
	for _, version := range versions {
		currentToken, state, err := s.resolveCurrentMemoryGraphVersion(ctx, workspaceID, version)
		if err != nil {
			return MemoryGraphDriftReport{}, err
		}
		item := MemoryGraphVersionStatus{
			RefKind:             strings.TrimSpace(version.RefKind),
			RefID:               strings.TrimSpace(version.RefID),
			StoredVersionToken:  strings.TrimSpace(version.VersionToken),
			CurrentVersionToken: strings.TrimSpace(currentToken),
			Weight:              clampUnitInterval(version.Weight),
			State:               state,
		}
		if _, aliased := aliasIndex[memoryGraphVersionKey(version.RefKind, version.RefID)]; aliased {
			item.State = "ALIASED_SOURCE"
			report.Items = append(report.Items, item)
			continue
		}
		switch state {
		case "CURRENT":
			report.ComparedRefCount++
		case "STALE":
			report.ComparedRefCount++
			report.DriftedRefCount++
			product *= 1 - clampUnitInterval(version.Weight)
		case "MISSING_SOURCE":
			report.MissingRefCount++
			report.DriftedRefCount++
			product *= 1 - clampUnitInterval(version.Weight)
		default:
			report.UnresolvedRefCount++
		}
		report.Items = append(report.Items, item)
	}
	report.Drift = clampUnitInterval(1 - product)
	switch {
	case report.DriftedRefCount > 0 || report.MissingRefCount > 0:
		report.Status = "STALE"
	case report.UnresolvedRefCount > 0 && report.ComparedRefCount == 0:
		report.Status = "UNRESOLVED"
	case report.UnresolvedRefCount > 0:
		report.Status = "PARTIAL"
	default:
		report.Status = "CURRENT"
	}
	return report, nil
}

func memoryGraphVersionKey(refKind, refID string) string {
	return strings.TrimSpace(refKind) + "|" + strings.TrimSpace(refID)
}

func memoryGraphRootSegmentAliasIndex(workspaceID string, versions []MemoryGraphNodeVersionRecord) map[string]struct{} {
	if len(versions) == 0 {
		return nil
	}
	keys := make(map[string]struct{}, len(versions))
	for _, version := range versions {
		keys[memoryGraphVersionKey(version.RefKind, version.RefID)] = struct{}{}
	}
	aliases := map[string]struct{}{}
	for _, version := range versions {
		sourceKind, sourceRef, ok := memoryRootSegmentAliasTarget(workspaceID, version.RefKind, version.RefID)
		if !ok {
			continue
		}
		if _, exists := keys[memoryGraphVersionKey(sourceKind, sourceRef)]; exists {
			aliases[memoryGraphVersionKey(version.RefKind, version.RefID)] = struct{}{}
		}
	}
	if len(aliases) == 0 {
		return nil
	}
	return aliases
}

func memoryRootSegmentAliasTarget(workspaceID, refKind, refID string) (string, string, bool) {
	if strings.TrimSpace(refKind) != "segment_ref" {
		return "", "", false
	}
	refID = strings.TrimSpace(refID)
	if refID == "" || !strings.HasSuffix(refID, "#root") {
		return "", "", false
	}
	sourceKind, sourceRef, err := parseWorkspaceSegmentRef(workspaceID, refID)
	if err != nil {
		return "", "", false
	}
	switch sourceKind {
	case "workspace_doc":
		return "workspace_doc", sourceRef, true
	case "workspace_artifact":
		return "artifact_ref", sourceRef, true
	default:
		return "", "", false
	}
}

func (s *Store) applyMemoryGraphDriftToNodes(ctx context.Context, workspaceID string, nodes []MemoryGraphNodeRecord) error {
	for idx := range nodes {
		versions, err := s.listMemoryGraphNodeVersions(ctx, workspaceID, nodes[idx].MemoryID)
		if err != nil {
			return err
		}
		report, err := s.buildMemoryGraphDriftReport(ctx, workspaceID, versions)
		if err != nil {
			return err
		}
		nodes[idx].Drift = memoryGraphEffectiveDrift(nodes[idx].Drift, report)
		applyMemoryGraphRecoveryState(&nodes[idx], &report)
	}
	return nil
}

func uniqueMemoryGraphNodeRefs(refs []MemoryGraphNodeRefInput) []MemoryGraphNodeRefInput {
	if len(refs) == 0 {
		return nil
	}
	index := map[string]MemoryGraphNodeRefInput{}
	for _, ref := range refs {
		key := strings.Join([]string{
			strings.TrimSpace(ref.MemoryID),
			strings.TrimSpace(ref.RefKind),
			strings.TrimSpace(ref.RefID),
			strings.TrimSpace(ref.RefRole),
		}, "|")
		existing, ok := index[key]
		if !ok {
			index[key] = ref
			continue
		}
		if strings.TrimSpace(existing.RefValue) == "" && strings.TrimSpace(ref.RefValue) != "" {
			existing.RefValue = ref.RefValue
		}
		if strings.TrimSpace(existing.MetadataJSON) == "" || strings.TrimSpace(existing.MetadataJSON) == "{}" {
			if strings.TrimSpace(ref.MetadataJSON) != "" && strings.TrimSpace(ref.MetadataJSON) != "{}" {
				existing.MetadataJSON = ref.MetadataJSON
			}
		}
		if ref.Weight > existing.Weight {
			existing.Weight = ref.Weight
		}
		index[key] = existing
	}
	out := make([]MemoryGraphNodeRefInput, 0, len(index))
	for _, ref := range index {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RefKind != out[j].RefKind {
			return out[i].RefKind < out[j].RefKind
		}
		if out[i].RefID != out[j].RefID {
			return out[i].RefID < out[j].RefID
		}
		return out[i].RefRole < out[j].RefRole
	})
	return out
}

func uniqueMemoryGraphNodeVersions(versions []MemoryGraphNodeVersionInput) []MemoryGraphNodeVersionInput {
	if len(versions) == 0 {
		return nil
	}
	index := map[string]MemoryGraphNodeVersionInput{}
	for _, version := range versions {
		key := strings.Join([]string{
			strings.TrimSpace(version.MemoryID),
			strings.TrimSpace(version.RefKind),
			strings.TrimSpace(version.RefID),
		}, "|")
		existing, ok := index[key]
		if !ok {
			index[key] = version
			continue
		}
		if strings.TrimSpace(existing.VersionToken) == "" && strings.TrimSpace(version.VersionToken) != "" {
			existing.VersionToken = version.VersionToken
		}
		if version.Weight > existing.Weight {
			existing.Weight = version.Weight
		}
		index[key] = existing
	}
	out := make([]MemoryGraphNodeVersionInput, 0, len(index))
	for _, version := range index {
		out = append(out, version)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RefKind != out[j].RefKind {
			return out[i].RefKind < out[j].RefKind
		}
		return out[i].RefID < out[j].RefID
	})
	return out
}

func uniqueMemoryGraphEdges(edges []MemoryGraphEdgeInput) []MemoryGraphEdgeInput {
	if len(edges) == 0 {
		return nil
	}
	index := map[string]MemoryGraphEdgeInput{}
	for _, edge := range edges {
		key := strings.Join([]string{
			strings.TrimSpace(edge.FromMemoryID),
			strings.TrimSpace(edge.ToMemoryID),
			strings.TrimSpace(edge.EdgeType),
			strings.TrimSpace(edge.SourceKind),
			strings.TrimSpace(edge.SourceID),
		}, "|")
		existing, ok := index[key]
		if !ok {
			index[key] = edge
			continue
		}
		if edge.Weight > existing.Weight {
			existing.Weight = edge.Weight
		}
		if strings.TrimSpace(existing.MetadataJSON) == "" || strings.TrimSpace(existing.MetadataJSON) == "{}" {
			if strings.TrimSpace(edge.MetadataJSON) != "" && strings.TrimSpace(edge.MetadataJSON) != "{}" {
				existing.MetadataJSON = edge.MetadataJSON
			}
		}
		index[key] = existing
	}
	out := make([]MemoryGraphEdgeInput, 0, len(index))
	for _, edge := range index {
		out = append(out, edge)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EdgeType != out[j].EdgeType {
			return out[i].EdgeType < out[j].EdgeType
		}
		if out[i].ToMemoryID != out[j].ToMemoryID {
			return out[i].ToMemoryID < out[j].ToMemoryID
		}
		return out[i].SourceID < out[j].SourceID
	})
	return out
}

func scanKnowledgeClaimRelationRecord(scanner interface{ Scan(dest ...any) error }) (KnowledgeClaimRelationRecord, error) {
	var record KnowledgeClaimRelationRecord
	if err := scanner.Scan(
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
		return KnowledgeClaimRelationRecord{}, err
	}
	return record, nil
}

func memoryGraphEffectiveDrift(existing float64, report MemoryGraphDriftReport) float64 {
	if report.ComparedRefCount == 0 && report.DriftedRefCount == 0 && report.MissingRefCount == 0 {
		return clampUnitInterval(existing)
	}
	return clampUnitInterval(report.Drift)
}
