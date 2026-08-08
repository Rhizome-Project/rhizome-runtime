package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type tarjanNode struct {
	id      string
	index   int
	lowlink int
	onStack bool
}

func tarjanSCC(graph map[string][]string) [][]string {
	var index int
	var stack []string
	nodes := make(map[string]*tarjanNode)
	var sccs [][]string

	var strongconnect func(string)
	strongconnect = func(v string) {
		nodes[v] = &tarjanNode{
			id:      v,
			index:   index,
			lowlink: index,
			onStack: true,
		}
		index++
		stack = append(stack, v)

		for _, w := range graph[v] {
			if nodes[w] == nil {
				strongconnect(w)
				if nodes[w].lowlink < nodes[v].lowlink {
					nodes[v].lowlink = nodes[w].lowlink
				}
			} else if nodes[w].onStack {
				if nodes[w].index < nodes[v].lowlink {
					nodes[v].lowlink = nodes[w].index
				}
			}
		}

		if nodes[v].lowlink == nodes[v].index {
			var scc []string
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				nodes[w].onStack = false
				scc = append(scc, w)
				if w == v {
					break
				}
			}
			sccs = append(sccs, scc)
		}
	}

	for v := range graph {
		if nodes[v] == nil {
			strongconnect(v)
		}
	}

	return sccs
}

func aggregateMetaTensionScores(members []TensionRecord) (int, int) {
	if len(members) == 0 {
		return 0, 0
	}
	baseProduct := 1.0
	maxVisibility := 0.0
	for _, member := range members {
		baseImportance := normalizedTensionScore(member.BaseScore)
		surfacedPriority := normalizedTensionScore(member.SurfaceScore)
		baseProduct *= 1 - baseImportance
		if visibility := tensionVisibilityScore(baseImportance, surfacedPriority); visibility > maxVisibility {
			maxVisibility = visibility
		}
	}
	baseImportance := 1 - baseProduct
	surfacedPriority := baseImportance * maxVisibility
	return roundedPercentScore(baseImportance), roundedPercentScore(surfacedPriority)
}

func roundedPercentScore(value float64) int {
	if value < 0 {
		value = 0
	}
	if value > 1 {
		value = 1
	}
	return clampInt(int(math.Round(value*100)), 0, 100)
}

type TensionCondenseInput struct {
	WorkspaceID                string
	ActorID                    string
	Reason                     string
	PromptContextEnvelope      map[string]any
	PromptContextSurface       string
	PromptContextPrincipalType string
	PromptContextPrincipalID   string
}

type TensionCondenseResult struct {
	WorkspaceID               string               `json:"workspace_id"`
	CondensedAt               string               `json:"condensed_at"`
	ComponentCount            int                  `json:"component_count"`
	ProcessedComponentCount   int                  `json:"processed_component_count"`
	SkippedMetaComponentCount int                  `json:"skipped_meta_component_count"`
	Changed                   bool                 `json:"changed"`
	CreatedCount              int                  `json:"created_count"`
	UpdatedCount              int                  `json:"updated_count"`
	ResurrectedCount          int                  `json:"resurrected_count"`
	StaleMetaArchivedCount    int                  `json:"stale_meta_archived_count"`
	DependencyAddedCount      int                  `json:"dependency_added_count"`
	DependencyRemovedCount    int                  `json:"dependency_removed_count"`
	EvidenceLinkedCount       int                  `json:"evidence_linked_count"`
	MetaTensionIDs            []string             `json:"meta_tension_ids,omitempty"`
	StaleMetaTensionIDs       []string             `json:"stale_meta_tension_ids,omitempty"`
	Events                    []RuntimeEventRecord `json:"events,omitempty"`
}

// RefreshMetaTensions computes SCCs on the active tension dependency graph
// and collapses components of size > 1 into a META-tension. This legacy helper
// preserves compatibility for direct callers; production RPC paths should call
// RefreshMetaTensionsWithContext so the condensation is prompt-context-bound.
func (s *Store) RefreshMetaTensions(ctx context.Context, workspaceID string) error {
	_, err := s.refreshMetaTensions(ctx, TensionCondenseInput{
		WorkspaceID: workspaceID,
		ActorID:     "tension_scc",
	}, false)
	return err
}

func (s *Store) RefreshMetaTensionsWithContext(ctx context.Context, input TensionCondenseInput) (TensionCondenseResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.ActorID = strings.TrimSpace(input.ActorID)
	input.Reason = strings.TrimSpace(input.Reason)
	input.PromptContextSurface = tensionPromptContextSurface(input.PromptContextSurface, "tension.condensed")
	input.PromptContextPrincipalType = strings.TrimSpace(input.PromptContextPrincipalType)
	input.PromptContextPrincipalID = strings.TrimSpace(input.PromptContextPrincipalID)
	if input.WorkspaceID == "" {
		return TensionCondenseResult{}, fmt.Errorf("workspace_id is required")
	}
	if input.ActorID == "" {
		return TensionCondenseResult{}, fmt.Errorf("actor_id is required")
	}
	if err := validateTensionCondensePromptContextSurface(input.PromptContextEnvelope, input.PromptContextSurface); err != nil {
		return TensionCondenseResult{}, err
	}
	return s.refreshMetaTensions(ctx, input, true)
}

func validateTensionCondensePromptContextSurface(envelope map[string]any, surface string) error {
	if envelope == nil {
		return nil
	}
	surface = strings.TrimSpace(surface)
	if surface == "" || surface == "workspace.tension.condense" {
		return nil
	}
	return fmt.Errorf("workspace_tension prompt context surface %q is not valid for workspace.tension.condense", surface)
}

func (s *Store) refreshMetaTensions(ctx context.Context, input TensionCondenseInput, usePromptContext bool) (TensionCondenseResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.ActorID = strings.TrimSpace(input.ActorID)
	input.Reason = strings.TrimSpace(input.Reason)
	if input.ActorID == "" {
		input.ActorID = "tension_scc"
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result := TensionCondenseResult{
		WorkspaceID: input.WorkspaceID,
		CondensedAt: now,
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return result, fmt.Errorf("RefreshMetaTensions BeginTx: %w", err)
	}
	defer tx.Rollback()

	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInputTx(ctx, tx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		_ = tx.Rollback()
		return result, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		graph, err := s.getActiveTensionGraphTx(ctx, tx, input.WorkspaceID)
		if err != nil {
			return fmt.Errorf("RefreshMetaTensions GetActiveTensionGraph: %w", err)
		}
		sccs := tarjanSCC(graph)
		result.ComponentCount = len(sccs)
		currentMetaIDs := make(map[string]bool)

		for _, scc := range sccs {
			if len(scc) <= 1 {
				continue
			}
			sort.Strings(scc)
			hashInput := input.WorkspaceID + ":" + strings.Join(scc, ",")
			hashBytes := sha256.Sum256([]byte(hashInput))
			sccHash := hex.EncodeToString(hashBytes[:])
			metaTensionID := "ten_meta_" + hex.EncodeToString(hashBytes[:12])
			promptContext := tensionRuntimePromptContext{
				Envelope:       input.PromptContextEnvelope,
				Surface:        input.PromptContextSurface,
				PrincipalType:  input.PromptContextPrincipalType,
				PrincipalID:    input.PromptContextPrincipalID,
				SCCMemberIDs:   append([]string{}, scc...),
				SCCHash:        sccHash,
				SCCMemberCount: len(scc),
			}

			members := make([]TensionRecord, 0, len(scc))
			skipCondensation := false
			for _, memberID := range scc {
				member, err := s.loadTensionRecord(ctx, tx, input.WorkspaceID, memberID)
				if err != nil {
					return fmt.Errorf("load SCC member tension %q: %w", memberID, err)
				}
				if isMetaTensionType(member.TensionType) {
					skipCondensation = true
					break
				}
				members = append(members, member)
			}
			if skipCondensation {
				result.SkippedMetaComponentCount++
				continue
			}
			currentMetaIDs[metaTensionID] = true
			result.ProcessedComponentCount++

			protoClusterID := members[0].ProtoClusterID
			baseScore, surfaceScore := aggregateMetaTensionScores(members)
			existingMeta, err := s.loadTensionRecord(ctx, tx, input.WorkspaceID, metaTensionID)
			if err == nil && existingMeta.TensionID == metaTensionID {
				previousMeta := existingMeta
				existingMeta.ProtoClusterID = protoClusterID
				existingMeta.Title = fmt.Sprintf("Meta-Tension: Cycle of %d blocked tensions", len(scc))
				existingMeta.Summary = "Automatically generated condensation of a cyclic dependency graph (SCC)."
				existingMeta.AnchorKind = "scc_condensation"
				existingMeta.AnchorRef = fmt.Sprintf("%d_members", len(scc))
				existingMeta.BaseScore = baseScore
				existingMeta.SurfaceScore = surfaceScore
				existingMeta.UpdatedAt = now

				eventType := ""
				action := ""
				if existingMeta.LifecycleState == tensionLifecycleMeta || existingMeta.LifecycleState == tensionLifecycleResolved || existingMeta.LifecycleState == tensionLifecycleArchived || existingMeta.LifecycleState == tensionLifecycleSuperseded || existingMeta.LifecycleState == tensionLifecycleDisputed {
					existingMeta.LifecycleState = tensionLifecycleEmergent
					eventType = "tension.emergent"
					action = "resurrected"
					result.ResurrectedCount++
				} else if tensionRecordChanged(previousMeta, existingMeta) {
					eventType = "tension.updated"
					action = "updated"
					result.UpdatedCount++
				}
				if eventType != "" {
					if err := s.upsertTensionTx(ctx, tx, existingMeta); err != nil {
						return err
					}
					promptContext.CondenseAction = action
					event, err := s.appendMetaCondenseRuntimeEventTx(ctx, tx, authority, existingMeta, eventType, input.ActorID, input.Reason, promptContext, usePromptContext)
					if err != nil {
						return err
					}
					result.Events = append(result.Events, event)
				}
			} else if err == nil {
				return fmt.Errorf("unexpected blank meta tension row for %s", metaTensionID)
			} else if !isTensionNotFoundErr(err) {
				return fmt.Errorf("load meta tension %q: %w", metaTensionID, err)
			} else {
				newMeta := TensionRecord{
					TensionID:      metaTensionID,
					WorkspaceID:    input.WorkspaceID,
					ProtoClusterID: protoClusterID,
					TensionType:    "meta-tension",
					LifecycleState: tensionLifecycleEmergent,
					ReviewStatus:   tensionReviewPending,
					Title:          fmt.Sprintf("Meta-Tension: Cycle of %d blocked tensions", len(scc)),
					Summary:        "Automatically generated condensation of a cyclic dependency graph (SCC).",
					AnchorKind:     "scc_condensation",
					AnchorRef:      fmt.Sprintf("%d_members", len(scc)),
					BaseScore:      baseScore,
					SurfaceScore:   surfaceScore,
					CreatedAt:      now,
					UpdatedAt:      now,
				}
				if err := s.upsertTensionTx(ctx, tx, newMeta); err != nil {
					return err
				}
				promptContext.CondenseAction = "created"
				event, err := s.appendMetaCondenseRuntimeEventTx(ctx, tx, authority, newMeta, "tension.emergent", input.ActorID, input.Reason, promptContext, usePromptContext)
				if err != nil {
					return err
				}
				result.CreatedCount++
				result.Events = append(result.Events, event)
			}
			result.MetaTensionIDs = append(result.MetaTensionIDs, metaTensionID)

			for _, member := range members {
				edge, changed, err := s.upsertTensionCondenseDependencyTx(ctx, tx, input.WorkspaceID, member.TensionID, metaTensionID, now)
				if err != nil {
					return err
				}
				if changed {
					result.DependencyAddedCount++
					memberForEvent := member
					memberForEvent.UpdatedAt = now
					dependencyContext := promptContext
					dependencyContext.DependsOnTensionID = metaTensionID
					dependencyContext.DependencyType = edge.DependencyType
					dependencyContext.CondenseAction = "dependency_linked"
					event, err := s.appendMetaCondenseRuntimeEventTx(ctx, tx, authority, memberForEvent, "tension.dependency.added", input.ActorID, input.Reason, dependencyContext, usePromptContext)
					if err != nil {
						return err
					}
					result.Events = append(result.Events, event)
				}

				ev := TensionEvidenceRecord{
					TensionID:    metaTensionID,
					WorkspaceID:  input.WorkspaceID,
					EvidenceKind: "member_tension",
					EvidenceRef:  member.TensionID,
					Weight:       1,
					Summary:      "Part of the SCC cyclic block",
					CreatedAt:    now,
				}
				if err := s.upsertTensionEvidenceTx(ctx, tx, input.WorkspaceID, metaTensionID, []TensionEvidenceRecord{ev}); err != nil {
					return err
				}
				result.EvidenceLinkedCount++
			}
		}
		result.MetaTensionIDs = uniqueSortedStrings(result.MetaTensionIDs)
		if err := s.cleanupStaleMetaTensionsTx(ctx, tx, authority, input, &result, currentMetaIDs, now, usePromptContext); err != nil {
			return err
		}
		result.StaleMetaTensionIDs = uniqueSortedStrings(result.StaleMetaTensionIDs)
		result.Changed = result.CreatedCount > 0 || result.UpdatedCount > 0 || result.ResurrectedCount > 0 || result.StaleMetaArchivedCount > 0 || result.DependencyAddedCount > 0 || result.DependencyRemovedCount > 0
		if usePromptContext {
			event, err := s.appendTensionCondenseSummaryEventTx(ctx, tx, authority, input, result, now)
			if err != nil {
				return err
			}
			result.Events = append(result.Events, event)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return result, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit meta tension refresh tx: %w", err)
	}
	return result, nil
}

func (s *Store) cleanupStaleMetaTensionsTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input TensionCondenseInput, result *TensionCondenseResult, currentMetaIDs map[string]bool, now string, usePromptContext bool) error {
	rows, err := tx.QueryContext(ctx, `SELECT tension_id
		FROM workspace_tensions
		WHERE workspace_id = ?
		  AND tension_type = 'meta-tension'
		  AND anchor_kind = 'scc_condensation'
		  AND lifecycle_state IN (?, ?, ?)
		ORDER BY tension_id`, input.WorkspaceID, tensionLifecycleEmergent, tensionLifecycleActive, tensionLifecycleMeta)
	if err != nil {
		return fmt.Errorf("query stale meta tensions: %w", err)
	}
	defer rows.Close()

	var candidateIDs []string
	for rows.Next() {
		var metaID string
		if err := rows.Scan(&metaID); err != nil {
			return err
		}
		metaID = strings.TrimSpace(metaID)
		if metaID != "" && !currentMetaIDs[metaID] {
			candidateIDs = append(candidateIDs, metaID)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, metaID := range candidateIDs {
		memberIDs, err := s.listMetaCondenseMemberDependencyIDsTx(ctx, tx, input.WorkspaceID, metaID)
		if err != nil {
			return err
		}
		members := make(map[string]TensionRecord, len(memberIDs))
		for _, memberID := range memberIDs {
			member, err := s.loadTensionRecord(ctx, tx, input.WorkspaceID, memberID)
			if err != nil {
				return fmt.Errorf("stale meta tension %s has SUBSUMED_BY edge from missing member %s; cleanup requires durable removal evidence: %w", metaID, memberID, err)
			}
			members[memberID] = member
		}
		if len(memberIDs) > 0 {
			res, err := tx.ExecContext(ctx, `DELETE FROM workspace_tension_dependencies
				WHERE workspace_id = ?
				  AND depends_on_tension_id = ?
				  AND dependency_type = 'SUBSUMED_BY'`, input.WorkspaceID, metaID)
			if err != nil {
				return fmt.Errorf("remove stale meta SUBSUMED_BY edges for %s: %w", metaID, err)
			}
			removedRows, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("count removed stale meta SUBSUMED_BY edges for %s: %w", metaID, err)
			}
			if int(removedRows) != len(memberIDs) {
				return fmt.Errorf("stale meta tension %s removed %d SUBSUMED_BY edges, expected %d", metaID, removedRows, len(memberIDs))
			}
			result.DependencyRemovedCount += int(removedRows)
			for _, memberID := range memberIDs {
				member := members[memberID]
				member.UpdatedAt = now
				dependencyContext := tensionRuntimePromptContext{
					Envelope:           input.PromptContextEnvelope,
					Surface:            input.PromptContextSurface,
					PrincipalType:      input.PromptContextPrincipalType,
					PrincipalID:        input.PromptContextPrincipalID,
					DependsOnTensionID: metaID,
					DependencyType:     "SUBSUMED_BY",
					SCCMemberIDs:       append([]string{}, memberIDs...),
					SCCMemberCount:     len(memberIDs),
					CondenseAction:     "stale_dependency_removed",
				}
				event, err := s.appendMetaCondenseRuntimeEventTx(ctx, tx, authority, member, "tension.dependency.removed", input.ActorID, input.Reason, dependencyContext, usePromptContext)
				if err != nil {
					return err
				}
				result.Events = append(result.Events, event)
			}
		}

		meta, err := s.loadTensionRecord(ctx, tx, input.WorkspaceID, metaID)
		if err != nil {
			if isTensionNotFoundErr(err) {
				continue
			}
			return err
		}
		meta.LifecycleState = tensionLifecycleArchived
		meta.ArchivedBy = strings.TrimSpace(input.ActorID)
		meta.UpdatedAt = now
		if err := s.upsertTensionTx(ctx, tx, meta); err != nil {
			return err
		}
		promptContext := tensionRuntimePromptContext{
			Envelope:       input.PromptContextEnvelope,
			Surface:        input.PromptContextSurface,
			PrincipalType:  input.PromptContextPrincipalType,
			PrincipalID:    input.PromptContextPrincipalID,
			SCCMemberIDs:   append([]string{}, memberIDs...),
			SCCMemberCount: len(memberIDs),
			CondenseAction: "stale_meta_archived",
		}
		event, err := s.appendMetaCondenseRuntimeEventTx(ctx, tx, authority, meta, "tension.archived", input.ActorID, firstNonEmpty(input.Reason, "stale_scc_cycle_resolved"), promptContext, usePromptContext)
		if err != nil {
			return err
		}
		result.StaleMetaArchivedCount++
		result.StaleMetaTensionIDs = append(result.StaleMetaTensionIDs, meta.TensionID)
		result.Events = append(result.Events, event)
	}
	return nil
}

func (s *Store) listMetaCondenseMemberDependencyIDsTx(ctx context.Context, tx *sql.Tx, workspaceID, metaTensionID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT tension_id
		FROM workspace_tension_dependencies
		WHERE workspace_id = ?
		  AND depends_on_tension_id = ?
		  AND dependency_type = 'SUBSUMED_BY'
		ORDER BY tension_id`, workspaceID, metaTensionID)
	if err != nil {
		return nil, fmt.Errorf("query meta member dependencies: %w", err)
	}
	defer rows.Close()

	var memberIDs []string
	for rows.Next() {
		var memberID string
		if err := rows.Scan(&memberID); err != nil {
			return nil, err
		}
		if memberID = strings.TrimSpace(memberID); memberID != "" {
			memberIDs = append(memberIDs, memberID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return uniqueSortedStrings(memberIDs), nil
}

func (s *Store) getActiveTensionGraphTx(ctx context.Context, tx *sql.Tx, workspaceID string) (map[string][]string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, fmt.Errorf("workspace_id is required")
	}
	rows, err := tx.QueryContext(ctx, `SELECT d.tension_id, d.depends_on_tension_id
		FROM workspace_tension_dependencies d
		JOIN workspace_tensions t1 ON d.workspace_id = t1.workspace_id AND d.tension_id = t1.tension_id
		JOIN workspace_tensions t2 ON d.workspace_id = t2.workspace_id AND d.depends_on_tension_id = t2.tension_id
		WHERE d.workspace_id = ?
		  AND t1.lifecycle_state IN ('ACTIVE', 'EMERGENT')
		  AND t2.lifecycle_state IN ('ACTIVE', 'EMERGENT')`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("query tension graph: %w", err)
	}
	defer rows.Close()

	graph := make(map[string][]string)
	for rows.Next() {
		var src, dst string
		if err := rows.Scan(&src, &dst); err != nil {
			return nil, err
		}
		graph[src] = append(graph[src], dst)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return graph, nil
}

func (s *Store) upsertTensionCondenseDependencyTx(ctx context.Context, tx *sql.Tx, workspaceID, tensionID, metaTensionID, now string) (TensionDependencyEdge, bool, error) {
	edge := TensionDependencyEdge{
		WorkspaceID:        workspaceID,
		TensionID:          tensionID,
		DependsOnTensionID: metaTensionID,
		DependencyType:     "SUBSUMED_BY",
	}
	var existingDependencyType string
	err := tx.QueryRowContext(ctx, `SELECT dependency_type FROM workspace_tension_dependencies WHERE workspace_id = ? AND tension_id = ? AND depends_on_tension_id = ?`, workspaceID, tensionID, metaTensionID).Scan(&existingDependencyType)
	if err == nil {
		if strings.TrimSpace(existingDependencyType) == edge.DependencyType {
			return edge, false, nil
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workspace_tension_dependencies SET dependency_type = ? WHERE workspace_id = ? AND tension_id = ? AND depends_on_tension_id = ?`, edge.DependencyType, workspaceID, tensionID, metaTensionID); err != nil {
			return TensionDependencyEdge{}, false, err
		}
		return edge, true, nil
	}
	if err != sql.ErrNoRows {
		return TensionDependencyEdge{}, false, err
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_tension_dependencies WHERE workspace_id = ? AND tension_id = ?`, workspaceID, tensionID).Scan(&count); err != nil {
		return TensionDependencyEdge{}, false, fmt.Errorf("check dependency bounds: %w", err)
	}
	if count >= 20 {
		return TensionDependencyEdge{}, false, errors.New("tension dependency threshold exceeded: cannot attach more dependencies")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO workspace_tension_dependencies (workspace_id, tension_id, depends_on_tension_id, dependency_type, created_at) VALUES (?, ?, ?, ?, ?)`, workspaceID, tensionID, metaTensionID, edge.DependencyType, now); err != nil {
		return TensionDependencyEdge{}, false, err
	}
	return edge, true, nil
}

func (s *Store) appendMetaCondenseRuntimeEventTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record TensionRecord, eventType, actorID, reason string, promptContext tensionRuntimePromptContext, usePromptContext bool) (RuntimeEventRecord, error) {
	if usePromptContext {
		return s.appendTensionRuntimeEventWithAuthorityAndPromptContextTx(ctx, tx, authority, record, nil, eventType, "system", actorID, reason, promptContext)
	}
	return s.appendTensionRuntimeEventWithAuthorityTx(ctx, tx, authority, record, nil, eventType, "system", actorID, reason)
}

func (s *Store) appendTensionCondenseSummaryEventTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input TensionCondenseInput, result TensionCondenseResult, createdAt string) (RuntimeEventRecord, error) {
	payload := map[string]any{
		"workspace_id":                 strings.TrimSpace(input.WorkspaceID),
		"typed_event_type":             "TENSION_CONDENSE",
		"event_kind":                   "tension.condensed",
		"component_count":              result.ComponentCount,
		"processed_component_count":    result.ProcessedComponentCount,
		"skipped_meta_component_count": result.SkippedMetaComponentCount,
		"changed":                      result.Changed,
		"created_count":                result.CreatedCount,
		"updated_count":                result.UpdatedCount,
		"resurrected_count":            result.ResurrectedCount,
		"stale_meta_archived_count":    result.StaleMetaArchivedCount,
		"dependency_added_count":       result.DependencyAddedCount,
		"dependency_removed_count":     result.DependencyRemovedCount,
		"evidence_linked_count":        result.EvidenceLinkedCount,
		"meta_tension_ids":             uniqueSortedStrings(result.MetaTensionIDs),
		"stale_meta_tension_ids":       uniqueSortedStrings(result.StaleMetaTensionIDs),
		"actor_type":                   "system",
		"actor_id":                     strings.TrimSpace(input.ActorID),
		"reason":                       strings.TrimSpace(input.Reason),
	}
	fields := map[string]string{
		"workspace_id":              strings.TrimSpace(input.WorkspaceID),
		"event_kind":                "tension.condensed",
		"actor_type":                "system",
		"actor_id":                  strings.TrimSpace(input.ActorID),
		"changed":                   fmt.Sprintf("%t", result.Changed),
		"component_count":           fmt.Sprintf("%d", result.ComponentCount),
		"processed_component_count": fmt.Sprintf("%d", result.ProcessedComponentCount),
		"dependency_added_count":    fmt.Sprintf("%d", result.DependencyAddedCount),
		"dependency_removed_count":  fmt.Sprintf("%d", result.DependencyRemovedCount),
		"stale_meta_archived_count": fmt.Sprintf("%d", result.StaleMetaArchivedCount),
	}
	if principalType := strings.TrimSpace(input.PromptContextPrincipalType); principalType != "" {
		fields["principal_type"] = principalType
	}
	if principalID := strings.TrimSpace(input.PromptContextPrincipalID); principalID != "" {
		fields["principal_id"] = principalID
	}
	if metaIDs := uniqueSortedStrings(result.MetaTensionIDs); len(metaIDs) > 0 {
		fields["meta_tension_ids"] = strings.Join(metaIDs, ",")
	}
	if staleMetaIDs := uniqueSortedStrings(result.StaleMetaTensionIDs); len(staleMetaIDs) > 0 {
		fields["stale_meta_tension_ids"] = strings.Join(staleMetaIDs, ",")
	}
	var err error
	payload, err = attachWorkspaceTensionPromptContextEnvelope(payload, input.PromptContextEnvelope, "workspace.tension.condense", fields)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: strings.TrimSpace(input.WorkspaceID),
		EventType:   "tension.condensed",
		EntityType:  "workspace_tension_condense",
		EntityID:    strings.TrimSpace(input.WorkspaceID),
		ActorType:   "system",
		ActorID:     strings.TrimSpace(input.ActorID),
		PayloadJSON: mustJSON(payload),
		CreatedAt:   createdAt,
	})
}
