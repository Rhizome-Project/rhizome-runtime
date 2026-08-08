package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

func deriveTaskResultPromotionCandidate(
	task WorkspaceTaskRecord,
	session AgentSessionStateRecord,
	runID string,
	result StructuredTaskResult,
	nodeType localMemoryNodeType,
	tensionID string,
	clusterID string,
	artifactRefs []string,
	docKeys []string,
) *LocalPromotionCandidate {
	memoryType := promotionMemoryTypeForResult(result, nodeType)
	if memoryType == "" {
		return nil
	}
	body := strings.TrimSpace(result.MemoryBody)
	if body == "" {
		body = renderDerivedPromotionBody(result, artifactRefs, docKeys)
	}
	if body == "" {
		return nil
	}

	return &LocalPromotionCandidate{
		NodeType:       nodeType,
		MemoryType:     memoryType,
		SourceID:       strings.TrimSpace(runID),
		Title:          firstNonEmpty(result.MemoryTitle, result.Summary),
		Body:           body,
		Summary:        strings.TrimSpace(result.Summary),
		TaskID:         strings.TrimSpace(task.TaskID),
		SessionID:      strings.TrimSpace(session.SessionID),
		TensionID:      strings.TrimSpace(tensionID),
		ProtoClusterID: strings.TrimSpace(clusterID),
		ArtifactRefs:   uniqueTrimmedFocusStrings(artifactRefs),
		DocKeys:        uniqueTrimmedFocusStrings(docKeys),
	}
}

func promotionMemoryTypeForResult(result StructuredTaskResult, nodeType localMemoryNodeType) string {
	switch strings.ToUpper(strings.TrimSpace(result.MemoryType)) {
	case "DECISION", "PROCEDURE", "INCIDENT", "NOTE", "LESSON":
		return strings.ToUpper(strings.TrimSpace(result.MemoryType))
	}

	switch nodeType {
	case localMemoryNodeDecision:
		return "DECISION"
	case localMemoryNodeProcedure:
		return "PROCEDURE"
	case localMemoryNodeBlocker, localMemoryNodeAntiProcedure:
		return "INCIDENT"
	}

	if normalizeOutcome(result.Outcome) == "blocked" {
		return "INCIDENT"
	}
	if strings.TrimSpace(result.MemoryBody) != "" {
		return "NOTE"
	}
	return ""
}

func renderDerivedPromotionBody(result StructuredTaskResult, artifactRefs, docKeys []string) string {
	var b strings.Builder
	b.WriteString("# Local Memory Promotion\n\n")
	b.WriteString(fmt.Sprintf("- outcome: %s\n", normalizeOutcome(result.Outcome)))
	b.WriteString(fmt.Sprintf("- summary: %s\n", firstNonEmpty(result.Summary, "unspecified")))
	if result.NextAction != "" {
		b.WriteString(fmt.Sprintf("- next_action: %s\n", oneLine(result.NextAction)))
	}
	if result.OwnerAction != "" {
		b.WriteString(fmt.Sprintf("- owner_action: %s\n", oneLine(result.OwnerAction)))
	}
	if result.HumanReason != "" {
		b.WriteString(fmt.Sprintf("- human_reason: %s\n", oneLine(result.HumanReason)))
	}
	if result.Materialize.DocKey != "" {
		b.WriteString(fmt.Sprintf("- materialized_doc: %s\n", strings.TrimSpace(result.Materialize.DocKey)))
	}
	if result.Details != "" {
		b.WriteString("\n## Details\n")
		b.WriteString(strings.TrimSpace(result.Details))
		b.WriteString("\n")
	}
	if len(result.BlockedOn) > 0 {
		b.WriteString("\n## Blocked On\n")
		for _, blocker := range result.BlockedOn {
			b.WriteString(fmt.Sprintf("- %s: %s\n", firstNonEmpty(strings.TrimSpace(blocker.Kind), "unknown"), firstNonEmpty(strings.TrimSpace(blocker.Detail), "unspecified")))
		}
	}
	if len(docKeys) > 0 {
		b.WriteString("\n## Related Docs\n")
		for _, key := range docKeys {
			b.WriteString("- " + strings.TrimSpace(key) + "\n")
		}
	}
	if len(artifactRefs) > 0 {
		b.WriteString("\n## Related Artifacts\n")
		for _, ref := range artifactRefs {
			b.WriteString("- " + strings.TrimSpace(ref) + "\n")
		}
	}
	return strings.TrimSpace(b.String())
}

func (r *Runtime) runMemorySyncLoop(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.MemorySyncEvery)
	defer ticker.Stop()

	var backoff transientBackoff
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.flushPendingMemoryPromotions(ctx, r.cfg.MaxPromotionSyncBatch); err != nil {
				log.Printf("[memory] sync loop degraded: %v", err)
				if !sleepContext(ctx, backoff.Next(2*time.Second, 30*time.Second)) {
					return nil
				}
				continue
			}
			backoff.Reset()
		}
	}
}

func (r *Runtime) flushPendingMemoryPromotions(ctx context.Context, limit int) error {
	service := r.memoryService()
	if service == nil {
		return nil
	}
	candidates := service.pendingPromotions(limit)
	var firstErr error
	for _, candidate := range candidates {
		if err := r.syncPromotionCandidate(ctx, candidate); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *Runtime) syncPromotionCandidate(ctx context.Context, candidate LocalPromotionCandidate) error {
	memoryType := promotionMemoryTypeForCandidate(candidate)
	if memoryType == "" {
		return nil
	}
	body := strings.TrimSpace(candidate.Body)
	if body == "" {
		body = strings.TrimSpace(candidate.Summary)
	}
	if body == "" {
		return nil
	}

	memoryID := firstNonEmpty(strings.TrimSpace(candidate.MemoryID), promotionMemoryID(candidate.CandidateID))
	claimID := strings.TrimSpace(candidate.ClaimID)
	if claimType := promotionClaimType(memoryType); claimType != "" {
		claimID = firstNonEmpty(claimID, promotionClaimID(candidate.CandidateID))
	}
	if skipReason, err := r.memoryPromotionSkipReason(ctx, candidate); err != nil {
		_ = r.markMemoryPromotionFailed(candidate.CandidateID, memoryID, claimID, err)
		return err
	} else if skipReason != "" {
		return r.markMemoryPromotionSkipped(candidate.CandidateID, memoryID, claimID, skipReason)
	}
	if err := r.recordMemoryPromotionAttempt(candidate.CandidateID, memoryID, claimID); err != nil {
		return err
	}

	record, err := r.client.WriteMemory(ctx, WorkspaceMemoryWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		MemoryID:    memoryID,
		MemoryType:  memoryType,
		Title:       firstNonEmpty(candidate.Title, candidate.Summary, memoryType+" memory"),
		Body:        body,
		Summary:     firstNonEmpty(candidate.Summary, candidate.Title, memoryType+" memory"),
		AgentID:     r.cfg.AgentID,
		SessionID:   strings.TrimSpace(candidate.SessionID),
		TaskID:      strings.TrimSpace(candidate.TaskID),
		SourceKind:  "local_memory_promotion",
		SourceID:    firstNonEmpty(strings.TrimSpace(candidate.SourceID), strings.TrimSpace(candidate.CandidateID)),
		Tags: []string{
			"rnar",
			"local-memory-sync",
			strings.ToLower(memoryType),
		},
		Importance: promotionImportance(memoryType),
		Confidence: promotionConfidence(memoryType),
	})
	if err != nil {
		_ = r.markMemoryPromotionFailed(candidate.CandidateID, memoryID, claimID, err)
		return err
	}
	memoryID = firstNonEmpty(strings.TrimSpace(record.MemoryID), memoryID)

	if claimType := promotionClaimType(memoryType); claimType != "" {
		if err := r.client.WriteClaim(ctx, KnowledgeClaimWriteInput{
			WorkspaceID: r.cfg.WorkspaceID,
			ClaimID:     claimID,
			ClaimType:   claimType,
			Status:      "ACTIVE",
			Subject:     firstNonEmpty(candidate.Title, candidate.Summary, claimType),
			Body:        body,
			Summary:     firstNonEmpty(candidate.Summary, candidate.Title, claimType),
			Confidence:  promotionConfidence(memoryType),
			SourceKind:  "local_memory_promotion",
			SourceID:    firstNonEmpty(strings.TrimSpace(candidate.SourceID), strings.TrimSpace(candidate.CandidateID)),
			MemoryID:    memoryID,
			TaskID:      strings.TrimSpace(candidate.TaskID),
			SessionID:   strings.TrimSpace(candidate.SessionID),
			AgentID:     r.cfg.AgentID,
			Evidence:    promotionEvidence(candidate),
			Tags: []string{
				"rnar",
				"local-memory-sync",
				strings.ToLower(claimType),
			},
		}); err != nil {
			_ = r.markMemoryPromotionFailed(candidate.CandidateID, memoryID, claimID, err)
			return err
		}
	}

	return r.markMemoryPromotionPromoted(candidate.CandidateID, memoryID, claimID)
}

func (r *Runtime) memoryPromotionSkipReason(ctx context.Context, candidate LocalPromotionCandidate) (string, error) {
	taskID := strings.TrimSpace(candidate.TaskID)
	if taskID == "" {
		return "", nil
	}
	if r == nil || r.client == nil {
		return "", nil
	}
	tasks, err := r.client.ListTasks(ctx, r.cfg.WorkspaceID)
	if err != nil {
		return "", fmt.Errorf("validate local memory promotion task scope: %w", err)
	}

	var matched *WorkspaceTaskRecord
	for i := range tasks {
		if sameWorkspaceDocTaskID(tasks[i].TaskID, taskID) {
			matched = &tasks[i]
			break
		}
	}
	if matched == nil {
		return fmt.Sprintf("skipped stale local memory promotion: task %s is not present in workspace task list", taskID), nil
	}
	if taskSubmitTaskIsTerminal(*matched) {
		return fmt.Sprintf("skipped stale local memory promotion: task %s is terminal with status %s", taskID, strings.ToUpper(strings.TrimSpace(matched.Status))), nil
	}

	activeTask := r.currentTaskCopy()
	activeProjectID := workspaceTaskProjectID(activeTask)
	candidateProjectID := strings.TrimSpace(matched.ProjectID)
	if activeProjectID != "" && candidateProjectID != "" && !sameWorkspaceDocProjectID(candidateProjectID, activeProjectID) {
		return fmt.Sprintf("skipped stale local memory promotion: task %s belongs to project %s while active project is %s", taskID, candidateProjectID, activeProjectID), nil
	}
	return "", nil
}

func promotionMemoryTypeForCandidate(candidate LocalPromotionCandidate) string {
	switch strings.ToUpper(strings.TrimSpace(candidate.MemoryType)) {
	case "DECISION", "PROCEDURE", "INCIDENT", "NOTE", "LESSON":
		return strings.ToUpper(strings.TrimSpace(candidate.MemoryType))
	}
	switch candidate.NodeType {
	case localMemoryNodeDecision:
		return "DECISION"
	case localMemoryNodeProcedure:
		return "PROCEDURE"
	case localMemoryNodeBlocker, localMemoryNodeAntiProcedure:
		return "INCIDENT"
	default:
		if strings.TrimSpace(candidate.Body) != "" {
			return "NOTE"
		}
		return ""
	}
}

func promotionClaimType(memoryType string) string {
	switch strings.ToUpper(strings.TrimSpace(memoryType)) {
	case "DECISION":
		return "DECISION"
	case "PROCEDURE":
		return "PROCEDURE"
	case "INCIDENT":
		return "INCIDENT"
	default:
		return ""
	}
}

func promotionMemoryID(candidateID string) string {
	return "mem." + sanitizePathComponent(firstNonEmpty(strings.TrimSpace(candidateID), "local"))
}

func promotionClaimID(candidateID string) string {
	return "claim." + sanitizePathComponent(firstNonEmpty(strings.TrimSpace(candidateID), "local"))
}

func promotionEvidence(candidate LocalPromotionCandidate) []string {
	evidence := []string{"local-promotion:" + firstNonEmpty(candidate.CandidateID, candidate.SourceID)}
	for _, key := range uniqueTrimmedFocusStrings(candidate.DocKeys) {
		evidence = append(evidence, "doc:"+key)
	}
	for _, ref := range uniqueTrimmedFocusStrings(candidate.ArtifactRefs) {
		evidence = append(evidence, "artifact:"+ref)
	}
	return evidence
}

func promotionImportance(memoryType string) float64 {
	switch strings.ToUpper(strings.TrimSpace(memoryType)) {
	case "DECISION":
		return 0.8
	case "PROCEDURE":
		return 0.72
	case "INCIDENT":
		return 0.88
	case "LESSON":
		return 0.62
	default:
		return 0.55
	}
}

func promotionConfidence(memoryType string) float64 {
	switch strings.ToUpper(strings.TrimSpace(memoryType)) {
	case "DECISION":
		return 0.72
	case "PROCEDURE":
		return 0.68
	case "INCIDENT":
		return 0.85
	case "LESSON":
		return 0.58
	default:
		return 0.52
	}
}

func (r *Runtime) recordMemoryPromotionAttempt(candidateID, memoryID, claimID string) error {
	service := r.memoryService()
	if service == nil {
		return nil
	}
	return service.recordPromotionAttempt(candidateID, memoryID, claimID)
}

func (r *Runtime) markMemoryPromotionFailed(candidateID, memoryID, claimID string, cause error) error {
	service := r.memoryService()
	if service == nil {
		return nil
	}
	return service.markPromotionFailed(candidateID, memoryID, claimID, cause)
}

func (r *Runtime) markMemoryPromotionPromoted(candidateID, memoryID, claimID string) error {
	service := r.memoryService()
	if service == nil {
		return nil
	}
	return service.markPromotionPromoted(candidateID, memoryID, claimID)
}

func (r *Runtime) markMemoryPromotionSkipped(candidateID, memoryID, claimID, reason string) error {
	service := r.memoryService()
	if service == nil {
		return nil
	}
	return service.markPromotionSkipped(candidateID, memoryID, claimID, reason)
}
