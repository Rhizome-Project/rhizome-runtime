package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const agentBacklogPromotionContractVersion = "agent-personal-backlog-promotion/v1"

type AgentBacklogPromotionTarget struct {
	WorkspaceID         string
	AgentID             string
	OwnerUserID         string
	ProjectID           string
	ProjectLane         string
	TaskKind            string
	TaskTemplate        string
	Priority            string
	RequiresProjectGate *bool
	DependencyTaskIDs   []string
	WriteScopeHints     []string
	TaskRequirements    map[string]any
	Tags                []string
	Reason              string
}

type AgentBacklogPromotionContract struct {
	ContractVersion    string         `json:"contract_version"`
	WorkspaceID        string         `json:"workspace_id"`
	AgentID            string         `json:"agent_id"`
	ItemID             string         `json:"item_id"`
	DedupKey           string         `json:"dedup_key"`
	HeartbeatID        string         `json:"heartbeat_id,omitempty"`
	Kind               string         `json:"kind,omitempty"`
	Title              string         `json:"title"`
	Summary            string         `json:"summary,omitempty"`
	Score              int            `json:"score,omitempty"`
	TaskID             string         `json:"task_id"`
	TaskTitle          string         `json:"task_title"`
	TaskDescription    string         `json:"task_description"`
	TaskKind           string         `json:"task_kind"`
	TaskTemplate       string         `json:"task_template"`
	ProjectID          string         `json:"project_id,omitempty"`
	ProjectLane        string         `json:"project_lane,omitempty"`
	Priority           string         `json:"priority"`
	DocKey             string         `json:"doc_key"`
	DocTitle           string         `json:"doc_title"`
	DocContent         string         `json:"doc_content"`
	EvidenceRefs       []string       `json:"evidence_refs,omitempty"`
	DependencyTaskIDs  []string       `json:"dependency_task_ids,omitempty"`
	WriteScopeHints    []string       `json:"write_scope_hints,omitempty"`
	TaskRequirements   map[string]any `json:"task_requirements,omitempty"`
	Tags               []string       `json:"tags,omitempty"`
	Reason             string         `json:"reason,omitempty"`
	CreatedFromBacklog string         `json:"created_from_backlog,omitempty"`
}

func (s *AgentInternalSessionStore) ListBacklogPromotionCandidates(limit, minScore int, now time.Time) []AgentPersonalBacklogItem {
	if s == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]AgentPersonalBacklogItem, 0, len(s.state.Backlog))
	for _, item := range s.state.Backlog {
		item = normalizeAgentPersonalBacklogItem(item, now)
		if item.Stale || item.Status == "stale" || item.Status == "promoted" || item.Status == "completed" {
			continue
		}
		if item.Score < minScore {
			continue
		}
		if backlogItemSuppressedAt(item, now) {
			continue
		}
		if item.Status != "open" && item.Status != "suppressed" {
			continue
		}
		items = append(items, item)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score > items[j].Score
		}
		if items[i].UpdatedAt != items[j].UpdatedAt {
			return items[i].UpdatedAt < items[j].UpdatedAt
		}
		if items[i].CreatedAt != items[j].CreatedAt {
			return items[i].CreatedAt < items[j].CreatedAt
		}
		return items[i].ItemID < items[j].ItemID
	})
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return cloneAgentPersonalBacklogItems(items)
}

func (s *AgentInternalSessionStore) SuppressBacklogItem(itemID string, until time.Time, reason string, at time.Time) error {
	if s == nil {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.backlogIndex[strings.TrimSpace(itemID)]
	if !ok {
		return fmt.Errorf("personal backlog item %q not found", itemID)
	}
	item := s.state.Backlog[idx]
	if backlogStatusIsTerminal(item.Status) || item.Stale {
		return fmt.Errorf("personal backlog item %q is %s and cannot be suppressed", itemID, normalizeAgentPersonalBacklogStatus(item.Status))
	}
	item.Status = "suppressed"
	item.UpdatedAt = at.UTC().Format(time.RFC3339Nano)
	if until.IsZero() {
		until = at.Add(24 * time.Hour)
	}
	item.SuppressedUntil = until.UTC().Format(time.RFC3339Nano)
	item.Meta = mergeStringMaps(item.Meta, map[string]string{"suppressed_reason": firstNonEmpty(reason, "suppressed")})
	s.state.Backlog[idx] = normalizeAgentPersonalBacklogItem(item, at)
	s.reindexLocked()
	return s.saveLocked(at)
}

func (s *AgentInternalSessionStore) CompleteBacklogItem(itemID string, refs []string, reason string, at time.Time) error {
	if s == nil {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.backlogIndex[strings.TrimSpace(itemID)]
	if !ok {
		return fmt.Errorf("personal backlog item %q not found", itemID)
	}
	item := s.state.Backlog[idx]
	item.Status = "completed"
	item.CompletedAt = at.UTC().Format(time.RFC3339Nano)
	item.UpdatedAt = item.CompletedAt
	item.PromotionRefs = uniqueTrimmedCSVStrings(append(item.PromotionRefs, refs...))
	item.Meta = mergeStringMaps(item.Meta, map[string]string{"completed_reason": firstNonEmpty(reason, "completed")})
	s.state.Backlog[idx] = normalizeAgentPersonalBacklogItem(item, at)
	s.reindexLocked()
	return s.saveLocked(at)
}

func (s *AgentInternalSessionStore) BuildBacklogPromotionContract(itemID string, target AgentBacklogPromotionTarget, at time.Time) (AgentBacklogPromotionContract, error) {
	if s == nil {
		return AgentBacklogPromotionContract{}, fmt.Errorf("personal backlog store is nil")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.backlogItemLocked(itemID, at)
	if !ok {
		return AgentBacklogPromotionContract{}, fmt.Errorf("personal backlog item %q not found", itemID)
	}
	target.WorkspaceID = firstNonEmpty(target.WorkspaceID, s.state.WorkspaceID)
	target.AgentID = firstNonEmpty(target.AgentID, s.state.AgentID)
	return buildAgentBacklogPromotionContract(item, target, at)
}

func (s *AgentInternalSessionStore) PromoteBacklogItem(ctx context.Context, client *RhizomeClient, itemID string, target AgentBacklogPromotionTarget, at time.Time) (AgentBacklogPromotionContract, error) {
	if s == nil {
		return AgentBacklogPromotionContract{}, fmt.Errorf("personal backlog store is nil")
	}
	if client == nil {
		return AgentBacklogPromotionContract{}, fmt.Errorf("rhizome client is nil")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	contract, err := s.BuildBacklogPromotionContract(itemID, target, at)
	if err != nil {
		return contract, err
	}
	refs := []string{"task:" + contract.TaskID, "doc:" + contract.DocKey}
	if s.backlogItemAlreadyPromoted(itemID, refs) {
		return contract, nil
	}
	taskExists := false
	if tasks, err := client.ListTasks(ctx, contract.WorkspaceID); err != nil {
		return contract, err
	} else {
		for _, task := range tasks {
			if strings.EqualFold(strings.TrimSpace(task.TaskID), contract.TaskID) {
				taskExists = true
				break
			}
		}
	}
	docExists := false
	if doc, ok, err := client.GetDoc(ctx, contract.WorkspaceID, contract.DocKey); err != nil {
		return contract, err
	} else if ok && strings.TrimSpace(doc.DocKey) != "" {
		docExists = true
	}
	if !docExists {
		if _, err := client.PutDoc(ctx, WorkspaceDocPutInput{
			WorkspaceID: contract.WorkspaceID,
			DocKey:      contract.DocKey,
			Title:       contract.DocTitle,
			Content:     contract.DocContent,
			UpdatedBy:   contract.AgentID,
		}); err != nil {
			return contract, err
		}
	}
	if !taskExists {
		requiresProjectGate := target.RequiresProjectGate
		if taskSubmitRequiresHardProjectGate(contract.ProjectID, contract.TaskKind, contract.ProjectLane) {
			requiresProjectGate = boolPtr(true)
		}
		_, err := client.SubmitTask(ctx, TaskSubmitInput{
			WorkspaceID:         contract.WorkspaceID,
			TaskID:              contract.TaskID,
			OwnerUserID:         firstNonEmpty(target.OwnerUserID, contract.AgentID),
			Priority:            contract.Priority,
			Title:               contract.TaskTitle,
			Description:         contract.TaskDescription,
			TaskKind:            contract.TaskKind,
			TaskTemplate:        contract.TaskTemplate,
			ProjectID:           contract.ProjectID,
			ProjectLane:         contract.ProjectLane,
			RequiresProjectGate: requiresProjectGate,
			DependencyTaskIDs:   contract.DependencyTaskIDs,
			WriteScopeHints:     contract.WriteScopeHints,
			TaskRequirements:    contract.TaskRequirements,
			Tags:                contract.Tags,
			LinkedBy:            contract.AgentID,
		})
		if err != nil {
			return contract, err
		}
	}
	if err := s.MarkBacklogItemPromoted(itemID, refs, at); err != nil {
		return contract, err
	}
	return contract, nil
}

func (s *AgentInternalSessionStore) backlogItemLocked(itemID string, at time.Time) (AgentPersonalBacklogItem, bool) {
	idx, ok := s.backlogIndex[strings.TrimSpace(itemID)]
	if !ok {
		return AgentPersonalBacklogItem{}, false
	}
	return normalizeAgentPersonalBacklogItem(s.state.Backlog[idx], at), true
}

func (s *AgentInternalSessionStore) backlogItemAlreadyPromoted(itemID string, refs []string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.backlogItemLocked(itemID, time.Now().UTC())
	if !ok {
		return false
	}
	return item.Status == "promoted" && allTrimmedStringsPresent(item.PromotionRefs, refs)
}

func buildAgentBacklogPromotionContract(item AgentPersonalBacklogItem, target AgentBacklogPromotionTarget, at time.Time) (AgentBacklogPromotionContract, error) {
	workspaceID := strings.TrimSpace(target.WorkspaceID)
	agentID := strings.TrimSpace(target.AgentID)
	if workspaceID == "" || agentID == "" {
		return AgentBacklogPromotionContract{}, fmt.Errorf("workspace_id and agent_id are required for backlog promotion")
	}
	if strings.TrimSpace(item.ItemID) == "" {
		return AgentBacklogPromotionContract{}, fmt.Errorf("personal backlog item id is required")
	}
	if strings.TrimSpace(item.DedupKey) == "" {
		return AgentBacklogPromotionContract{}, fmt.Errorf("personal backlog item %s has no dedupe key", item.ItemID)
	}
	if item.Stale || item.Status == "stale" || item.Status == "completed" {
		return AgentBacklogPromotionContract{}, fmt.Errorf("personal backlog item %s is %s and cannot be promoted", item.ItemID, item.Status)
	}
	if item.Status == "suppressed" && backlogItemSuppressedAt(item, at) {
		return AgentBacklogPromotionContract{}, fmt.Errorf("personal backlog item %s is suppressed until %s", item.ItemID, item.SuppressedUntil)
	}
	taskKind, projectLane := normalizeTaskSubmitTaskKindAndLane(firstNonEmpty(target.TaskKind, "EXECUTION"), target.ProjectLane)
	taskTemplate := firstNonEmpty(strings.ToLower(strings.TrimSpace(target.TaskTemplate)), "generic")
	priority := firstNonEmpty(strings.ToLower(strings.TrimSpace(target.Priority)), backlogPriorityForScore(item.Score))
	taskID := deterministicBacklogPromotionTaskID(workspaceID, agentID, item, target)
	docKey := "task." + taskID
	title := firstNonEmpty(item.Title, "Autonomous backlog initiative")
	taskTitle := trimBacklogTaskTitle("Address: " + title)
	reason := firstNonEmpty(target.Reason, "promoted from personal backlog")
	tags := uniqueTrimmedCSVStrings(target.Tags, []string{"agent-backlog", sanitizeRefSegment(agentID), sanitizeRefSegment(item.Kind)})
	description := renderAgentBacklogTaskDescription(item, agentID, reason, docKey)
	contract := AgentBacklogPromotionContract{
		ContractVersion:    agentBacklogPromotionContractVersion,
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		ItemID:             strings.TrimSpace(item.ItemID),
		DedupKey:           strings.TrimSpace(item.DedupKey),
		HeartbeatID:        strings.TrimSpace(item.HeartbeatID),
		Kind:               strings.TrimSpace(item.Kind),
		Title:              title,
		Summary:            strings.TrimSpace(item.Summary),
		Score:              item.Score,
		TaskID:             taskID,
		TaskTitle:          taskTitle,
		TaskDescription:    description,
		TaskKind:           taskKind,
		TaskTemplate:       taskTemplate,
		ProjectID:          strings.TrimSpace(target.ProjectID),
		ProjectLane:        projectLane,
		Priority:           priority,
		DocKey:             docKey,
		DocTitle:           "Autonomous Backlog Promotion - " + title,
		EvidenceRefs:       publicBacklogEvidenceRefs(item.EvidenceRefs),
		DependencyTaskIDs:  uniqueTrimmedCSVStrings(target.DependencyTaskIDs),
		WriteScopeHints:    uniqueTrimmedCSVStrings(target.WriteScopeHints),
		TaskRequirements:   cloneStringAnyMap(target.TaskRequirements),
		Tags:               tags,
		Reason:             reason,
		CreatedFromBacklog: item.ItemID,
	}
	contract.DocContent = renderAgentBacklogPromotionDoc(contract, item, at)
	if err := validateTaskSubmitImplementationDeliverable(contract.TaskKind, contract.ProjectLane, contract.TaskTitle, contract.TaskDescription); err != nil {
		return AgentBacklogPromotionContract{}, err
	}
	return contract, nil
}

func publicBacklogEvidenceRefs(refs []string) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range uniqueTrimmedCSVStrings(refs) {
		lower := strings.ToLower(strings.TrimSpace(ref))
		if lower == "" ||
			strings.HasPrefix(lower, "internal_session:") ||
			strings.HasPrefix(lower, "internal:") ||
			strings.HasPrefix(lower, "local:") ||
			strings.HasPrefix(lower, "memory:local") {
			continue
		}
		out = append(out, ref)
	}
	return uniqueTrimmedCSVStrings(out)
}

func cloneStringAnyMap(source map[string]any) map[string]any {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]any, len(source))
	for key, value := range source {
		switch typed := value.(type) {
		case []string:
			out[key] = append([]string(nil), typed...)
		case []any:
			out[key] = append([]any(nil), typed...)
		case map[string]any:
			out[key] = cloneStringAnyMap(typed)
		default:
			out[key] = value
		}
	}
	return out
}

func deterministicBacklogPromotionTaskID(workspaceID, agentID string, item AgentPersonalBacklogItem, target AgentBacklogPromotionTarget) string {
	if strings.EqualFold(strings.TrimSpace(item.Meta["finding_source"]), internalHeartbeatProjectInitiativeSensorSource) &&
		strings.TrimSpace(target.ProjectID) != "" {
		projectID := strings.TrimSpace(target.ProjectID)
		seed := strings.Join([]string{workspaceID, projectID, strings.TrimSpace(item.DedupKey)}, "\x00")
		return "task-agent-backlog-project-role-" + sanitizeRefSegment(projectID) + "-" + shortRefHash(seed)
	}
	seed := strings.Join([]string{workspaceID, agentID, item.ItemID, item.DedupKey}, "\x00")
	return "task-agent-backlog-" + sanitizeRefSegment(agentID) + "-" + shortRefHash(seed)
}

func backlogPriorityForScore(score int) string {
	switch {
	case score >= 90:
		return "critical"
	case score >= 70:
		return "high"
	case score >= 40:
		return "normal"
	default:
		return "low"
	}
}

func trimBacklogTaskTitle(title string) string {
	title = strings.TrimSpace(title)
	if len(title) <= 120 {
		return title
	}
	return strings.TrimSpace(title[:117]) + "..."
}

func renderAgentBacklogTaskDescription(item AgentPersonalBacklogItem, agentID, reason, docKey string) string {
	var b strings.Builder
	b.WriteString("Autonomous initiative promoted from agent personal backlog.\n\n")
	b.WriteString("- agent_id: ")
	b.WriteString(strings.TrimSpace(agentID))
	b.WriteString("\n- backlog_item_id: ")
	b.WriteString(strings.TrimSpace(item.ItemID))
	b.WriteString("\n- dedup_key: ")
	b.WriteString(strings.TrimSpace(item.DedupKey))
	b.WriteString("\n- source_heartbeat: ")
	b.WriteString(firstNonEmpty(item.HeartbeatID, "unknown"))
	b.WriteString("\n- promotion_doc: ")
	b.WriteString(strings.TrimSpace(docKey))
	b.WriteString("\n- reason: ")
	b.WriteString(strings.TrimSpace(reason))
	b.WriteString("\n\n## Finding\n")
	b.WriteString(firstNonEmpty(item.Summary, item.Title))
	b.WriteString("\n\n")
	appendMarkdownList(&b, "## Evidence", publicBacklogEvidenceRefs(item.EvidenceRefs))
	b.WriteString("## Acceptance\n")
	b.WriteString("- Verify the issue against current workspace docs, task state, and product evidence before editing.\n")
	b.WriteString("- If the issue is still real, implement or coordinate the smallest durable fix.\n")
	b.WriteString("- Publish reviewable evidence and update the promotion doc or task notes with the outcome.\n")
	return strings.TrimSpace(b.String())
}

func renderAgentBacklogPromotionDoc(contract AgentBacklogPromotionContract, item AgentPersonalBacklogItem, at time.Time) string {
	var b strings.Builder
	b.WriteString("# Autonomous Backlog Promotion - ")
	b.WriteString(contract.Title)
	b.WriteString("\n\n")
	b.WriteString("- contract_version: ")
	b.WriteString(contract.ContractVersion)
	b.WriteString("\n- workspace_id: ")
	b.WriteString(contract.WorkspaceID)
	b.WriteString("\n- agent_id: ")
	b.WriteString(contract.AgentID)
	b.WriteString("\n- backlog_item_id: ")
	b.WriteString(contract.ItemID)
	b.WriteString("\n- dedup_key: ")
	b.WriteString(contract.DedupKey)
	b.WriteString("\n- task_id: ")
	b.WriteString(contract.TaskID)
	b.WriteString("\n- score: ")
	b.WriteString(fmt.Sprint(contract.Score))
	b.WriteString("\n- promoted_at: ")
	b.WriteString(at.UTC().Format(time.RFC3339Nano))
	b.WriteString("\n\n## Finding\n")
	b.WriteString(firstNonEmpty(contract.Summary, item.Title))
	b.WriteString("\n\n")
	appendMarkdownList(&b, "## Evidence", contract.EvidenceRefs)
	b.WriteString("## Promotion Contract\n")
	b.WriteString("- This document is deterministic for the backlog item and should be updated, not duplicated.\n")
	b.WriteString("- The paired task id is stable: `")
	b.WriteString(contract.TaskID)
	b.WriteString("`.\n")
	b.WriteString("- Duplicate observations should merge back into local personal memory unless a genuinely new issue is found.\n")
	if len(contract.TaskRequirements) > 0 {
		if raw, err := json.MarshalIndent(contract.TaskRequirements, "", "  "); err == nil {
			b.WriteString("\n## Task Requirements\n")
			b.WriteString("```json\n")
			b.Write(raw)
			b.WriteString("\n```\n")
		}
	}
	return strings.TrimSpace(b.String())
}

func backlogItemSuppressedAt(item AgentPersonalBacklogItem, now time.Time) bool {
	if strings.TrimSpace(item.SuppressedUntil) == "" {
		return item.Status == "suppressed"
	}
	until, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.SuppressedUntil))
	if err != nil {
		return item.Status == "suppressed"
	}
	return now.Before(until)
}

func backlogStatusIsTerminal(status string) bool {
	switch normalizeAgentPersonalBacklogStatus(status) {
	case "promoted", "completed", "stale":
		return true
	default:
		return false
	}
}

func cloneAgentPersonalBacklogItems(items []AgentPersonalBacklogItem) []AgentPersonalBacklogItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]AgentPersonalBacklogItem, len(items))
	copy(out, items)
	for idx := range out {
		out[idx].EvidenceRefs = append([]string(nil), out[idx].EvidenceRefs...)
		out[idx].PromotionRefs = append([]string(nil), out[idx].PromotionRefs...)
		out[idx].TaskIDs = append([]string(nil), out[idx].TaskIDs...)
		out[idx].DocKeys = append([]string(nil), out[idx].DocKeys...)
		out[idx].StaleReasons = append([]string(nil), out[idx].StaleReasons...)
		out[idx].Meta = mergeStringMaps(out[idx].Meta, nil)
	}
	return out
}
