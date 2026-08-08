package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

type runtimeOwnerBoundRequirement struct {
	Kind            string
	ProjectID       string
	QueueID         string
	ItemID          string
	BranchID        string
	BranchName      string
	RequiredAgentID string
	RepairNeeded    bool
	Reason          string
}

func (r *Runtime) ownerBoundRequirementForTask(ctx context.Context, task WorkspaceTaskRecord) (runtimeOwnerBoundRequirement, bool, error) {
	projectID := strings.TrimSpace(task.ProjectID)
	if r == nil || r.client == nil || strings.TrimSpace(r.cfg.WorkspaceID) == "" || projectID == "" || !runtimeTaskHasOwnerBoundSignal(task) {
		return runtimeOwnerBoundRequirement{}, false, nil
	}
	coordination, err := r.client.GetProjectCoordination(ctx, strings.TrimSpace(r.cfg.WorkspaceID), projectID)
	if err != nil {
		return runtimeOwnerBoundRequirement{}, false, err
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil || !ok || !runtimeOwnerBoundRequirementNeedsBranchExpansion(req) {
		return req, ok, err
	}
	branches, err := r.client.ListProjectBranches(ctx, strings.TrimSpace(r.cfg.WorkspaceID), projectID, true)
	if err != nil {
		return runtimeOwnerBoundRequirement{}, false, err
	}
	coordination.Branches = runtimeOwnerBoundMergeBranches(coordination.Branches, branches)
	return runtimeOwnerBoundRequirementFromCoordination(task, coordination)
}

func runtimeOwnerBoundRequirementFromCoordination(task WorkspaceTaskRecord, coordination ProjectCoordinationRecord) (runtimeOwnerBoundRequirement, bool, error) {
	projectID := firstNonEmpty(strings.TrimSpace(task.ProjectID), strings.TrimSpace(coordination.Project.ProjectID))
	if projectID == "" || !runtimeTaskHasOwnerBoundSignal(task) {
		return runtimeOwnerBoundRequirement{}, false, nil
	}
	queueID, itemID, branchID := runtimeOwnerBoundPatchQueueRefsFromTask(task)
	req := runtimeOwnerBoundRequirement{
		Kind:            firstNonEmpty(runtimeTaskTagValue(task, "owner-bound-kind:", "owner-bound-kind=", "owner_bound_kind:", "owner_bound_kind="), runtimeTaskRequirementString(task, "owner_bound_kind", "owner-bound-kind", "patch_queue_task_kind"), runtimeTaskImplicitOwnerBoundKind(task), "patch_queue_submit"),
		ProjectID:       projectID,
		QueueID:         firstNonEmpty(runtimeTaskTagValue(task, "queue:", "queue=", "queue-id:", "queue-id=", "queue_id:", "queue_id="), queueID),
		ItemID:          firstNonEmpty(runtimeTaskTagValue(task, "item:", "item=", "item-id:", "item-id=", "item_id:", "item_id="), itemID),
		BranchID:        firstNonEmpty(runtimeTaskTagValue(task, "owner-branch:", "owner-branch=", "owner_branch:", "owner_branch=", "branch:", "branch=", "branch-id:", "branch-id=", "branch_id:", "branch_id="), branchID, pointerValue(task.ClaimBranchID)),
		BranchName:      firstNonEmpty(runtimeTaskTextFieldValue([]string{task.Title, task.Description}, "branch_name"), runtimeTaskTextFieldValue([]string{task.Title, task.Description}, "Branch name")),
		RequiredAgentID: firstNonEmpty(runtimeTaskTagValue(task, "required-agent:", "required-agent=", "required_agent:", "required_agent=", "required-agent-id:", "required-agent-id=", "owner-agent:", "owner-agent=", "owner_agent:", "owner_agent="), runtimeTaskRequirementString(task, "required_agent_id", "required_agent", "owner_agent_id", "owner_agent")),
	}
	if req.BranchID == "" && (req.QueueID != "" || req.ItemID != "") {
		if req.QueueID == "" || req.ItemID == "" {
			req.RepairNeeded = true
			req.Reason = "owner-bound patch queue reference must include both queue_id and item_id when branch_id is absent"
			return req, true, nil
		}
		if item, ok := runtimeOwnerBoundPatchQueueItem(coordination.PatchQueueItems, req.QueueID, req.ItemID); ok {
			req.QueueID = firstNonEmpty(req.QueueID, strings.TrimSpace(item.QueueID))
			req.ItemID = firstNonEmpty(req.ItemID, strings.TrimSpace(item.ItemID))
			req.BranchID = strings.TrimSpace(item.BranchID)
		} else {
			req.RepairNeeded = true
			req.Reason = "owner-bound patch queue reference did not resolve to a branch"
			return req, true, nil
		}
	}
	if strings.EqualFold(strings.TrimSpace(req.Kind), "active_lane_publication") &&
		strings.TrimSpace(req.BranchID) == "" &&
		strings.TrimSpace(req.BranchName) == "" &&
		strings.TrimSpace(req.RequiredAgentID) == "" {
		if branch, ok, ambiguous := runtimeOwnerBoundUniqueOpenBranchForProject(coordination.Branches); ok {
			req.BranchID = strings.TrimSpace(branch.BranchID)
			req.BranchName = strings.TrimSpace(branch.BranchName)
		} else if ambiguous {
			req.RepairNeeded = true
			req.Reason = "active-lane publication task matches multiple open project branches"
			return req, true, nil
		} else {
			req.RepairNeeded = true
			req.Reason = "active-lane publication task has no open project branch owner"
			return req, true, nil
		}
	}
	if strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") &&
		strings.TrimSpace(req.BranchID) == "" &&
		strings.TrimSpace(req.BranchName) == "" &&
		strings.TrimSpace(req.RequiredAgentID) != "" {
		if branch, ok, ambiguous := runtimeOwnerBoundUniqueOpenBranchForOwner(coordination.Branches, req.RequiredAgentID); ok {
			req.BranchID = strings.TrimSpace(branch.BranchID)
			req.BranchName = strings.TrimSpace(branch.BranchName)
		} else if ambiguous {
			req.RepairNeeded = true
			req.Reason = fmt.Sprintf("owner-bound patch queue submit task matches multiple open branches for required agent %s", strings.TrimSpace(req.RequiredAgentID))
			return req, true, nil
		}
	}
	if strings.TrimSpace(req.BranchID) == "" && strings.TrimSpace(req.BranchName) == "" {
		if branch, ok, ambiguous := runtimeOwnerBoundBranchMentionedInTask(coordination.Branches, task); ok {
			req.BranchID = strings.TrimSpace(branch.BranchID)
			req.BranchName = strings.TrimSpace(branch.BranchName)
		} else if ambiguous {
			req.RepairNeeded = true
			req.Reason = "owner-bound task mentions multiple registered branches"
			return req, true, nil
		}
	}
	if branch, ok := runtimeOwnerBoundResolveBranch(coordination.Branches, req.BranchID, req.BranchName); ok {
		req.BranchID = strings.TrimSpace(branch.BranchID)
		req.BranchName = strings.TrimSpace(branch.BranchName)
		if owner := strings.TrimSpace(branch.AgentID); owner != "" {
			if taggedOwner := strings.TrimSpace(req.RequiredAgentID); taggedOwner != "" && taggedOwner != owner {
				req.RequiredAgentID = owner
				req.RepairNeeded = true
				req.Reason = fmt.Sprintf("owner-bound required agent %s conflicts with branch owner %s", taggedOwner, owner)
			} else {
				req.RequiredAgentID = owner
			}
		} else {
			req.RepairNeeded = true
			req.Reason = "owner-bound branch has no recorded owner"
		}
	} else if req.BranchID != "" {
		req.RepairNeeded = true
		req.Reason = "owner-bound branch is not registered in project coordination"
	}
	if strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") && strings.TrimSpace(req.BranchID) == "" && !req.RepairNeeded {
		req.RepairNeeded = true
		req.Reason = "owner-bound patch queue submit task does not identify a concrete branch"
	}
	if req.RequiredAgentID == "" && !req.RepairNeeded {
		req.RepairNeeded = true
		req.Reason = "owner-bound task does not identify a required agent"
	}
	return req, true, nil
}

func runtimeTaskHasOwnerBoundSignal(task WorkspaceTaskRecord) bool {
	if runtimeOwnerBoundTaskHasTag(task, "owner-bound") ||
		runtimeOwnerBoundTaskHasTag(task, "owner-submit", "owner_submit", "branch-owner-submit", "branch_owner_submit") ||
		runtimeOwnerBoundTaskHasTagPrefix(task,
			"owner-bound-kind:", "owner-bound-kind=", "owner_bound_kind:", "owner_bound_kind=",
			"required-agent:", "required-agent=", "required_agent:", "required_agent=",
			"required-agent-id:", "required-agent-id=",
			"owner-agent:", "owner-agent=", "owner_agent:", "owner_agent=",
			"owner-branch:", "owner-branch=", "owner_branch:", "owner_branch=",
		) {
		return true
	}
	if runtimeTaskLooksPatchQueueRevisionOwnerBound(task) {
		return true
	}
	// Descriptions often include copied task lists and historical hints. Only
	// treat owner-only wording in the task identity/title/template/tags as a
	// routing contract for this task.
	fullText := strings.ToLower(strings.Join([]string{
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(task.Title),
		strings.TrimSpace(task.TaskTemplate),
		strings.Join(task.Tags, " "),
	}, "\n"))
	return runtimeTextHasPositiveOwnerBoundSignal(fullText) || runtimeTaskLooksActiveLanePublication(task)
}

func runtimeTaskImplicitOwnerBoundKind(task WorkspaceTaskRecord) string {
	if runtimeTaskLooksActiveLanePublication(task) {
		return "active_lane_publication"
	}
	if runtimeTaskLooksPatchQueueRevisionOwnerBound(task) {
		return "patch_queue_revision"
	}
	return ""
}

func runtimeTaskLooksPatchQueueRevisionOwnerBound(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	kind := strings.ToLower(strings.TrimSpace(runtimeTaskRequirementString(task, "patch_queue_task_kind", "owner_bound_kind", "owner-bound-kind")))
	transition := strings.ToLower(strings.TrimSpace(runtimeTaskRequirementString(task, "required_transition")))
	decisiveKind := strings.ToLower(strings.TrimSpace(runtimeTaskRequirementString(task, "decisive_path_kind")))
	if kind != "revision" && kind != "patch_queue_revision" &&
		transition != "project_patch_queue_revision_commit_review_submit" &&
		decisiveKind != "patch_queue_revision_followup" {
		return false
	}
	queueID, itemID, branchID := runtimeOwnerBoundPatchQueueRefsFromTask(task)
	return strings.TrimSpace(queueID) != "" && strings.TrimSpace(itemID) != "" && strings.TrimSpace(branchID) != ""
}

func runtimeTaskLooksActiveLanePublication(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(task.Title),
		strings.TrimSpace(task.TaskTemplate),
		strings.TrimSpace(task.ProjectLane),
		strings.Join(task.Tags, " "),
	}, "\n"))
	if strings.TrimSpace(text) == "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(task.ProjectLane), "integration") && containsAnySignal(text, []string{
		"integrate",
		"integration convergence",
		"implementation lanes",
		"merge lanes",
		"merge lane",
		"cross-lane",
		"cross lane",
	}) {
		return false
	}
	return containsAnySignal(text, []string{
		"candidate provenance",
		"publish provenance",
		"provenance status",
		"provenance review",
		"candidate publication",
		"candidate-publication",
		"runnable candidate publication",
		"runnable-candidate-publication",
		"publish candidate",
		"publish runnable candidate",
		"publish exact runnable",
		"publish exact runnable candidate",
		"publish review-ready",
		"publish review ready",
		"review-ready publication",
		"review ready publication",
		"publish candidate evidence",
		"publication-gap",
		"publication gap",
		"lane provenance",
		"implementation lane provenance",
		"publish durable provenance",
		"publish current status",
		"publish current evidence",
	})
}

func runtimeTextHasPositiveOwnerBoundSignal(fullText string) bool {
	fullText = strings.ToLower(strings.TrimSpace(fullText))
	if fullText == "" {
		return false
	}
	for _, negated := range []string{
		"not owner-only",
		"not owner only",
		"not owner-bound",
		"not owner bound",
		"not branch-owner-only",
		"not branch owner only",
		"is not owner-only",
		"is not owner only",
		"is not owner-bound",
		"is not owner bound",
		"is not branch-owner-only",
		"is not branch owner only",
		"without claiming owner-only",
		"without claiming owner only",
		"without claiming owner-submit",
		"without claiming owner submit",
		"without claiming branch-owner-only",
		"without claiming branch owner only",
		"without claiming branch-owner-submit",
		"without claiming branch owner submit",
		"not owner-submit",
		"not owner submit",
		"not branch-owner-submit",
		"not branch owner submit",
	} {
		if strings.Contains(fullText, negated) {
			return false
		}
	}
	return strings.Contains(fullText, "owner-only") ||
		strings.Contains(fullText, "owner only") ||
		strings.Contains(fullText, "branch-owner-only") ||
		strings.Contains(fullText, "branch owner only") ||
		strings.Contains(fullText, "owner requeue submit") ||
		strings.Contains(fullText, "branch owner submit")
}

func runtimeOwnerBoundTaskHasTag(task WorkspaceTaskRecord, tags ...string) bool {
	for _, existing := range task.Tags {
		existing = strings.ToLower(strings.TrimSpace(existing))
		for _, tag := range tags {
			if existing == strings.ToLower(strings.TrimSpace(tag)) {
				return true
			}
		}
	}
	return false
}

func runtimeOwnerBoundTaskHasTagPrefix(task WorkspaceTaskRecord, prefixes ...string) bool {
	for _, existing := range task.Tags {
		existing = strings.ToLower(strings.TrimSpace(existing))
		for _, prefix := range prefixes {
			prefix = strings.ToLower(strings.TrimSpace(prefix))
			if prefix != "" && strings.HasPrefix(existing, prefix) {
				return true
			}
		}
	}
	return false
}

func runtimeTaskTagValue(task WorkspaceTaskRecord, prefixes ...string) string {
	for _, existing := range task.Tags {
		trimmed := strings.TrimSpace(existing)
		lower := strings.ToLower(trimmed)
		for _, prefix := range prefixes {
			prefix = strings.ToLower(strings.TrimSpace(prefix))
			if prefix == "" || !strings.HasPrefix(lower, prefix) {
				continue
			}
			if value := strings.TrimSpace(trimmed[len(prefix):]); value != "" {
				return strings.Trim(value, "`'\"")
			}
		}
	}
	return ""
}

func runtimeOwnerBoundPatchQueueRefsFromTask(task WorkspaceTaskRecord) (string, string, string) {
	texts := []string{task.Title, task.Description}
	queueID := firstNonEmpty(runtimeTaskRequirementString(task, "queue_id", "queue", "patch_queue_id"), runtimeTaskTextFieldValue(texts, "queue_id"))
	itemID := firstNonEmpty(runtimeTaskRequirementString(task, "item_id", "item", "patch_queue_item_id"), runtimeTaskTextFieldValue(texts, "item_id"))
	branchID := firstNonEmpty(runtimeTaskRequirementString(task, "branch_id", "branch", "owner_branch"), runtimeTaskTextFieldValue(texts, "branch_id"))
	if branchID == "" {
		branchID = runtimeTaskTextFieldValue(texts, "Branch ID")
	}
	if queueID == "" || itemID == "" {
		combined := runtimeTaskTextFieldValue(texts, "Patch queue")
		if left, right, ok := strings.Cut(combined, "/"); ok {
			queueID = firstNonEmpty(queueID, strings.TrimSpace(left))
			itemID = firstNonEmpty(itemID, strings.TrimSpace(right))
		}
	}
	return strings.TrimSpace(queueID), strings.TrimSpace(itemID), strings.TrimSpace(branchID)
}

func runtimeTaskRequirementString(task WorkspaceTaskRecord, keys ...string) string {
	raw := strings.TrimSpace(task.TaskRequirementsJSON)
	if raw == "" || raw == "{}" || len(keys) == 0 {
		return ""
	}
	var values map[string]any
	if err := json.Unmarshal([]byte(raw), &values); err != nil || len(values) == 0 {
		return ""
	}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value, ok := values[key]; ok {
			switch typed := value.(type) {
			case string:
				if out := strings.TrimSpace(typed); out != "" {
					return out
				}
			case fmt.Stringer:
				if out := strings.TrimSpace(typed.String()); out != "" {
					return out
				}
			default:
				if out := strings.TrimSpace(fmt.Sprint(typed)); out != "" && out != "<nil>" {
					return out
				}
			}
		}
	}
	return ""
}

func runtimeTaskTextFieldValue(texts []string, key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	for _, text := range texts {
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "-*"))
			fieldKey, value, ok := strings.Cut(line, ":")
			if !ok {
				fieldKey, value, ok = strings.Cut(line, "=")
			}
			if ok && strings.ToLower(strings.TrimSpace(fieldKey)) == key {
				return strings.Trim(strings.TrimSpace(value), "`'\"")
			}
			if value := runtimeInlineTaskTextFieldValue(line, key); value != "" {
				return value
			}
		}
	}
	return ""
}

func runtimeInlineTaskTextFieldValue(text, key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return ""
	}
	lower := strings.ToLower(text)
	for _, sep := range []string{"=", ":"} {
		marker := key + sep
		idx := strings.Index(lower, marker)
		if idx < 0 {
			continue
		}
		value := strings.TrimLeft(text[idx+len(marker):], " \t`'\"")
		if value == "" {
			continue
		}
		end := len(value)
		for i, r := range value {
			if strings.ContainsRune(" \t\r\n,);]}`'\"", r) {
				end = i
				break
			}
		}
		if trimmed := strings.TrimSpace(value[:end]); trimmed != "" {
			return strings.Trim(trimmed, "`'\"")
		}
	}
	return ""
}

func runtimeOwnerBoundPatchQueueItem(items []ProjectPatchQueueItemRecord, queueID, itemID string) (ProjectPatchQueueItemRecord, bool) {
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	for _, item := range items {
		if queueID != "" && strings.TrimSpace(item.QueueID) != queueID {
			continue
		}
		if itemID != "" && strings.TrimSpace(item.ItemID) != itemID {
			continue
		}
		return item, true
	}
	return ProjectPatchQueueItemRecord{}, false
}

func runtimeOwnerBoundResolveBranch(branches []ProjectBranchRecord, branchID, branchName string) (ProjectBranchRecord, bool) {
	branchID = strings.TrimSpace(branchID)
	branchName = strings.TrimSpace(branchName)
	for _, branch := range branches {
		if branchID != "" && strings.TrimSpace(branch.BranchID) == branchID {
			return branch, true
		}
	}
	if branchName != "" {
		for _, branch := range branches {
			if strings.TrimSpace(branch.BranchName) == branchName {
				return branch, true
			}
		}
	}
	return ProjectBranchRecord{}, false
}

func runtimeOwnerBoundUniqueOpenBranchForOwner(branches []ProjectBranchRecord, ownerAgentID string) (ProjectBranchRecord, bool, bool) {
	ownerAgentID = strings.TrimSpace(ownerAgentID)
	if ownerAgentID == "" {
		return ProjectBranchRecord{}, false, false
	}
	var preferred []ProjectBranchRecord
	var fallback []ProjectBranchRecord
	for _, branch := range branches {
		if strings.TrimSpace(branch.AgentID) != ownerAgentID {
			continue
		}
		if runtimeOwnerBoundBranchStatusIsTerminal(branch.Status) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(branch.Status), "READY_FOR_REVIEW") {
			preferred = append(preferred, branch)
			continue
		}
		fallback = append(fallback, branch)
	}
	switch {
	case len(preferred) == 1:
		return preferred[0], true, false
	case len(preferred) > 1:
		return ProjectBranchRecord{}, false, true
	case len(fallback) == 1:
		return fallback[0], true, false
	case len(fallback) > 1:
		return ProjectBranchRecord{}, false, true
	default:
		return ProjectBranchRecord{}, false, false
	}
}

func runtimeOwnerBoundUniqueOpenBranchForProject(branches []ProjectBranchRecord) (ProjectBranchRecord, bool, bool) {
	var preferred []ProjectBranchRecord
	var fallback []ProjectBranchRecord
	for _, branch := range branches {
		if runtimeOwnerBoundBranchStatusIsTerminal(branch.Status) {
			continue
		}
		if strings.TrimSpace(branch.AgentID) == "" {
			continue
		}
		if strings.TrimSpace(branch.ActiveTaskID) != "" || strings.EqualFold(strings.TrimSpace(branch.Status), "ACTIVE") {
			preferred = append(preferred, branch)
			continue
		}
		fallback = append(fallback, branch)
	}
	switch {
	case len(preferred) == 1:
		return preferred[0], true, false
	case len(preferred) > 1:
		return ProjectBranchRecord{}, false, true
	case len(fallback) == 1:
		return fallback[0], true, false
	case len(fallback) > 1:
		return ProjectBranchRecord{}, false, true
	default:
		return ProjectBranchRecord{}, false, false
	}
}

func runtimeOwnerBoundBranchMentionedInTask(branches []ProjectBranchRecord, task WorkspaceTaskRecord) (ProjectBranchRecord, bool, bool) {
	identityText := strings.ToLower(strings.Join([]string{
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(task.Title),
		strings.Join(task.Tags, " "),
	}, "\n"))
	if branch, ok, ambiguous := runtimeOwnerBoundBranchIDMentionedInText(branches, identityText); ok || ambiguous {
		return branch, ok, ambiguous
	}
	if branch, ok, ambiguous := runtimeOwnerBoundBranchNameMentionedInText(branches, identityText); ok || ambiguous {
		return branch, ok, ambiguous
	}
	fullText := strings.ToLower(strings.Join([]string{
		identityText,
		strings.TrimSpace(task.Description),
	}, "\n"))
	if branch, ok, ambiguous := runtimeOwnerBoundBranchIDMentionedInText(branches, fullText); ok || ambiguous {
		return branch, ok, ambiguous
	}
	return runtimeOwnerBoundBranchNameMentionedInText(branches, fullText)
}

func runtimeOwnerBoundBranchIDMentionedInText(branches []ProjectBranchRecord, text string) (ProjectBranchRecord, bool, bool) {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return ProjectBranchRecord{}, false, false
	}
	var matches []ProjectBranchRecord
	seen := map[string]struct{}{}
	for _, branch := range branches {
		branchID := strings.TrimSpace(branch.BranchID)
		if branchID == "" {
			continue
		}
		if !runtimeOwnerBoundTextContainsIdentifier(text, branchID) {
			continue
		}
		if _, ok := seen[branchID]; ok {
			continue
		}
		seen[branchID] = struct{}{}
		matches = append(matches, branch)
	}
	switch len(matches) {
	case 0:
		return ProjectBranchRecord{}, false, false
	case 1:
		return matches[0], true, false
	default:
		return ProjectBranchRecord{}, false, true
	}
}

func runtimeOwnerBoundBranchNameMentionedInText(branches []ProjectBranchRecord, text string) (ProjectBranchRecord, bool, bool) {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return ProjectBranchRecord{}, false, false
	}
	var matches []ProjectBranchRecord
	seen := map[string]struct{}{}
	for _, branch := range branches {
		branchID := strings.TrimSpace(branch.BranchID)
		branchName := strings.TrimSpace(branch.BranchName)
		if branchName == "" {
			continue
		}
		if !runtimeOwnerBoundTextContainsIdentifier(text, branchName) {
			continue
		}
		key := firstNonEmpty(branchID, branchName)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		matches = append(matches, branch)
	}
	return runtimeOwnerBoundSelectMentionedBranch(matches)
}

func runtimeOwnerBoundSelectMentionedBranch(matches []ProjectBranchRecord) (ProjectBranchRecord, bool, bool) {
	switch len(matches) {
	case 0:
		return ProjectBranchRecord{}, false, false
	case 1:
		return matches[0], true, false
	}
	var live []ProjectBranchRecord
	for _, branch := range matches {
		if !runtimeOwnerBoundBranchStatusIsTerminal(branch.Status) {
			live = append(live, branch)
		}
	}
	switch len(live) {
	case 1:
		return live[0], true, false
	case 0:
		return ProjectBranchRecord{}, false, true
	default:
		return ProjectBranchRecord{}, false, true
	}
}

func runtimeOwnerBoundTextContainsIdentifier(text, identifier string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	identifier = strings.ToLower(strings.TrimSpace(identifier))
	if text == "" || identifier == "" {
		return false
	}
	offset := 0
	for {
		idx := strings.Index(text[offset:], identifier)
		if idx < 0 {
			return false
		}
		start := offset + idx
		end := start + len(identifier)
		if runtimeOwnerBoundIdentifierBoundary(text, start-1) && runtimeOwnerBoundIdentifierBoundary(text, end) {
			return true
		}
		offset = start + 1
	}
}

func runtimeOwnerBoundIdentifierBoundary(text string, idx int) bool {
	if idx < 0 || idx >= len(text) {
		return true
	}
	ch := text[idx]
	return !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '/' || ch == '.')
}

func runtimeOwnerBoundRequirementNeedsBranchExpansion(req runtimeOwnerBoundRequirement) bool {
	if strings.TrimSpace(req.BranchID) != "" || strings.TrimSpace(req.BranchName) != "" {
		return false
	}
	if !req.RepairNeeded {
		return false
	}
	reason := strings.ToLower(strings.TrimSpace(req.Reason))
	return strings.Contains(reason, "does not identify a concrete branch") ||
		strings.Contains(reason, "mentions multiple registered branches")
}

func runtimeOwnerBoundMergeBranches(primary, extra []ProjectBranchRecord) []ProjectBranchRecord {
	if len(extra) == 0 {
		return primary
	}
	merged := append([]ProjectBranchRecord{}, primary...)
	seen := map[string]struct{}{}
	for _, branch := range merged {
		key := firstNonEmpty(strings.TrimSpace(branch.BranchID), strings.TrimSpace(branch.BranchName))
		if key != "" {
			seen[key] = struct{}{}
		}
	}
	for _, branch := range extra {
		key := firstNonEmpty(strings.TrimSpace(branch.BranchID), strings.TrimSpace(branch.BranchName))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, branch)
	}
	return merged
}

func runtimeOwnerBoundBranchStatusIsTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "MERGED", "INTEGRATED", "CLOSED", "CANCELLED", "CANCELED", "ABANDONED", "FAILED", "RELEASED", "EXPIRED":
		return true
	default:
		return false
	}
}

func (r *Runtime) delegatedOwnerBoundTaskBlocker(ctx context.Context, task WorkspaceTaskRecord) string {
	req, ok, err := r.ownerBoundRequirementForTask(ctx, task)
	if err != nil {
		return fmt.Sprintf("task %s owner-bound routing could not be verified before acceptance: %v", strings.TrimSpace(task.TaskID), err)
	}
	if !ok {
		return ""
	}
	if req.RepairNeeded || strings.TrimSpace(req.RequiredAgentID) == "" {
		return fmt.Sprintf("owner-bound task %s needs strategic repair before delegation: %s", strings.TrimSpace(task.TaskID), firstNonEmpty(strings.TrimSpace(req.Reason), "missing branch owner"))
	}
	if accepted, err := r.ownerBoundTaskAlreadyAccepted(ctx, task, req); err != nil {
		return fmt.Sprintf("owner-bound task %s accepted-state check failed before delegation: %v", strings.TrimSpace(task.TaskID), err)
	} else if accepted {
		return fmt.Sprintf("owner-bound task %s is stale because branch_id=%s already has an ACCEPTED same-head patch queue decision; close the stale task instead of queueing runtime_switch_task", strings.TrimSpace(task.TaskID), strings.TrimSpace(req.BranchID))
	}
	if strings.TrimSpace(req.RequiredAgentID) != strings.TrimSpace(r.cfg.AgentID) {
		return fmt.Sprintf("owner-bound task %s requires branch owner %s for branch_id=%s; delegate to that agent instead of queueing runtime_switch_task here", strings.TrimSpace(task.TaskID), strings.TrimSpace(req.RequiredAgentID), strings.TrimSpace(req.BranchID))
	}
	return ""
}

func (r *Runtime) maybeYieldOwnerBoundActiveTask(ctx context.Context, task WorkspaceTaskRecord, session *AgentSessionStateRecord) (bool, error) {
	req, ok, err := r.ownerBoundRequirementForTask(ctx, task)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	agentID := strings.TrimSpace(r.cfg.AgentID)
	if accepted, err := r.ownerBoundTaskAlreadyAccepted(ctx, task, req); err != nil {
		return false, err
	} else if accepted && strings.TrimSpace(req.RequiredAgentID) == agentID {
		return r.completeAcceptedOwnerBoundTask(ctx, task, session, req)
	}
	if !req.RepairNeeded && strings.TrimSpace(req.RequiredAgentID) == agentID {
		return false, nil
	}
	taskID := strings.TrimSpace(task.TaskID)
	ownerID := strings.TrimSpace(req.RequiredAgentID)
	summary := fmt.Sprintf("Owner-bound handoff: releasing %s because branch_id=%s belongs to %s.", taskID, strings.TrimSpace(req.BranchID), firstNonEmpty(ownerID, "unresolved owner"))
	payload := map[string]any{
		"handoff_state":         "owner_bound_released",
		"task_id":               taskID,
		"from_agent_id":         agentID,
		"branch_owner_agent_id": ownerID,
		"branch_id":             strings.TrimSpace(req.BranchID),
		"branch_name":           strings.TrimSpace(req.BranchName),
		"queue_id":              strings.TrimSpace(req.QueueID),
		"item_id":               strings.TrimSpace(req.ItemID),
		"repair_needed":         req.RepairNeeded,
		"reason":                firstNonEmpty(strings.TrimSpace(req.Reason), "non-owner active claim"),
	}
	raw, _ := json.Marshal(payload)
	if err := r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: strings.TrimSpace(r.cfg.WorkspaceID),
		AgentID:     agentID,
		UpdateType:  "coordination",
		Summary:     summary,
		PayloadJSON: string(raw),
	}); err != nil {
		log.Printf("[owner-bound] handoff evidence post failed for %s: %v", taskID, err)
	}
	if err := r.client.ReleaseTask(ctx, TaskReleaseInput{
		WorkspaceID: strings.TrimSpace(r.cfg.WorkspaceID),
		AgentID:     agentID,
		TaskID:      taskID,
		Reason:      summary,
	}); err != nil {
		return false, fmt.Errorf("release owner-bound non-owner claim %s: %w", taskID, err)
	}
	r.mu.Lock()
	if r.activeTask != nil && strings.TrimSpace(r.activeTask.TaskID) == taskID {
		r.activeTask = nil
		r.activeSession = nil
		r.activeRunID = ""
		r.activeWorkPacket = nil
		r.scratch.ActiveTaskID = ""
		r.scratch.ActiveSessionID = ""
		r.scratch.ActiveRunID = ""
		r.scratch.LastSummary = summary
		r.invalidateFocusLocked()
	}
	stateCopy := r.scratch
	r.mu.Unlock()
	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		log.Printf("[owner-bound] scratch clear failed after releasing %s: %v", taskID, err)
	}
	if session != nil && strings.TrimSpace(session.SessionID) != "" {
		keep := false
		if _, err := r.client.SessionEvent(ctx, "agent.session.end", SessionEventInput{
			WorkspaceID:       strings.TrimSpace(r.cfg.WorkspaceID),
			SessionID:         strings.TrimSpace(session.SessionID),
			AgentID:           agentID,
			TaskID:            taskID,
			Summary:           summary,
			Status:            "ENDED",
			KeepSessionActive: &keep,
			HandoffTo:         ownerID,
		}); err != nil {
			log.Printf("[owner-bound] session end failed after releasing %s: %v", taskID, err)
		}
	}
	if err := r.syncPresenceDocs(ctx); err != nil {
		log.Printf("[owner-bound] presence sync failed after releasing %s: %v", taskID, err)
	}
	return true, nil
}

func (r *Runtime) ownerBoundTaskAlreadyAccepted(ctx context.Context, task WorkspaceTaskRecord, req runtimeOwnerBoundRequirement) (bool, error) {
	if r == nil || r.client == nil {
		return false, nil
	}
	projectID := firstNonEmpty(strings.TrimSpace(task.ProjectID), strings.TrimSpace(req.ProjectID))
	if projectID == "" {
		return false, nil
	}
	coordination, err := r.client.GetProjectCoordination(ctx, strings.TrimSpace(r.cfg.WorkspaceID), projectID)
	if err != nil {
		return false, err
	}
	_, _, ok := runtimeOwnerBoundAcceptedBranchDecision(coordination, req)
	return ok, nil
}

func runtimeOwnerBoundAcceptedBranchDecision(coordination ProjectCoordinationRecord, req runtimeOwnerBoundRequirement) (ProjectBranchRecord, ProjectPatchQueueItemRecord, bool) {
	if !strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") || strings.TrimSpace(req.BranchID) == "" {
		return ProjectBranchRecord{}, ProjectPatchQueueItemRecord{}, false
	}
	var matchedBranch ProjectBranchRecord
	foundBranch := false
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchID) != strings.TrimSpace(req.BranchID) {
			continue
		}
		matchedBranch = branch
		foundBranch = true
		break
	}
	if !foundBranch || !runtimeOwnerBoundBranchStatusIsTerminal(matchedBranch.Status) || strings.TrimSpace(matchedBranch.HeadSHA) == "" {
		return ProjectBranchRecord{}, ProjectPatchQueueItemRecord{}, false
	}
	for _, item := range coordination.PatchQueueItems {
		if strings.TrimSpace(item.BranchID) != strings.TrimSpace(matchedBranch.BranchID) {
			continue
		}
		if strings.TrimSpace(item.HeadSHA) != strings.TrimSpace(matchedBranch.HeadSHA) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
			continue
		}
		return matchedBranch, item, true
	}
	return ProjectBranchRecord{}, ProjectPatchQueueItemRecord{}, false
}

func (r *Runtime) completeAcceptedOwnerBoundTask(ctx context.Context, task WorkspaceTaskRecord, session *AgentSessionStateRecord, req runtimeOwnerBoundRequirement) (bool, error) {
	taskID := strings.TrimSpace(task.TaskID)
	agentID := strings.TrimSpace(r.cfg.AgentID)
	summary := fmt.Sprintf("Completing stale owner-submit task %s because branch_id=%s already has an ACCEPTED same-head patch queue decision.", taskID, strings.TrimSpace(req.BranchID))
	payload := map[string]any{
		"handoff_state":     "owner_bound_terminal_noop",
		"task_id":           taskID,
		"agent_id":          agentID,
		"branch_id":         strings.TrimSpace(req.BranchID),
		"branch_name":       strings.TrimSpace(req.BranchName),
		"queue_id":          strings.TrimSpace(req.QueueID),
		"item_id":           strings.TrimSpace(req.ItemID),
		"reason":            "accepted_same_head_patch_queue_decision",
		"recommended_state": "completed",
	}
	raw, _ := json.Marshal(payload)
	if err := r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: strings.TrimSpace(r.cfg.WorkspaceID),
		AgentID:     agentID,
		UpdateType:  "coordination",
		Summary:     summary,
		PayloadJSON: string(raw),
	}); err != nil {
		log.Printf("[owner-bound] terminal noop evidence post failed for %s: %v", taskID, err)
	}
	if err := r.client.CompleteTask(ctx, TaskCompleteInput{
		WorkspaceID: strings.TrimSpace(r.cfg.WorkspaceID),
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     summary,
	}); err != nil {
		return false, fmt.Errorf("complete stale owner-bound task %s: %w", taskID, err)
	}
	r.mu.Lock()
	if r.activeTask != nil && strings.TrimSpace(r.activeTask.TaskID) == taskID {
		r.activeTask = nil
		r.activeSession = nil
		r.activeRunID = ""
		r.activeWorkPacket = nil
		r.scratch.ActiveTaskID = ""
		r.scratch.ActiveSessionID = ""
		r.scratch.ActiveRunID = ""
		r.scratch.LastSummary = summary
		r.invalidateFocusLocked()
	}
	stateCopy := r.scratch
	r.mu.Unlock()
	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		log.Printf("[owner-bound] scratch clear failed after completing stale %s: %v", taskID, err)
	}
	if session != nil && strings.TrimSpace(session.SessionID) != "" {
		keep := false
		if _, err := r.client.SessionEvent(ctx, "agent.session.end", SessionEventInput{
			WorkspaceID:       strings.TrimSpace(r.cfg.WorkspaceID),
			SessionID:         strings.TrimSpace(session.SessionID),
			AgentID:           agentID,
			TaskID:            taskID,
			Summary:           summary,
			Status:            "ENDED",
			KeepSessionActive: &keep,
		}); err != nil {
			log.Printf("[owner-bound] session end failed after completing stale %s: %v", taskID, err)
		}
	}
	if err := r.syncPresenceDocs(ctx); err != nil {
		log.Printf("[owner-bound] presence sync failed after completing stale %s: %v", taskID, err)
	}
	return true, nil
}
