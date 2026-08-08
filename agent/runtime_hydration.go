package main

import (
	"context"
	"encoding/json"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"
)

const runtimeHydrationTTL = 75 * time.Second

type hydrationCacheScope struct {
	TaskID           string   `json:"task_id"`
	DocKeys          []string `json:"doc_keys,omitempty"`
	IncludeAllDocs   bool     `json:"include_all_docs,omitempty"`
	UpdatesLimit     int      `json:"updates_limit,omitempty"`
	ArtifactLimit    int      `json:"artifact_limit,omitempty"`
	RelatedTaskLimit int      `json:"related_task_limit,omitempty"`
}

func hydrationTaskID(bundle *TaskHydrationBundle) string {
	if bundle == nil {
		return ""
	}
	if bundle.WorkspaceTask != nil && strings.TrimSpace(bundle.WorkspaceTask.TaskID) != "" {
		return strings.TrimSpace(bundle.WorkspaceTask.TaskID)
	}
	return strings.TrimSpace(bundle.Task.TaskID)
}

func defaultHydrationDocKeys(taskID string) []string {
	taskID = strings.TrimSpace(taskID)
	keys := []string{
		"current_context",
		"decisions",
		"open_questions",
		"handoff",
		"tooling",
		"autonomy_policy",
	}
	if taskID != "" {
		keys = append(defaultHydrationTaskDocKeys(taskID), keys...)
	}
	return keys
}

func defaultHydrationTaskDocKeys(taskID string) []string {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil
	}
	base := "task." + taskID
	return []string{
		base,
		base + ".result",
		base + ".evidence_gap",
		base + ".artifact_reality_check",
		base + ".candidate_provenance",
		base + ".provenance_review_status",
		base + ".review_feedback",
		base + ".review_ready_status",
		base + ".implementation_evidence",
		base + ".verification",
	}
}

func canonicalHydrationInput(taskID string) TaskHydrationInput {
	includeAllDocs := boolPtr(false)
	return TaskHydrationInput{
		WorkspaceID:      "",
		TaskID:           strings.TrimSpace(taskID),
		DocKeys:          defaultHydrationDocKeys(taskID),
		IncludeAllDocs:   includeAllDocs,
		UpdatesLimit:     10,
		ArtifactLimit:    10,
		RelatedTaskLimit: 10,
	}
}

var taskDocKeyCandidatePattern = regexp.MustCompile(`[A-Za-z0-9][A-Za-z0-9._/-]{2,}[A-Za-z0-9]`)

func canonicalHydrationInputForTask(task *WorkspaceTaskRecord) TaskHydrationInput {
	taskID := ""
	if task != nil {
		taskID = strings.TrimSpace(task.TaskID)
	}
	input := canonicalHydrationInput(taskID)
	if task == nil {
		return input
	}
	input.DocKeys = uniqueTrimmedFocusStrings(append(input.DocKeys, docKeysMentionedByTask(*task)...))
	return input
}

func docKeysMentionedByTask(task WorkspaceTaskRecord) []string {
	text := strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.CloseReason,
	}, "\n")
	matches := taskDocKeyCandidatePattern.FindAllString(text, -1)
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		key := strings.Trim(strings.TrimSpace(match), ".,;:()[]{}<>\"'")
		lower := strings.ToLower(key)
		if key == "" || strings.Contains(lower, "://") {
			continue
		}
		if !strings.Contains(key, ".") {
			continue
		}
		out = append(out, key)
		if len(out) >= 16 {
			break
		}
	}
	return uniqueTrimmedFocusStrings(out)
}

func scopeForTaskHydrationInput(input TaskHydrationInput) hydrationCacheScope {
	scope := hydrationCacheScope{
		TaskID:           strings.TrimSpace(input.TaskID),
		DocKeys:          uniqueTrimmedFocusStrings(input.DocKeys),
		UpdatesLimit:     input.UpdatesLimit,
		ArtifactLimit:    input.ArtifactLimit,
		RelatedTaskLimit: input.RelatedTaskLimit,
	}
	if input.IncludeAllDocs != nil {
		scope.IncludeAllDocs = *input.IncludeAllDocs
	}
	sort.Strings(scope.DocKeys)
	return scope
}

func hydrationScopeKey(input TaskHydrationInput) string {
	scope := scopeForTaskHydrationInput(input)
	raw, err := json.Marshal(scope)
	if err != nil {
		return strings.TrimSpace(scope.TaskID)
	}
	return string(raw)
}

func cloneTaskHydrationBundle(bundle *TaskHydrationBundle) *TaskHydrationBundle {
	if bundle == nil {
		return nil
	}
	copy := *bundle
	if bundle.Workspace != nil {
		workspaceCopy := *bundle.Workspace
		copy.Workspace = &workspaceCopy
	}
	if bundle.WorkspaceTask != nil {
		taskCopy := *bundle.WorkspaceTask
		copy.WorkspaceTask = &taskCopy
	}
	copy.Task = cloneTaskStatus(bundle.Task)
	copy.Docs = append([]WorkspaceDocRecord(nil), bundle.Docs...)
	copy.TaskLinks = append([]WorkspaceTaskLinkRecord(nil), bundle.TaskLinks...)
	if len(bundle.RelatedTasks) > 0 {
		copy.RelatedTasks = make([]TaskStatus, 0, len(bundle.RelatedTasks))
		for _, task := range bundle.RelatedTasks {
			copy.RelatedTasks = append(copy.RelatedTasks, cloneTaskStatus(task))
		}
	}
	copy.Artifacts = append([]WorkspaceArtifactRecord(nil), bundle.Artifacts...)
	copy.Updates = append([]AgentUpdateRecord(nil), bundle.Updates...)
	copy.SideEffects = append([]AgentUpdateSideEffectV1(nil), bundle.SideEffects...)
	return &copy
}

func cloneTaskStatus(task TaskStatus) TaskStatus {
	copy := task
	if len(task.NodeCounts) > 0 {
		copy.NodeCounts = make(map[string]int, len(task.NodeCounts))
		for key, value := range task.NodeCounts {
			copy.NodeCounts[key] = value
		}
	}
	if len(task.Nodes) > 0 {
		copy.Nodes = make([]TaskStatusNode, 0, len(task.Nodes))
		for _, node := range task.Nodes {
			nodeCopy := node
			nodeCopy.DependsOn = append([]string(nil), node.DependsOn...)
			copy.Nodes = append(copy.Nodes, nodeCopy)
		}
	}
	return copy
}

func (r *Runtime) cacheHydration(bundle *TaskHydrationBundle, input TaskHydrationInput) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if bundle == nil {
		r.clearHydrationLocked()
		r.invalidateFocusLocked()
		return
	}
	r.activeHydration = cloneTaskHydrationBundle(bundle)
	r.hydrationScope = hydrationScopeKey(input)
	r.hydrationAt = time.Now().UTC()
	r.hydrationStale = false
	r.invalidateFocusLocked()
	if r.memory != nil {
		r.memory.invalidatePacketCache()
	}
}

func (r *Runtime) markHydrationStale() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markHydrationStaleLocked()
}

func (r *Runtime) markHydrationStaleLocked() {
	if r.activeHydration == nil && !r.hydrationStale {
		return
	}
	r.hydrationStale = true
	r.invalidateFocusLocked()
	if r.memory != nil {
		r.memory.invalidatePacketCache()
	}
}

func (r *Runtime) clearHydrationLocked() {
	r.activeHydration = nil
	r.hydrationScope = ""
	r.hydrationAt = time.Time{}
	r.hydrationStale = false
	if r.memory != nil {
		r.memory.invalidatePacketCache()
	}
}

func (r *Runtime) cachedHydration(input TaskHydrationInput) *TaskHydrationBundle {
	taskID := strings.TrimSpace(input.TaskID)
	scope := hydrationScopeKey(input)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeHydration == nil {
		return nil
	}
	if r.hydrationStale {
		return nil
	}
	if hydrationTaskID(r.activeHydration) != taskID {
		return nil
	}
	if strings.TrimSpace(r.hydrationScope) != scope {
		return nil
	}
	if r.hydrationAt.IsZero() || time.Since(r.hydrationAt) >= runtimeHydrationTTL {
		return nil
	}
	return cloneTaskHydrationBundle(r.activeHydration)
}

func (r *Runtime) taskHydration(ctx context.Context, task *WorkspaceTaskRecord) *TaskHydrationBundle {
	if task == nil || r.client == nil {
		return nil
	}

	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return nil
	}

	input := canonicalHydrationInputForTask(task)
	input.WorkspaceID = r.cfg.WorkspaceID

	if hydration := r.cachedHydration(input); hydration != nil {
		return hydration
	}

	bundle, err := r.client.HydrateTask(ctx, input)
	if err != nil {
		if shouldFallbackNativeAgentWork(err) {
			return nil
		}
		log.Printf("[runtime] task hydration degraded for %s: %v", taskID, err)
		return nil
	}
	r.cacheHydration(&bundle, input)
	return cloneTaskHydrationBundle(&bundle)
}
