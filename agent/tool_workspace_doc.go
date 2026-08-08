package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type WorkspaceDocGetTool struct {
	client         *RhizomeClient
	workspaceID    string
	runtimeBinding func() AgentRuntimeBinding
}

type WorkspaceDocPutTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
	draftGate   func() WorkspaceDocDraftGateState
}

type WorkspaceDocDraftGateState struct {
	Active    bool
	TaskID    string
	RunID     string
	PeerID    string
	RequestID string
	State     string
	Reason    string
}

func NewWorkspaceDocGetTool(client *RhizomeClient, workspaceID string) *WorkspaceDocGetTool {
	return &WorkspaceDocGetTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
	}
}

func (t *WorkspaceDocGetTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *WorkspaceDocGetTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func NewWorkspaceDocPutTool(client *RhizomeClient, workspaceID, agentID string) *WorkspaceDocPutTool {
	return &WorkspaceDocPutTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
	}
}

func (t *WorkspaceDocPutTool) WithDraftGate(provider func() WorkspaceDocDraftGateState) *WorkspaceDocPutTool {
	if t != nil {
		t.draftGate = provider
	}
	return t
}

func normalizeWorkspaceDocKeyArg(value string) (string, string, bool) {
	original := strings.TrimSpace(value)
	docKey := strings.TrimSpace(strings.Trim(original, "`\"'"))
	for {
		if !strings.HasPrefix(strings.ToLower(docKey), "doc:") {
			break
		}
		docKey = strings.TrimSpace(strings.Trim(docKey[len("doc:"):], "`\"'"))
	}
	return docKey, original, docKey != original
}

func (t *WorkspaceDocGetTool) Name() string { return "workspace_doc_get" }

func (t *WorkspaceDocGetTool) Description() string {
	return "Read a canonical Rhizome workspace document by doc_key. Use this for workspace doc keys; read_file only reads local files."
}

func (t *WorkspaceDocGetTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"doc_key": map[string]any{
				"type":        "string",
				"description": "Canonical workspace document key, for example current_context or task.some-task-id.",
			},
		},
		"required": []string{"doc_key"},
	}
}

func (t *WorkspaceDocGetTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" {
		return &ToolResult{Output: "workspace_doc_get is disabled: missing client or workspace identity", IsError: true}
	}
	docKey, originalDocKey, canonicalized := normalizeWorkspaceDocKeyArg(stringArg(args, "doc_key"))
	if docKey == "" {
		return &ToolResult{Output: "doc_key is required", IsError: true}
	}
	doc, ok, err := t.client.GetDoc(ctx, t.workspaceID, docKey)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("workspace_doc_get failed for %s: %v", docKey, err), IsError: true}
	}
	if !ok && canonicalized && originalDocKey != "" {
		if legacyDoc, legacyOK, legacyErr := t.client.GetDoc(ctx, t.workspaceID, originalDocKey); legacyErr != nil {
			return &ToolResult{Output: fmt.Sprintf("workspace_doc_get failed for canonical %s and legacy %s: %v", docKey, originalDocKey, legacyErr), IsError: true}
		} else if legacyOK {
			doc = legacyDoc
			ok = true
		}
	}
	if !ok {
		return &ToolResult{Output: fmt.Sprintf("workspace doc not found: %s. If this document is required, block on dependency instead of reading local files or retrying indefinitely.", docKey), IsError: true}
	}
	if err := t.validateProjectScope(ctx, doc); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	payload := map[string]any{
		"doc_key":    doc.DocKey,
		"title":      doc.Title,
		"updated_by": doc.UpdatedBy,
		"updated_at": doc.UpdatedAt,
		"sha":        doc.SHA,
		"content":    doc.Content,
	}
	if canonicalized {
		payload["canonicalized_from"] = originalDocKey
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: doc.Content}
	}
	return &ToolResult{Output: string(raw)}
}

func (t *WorkspaceDocGetTool) validateProjectScope(ctx context.Context, doc WorkspaceDocRecord) error {
	if t == nil || t.runtimeBinding == nil {
		return nil
	}
	binding := t.runtimeBinding()
	currentProjectID := strings.TrimSpace(binding.ProjectID)
	if currentProjectID == "" {
		return nil
	}
	if keyProjectID, _, ok := workspaceDocProjectKeyParts(doc.DocKey); ok && !sameWorkspaceDocProjectID(keyProjectID, currentProjectID) {
		return fmt.Errorf("workspace_doc_get blocked stale cross-project document %s: document key project_id=%s does not match active project_id=%s. Treat this as stale context; use current project coordination/tasks instead of reusing old project docs.", strings.TrimSpace(doc.DocKey), keyProjectID, currentProjectID)
	}
	docProjectID := workspaceDocProjectID(doc.Content)
	if docProjectID != "" && !sameWorkspaceDocProjectID(docProjectID, currentProjectID) {
		return fmt.Errorf("workspace_doc_get blocked stale cross-project document %s: document project_id=%s does not match active project_id=%s. Treat this as stale context; use current project coordination/tasks instead of reusing old task docs.", strings.TrimSpace(doc.DocKey), docProjectID, currentProjectID)
	}

	docTaskID := firstNonEmpty(workspaceDocTaskIDFromDocKey(doc.DocKey), workspaceDocTaskID(doc.Content))
	if docTaskID == "" || sameWorkspaceDocTaskID(docTaskID, binding.TaskID) {
		return nil
	}
	if t.client == nil || t.workspaceID == "" {
		return nil
	}
	bundle, err := t.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      t.workspaceID,
		TaskID:           docTaskID,
		UpdatesLimit:     0,
		ArtifactLimit:    0,
		RelatedTaskLimit: 0,
	})
	if err != nil {
		return fmt.Errorf("workspace_doc_get blocked unverifiable task-scoped document %s: document task_id=%s could not be verified in active project_id=%s: %v. Treat this as stale context; use current project coordination/tasks instead.", strings.TrimSpace(doc.DocKey), docTaskID, currentProjectID, err)
	}
	taskProjectID := firstNonEmpty(strings.TrimSpace(bundle.Task.ProjectID), workspaceTaskProjectID(bundle.WorkspaceTask))
	if taskProjectID == "" || sameWorkspaceDocProjectID(taskProjectID, currentProjectID) {
		return nil
	}
	return fmt.Errorf("workspace_doc_get blocked stale cross-project task document %s: document task_id=%s belongs to project_id=%s, active project_id=%s. Treat this as stale context; use current project coordination/tasks instead of reusing old task docs.", strings.TrimSpace(doc.DocKey), docTaskID, taskProjectID, currentProjectID)
}

func workspaceDocProjectKeyParts(docKey string) (projectID, suffix string, ok bool) {
	parts := strings.Split(strings.TrimSpace(docKey), ".")
	if len(parts) < 3 || !strings.EqualFold(parts[0], "project") {
		return "", "", false
	}
	projectID = strings.TrimSpace(parts[1])
	suffix = strings.TrimSpace(strings.Join(parts[2:], "."))
	if projectID == "" || suffix == "" {
		return "", "", false
	}
	return projectID, suffix, true
}

func workspaceDocTaskIDFromDocKey(docKey string) string {
	docKey = strings.TrimSpace(docKey)
	if len(docKey) <= len("task.") || !strings.HasPrefix(strings.ToLower(docKey), "task.") {
		return ""
	}
	rest := docKey[len("task."):]
	if !strings.HasPrefix(strings.ToLower(rest), "task-") {
		return ""
	}
	for idx, r := range rest {
		switch r {
		case '.', '/', '\\':
			return strings.TrimSpace(rest[:idx])
		}
	}
	return strings.TrimSpace(rest)
}

func sameWorkspaceDocTaskID(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	return strings.EqualFold(left, right) || sanitizeDocKeySegment(left) == sanitizeDocKeySegment(right)
}

func workspaceTaskProjectID(task *WorkspaceTaskRecord) string {
	if task == nil {
		return ""
	}
	return strings.TrimSpace(task.ProjectID)
}

func workspaceDocProjectID(content string) string {
	return workspaceDocHeaderValue(content, "project_id")
}

func workspaceDocTaskID(content string) string {
	return workspaceDocHeaderValue(content, "task_id")
}

func workspaceDocHeaderValue(content, name string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "-"))
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), name) {
			continue
		}
		candidate := strings.Trim(strings.TrimSpace(value), "`\"'")
		if candidate != "" {
			return candidate
		}
	}
	return ""
}

func sameWorkspaceDocProjectID(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	return strings.EqualFold(left, right) || sanitizeDocKeySegment(left) == sanitizeDocKeySegment(right)
}

func (t *WorkspaceDocPutTool) Name() string { return "workspace_doc_put" }

func (t *WorkspaceDocPutTool) Description() string {
	return "Create or update a canonical Rhizome workspace document. Use this daemon-safe path for durable task artifacts instead of local file writes."
}

func (t *WorkspaceDocPutTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"doc_key": map[string]any{
				"type":        "string",
				"description": "Canonical workspace document key, for example task.some-task-id.result.",
			},
			"title": map[string]any{
				"type":        "string",
				"description": "Human-readable document title.",
			},
			"content": map[string]any{
				"type":        "string",
				"description": "Full document content to store in Rhizome.",
			},
			"expected_sha": map[string]any{
				"type":        "string",
				"description": "Optional optimistic concurrency SHA from workspace_doc_get.",
			},
		},
		"required": []string{"doc_key", "title", "content"},
	}
}

func (t *WorkspaceDocPutTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "workspace_doc_put is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	docKey, originalDocKey, canonicalized := normalizeWorkspaceDocKeyArg(stringArg(args, "doc_key"))
	title := strings.TrimSpace(stringArg(args, "title"))
	content := strings.TrimSpace(stringArg(args, "content"))
	expectedSHA := strings.TrimSpace(stringArg(args, "expected_sha"))
	switch {
	case docKey == "":
		return &ToolResult{Output: "doc_key is required", IsError: true}
	case title == "":
		return &ToolResult{Output: "title is required", IsError: true}
	case content == "":
		return &ToolResult{Output: "content is required", IsError: true}
	}
	gate := t.currentDraftGate()
	draftApplied := false
	if gate.Active && workspaceDocPutShouldMarkDraft(docKey, title, content) {
		title, content = markWorkspaceDocPutDraft(title, content, gate)
		draftApplied = true
	}
	sha, content, err := t.putDocWithConflictRetry(ctx, docKey, title, content, expectedSHA)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("workspace_doc_put failed for %s: %v", docKey, err), IsError: true}
	}
	projectProfileSync, syncErr := t.syncProjectProfileFromDoc(ctx, docKey, title, content)
	if syncErr != nil {
		return &ToolResult{Output: fmt.Sprintf("workspace_doc_put stored %s sha=%s but project profile sync failed: %v. Do not retry the doc write blindly; repair the project profile/gate reference.", docKey, sha, syncErr), IsError: true}
	}
	payload := map[string]any{
		"doc_key":    docKey,
		"title":      title,
		"updated_by": t.agentID,
		"sha":        sha,
		"status":     "stored",
	}
	if canonicalized {
		payload["canonicalized_from"] = originalDocKey
	}
	if projectProfileSync != nil {
		payload["project_profile_sync"] = projectProfileSync
	}
	if draftApplied {
		payload["status"] = "draft_pending_peer_review"
		payload["coordination_gate"] = map[string]any{
			"state":      strings.TrimSpace(gate.State),
			"task_id":    strings.TrimSpace(gate.TaskID),
			"run_id":     strings.TrimSpace(gate.RunID),
			"peer_id":    strings.TrimSpace(gate.PeerID),
			"request_id": strings.TrimSpace(gate.RequestID),
			"reason":     strings.TrimSpace(gate.Reason),
		}
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("stored workspace doc %s sha=%s", docKey, sha)}
	}
	return &ToolResult{Output: string(raw)}
}

func (t *WorkspaceDocPutTool) syncProjectProfileFromDoc(ctx context.Context, docKey, title, content string) (map[string]any, error) {
	projectID, field, ok := projectProfileDocSyncTarget(docKey)
	if !ok {
		return nil, nil
	}
	projectID = canonicalProjectProfileDocProjectID(projectID, content)
	result := map[string]any{
		"project_id": projectID,
		"field":      field,
		"doc_key":    docKey,
	}
	if workspaceDocLooksDraft(title, content) {
		result["status"] = "skipped_draft"
		return result, nil
	}
	input := ProjectProfileUpdateInput{
		WorkspaceID: t.workspaceID,
		ProjectID:   projectID,
		ActorID:     t.agentID,
	}
	switch field {
	case "design_doc_id":
		input.DesignDocID = docKey
	case "implementation_plan_doc_id":
		input.ImplementationPlanDocID = docKey
	case "design_and_plan_doc_ids":
		input.DesignDocID = docKey
		input.ImplementationPlanDocID = docKey
	default:
		return nil, nil
	}
	if _, err := t.client.UpdateProjectProfile(ctx, input); err != nil {
		return result, err
	}
	result["status"] = "synced"
	return result, nil
}

func projectProfileDocSyncTarget(docKey string) (string, string, bool) {
	docKey = strings.TrimSpace(docKey)
	if !strings.HasPrefix(docKey, "project.") {
		return "", "", false
	}
	rest := strings.TrimPrefix(docKey, "project.")
	idx := strings.LastIndex(rest, ".")
	if idx <= 0 || idx == len(rest)-1 {
		return "", "", false
	}
	projectID := strings.TrimSpace(rest[:idx])
	suffix := strings.ToLower(strings.TrimSpace(rest[idx+1:]))
	suffix = strings.NewReplacer("-", "_", " ", "_").Replace(suffix)
	switch suffix {
	case "design", "design_doc", "design_document":
		return projectID, "design_doc_id", true
	case "implementation_plan", "impl_plan", "plan":
		return projectID, "implementation_plan_doc_id", true
	case "design_and_plan", "design_plan", "design_implementation_plan", "design_and_implementation_plan":
		return projectID, "design_and_plan_doc_ids", true
	default:
		return "", "", false
	}
}

func canonicalProjectProfileDocProjectID(projectID, content string) string {
	projectID = strings.TrimSpace(projectID)
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "-"))
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "project_id") {
			continue
		}
		candidate := strings.Trim(strings.TrimSpace(value), "`\"'")
		if candidate == "" {
			continue
		}
		if sanitizeDocKeySegment(candidate) == sanitizeDocKeySegment(projectID) {
			return candidate
		}
	}
	return projectID
}

func workspaceDocLooksDraft(title, content string) bool {
	title = strings.ToLower(strings.TrimSpace(title))
	content = strings.ToLower(strings.TrimSpace(content))
	return strings.HasPrefix(title, "draft") || strings.HasPrefix(content, "draft pending peer review")
}

func (t *WorkspaceDocPutTool) putDocWithConflictRetry(ctx context.Context, docKey, title, content, expectedSHA string) (string, string, error) {
	for attempt := 1; attempt <= defaultDocConflictRetryLimit; attempt++ {
		sha, err := t.client.PutDoc(ctx, WorkspaceDocPutInput{
			WorkspaceID: t.workspaceID,
			DocKey:      docKey,
			Title:       title,
			Content:     content,
			UpdatedBy:   t.agentID,
			ExpectedSHA: expectedSHA,
		})
		if err == nil {
			return sha, content, nil
		}
		if !isDocumentConflictError(err) {
			return "", content, err
		}
		if attempt == defaultDocConflictRetryLimit {
			return "", content, fmt.Errorf("document conflict retry ceiling reached after %d attempts: %w", defaultDocConflictRetryLimit, err)
		}
		doc, ok, getErr := t.client.GetDoc(ctx, t.workspaceID, docKey)
		if getErr != nil {
			return "", content, getErr
		}
		if !ok {
			return "", content, fmt.Errorf("document conflict retry ceiling reached after %d attempts: remote doc missing", defaultDocConflictRetryLimit)
		}
		content = mergeDocContents(doc.Content, content, t.agentID)
		expectedSHA = doc.SHA
	}
	return "", content, fmt.Errorf("document conflict retry ceiling reached after %d attempts", defaultDocConflictRetryLimit)
}

func (t *WorkspaceDocPutTool) currentDraftGate() WorkspaceDocDraftGateState {
	if t == nil || t.draftGate == nil {
		return WorkspaceDocDraftGateState{}
	}
	return t.draftGate()
}

func workspaceDocPutShouldMarkDraft(docKey, title, content string) bool {
	text := strings.ToLower(strings.Join([]string{
		strings.TrimSpace(docKey),
		strings.TrimSpace(title),
		firstLine(strings.TrimSpace(content)),
	}, "\n"))
	for _, marker := range []string{
		".result",
		"result",
		"final",
		"deliverable",
		"completion",
		"completed",
		"summary",
		"report",
		"acceptance",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func markWorkspaceDocPutDraft(title, content string, gate WorkspaceDocDraftGateState) (string, string) {
	title = strings.TrimSpace(title)
	content = strings.TrimSpace(content)
	if !strings.HasPrefix(strings.ToLower(title), "draft - ") {
		title = "Draft - " + title
	}
	if strings.HasPrefix(strings.ToLower(content), "draft pending peer review") {
		return title, content
	}
	var b strings.Builder
	b.WriteString("DRAFT PENDING PEER REVIEW\n")
	if reason := strings.TrimSpace(gate.Reason); reason != "" {
		b.WriteString(reason)
		b.WriteString("\n")
	}
	if requestID := strings.TrimSpace(gate.RequestID); requestID != "" {
		b.WriteString("coordination_request_id: ")
		b.WriteString(requestID)
		b.WriteString("\n")
	}
	if peerID := strings.TrimSpace(gate.PeerID); peerID != "" {
		b.WriteString("coordination_peer_id: ")
		b.WriteString(peerID)
		b.WriteString("\n")
	}
	b.WriteString("\n")
	b.WriteString(content)
	return title, b.String()
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if idx := strings.IndexAny(s, "\r\n"); idx >= 0 {
		return strings.TrimSpace(s[:idx])
	}
	return s
}
