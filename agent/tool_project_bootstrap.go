package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

const defaultProjectLeadLeaseSeconds = 3600

type ProjectBootstrapTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	ownerUserID    string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectBootstrapTool(client *RhizomeClient, workspaceID, agentID, ownerUserID string) *ProjectBootstrapTool {
	return &ProjectBootstrapTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		ownerUserID: strings.TrimSpace(ownerUserID),
	}
}

func (t *ProjectBootstrapTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectBootstrapTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectBootstrapTool) Name() string { return "project_bootstrap" }

func (t *ProjectBootstrapTool) Description() string {
	return "Create or attach a Rhizome Project for broad product-scale work before implementation. It records the project intake doc, strategic lead, phase, root-task project fields, and an optional spec task."
}

func (t *ProjectBootstrapTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{
				"type":        "string",
				"description": "Optional stable project id. Leave blank to find by title or create one.",
			},
			"title": map[string]any{
				"type":        "string",
				"description": "Project title, not a subtask title.",
			},
			"goal": map[string]any{
				"type":        "string",
				"description": "Project goal and operator intent in a few concrete sentences.",
			},
			"root_task_id": map[string]any{
				"type":        "string",
				"description": "Current broad/root task id to attach to the project. Required so the original broad task cannot continue unscoped.",
			},
			"repo_required": map[string]any{
				"type":        "boolean",
				"description": "Whether the project needs a git repository before implementation. Defaults true unless explicitly false.",
			},
			"repo_status": map[string]any{
				"type":        "string",
				"description": "Repository status: NOT_REQUIRED, MISSING, READY, BLOCKED, or UNKNOWN.",
			},
			"repo_url": map[string]any{
				"type":        "string",
				"description": "Optional canonical repository URL if already known.",
			},
			"repo_default_branch": map[string]any{
				"type":        "string",
				"description": "Optional default branch, usually main.",
			},
			"desired_phase": map[string]any{
				"type":        "string",
				"description": "Phase to enter after lead claim. Defaults to SPEC.",
			},
			"design_doc_id": map[string]any{
				"type":        "string",
				"description": "Optional already-approved design doc key. Do not set for a draft.",
			},
			"implementation_plan_doc_id": map[string]any{
				"type":        "string",
				"description": "Optional already-approved implementation plan doc key. Do not set for a draft.",
			},
			"create_spec_task": map[string]any{
				"type":        "boolean",
				"description": "Whether to create a project-scoped spec/design task if one is not already open. Defaults true.",
			},
		},
		"required": []string{"title", "goal", "root_task_id"},
	}
}

func (t *ProjectBootstrapTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_bootstrap is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	title := strings.TrimSpace(stringArg(args, "title"))
	goal := strings.TrimSpace(stringArg(args, "goal"))
	if title == "" {
		return &ToolResult{Output: "title is required", IsError: true}
	}
	if goal == "" {
		return &ToolResult{Output: "goal is required", IsError: true}
	}

	rootTaskID := strings.TrimSpace(stringArg(args, "root_task_id"))
	if rootTaskID == "" {
		return &ToolResult{Output: "root_task_id is required so the broad task can be attached to the Project before implementation", IsError: true}
	}
	projectID := strings.TrimSpace(stringArg(args, "project_id"))
	binding := t.currentRuntimeBinding()
	if err := validateProjectBootstrapRuntimeBinding(binding, rootTaskID, projectID); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if projectID == "" && strings.TrimSpace(binding.ProjectID) != "" {
		projectID = strings.TrimSpace(binding.ProjectID)
	}
	desiredPhase, phaseErr := normalizeProjectBootstrapDesiredPhase(stringArg(args, "desired_phase"))
	if phaseErr != nil {
		return &ToolResult{Output: phaseErr.Error(), IsError: true}
	}
	rootBundle, err := t.verifyRootTask(ctx, rootTaskID)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if err := validateProjectBootstrapRootTaskShape(binding, rootBundle); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	sourceDocKeys := sourceDocKeysFromTaskBundle(rootBundle)
	if projectID == "" {
		projectID = projectBootstrapIDFromRootTask(rootTaskID)
	}
	bindingProjectID := strings.TrimSpace(binding.ProjectID)
	projects, err := t.client.ListProjects(ctx, t.workspaceID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_bootstrap failed listing projects: %v", err), IsError: true}
	}
	var project ProjectRecord
	var existed bool
	if bindingProjectID != "" {
		project, existed = selectBootstrapProjectByID(projects, projectID)
	} else {
		project, existed = selectBootstrapProject(projects, projectID, title)
	}
	created := false
	if !existed {
		var createErr error
		project, createErr = t.client.CreateProject(ctx, ProjectCreateInput{
			WorkspaceID: t.workspaceID,
			ProjectID:   projectID,
			Title:       title,
			Description: goal,
			CreatedBy:   t.agentID,
		})
		if createErr != nil {
			projects, retryErr := t.client.ListProjects(ctx, t.workspaceID)
			if retryErr == nil {
				var retryProject ProjectRecord
				var ok bool
				if bindingProjectID != "" {
					retryProject, ok = selectBootstrapProjectByID(projects, projectID)
				} else {
					retryProject, ok = selectBootstrapProject(projects, projectID, title)
				}
				if ok {
					project = retryProject
					existed = true
				}
			}
		}
		if createErr != nil && !existed {
			return &ToolResult{Output: fmt.Sprintf("project_bootstrap failed creating project %s: %v", projectID, createErr), IsError: true}
		}
		created = !existed
	} else {
		projectID = project.ProjectID
	}

	existingProfile := ProjectProfileRecord{}
	if existed && bindingProjectID != "" && projectBootstrapShouldPreserveExistingProfile(args) {
		coordination, coordErr := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
		if coordErr != nil {
			return &ToolResult{Output: fmt.Sprintf("project_bootstrap failed reading existing project coordination for %s before profile update: %v", projectID, coordErr), IsError: true}
		}
		existingProfile = coordination.Profile
	}
	repoRequired := bootstrapRepoRequired(args, title, goal)
	if optionalBoolArg(args, "repo_required") == nil && sameWorkspaceDocProjectID(existingProfile.ProjectID, projectID) {
		repoRequired = existingProfile.RepoRequired
	}
	repoStatus := firstNonEmpty(strings.ToUpper(strings.TrimSpace(stringArg(args, "repo_status"))), bootstrapDefaultRepoStatus(repoRequired))
	repoURL := strings.TrimSpace(stringArg(args, "repo_url"))
	defaultBranch := strings.TrimSpace(stringArg(args, "repo_default_branch"))
	if sameWorkspaceDocProjectID(existingProfile.ProjectID, projectID) {
		if strings.TrimSpace(stringArg(args, "repo_status")) == "" && strings.TrimSpace(existingProfile.RepoStatus) != "" {
			repoStatus = strings.TrimSpace(existingProfile.RepoStatus)
		}
		if repoURL == "" {
			repoURL = strings.TrimSpace(existingProfile.RepoURL)
		}
		if defaultBranch == "" {
			defaultBranch = strings.TrimSpace(existingProfile.RepoDefaultBranch)
		}
	}
	designDocID := strings.TrimSpace(stringArg(args, "design_doc_id"))
	implementationPlanDocID := strings.TrimSpace(stringArg(args, "implementation_plan_doc_id"))
	intakeDocKey := "project." + sanitizeDocKeySegment(projectID) + ".intake"

	if _, err := t.client.PutDoc(ctx, WorkspaceDocPutInput{
		WorkspaceID: t.workspaceID,
		DocKey:      intakeDocKey,
		Title:       "Project Intake - " + title,
		Content:     renderProjectBootstrapIntakeDoc(project, goal, t.agentID, rootTaskID, repoRequired, repoStatus, sourceDocKeys),
		UpdatedBy:   t.agentID,
	}); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_bootstrap failed writing intake doc %s: %v", intakeDocKey, err), IsError: true}
	}
	sourceRefsDocKey := ""
	if len(sourceDocKeys) > 0 {
		sourceRefsDocKey = projectSourceRefsDocKey(projectID)
		if _, err := t.client.PutDoc(ctx, WorkspaceDocPutInput{
			WorkspaceID: t.workspaceID,
			DocKey:      sourceRefsDocKey,
			Title:       "Project Source Artifact References - " + title,
			Content:     renderProjectSourceRefsDoc(project, rootTaskID, sourceDocKeys),
			UpdatedBy:   t.agentID,
		}); err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_bootstrap failed writing source refs doc %s: %v", sourceRefsDocKey, err), IsError: true}
		}
	}

	profile, err := t.client.UpdateProjectProfile(ctx, ProjectProfileUpdateInput{
		WorkspaceID:             t.workspaceID,
		ProjectID:               projectID,
		ActorID:                 t.agentID,
		Goal:                    goal,
		DesignDocID:             designDocID,
		ImplementationPlanDocID: implementationPlanDocID,
		RepoRequired:            &repoRequired,
		RepoStatus:              repoStatus,
		RepoURL:                 repoURL,
		RepoDefaultBranch:       defaultBranch,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_bootstrap failed updating profile for %s: %v", projectID, err), IsError: true}
	}

	role, err := t.client.ClaimProjectLead(ctx, ProjectLeadClaimInput{
		WorkspaceID:  t.workspaceID,
		ProjectID:    projectID,
		ActorID:      t.agentID,
		AgentID:      t.agentID,
		LeaseSeconds: defaultProjectLeadLeaseSeconds,
		Summary:      "Provisional strategic lead for project intake and spec coordination.",
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_bootstrap failed claiming strategic lead for %s: %v", projectID, err), IsError: true}
	}

	phaseTransitioned := false
	if desiredPhase != "" && desiredPhase != "INTAKE" && strings.EqualFold(profile.CurrentPhase, "INTAKE") {
		profile, err = t.client.TransitionProjectPhase(ctx, ProjectPhaseTransitionInput{
			WorkspaceID: t.workspaceID,
			ProjectID:   projectID,
			ActorID:     t.agentID,
			ToPhase:     desiredPhase,
			Reason:      "Project bootstrap completed; design/spec coordination should start before implementation.",
		})
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_bootstrap failed transitioning %s to %s: %v", projectID, desiredPhase, err), IsError: true}
		}
		phaseTransitioned = true
	}

	rootLinked := false
	if rootTaskID != "" {
		taskKind := "COORDINATION"
		projectLane := "strategy"
		requiresGate := false
		if _, err := t.client.PutTaskProjectFields(ctx, TaskProjectFieldsPutInput{
			WorkspaceID:         t.workspaceID,
			TaskID:              rootTaskID,
			ProjectID:           &projectID,
			TaskKind:            &taskKind,
			ProjectLane:         &projectLane,
			RequiresProjectGate: &requiresGate,
			ActorID:             t.agentID,
		}); err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_bootstrap failed linking root task %s to project %s: %v", rootTaskID, projectID, err), IsError: true}
		}
		rootLinked = true
	}

	specTaskID := ""
	createSpecTask := true
	if explicit := optionalBoolArg(args, "create_spec_task"); explicit != nil {
		createSpecTask = *explicit
	}
	if createSpecTask {
		specTaskID, err = t.ensureSpecTask(ctx, projectID, title, rootTaskID, sourceDocKeys)
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_bootstrap failed ensuring spec task for project %s: %v", projectID, err), IsError: true}
		}
	}

	coordination, coordErr := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	payload := map[string]any{
		"project_id":         projectID,
		"project_title":      firstNonEmpty(project.Title, title),
		"created":            created,
		"intake_doc_key":     intakeDocKey,
		"strategic_lead_id":  role.RoleID,
		"lead_agent_id":      role.AgentID,
		"current_phase":      profile.CurrentPhase,
		"phase_transitioned": phaseTransitioned,
		"repo_required":      repoRequired,
		"repo_status":        profile.RepoStatus,
		"root_task_linked":   rootLinked,
		"spec_task_id":       specTaskID,
		"guidance":           "Root strategy coordination remains runnable while implementation gates are closed. Before opening implementation, publish canonical project.<project_id>.design_and_plan (or separate project.<project_id>.design and project.<project_id>.implementation_plan) so workspace_doc_put syncs design_doc_id and implementation_plan_doc_id. Implementation tasks must wait until project gates show design_doc_ready, implementation_plan_ready, strategic_lead_active, repo readiness/materialization, and implementation_phase_open.",
	}
	if len(sourceDocKeys) > 0 {
		payload["source_doc_keys"] = sourceDocKeys
		payload["source_refs_doc_key"] = sourceRefsDocKey
		payload["source_requirements_trace_doc_key"] = projectSourceRequirementsTraceDocKey(projectID)
		payload["spec_fidelity_gate"] = "implementation opens only after project planning docs preserve source_doc_keys in rhizome_source_requirements_trace_v1"
	}
	if coordErr == nil {
		payload["coordination_version"] = coordination.CoordinationVersion
		payload["implementation_ready"] = coordination.GateStatus.ImplementationReady
		payload["gate_overall_state"] = coordination.GateStatus.OverallState
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_bootstrap succeeded for %s", projectID)}
	}
	return &ToolResult{Output: string(raw)}
}

func (t *ProjectBootstrapTool) ensureSpecTask(ctx context.Context, projectID, projectTitle, rootTaskID string, sourceDocKeys []string) (string, error) {
	if taskID, ok, err := t.findSpecTask(ctx, projectID); err != nil || ok {
		return taskID, err
	}
	requiresGate := false
	input := TaskSubmitInput{
		WorkspaceID:         t.workspaceID,
		TaskID:              projectBootstrapSpecTaskID(projectID),
		OwnerUserID:         firstNonEmpty(t.ownerUserID, t.agentID),
		Priority:            "high",
		Title:               "Prepare project design document",
		Description:         renderProjectBootstrapSpecTaskDescription(projectID, projectTitle, rootTaskID, sourceDocKeys),
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "spec",
		RequiresProjectGate: &requiresGate,
		DependencyTaskIDs:   dependencyList(rootTaskID),
		Tags:                []string{"project-spec", "design-doc", "coordination"},
		LinkedBy:            t.agentID,
	}
	result, err := t.client.SubmitTask(ctx, input)
	if err != nil {
		if taskID, ok, listErr := t.findSpecTask(ctx, projectID); listErr == nil && ok {
			return taskID, nil
		}
		return "", err
	}
	taskID := firstNonEmpty(result.TaskID, input.TaskID)
	docKey := "task." + taskID
	_, docErr := t.client.PutDoc(ctx, WorkspaceDocPutInput{
		WorkspaceID: t.workspaceID,
		DocKey:      docKey,
		Title:       "Task Brief - Prepare project design document",
		Content: renderTaskSubmitCanonicalDoc(TaskSubmitDocInput{
			TaskID:              taskID,
			Title:               input.Title,
			Description:         input.Description,
			Priority:            input.Priority,
			TaskKind:            input.TaskKind,
			TaskTemplate:        input.TaskTemplate,
			ProjectID:           input.ProjectID,
			ProjectLane:         input.ProjectLane,
			RequiresProjectGate: input.RequiresProjectGate,
			DependencyTaskIDs:   input.DependencyTaskIDs,
			Tags:                input.Tags,
			CreatedBy:           t.agentID,
		}),
		UpdatedBy: t.agentID,
	})
	if docErr != nil {
		return taskID, docErr
	}
	return taskID, nil
}

func (t *ProjectBootstrapTool) verifyRootTask(ctx context.Context, rootTaskID string) (TaskHydrationBundle, error) {
	bundle, err := t.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID: t.workspaceID,
		TaskID:      rootTaskID,
	})
	if err != nil {
		return TaskHydrationBundle{}, fmt.Errorf("project_bootstrap root task preflight failed for %s: %w", rootTaskID, err)
	}
	if bundle.WorkspaceTask != nil {
		if holder := activeRootTaskClaimAgent(*bundle.WorkspaceTask); holder != "" && holder != t.agentID {
			return TaskHydrationBundle{}, fmt.Errorf("project_bootstrap root task preflight failed: root task %s is already actively claimed by %s; ask the claimant to bootstrap or hand off before claiming strategic lead", rootTaskID, holder)
		}
	}
	if strings.TrimSpace(bundle.Task.TaskID) == rootTaskID {
		return bundle, nil
	}
	if bundle.WorkspaceTask != nil && strings.TrimSpace(bundle.WorkspaceTask.TaskID) == rootTaskID {
		return bundle, nil
	}
	return TaskHydrationBundle{}, fmt.Errorf("project_bootstrap root task preflight failed: %s was not found in hydration response", rootTaskID)
}

func (t *ProjectBootstrapTool) currentRuntimeBinding() AgentRuntimeBinding {
	if t == nil || t.runtimeBinding == nil {
		return AgentRuntimeBinding{}
	}
	return t.runtimeBinding()
}

func validateProjectBootstrapRuntimeBinding(binding AgentRuntimeBinding, rootTaskID, requestedProjectID string) error {
	rootTaskID = strings.TrimSpace(rootTaskID)
	requestedProjectID = strings.TrimSpace(requestedProjectID)
	bindingTaskID := strings.TrimSpace(binding.TaskID)
	if bindingTaskID != "" && rootTaskID != "" && !sameWorkspaceDocTaskID(rootTaskID, bindingTaskID) {
		return fmt.Errorf("project_bootstrap blocked root_task_id %s because this runtime is currently bound to task %s; bootstrap the active task or hand off before retargeting project leadership", rootTaskID, bindingTaskID)
	}
	bindingProjectID := strings.TrimSpace(binding.ProjectID)
	if bindingProjectID != "" && requestedProjectID != "" && !sameWorkspaceDocProjectID(requestedProjectID, bindingProjectID) {
		return fmt.Errorf("project_bootstrap blocked project_id mismatch: active task is already bound to project_id %s; use that project_id instead of creating or attaching %s", bindingProjectID, requestedProjectID)
	}
	return nil
}

func validateProjectBootstrapRootTaskShape(binding AgentRuntimeBinding, bundle TaskHydrationBundle) error {
	activeProjectID := strings.TrimSpace(binding.ProjectID)
	if activeProjectID == "" {
		return nil
	}
	taskID := ""
	taskProjectID := ""
	taskKind := ""
	taskLane := ""
	if bundle.WorkspaceTask != nil && strings.TrimSpace(bundle.WorkspaceTask.TaskID) != "" {
		taskID = bundle.WorkspaceTask.TaskID
		taskProjectID = bundle.WorkspaceTask.ProjectID
		taskKind = bundle.WorkspaceTask.TaskKind
		taskLane = bundle.WorkspaceTask.ProjectLane
	} else if strings.TrimSpace(bundle.Task.TaskID) != "" {
		taskID = bundle.Task.TaskID
		taskProjectID = bundle.Task.ProjectID
		taskKind = bundle.Task.TaskKind
		taskLane = bundle.Task.ProjectLane
	}
	if strings.TrimSpace(taskID) == "" {
		return nil
	}
	taskProjectID = strings.TrimSpace(taskProjectID)
	if taskProjectID == "" || !sameWorkspaceDocProjectID(taskProjectID, activeProjectID) {
		return nil
	}
	kind := strings.ToUpper(strings.TrimSpace(taskKind))
	lane := strings.ToLower(strings.TrimSpace(taskLane))
	if kind == "EXECUTION" && lane != "" && lane != "strategy" && lane != "spec" && lane != "planning" && lane != "intake" {
		return fmt.Errorf("project_bootstrap blocked active project-bound product task %s on project_id %s (task_kind=%s project_lane=%s); use project_branch_commit/project_branch_review_ready/project_patch_queue_submit for implementation evidence instead of relinking the task as strategy/spec", strings.TrimSpace(taskID), activeProjectID, kind, lane)
	}
	return nil
}

func projectBootstrapShouldPreserveExistingProfile(args map[string]any) bool {
	if optionalBoolArg(args, "repo_required") == nil {
		return true
	}
	for _, key := range []string{"repo_status", "repo_url", "repo_default_branch"} {
		if strings.TrimSpace(stringArg(args, key)) == "" {
			return true
		}
	}
	return false
}

func activeRootTaskClaimAgent(task WorkspaceTaskRecord) string {
	if task.ClaimAgentID == nil || task.ClaimStatus == nil {
		return ""
	}
	switch strings.ToUpper(strings.TrimSpace(*task.ClaimStatus)) {
	case "CLAIMED", "BLOCKED":
		return strings.TrimSpace(*task.ClaimAgentID)
	default:
		return ""
	}
}

func (t *ProjectBootstrapTool) findSpecTask(ctx context.Context, projectID string) (string, bool, error) {
	tasks, err := t.client.ListTasks(ctx, t.workspaceID)
	if err != nil {
		return "", false, err
	}
	for _, task := range tasks {
		if taskSubmitTaskIsTerminal(task) || strings.TrimSpace(task.ProjectID) != projectID {
			continue
		}
		lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
		text := strings.ToLower(task.Title + "\n" + task.Description)
		if (lane == "spec" || lane == "strategy" || lane == "planning") && (strings.Contains(text, "design") || strings.Contains(text, "spec")) {
			return task.TaskID, true, nil
		}
	}
	return "", false, nil
}

func selectBootstrapProject(projects []ProjectRecord, projectID, title string) (ProjectRecord, bool) {
	projectID = strings.TrimSpace(projectID)
	titleKey := projectBootstrapMatchKey(title)
	for _, project := range projects {
		if projectID != "" && strings.TrimSpace(project.ProjectID) == projectID {
			return project, true
		}
	}
	if titleKey == "" {
		return ProjectRecord{}, false
	}
	for _, project := range projects {
		if !strings.EqualFold(strings.TrimSpace(project.Status), "ACTIVE") {
			continue
		}
		if projectBootstrapMatchKey(project.Title) == titleKey {
			return project, true
		}
	}
	return ProjectRecord{}, false
}

func selectBootstrapProjectByID(projects []ProjectRecord, projectID string) (ProjectRecord, bool) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return ProjectRecord{}, false
	}
	for _, project := range projects {
		if sameWorkspaceDocProjectID(project.ProjectID, projectID) {
			return project, true
		}
	}
	return ProjectRecord{}, false
}

func projectBootstrapMatchKey(value string) string {
	value = norm.NFC.String(strings.ToLower(strings.TrimSpace(value)))
	var b strings.Builder
	space := false
	for _, r := range value {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			b.WriteRune(r)
			space = false
		default:
			if !space && b.Len() > 0 {
				b.WriteByte(' ')
				space = true
			}
		}
	}
	return strings.TrimSpace(b.String())
}

func bootstrapRepoRequired(args map[string]any, title, goal string) bool {
	if explicit := optionalBoolArg(args, "repo_required"); explicit != nil {
		return *explicit
	}
	return true
}

func bootstrapDefaultRepoStatus(repoRequired bool) string {
	if repoRequired {
		return "MISSING"
	}
	return "NOT_REQUIRED"
}

func projectBootstrapIDFromRootTask(rootTaskID string) string {
	return sanitizeRefSegment("project-" + strings.TrimSpace(rootTaskID))
}

func projectBootstrapSpecTaskID(projectID string) string {
	return sanitizeRefSegment("task-" + strings.TrimSpace(projectID) + "-spec")
}

func normalizeProjectBootstrapDesiredPhase(value string) (string, error) {
	phase := strings.ToUpper(strings.TrimSpace(value))
	if phase == "" {
		return "SPEC", nil
	}
	switch phase {
	case "INTAKE", "SPEC":
		return phase, nil
	default:
		return "", fmt.Errorf("desired_phase must be INTAKE or SPEC during project_bootstrap; later phase transitions require project gates")
	}
}

func sanitizeDocKeySegment(value string) string {
	return sanitizeRefSegment(value)
}

func dependencyList(taskID string) []string {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil
	}
	return []string{taskID}
}

func renderProjectBootstrapIntakeDoc(project ProjectRecord, goal, agentID, rootTaskID string, repoRequired bool, repoStatus string, sourceDocKeys []string) string {
	var b strings.Builder
	b.WriteString("# Project Intake - ")
	b.WriteString(strings.TrimSpace(project.Title))
	b.WriteString("\n\n")
	b.WriteString("- project_id: ")
	b.WriteString(strings.TrimSpace(project.ProjectID))
	b.WriteString("\n")
	b.WriteString("- strategic_lead_candidate: ")
	b.WriteString(strings.TrimSpace(agentID))
	b.WriteString("\n")
	if rootTaskID != "" {
		b.WriteString("- root_task_id: ")
		b.WriteString(strings.TrimSpace(rootTaskID))
		b.WriteString("\n")
	}
	if len(sourceDocKeys) > 0 {
		b.WriteString("- source_refs_doc_key: ")
		b.WriteString(projectSourceRefsDocKey(project.ProjectID))
		b.WriteString("\n")
		b.WriteString("- source_requirements_trace_doc_key: ")
		b.WriteString(projectSourceRequirementsTraceDocKey(project.ProjectID))
		b.WriteString("\n")
		b.WriteString("- source_doc_keys: ")
		b.WriteString(strings.Join(sourceDocKeys, ", "))
		b.WriteString("\n")
	}
	b.WriteString("- repo_required: ")
	b.WriteString(fmt.Sprint(repoRequired))
	b.WriteString("\n")
	b.WriteString("- repo_status: ")
	b.WriteString(strings.TrimSpace(repoStatus))
	b.WriteString("\n\n")
	b.WriteString("## Goal\n")
	b.WriteString(strings.TrimSpace(goal))
	b.WriteString("\n\n")
	b.WriteString("## Coordination Contract\n")
	b.WriteString("- Treat this Project as the canonical source of truth for planning, roles, repo, branches, blockers, and acceptance evidence.\n")
	if len(sourceDocKeys) > 0 {
		b.WriteString("- Before compressing the root/operator task, read the source_doc_keys from project.")
		b.WriteString(strings.TrimSpace(project.ProjectID))
		b.WriteString(".source_refs. Preserve acceptance-critical anchors in `rhizome_source_requirements_trace_v1`; summaries must not replace the original source artifacts.\n")
	}
	b.WriteString("- Extract the operator/root task into a visible spec-fidelity checklist before implementation; publish it as project.")
	b.WriteString(strings.TrimSpace(project.ProjectID))
	b.WriteString(".acceptance_criteria or as an explicit acceptance criteria section in the design doc.\n")
	b.WriteString("- Acceptance criteria should use stable IDs and name observable user flows, required inputs/outputs, constraints, non-goals, and verification evidence.\n")
	b.WriteString("- Do not begin implementation until a design document, implementation plan, and acceptance-criteria mapping are accepted and project gates permit implementation.\n")
	b.WriteString("- Use canonical doc keys that open gates: project.")
	b.WriteString(strings.TrimSpace(project.ProjectID))
	b.WriteString(".design_and_plan, or separate project.")
	b.WriteString(strings.TrimSpace(project.ProjectID))
	b.WriteString(".design and project.")
	b.WriteString(strings.TrimSpace(project.ProjectID))
	b.WriteString(".implementation_plan. Product-contract and plan-review docs are useful evidence, but they do not by themselves satisfy design_doc_ready / implementation_plan_ready.\n")
	b.WriteString("- Split work into project-scoped tasks with explicit lanes, dependencies, and write scopes before code work starts.\n")
	return b.String()
}

func renderProjectBootstrapSpecTaskDescription(projectID, projectTitle, rootTaskID string, sourceDocKeys []string) string {
	var b strings.Builder
	b.WriteString("Create the first project design document for ")
	b.WriteString(strings.TrimSpace(projectTitle))
	b.WriteString(" (")
	b.WriteString(strings.TrimSpace(projectID))
	b.WriteString("). Define the problem, proposed architecture, task decomposition, acceptance criteria, test/review strategy, and integration path. ")
	b.WriteString("Extract the acceptance criteria from the operator/root task into a visible checklist with stable IDs, then map each proposed implementation, review, validation, and post-MVP lane to those IDs. ")
	if len(sourceDocKeys) > 0 {
		b.WriteString("Read and preserve these source_doc_keys before summarizing: ")
		b.WriteString(strings.Join(sourceDocKeys, ", "))
		b.WriteString(". Publish `")
		b.WriteString(projectSourceRequirementsTraceDocKey(projectID))
		b.WriteString("` or an equivalent planning section with fenced `rhizome_source_requirements_trace_v1` containing source_doc_keys, non-droppable acceptance-critical anchors, acceptance_criteria_refs, and adjacent_wrong_products/non_goals. ")
	}
	b.WriteString("Do not implement the product in this task; publish the design as canonical workspace doc `project.")
	b.WriteString(sanitizeDocKeySegment(projectID))
	b.WriteString(".design_and_plan` (or separate `project.")
	b.WriteString(sanitizeDocKeySegment(projectID))
	b.WriteString(".design` and `project.")
	b.WriteString(sanitizeDocKeySegment(projectID))
	b.WriteString(".implementation_plan`) and leave implementation gates closed until reviewed.")
	if rootTaskID != "" {
		b.WriteString("\n\nRoot task dependency: ")
		b.WriteString(strings.TrimSpace(rootTaskID))
	}
	return b.String()
}
