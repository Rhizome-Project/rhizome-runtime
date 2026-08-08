package model

import "strings"

type TaskTemplateSpec struct {
	Name         string   `json:"name"`
	DisplayName  string   `json:"display_name"`
	Description  string   `json:"description"`
	DefaultKind  string   `json:"default_kind"`
	AllowedKinds []string `json:"allowed_kinds"`
}

const (
	TaskTemplateGeneric     = "generic"
	TaskTemplateProject     = "project"
	TaskTemplateResearch    = "research"
	TaskTemplateBugfix      = "bugfix"
	TaskTemplateDeploy      = "deploy"
	TaskTemplateIntegration = "integration"
	TaskTemplateOps         = "ops"
	TaskTemplateTooling     = "tooling"
)

var taskTemplateCatalog = map[string]TaskTemplateSpec{
	TaskTemplateGeneric: {
		Name:         TaskTemplateGeneric,
		DisplayName:  "Generic",
		Description:  "General-purpose work item when no stronger template exists yet.",
		DefaultKind:  TaskKindExecution,
		AllowedKinds: []string{TaskKindExecution, TaskKindCoordination},
	},
	TaskTemplateProject: {
		Name:         TaskTemplateProject,
		DisplayName:  "Project",
		Description:  "Broad project kickoff, framing, planning, and autonomous decomposition work.",
		DefaultKind:  TaskKindCoordination,
		AllowedKinds: []string{TaskKindCoordination},
	},
	TaskTemplateResearch: {
		Name:         TaskTemplateResearch,
		DisplayName:  "Research",
		Description:  "Discovery, analysis, or exploration work that may end in docs and decisions.",
		DefaultKind:  TaskKindCoordination,
		AllowedKinds: []string{TaskKindExecution, TaskKindCoordination},
	},
	TaskTemplateBugfix: {
		Name:         TaskTemplateBugfix,
		DisplayName:  "Bugfix",
		Description:  "Concrete defect remediation with runnable implementation or validation steps.",
		DefaultKind:  TaskKindExecution,
		AllowedKinds: []string{TaskKindExecution},
	},
	TaskTemplateDeploy: {
		Name:         TaskTemplateDeploy,
		DisplayName:  "Deploy",
		Description:  "Rollout, cutover, rollback, or environment change affecting a live target.",
		DefaultKind:  TaskKindExecution,
		AllowedKinds: []string{TaskKindExecution, TaskKindCoordination},
	},
	TaskTemplateIntegration: {
		Name:         TaskTemplateIntegration,
		DisplayName:  "Integration",
		Description:  "Cross-system hookup, bridge work, or protocol alignment between surfaces.",
		DefaultKind:  TaskKindExecution,
		AllowedKinds: []string{TaskKindExecution, TaskKindCoordination},
	},
	TaskTemplateOps: {
		Name:         TaskTemplateOps,
		DisplayName:  "Ops",
		Description:  "Runtime health, incident response, maintenance, or operator-facing process work.",
		DefaultKind:  TaskKindCoordination,
		AllowedKinds: []string{TaskKindExecution, TaskKindCoordination},
	},
	TaskTemplateTooling: {
		Name:         TaskTemplateTooling,
		DisplayName:  "Tooling",
		Description:  "Tool registration, tool creation, wrappers, bridges, and agent enablement utilities.",
		DefaultKind:  TaskKindCoordination,
		AllowedKinds: []string{TaskKindExecution, TaskKindCoordination},
	},
}

func LookupTaskTemplate(name string) (TaskTemplateSpec, bool) {
	spec, ok := taskTemplateCatalog[strings.ToLower(strings.TrimSpace(name))]
	return spec, ok
}

func ListTaskTemplates() []TaskTemplateSpec {
	return []TaskTemplateSpec{
		taskTemplateCatalog[TaskTemplateGeneric],
		taskTemplateCatalog[TaskTemplateProject],
		taskTemplateCatalog[TaskTemplateResearch],
		taskTemplateCatalog[TaskTemplateBugfix],
		taskTemplateCatalog[TaskTemplateDeploy],
		taskTemplateCatalog[TaskTemplateIntegration],
		taskTemplateCatalog[TaskTemplateOps],
		taskTemplateCatalog[TaskTemplateTooling],
	}
}

func ValidTaskTemplateForKind(templateName, taskKind string) bool {
	spec, ok := LookupTaskTemplate(templateName)
	if !ok {
		return false
	}
	kind := strings.TrimSpace(taskKind)
	for _, allowed := range spec.AllowedKinds {
		if allowed == kind {
			return true
		}
	}
	return false
}
