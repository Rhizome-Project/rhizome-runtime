package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

func sourceDocKeysFromTaskRequirementsJSON(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
	return sourceDocKeysFromMap(payload)
}

func sourceDocKeysFromMap(payload map[string]any) []string {
	var out []string
	var walk func(key string, value any)
	walk = func(key string, value any) {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		keyLooksLikeSourceRef := sourceDocRequirementKey(normalizedKey)
		switch typed := value.(type) {
		case string:
			if keyLooksLikeSourceRef {
				out = append(out, sourceDocKeysFromValue(typed)...)
			}
		case []any:
			if keyLooksLikeSourceRef {
				for _, item := range typed {
					out = append(out, sourceDocKeysFromValue(fmt.Sprint(item))...)
				}
			}
		case []string:
			if keyLooksLikeSourceRef {
				for _, item := range typed {
					out = append(out, sourceDocKeysFromValue(item)...)
				}
			}
		case map[string]any:
			for childKey, childValue := range typed {
				walk(childKey, childValue)
			}
		}
	}
	for key, value := range payload {
		walk(key, value)
	}
	return uniqueTrimmedCSVStrings(out)
}

func sourceDocRequirementKey(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return false
	}
	switch key {
	case "operator_spec_doc", "operator_spec_doc_key", "operator_spec_doc_keys",
		"source_doc", "source_doc_key", "source_doc_keys",
		"source_spec_doc", "source_spec_doc_key", "source_spec_doc_keys",
		"requirement_source_ref", "requirement_source_refs",
		"requirement_doc_key", "requirement_doc_keys":
		return true
	}
	return (strings.Contains(key, "source") || strings.Contains(key, "spec") || strings.Contains(key, "requirement")) &&
		(strings.Contains(key, "doc_key") || strings.Contains(key, "doc_keys") || strings.Contains(key, "doc_ref") || strings.Contains(key, "doc_refs"))
}

func sourceDocKeysFromValue(value string) []string {
	value = strings.TrimSpace(strings.Trim(value, "`'\""))
	if value == "" {
		return nil
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		switch r {
		case ',', '\n', '\r', '\t':
			return true
		default:
			return false
		}
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(strings.Trim(part, "`'\"[]"))
		if validSourceDocKey(part) {
			out = append(out, part)
		}
	}
	return out
}

func validSourceDocKey(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 240 || strings.ContainsAny(value, " \t\r\n") {
		return false
	}
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-'
		if !ok {
			return false
		}
	}
	return true
}

func sourceDocKeysFromTaskBundle(bundle TaskHydrationBundle) []string {
	var out []string
	out = append(out, sourceDocKeysFromTaskRequirementsJSON(bundle.Task.TaskRequirementsJSON)...)
	if bundle.WorkspaceTask != nil {
		out = append(out, sourceDocKeysFromTaskRequirementsJSON(bundle.WorkspaceTask.TaskRequirementsJSON)...)
	}
	return uniqueTrimmedCSVStrings(out)
}

func projectSourceRefsDocKey(projectID string) string {
	return "project." + sanitizeDocKeySegment(projectID) + ".source_refs"
}

func projectSourceRequirementsTraceDocKey(projectID string) string {
	return "project." + sanitizeDocKeySegment(projectID) + ".source_requirements_trace"
}

func renderProjectSourceRefsDoc(project ProjectRecord, rootTaskID string, sourceDocKeys []string) string {
	var b strings.Builder
	b.WriteString("# Project Source Artifact References - ")
	b.WriteString(strings.TrimSpace(project.Title))
	b.WriteString("\n\n")
	b.WriteString("```rhizome_source_refs_v1\n")
	b.WriteString("project_id: ")
	b.WriteString(strings.TrimSpace(project.ProjectID))
	b.WriteString("\n")
	if strings.TrimSpace(rootTaskID) != "" {
		b.WriteString("root_task_id: ")
		b.WriteString(strings.TrimSpace(rootTaskID))
		b.WriteString("\n")
	}
	b.WriteString("source_doc_keys:\n")
	for _, key := range sourceDocKeys {
		b.WriteString("- ")
		b.WriteString(strings.TrimSpace(key))
		b.WriteString("\n")
	}
	b.WriteString("```\n\n")
	b.WriteString("## Operational Contract\n")
	b.WriteString("- These docs are source artifacts for the project intent. Summaries may compress them, but must not drop acceptance-critical product identity, required flows, constraints, or negative constraints.\n")
	b.WriteString("- Before IMPLEMENTATION opens, publish `")
	b.WriteString(projectSourceRequirementsTraceDocKey(project.ProjectID))
	b.WriteString("` or an equivalent project planning doc containing `rhizome_source_requirements_trace_v1`.\n")
	b.WriteString("- The trace must name the source_doc_keys, non-droppable acceptance-critical anchors, acceptance-criteria IDs, and adjacent wrong products/non-goals.\n")
	b.WriteString("- Implementation and review packets must evaluate artifacts against that trace, not only against a compressed primary-flow summary.\n")
	return strings.TrimSpace(b.String())
}

func sourceDocKeysFromSourceRefsText(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	var out []string
	inBlock := false
	inSourceDocList := false
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		lower := strings.ToLower(trimmed)
		if strings.HasPrefix(lower, "```rhizome_source_refs_v1") {
			inBlock = true
			inSourceDocList = false
			continue
		}
		if inBlock && strings.HasPrefix(trimmed, "```") {
			inBlock = false
			inSourceDocList = false
			continue
		}
		if !inBlock && !strings.Contains(lower, "source_doc") && !strings.Contains(lower, "operator_spec") {
			continue
		}
		if strings.HasPrefix(trimmed, "-") {
			if inSourceDocList {
				out = append(out, sourceDocKeysFromValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "-")))...)
			}
			continue
		}
		if idx := strings.Index(trimmed, ":"); idx >= 0 {
			key := strings.ToLower(strings.TrimSpace(trimmed[:idx]))
			inSourceDocList = sourceDocRequirementKey(key)
			if inSourceDocList {
				out = append(out, sourceDocKeysFromValue(strings.TrimSpace(trimmed[idx+1:]))...)
			} else if !inBlock {
				inSourceDocList = false
			}
		}
	}
	return uniqueTrimmedCSVStrings(out)
}
