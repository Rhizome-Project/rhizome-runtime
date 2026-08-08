package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	sourceRequirementsTraceSchema = "rhizome_source_requirements_trace_v1"
	specFidelityReviewSchema      = "rhizome_spec_fidelity_review_v1"
)

type projectSourceFidelityContext struct {
	Required         bool
	SourceRefsDocKey string
	TraceDocKey      string
	SourceDocKeys    []string
}

func projectSourceRefsDocKey(projectID string) string {
	projectID = sanitizeProjectDocKeySegment(projectID)
	if projectID == "" {
		return ""
	}
	return "project." + projectID + ".source_refs"
}

func projectSourceRequirementsTraceDocKey(projectID string) string {
	projectID = sanitizeProjectDocKeySegment(projectID)
	if projectID == "" {
		return ""
	}
	return "project." + projectID + ".source_requirements_trace"
}

func (s *Store) projectSourceFidelityContextTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) (projectSourceFidelityContext, error) {
	sourceRefsDocKey := projectSourceRefsDocKey(projectID)
	traceDocKey := projectSourceRequirementsTraceDocKey(projectID)
	if strings.TrimSpace(sourceRefsDocKey) == "" || strings.TrimSpace(traceDocKey) == "" {
		return projectSourceFidelityContext{}, nil
	}
	refsDoc, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, sourceRefsDocKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return projectSourceFidelityContext{}, nil
		}
		return projectSourceFidelityContext{}, err
	}
	fidelity := projectSourceFidelityContext{
		Required:         true,
		SourceRefsDocKey: sourceRefsDocKey,
		TraceDocKey:      traceDocKey,
		SourceDocKeys:    SourceDocKeysFromSourceRefsText(refsDoc.Content),
	}
	if refsDoc.ArchivedAt != nil {
		return fidelity, fmt.Errorf("source_refs doc %s is archived", sourceRefsDocKey)
	}
	if len(fidelity.SourceDocKeys) == 0 {
		return fidelity, fmt.Errorf("source_refs doc %s has no source_doc_keys", sourceRefsDocKey)
	}
	return fidelity, nil
}

func (s *Store) validateProjectSourceFidelityTraceTx(ctx context.Context, tx *sql.Tx, workspaceID string, fidelity projectSourceFidelityContext) error {
	if !fidelity.Required {
		return nil
	}
	traceDoc, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, fidelity.TraceDocKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("source_requirements_trace doc %s not found", fidelity.TraceDocKey)
		}
		return err
	}
	if traceDoc.ArchivedAt != nil {
		return fmt.Errorf("source_requirements_trace doc %s is archived", fidelity.TraceDocKey)
	}
	if !sourceRequirementsTraceComplete(traceDoc.Content, fidelity.SourceDocKeys) {
		return fmt.Errorf("source_requirements_trace doc %s must contain %s with source_doc_keys, acceptance-critical anchors, acceptance criteria refs, and adjacent wrong products/non-goals", fidelity.TraceDocKey, sourceRequirementsTraceSchema)
	}
	return nil
}

func validateProjectSourceFidelityReviewDoc(reviewDoc WorkspaceDocRecord, fidelity projectSourceFidelityContext) error {
	if !fidelity.Required {
		return nil
	}
	content := strings.TrimSpace(reviewDoc.Content)
	lower := strings.ToLower(content)
	if !strings.Contains(lower, "source artifact fidelity") && !strings.Contains(lower, "source_fidelity") {
		return fmt.Errorf("review_doc_key %s must include Source Artifact Fidelity review expectations", strings.TrimSpace(reviewDoc.DocKey))
	}
	if !sourceFidelityFieldContainsAll(content, []string{"source_refs_doc_key", "source refs doc key"}, []string{fidelity.SourceRefsDocKey}) {
		return fmt.Errorf("review_doc_key %s must reference %s", strings.TrimSpace(reviewDoc.DocKey), fidelity.SourceRefsDocKey)
	}
	if !sourceFidelityFieldContainsAll(content, []string{"source_requirements_trace_doc_key", "source requirements trace doc key", "source_trace_doc_key", "source trace doc key"}, []string{fidelity.TraceDocKey}) {
		return fmt.Errorf("review_doc_key %s must reference %s", strings.TrimSpace(reviewDoc.DocKey), fidelity.TraceDocKey)
	}
	return nil
}

func validateProjectPatchQueueAcceptedSourceFidelity(decisionText string, fidelity projectSourceFidelityContext) error {
	decisionText = strings.TrimSpace(decisionText)
	lower := strings.ToLower(decisionText)
	if lower == "" {
		return fmt.Errorf("ACCEPTED source-fidelity review requires decision evidence")
	}
	if strings.Contains(lower, "blocked_spec_drift") ||
		strings.Contains(lower, "spec_drift: true") ||
		strings.Contains(lower, "source_fidelity_status: failed") ||
		strings.Contains(lower, "source_fidelity_status: fail") ||
		strings.Contains(lower, "source fidelity status: failed") ||
		strings.Contains(lower, "source fidelity status: fail") {
		return fmt.Errorf("ACCEPTED source-fidelity review cannot carry failed/SPEC_DRIFT status")
	}
	if strings.Contains(lower, specFidelityReviewSchema) && sourceFidelityDecisionStatusPassed(lower) {
		if fidelity.Required && !sourceFidelityFieldContainsAll(lower, []string{"checked_source_doc_keys", "checked source doc keys", "source_doc_keys", "source doc keys"}, fidelity.SourceDocKeys) {
			return fmt.Errorf("ACCEPTED source-fidelity review requires checked_source_doc_keys for every source_doc_key")
		}
		return nil
	}
	if sourceFidelityDecisionStatusPassed(lower) {
		if fidelity.Required && !sourceFidelityFieldContainsAll(lower, []string{"checked_source_doc_keys", "checked source doc keys", "source_doc_keys", "source doc keys"}, fidelity.SourceDocKeys) {
			return fmt.Errorf("ACCEPTED source-fidelity review requires checked_source_doc_keys for every source_doc_key")
		}
		return nil
	}
	return fmt.Errorf("ACCEPTED source-fidelity review requires %s or source_fidelity_status: passed", specFidelityReviewSchema)
}

func sourceRequirementsTraceComplete(text string, sourceDocKeys []string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" || len(sourceDocKeys) == 0 {
		return len(sourceDocKeys) == 0
	}
	for _, block := range sourceFidelityFencedBlocks(text, sourceRequirementsTraceSchema) {
		if sourceRequirementsTraceBlockComplete(block, sourceDocKeys) {
			return true
		}
	}
	if block, ok := sourceFidelityInlineSchemaBlock(text, sourceRequirementsTraceSchema); ok && sourceRequirementsTraceBlockComplete(block, sourceDocKeys) {
		return true
	}
	return false
}

func sourceFidelityInlineSchemaBlock(text, schema string) (string, bool) {
	text = strings.TrimSpace(text)
	schema = strings.ToLower(strings.TrimSpace(schema))
	if text == "" || schema == "" {
		return "", false
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(strings.ToLower(line))
		if trimmed == schema || strings.HasPrefix(trimmed, schema+":") || sourceFidelitySchemaFieldMatches(trimmed, schema) {
			return strings.Join(lines[i:], "\n"), true
		}
	}
	return "", false
}

func sourceFidelitySchemaFieldMatches(line, schema string) bool {
	line = strings.TrimSpace(strings.ToLower(line))
	schema = strings.TrimSpace(strings.ToLower(schema))
	if line == "" || schema == "" {
		return false
	}
	for _, sep := range []string{":", "="} {
		prefix := "schema" + sep
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		value := sourceFidelityNormalizeFieldToken(strings.TrimSpace(line[len(prefix):]))
		return value == schema
	}
	return false
}

func sourceRequirementsTraceBlockComplete(block string, sourceDocKeys []string) bool {
	block = strings.ToLower(strings.TrimSpace(block))
	if block == "" {
		return false
	}
	if !sourceFidelityFieldContainsAll(block, []string{"source_doc_keys", "source doc keys"}, sourceDocKeys) {
		return false
	}
	hasAnchors := sourceFidelityFieldHasSpecificValue(block, []string{
		"acceptance_critical_anchors",
		"acceptance critical anchors",
		"non_droppable_requirements",
		"non-droppable requirements",
		"required_product_identity",
		"required product identity",
	})
	hasAcceptanceRefs := sourceFidelityFieldHasSpecificValue(block, []string{
		"acceptance_criteria_refs",
		"acceptance criteria refs",
		"acceptance_criteria",
		"acceptance criteria",
		"mvp_acceptance",
		"mvp acceptance",
	})
	hasWrongProducts := sourceFidelityFieldHasSpecificValue(block, []string{
		"adjacent_wrong_products",
		"adjacent_wrong_products_non_goals",
		"adjacent wrong products",
		"adjacent wrong products non goals",
		"non_goals",
		"non-goals",
		"negative_constraints",
		"negative constraints",
	})
	return hasAnchors && hasAcceptanceRefs && hasWrongProducts && sourceRequirementsTracePerSourceComplete(block, sourceDocKeys)
}

func sourceRequirementsTracePerSourceComplete(block string, sourceDocKeys []string) bool {
	uniqueSourceKeys := uniqueSourceFidelityRefs(sourceDocKeys)
	if len(uniqueSourceKeys) <= 1 {
		return true
	}
	mapBlock, ok := sourceFidelityNamedSection(block, []string{
		"source_anchor_map",
		"source anchor map",
		"anchors_by_source",
		"anchors by source",
		"requirements_by_source",
		"requirements by source",
		"source_requirements_by_doc",
		"source requirements by doc",
		"per_source_anchors",
		"per source anchors",
		"source_trace_matrix",
		"source trace matrix",
		"source_acceptance_map",
		"source acceptance map",
	})
	if !ok {
		return false
	}
	for _, sourceKey := range uniqueSourceKeys {
		if !sourceFidelitySectionHasSpecificValuesForSource(mapBlock, sourceKey) {
			return false
		}
	}
	return true
}

func uniqueSourceFidelityRefs(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		normalized := sourceFidelityNormalizeFieldToken(trimmed)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func sourceFidelityNamedSection(text string, names []string) (string, bool) {
	lines := strings.Split(text, "\n")
	for _, name := range names {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		for i, line := range lines {
			trimmed := strings.TrimSpace(strings.ToLower(line))
			if trimmed != name+":" && trimmed != "- "+name+":" {
				continue
			}
			startIndent := len(line) - len(strings.TrimLeft(line, " \t"))
			var section []string
			for _, next := range lines[i+1:] {
				if strings.TrimSpace(next) == "" {
					continue
				}
				nextIndent := len(next) - len(strings.TrimLeft(next, " \t"))
				trimmedNext := strings.TrimSpace(next)
				if nextIndent <= startIndent && strings.Contains(trimmedNext, ":") && !strings.HasPrefix(trimmedNext, "-") {
					break
				}
				section = append(section, next)
			}
			return strings.Join(section, "\n"), len(section) > 0
		}
	}
	return "", false
}

func sourceFidelitySectionHasSpecificValuesForSource(section, sourceKey string) bool {
	sourceKey = sourceFidelityNormalizeFieldToken(sourceKey)
	if section == "" || sourceKey == "" {
		return false
	}
	lines := strings.Split(section, "\n")
	for i, line := range lines {
		normalizedLine := sourceFidelityNormalizeFieldToken(line)
		if !strings.Contains(normalizedLine, sourceKey) {
			continue
		}
		baseIndent := len(line) - len(strings.TrimLeft(line, " \t"))
		for _, next := range lines[i+1:] {
			if strings.TrimSpace(next) == "" {
				continue
			}
			nextIndent := len(next) - len(strings.TrimLeft(next, " \t"))
			normalizedNext := sourceFidelityNormalizeFieldToken(next)
			if nextIndent <= baseIndent && strings.Contains(strings.TrimSpace(next), ":") {
				break
			}
			if strings.Contains(normalizedNext, sourceKey) {
				continue
			}
			if sourceFidelityTraceValueSpecific(normalizedNext) {
				return true
			}
		}
	}
	return false
}

func sourceFidelityFieldHasSpecificValue(text string, names []string) bool {
	for _, value := range sourceFidelityFieldValues(text, names) {
		if sourceFidelityTraceValueSpecific(value) {
			return true
		}
	}
	return false
}

func sourceFidelityTraceValueSpecific(value string) bool {
	value = sourceFidelityNormalizeFieldToken(value)
	if !sourceFidelityValueMeaningful(value) {
		return false
	}
	switch value {
	case "acceptance criteria",
		"acceptance criteria refs",
		"adjacent product",
		"adjacent wrong product",
		"adjacent wrong products",
		"app",
		"application",
		"basic functionality",
		"core experience",
		"core flow",
		"feature",
		"happy path",
		"implementation",
		"main feature",
		"main flow",
		"mvp",
		"primary flow",
		"product",
		"requirements",
		"source requirements",
		"source spec",
		"user experience",
		"user flow":
		return false
	default:
		return true
	}
}

func sourceFidelityFencedBlocks(text, schema string) []string {
	text = strings.TrimSpace(text)
	schema = strings.ToLower(strings.TrimSpace(schema))
	if text == "" || schema == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	blocks := []string{}
	inBlock := false
	var current []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		lower := strings.ToLower(trimmed)
		if !inBlock && strings.HasPrefix(lower, "```") && strings.Contains(lower, schema) {
			inBlock = true
			current = current[:0]
			continue
		}
		if inBlock && strings.HasPrefix(trimmed, "```") {
			inBlock = false
			blocks = append(blocks, strings.Join(current, "\n"))
			continue
		}
		if inBlock {
			current = append(current, line)
		}
	}
	return blocks
}

func sourceFidelityFieldHasValue(text string, names []string) bool {
	return len(sourceFidelityFieldValues(text, names)) > 0
}

func sourceFidelityFieldValues(text string, names []string) []string {
	lines := strings.Split(strings.ToLower(text), "\n")
	var out []string
	for _, name := range names {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		for i, line := range lines {
			trimmedLine := strings.TrimSpace(line)
			if sourceFidelityMarkdownHeadingMatchesField(trimmedLine, name) {
				out = append(out, sourceFidelityFollowingFieldValues(lines[i+1:])...)
				continue
			}
			for _, sep := range []string{":", "="} {
				token := name + sep
				idx := strings.Index(trimmedLine, token)
				if idx < 0 {
					continue
				}
				if !strings.HasPrefix(trimmedLine, token) && !strings.HasPrefix(trimmedLine, "- "+token) {
					continue
				}
				value := strings.TrimSpace(trimmedLine[idx+len(token):])
				out = append(out, sourceFidelitySplitFieldValues(value)...)
				if !sourceFidelityValueMeaningful(value) {
					out = append(out, sourceFidelityFollowingFieldValues(lines[i+1:])...)
				}
			}
		}
	}
	return out
}

func sourceFidelityFollowingFieldValues(lines []string) []string {
	var out []string
	for _, rawNext := range lines {
		if strings.TrimSpace(rawNext) == "" {
			continue
		}
		nextIndent := len(rawNext) - len(strings.TrimLeft(rawNext, " \t"))
		next := strings.TrimSpace(rawNext)
		if strings.HasPrefix(next, "#") {
			break
		}
		if strings.Contains(next, ":") && !strings.HasPrefix(next, "-") && nextIndent == 0 {
			break
		}
		if strings.HasPrefix(next, "-") {
			item := strings.TrimSpace(strings.TrimPrefix(next, "-"))
			if key, value, ok := strings.Cut(item, ":"); ok && sourceFidelityNestedTraceFieldAllowed(key) {
				out = append(out, sourceFidelitySplitFieldValues(value)...)
				continue
			}
			out = append(out, sourceFidelitySplitFieldValues(item)...)
			continue
		}
		if key, value, ok := strings.Cut(next, ":"); ok && nextIndent > 0 {
			if sourceFidelityNestedTraceFieldAllowed(key) || sourceFidelityValueMeaningful(value) {
				out = append(out, sourceFidelitySplitFieldValues(value)...)
				continue
			}
		}
		out = append(out, sourceFidelitySplitFieldValues(next)...)
	}
	return out
}

func sourceFidelityMarkdownHeadingMatchesField(line, field string) bool {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "#") {
		return false
	}
	line = strings.TrimSpace(strings.TrimLeft(line, "#"))
	line = strings.TrimSpace(strings.TrimSuffix(line, ":"))
	return sourceFidelityCanonicalFieldName(line) == sourceFidelityCanonicalFieldName(field)
}

func sourceFidelityCanonicalFieldName(value string) string {
	value = sourceFidelityNormalizeFieldToken(value)
	value = strings.NewReplacer("_", " ", "-", " ").Replace(value)
	return strings.Join(strings.Fields(value), " ")
}

func sourceFidelityFieldContainsAll(text string, names []string, required []string) bool {
	values := sourceFidelityFieldValues(text, names)
	if len(values) == 0 {
		return false
	}
	seen := map[string]struct{}{}
	for _, value := range values {
		value = sourceFidelityNormalizeFieldToken(value)
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	for _, item := range required {
		item = sourceFidelityNormalizeFieldToken(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; !ok {
			return false
		}
	}
	return true
}

func sourceFidelityNestedTraceFieldAllowed(key string) bool {
	switch sourceFidelityNormalizeFieldToken(key) {
	case "anchor", "id", "label", "name", "requirement", "criterion", "acceptance_id", "non_goal", "wrong_product":
		return true
	default:
		return sourceFidelityTraceListItemIDLabel(key)
	}
}

func sourceFidelityTraceListItemIDLabel(key string) bool {
	key = sourceFidelityNormalizeFieldToken(key)
	if key == "" {
		return false
	}
	parts := strings.FieldsFunc(key, func(r rune) bool {
		return r == '-' || r == '_' || r == ' ' || r == '.'
	})
	if len(parts) == 1 {
		part := parts[0]
		firstDigit := -1
		for i, r := range part {
			if r >= '0' && r <= '9' {
				firstDigit = i
				break
			}
		}
		if firstDigit <= 0 || firstDigit >= len(part) {
			return false
		}
		parts = []string{part[:firstDigit], part[firstDigit:]}
	}
	if len(parts) != 2 {
		return false
	}
	prefix, suffix := parts[0], parts[1]
	if prefix == "" || suffix == "" || len(prefix) > 12 || len(suffix) > 8 {
		return false
	}
	for _, r := range prefix {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	hasDigit := false
	for _, r := range suffix {
		switch {
		case r >= '0' && r <= '9':
			hasDigit = true
		case r >= 'a' && r <= 'z':
		default:
			return false
		}
	}
	return hasDigit
}

func sourceFidelitySplitFieldValues(value string) []string {
	value = strings.TrimSpace(value)
	if !sourceFidelityValueMeaningful(value) {
		return nil
	}
	value = strings.TrimSpace(strings.Trim(value, "[]{}"))
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = sourceFidelityNormalizeFieldToken(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func sourceFidelityNormalizeFieldToken(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimSpace(strings.Trim(value, "`'\"[]{}"))
	value = strings.TrimRight(value, ".,;")
	return strings.TrimSpace(value)
}

func sourceFidelityValueMeaningful(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && value != "[]" && value != "{}" && value != "none" && value != "n/a" && value != "tbd" && value != "todo"
}

func sourceFidelityDecisionStatusPassed(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	if sourceFidelityExactPassField(text, []string{
		"source_fidelity_status",
		"source fidelity status",
		"source_fidelity",
		"source fidelity",
	}) {
		return true
	}
	if !strings.Contains(text, specFidelityReviewSchema) {
		return false
	}
	for _, block := range sourceFidelityFencedBlocks(text, specFidelityReviewSchema) {
		if sourceFidelityExactPassField(block, []string{"status", "verdict", "source_fidelity_status", "source fidelity status"}) {
			return true
		}
	}
	return false
}

func sourceFidelityExactPassField(text string, names []string) bool {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "-"))
		if line == "" {
			continue
		}
		for _, name := range names {
			name = strings.ToLower(strings.TrimSpace(name))
			if name == "" {
				continue
			}
			for _, sep := range []string{":", "="} {
				prefix := name + sep
				if !strings.HasPrefix(line, prefix) {
					continue
				}
				value := strings.TrimSpace(line[len(prefix):])
				value = strings.TrimSpace(strings.Trim(value, "`'\"[]{}"))
				value = strings.TrimRight(value, ".,;")
				return value == "pass" || value == "passed"
			}
		}
	}
	return false
}

func sourceDocKeysFromTaskRequirementsJSON(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
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
	return uniqueTrimmedSourceDocKeys(out)
}

// SourceDocKeysFromSourceRefsText extracts only declared source_doc_keys from a
// rhizome_source_refs_v1 document. Metadata fields such as project_id/root_task_id
// are intentionally not source docs.
func SourceDocKeysFromSourceRefsText(text string) []string {
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
	return uniqueTrimmedSourceDocKeys(out)
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

func uniqueTrimmedSourceDocKeys(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}
