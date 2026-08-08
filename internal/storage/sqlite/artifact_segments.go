package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
)

type WorkspaceSegmentFilter struct {
	WorkspaceID string `json:"workspace_id"`
	DocKey      string `json:"doc_key,omitempty"`
	ArtifactRef string `json:"artifact_ref,omitempty"`
	SegmentRef  string `json:"segment_ref,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type WorkspaceSegmentRecord struct {
	SegmentRef   string `json:"segment_ref"`
	SourceKind   string `json:"source_kind"`
	SourceRef    string `json:"source_ref"`
	SourceTitle  string `json:"source_title,omitempty"`
	SegmentKind  string `json:"segment_kind"`
	Title        string `json:"title,omitempty"`
	Summary      string `json:"summary,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	HeadingLevel int    `json:"heading_level,omitempty"`
	Ordinal      int    `json:"ordinal,omitempty"`
	StartLine    int    `json:"start_line,omitempty"`
	EndLine      int    `json:"end_line,omitempty"`
	IsRoot       bool   `json:"is_root,omitempty"`
	GeneratedAt  string `json:"generated_at,omitempty"`
}

type WorkspaceSegmentSourceRecord struct {
	SourceKind      string `json:"source_kind"`
	SourceRef       string `json:"source_ref"`
	SourceTitle     string `json:"source_title,omitempty"`
	ContentType     string `json:"content_type,omitempty"`
	RootSegmentRef  string `json:"root_segment_ref,omitempty"`
	SegmentCount    int    `json:"segment_count"`
	HasRichSegments bool   `json:"has_rich_segments"`
}

type WorkspaceSegmentReport struct {
	WorkspaceID   string                         `json:"workspace_id"`
	TimeAuthority WorkspaceTimeAuthority         `json:"time_authority"`
	GeneratedAt   string                         `json:"generated_at"`
	Filter        WorkspaceSegmentFilter         `json:"filter"`
	Sources       []WorkspaceSegmentSourceRecord `json:"sources,omitempty"`
	Segments      []WorkspaceSegmentRecord       `json:"segments,omitempty"`
}

type workspaceHeadingSegment struct {
	level int
	title string
	line  int
}

func normalizeWorkspaceSegmentFilter(filter WorkspaceSegmentFilter) WorkspaceSegmentFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.DocKey = strings.TrimSpace(filter.DocKey)
	filter.ArtifactRef = strings.TrimSpace(filter.ArtifactRef)
	filter.SegmentRef = strings.TrimSpace(filter.SegmentRef)
	if filter.Limit <= 0 {
		filter.Limit = 200
	}
	return filter
}

func (s *Store) BuildWorkspaceSegmentReport(ctx context.Context, filter WorkspaceSegmentFilter) (WorkspaceSegmentReport, error) {
	filter = normalizeWorkspaceSegmentFilter(filter)
	if filter.WorkspaceID == "" {
		return WorkspaceSegmentReport{}, errors.New("workspace_id is required")
	}
	if filter.DocKey != "" && filter.ArtifactRef != "" {
		return WorkspaceSegmentReport{}, errors.New("doc_key and artifact_ref are mutually exclusive")
	}
	docKeys := []string{}
	artifactRefs := []string{}
	if filter.DocKey != "" {
		docKeys = []string{filter.DocKey}
	}
	if filter.ArtifactRef != "" {
		artifactRefs = []string{filter.ArtifactRef}
	}
	if filter.SegmentRef != "" {
		sourceKind, sourceRef, err := parseWorkspaceSegmentRef(filter.WorkspaceID, filter.SegmentRef)
		if err != nil {
			return WorkspaceSegmentReport{}, err
		}
		switch sourceKind {
		case "workspace_doc":
			docKeys = []string{sourceRef}
		case "workspace_artifact":
			artifactRefs = []string{sourceRef}
		default:
			return WorkspaceSegmentReport{}, fmt.Errorf("unsupported segment source kind: %s", sourceKind)
		}
	}
	segments, err := s.collectWorkspaceSegments(ctx, filter.WorkspaceID, docKeys, artifactRefs)
	if err != nil {
		return WorkspaceSegmentReport{}, err
	}
	if filter.SegmentRef != "" {
		target := strings.TrimSpace(filter.SegmentRef)
		filtered := make([]WorkspaceSegmentRecord, 0, 1)
		for _, segment := range segments {
			if strings.TrimSpace(segment.SegmentRef) == target {
				filtered = append(filtered, segment)
				break
			}
		}
		if len(filtered) == 0 {
			return WorkspaceSegmentReport{}, fmt.Errorf("segment not found: %s", target)
		}
		segments = filtered
	}
	sort.Slice(segments, func(i, j int) bool {
		left := segments[i]
		right := segments[j]
		if left.SourceKind != right.SourceKind {
			return left.SourceKind < right.SourceKind
		}
		if left.SourceRef != right.SourceRef {
			return left.SourceRef < right.SourceRef
		}
		if left.StartLine != right.StartLine {
			return left.StartLine < right.StartLine
		}
		return left.SegmentRef < right.SegmentRef
	})
	if filter.Limit > 0 && len(segments) > filter.Limit {
		segments = append([]WorkspaceSegmentRecord(nil), segments[:filter.Limit]...)
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return WorkspaceSegmentReport{}, err
	}
	generatedAt := generatedAtFromWorkspaceTimeAuthority(authority)
	for idx := range segments {
		segments[idx].GeneratedAt = generatedAt
	}
	return WorkspaceSegmentReport{
		WorkspaceID:   filter.WorkspaceID,
		TimeAuthority: authority,
		GeneratedAt:   generatedAt,
		Filter:        filter,
		Sources:       buildWorkspaceSegmentSources(segments),
		Segments:      segments,
	}, nil
}

func (s *Store) GetWorkspaceSegment(ctx context.Context, workspaceID, segmentRef string) (WorkspaceSegmentRecord, error) {
	report, err := s.BuildWorkspaceSegmentReport(ctx, WorkspaceSegmentFilter{
		WorkspaceID: workspaceID,
		SegmentRef:  segmentRef,
		Limit:       1,
	})
	if err != nil {
		return WorkspaceSegmentRecord{}, err
	}
	if len(report.Segments) == 0 {
		return WorkspaceSegmentRecord{}, fmt.Errorf("segment not found: %s", strings.TrimSpace(segmentRef))
	}
	return report.Segments[0], nil
}

func (s *Store) collectWorkspaceSegments(ctx context.Context, workspaceID string, docKeys, artifactRefs []string) ([]WorkspaceSegmentRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	out := []WorkspaceSegmentRecord{}
	if len(docKeys) == 0 && len(artifactRefs) == 0 {
		docs, err := s.listWorkspaceDocs(ctx, workspaceID)
		if err != nil {
			return nil, err
		}
		for _, doc := range docs {
			out = append(out, buildWorkspaceDocSegments(workspaceID, doc)...)
		}
		artifacts, err := s.listAllWorkspaceArtifacts(ctx, workspaceID)
		if err != nil {
			return nil, err
		}
		for _, artifact := range artifacts {
			out = append(out, buildWorkspaceArtifactSegments(workspaceID, artifact)...)
		}
		return out, nil
	}
	for _, docKey := range uniqueSortedStrings(docKeys) {
		doc, err := s.GetWorkspaceDoc(ctx, workspaceID, docKey)
		if err != nil {
			return nil, err
		}
		out = append(out, buildWorkspaceDocSegments(workspaceID, doc)...)
	}
	for _, artifactRef := range uniqueSortedStrings(artifactRefs) {
		artifact, err := s.loadWorkspaceArtifactByRef(ctx, workspaceID, artifactRef)
		if err != nil {
			return nil, err
		}
		out = append(out, buildWorkspaceArtifactSegments(workspaceID, artifact)...)
	}
	return out, nil
}

func (s *Store) loadWorkspaceArtifactByRef(ctx context.Context, workspaceID, artifactRef string) (WorkspaceArtifactRecord, error) {
	var row WorkspaceArtifactRecord
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT artifact_id, workspace_id, task_id, update_id, title, artifact_ref, kind, content_type,
		        created_by, metadata_json, created_at
		   FROM workspace_artifacts
		  WHERE workspace_id = ? AND artifact_ref = ?
		  ORDER BY created_at DESC, artifact_id DESC
		  LIMIT 1`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(artifactRef),
	).Scan(
		&row.ArtifactID,
		&row.WorkspaceID,
		&row.TaskID,
		&row.UpdateID,
		&row.Title,
		&row.ArtifactRef,
		&row.Kind,
		&row.ContentType,
		&row.CreatedBy,
		&row.MetadataJSON,
		&row.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceArtifactRecord{}, fmt.Errorf("workspace artifact not found: %s/%s", strings.TrimSpace(workspaceID), strings.TrimSpace(artifactRef))
		}
		return WorkspaceArtifactRecord{}, fmt.Errorf("query workspace artifact by ref: %w", err)
	}
	return row, nil
}

func (s *Store) listAllWorkspaceArtifacts(ctx context.Context, workspaceID string) ([]WorkspaceArtifactRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT artifact_id, workspace_id, task_id, update_id, title, artifact_ref, kind, content_type,
		        created_by, metadata_json, created_at
		   FROM workspace_artifacts
		  WHERE workspace_id = ?
		  ORDER BY created_at DESC, artifact_id DESC`,
		strings.TrimSpace(workspaceID),
	)
	if err != nil {
		return nil, fmt.Errorf("query all workspace artifacts: %w", err)
	}
	defer rows.Close()
	out := []WorkspaceArtifactRecord{}
	for rows.Next() {
		var row WorkspaceArtifactRecord
		var taskID, updateID sql.NullString
		if err := rows.Scan(
			&row.ArtifactID,
			&row.WorkspaceID,
			&taskID,
			&updateID,
			&row.Title,
			&row.ArtifactRef,
			&row.Kind,
			&row.ContentType,
			&row.CreatedBy,
			&row.MetadataJSON,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workspace artifact: %w", err)
		}
		row.TaskID = nullStringPtr(taskID)
		row.UpdateID = nullStringPtr(updateID)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace artifacts: %w", err)
	}
	return out, nil
}

func buildWorkspaceSegmentSources(segments []WorkspaceSegmentRecord) []WorkspaceSegmentSourceRecord {
	if len(segments) == 0 {
		return nil
	}
	type key struct {
		sourceKind string
		sourceRef  string
	}
	index := map[key]*WorkspaceSegmentSourceRecord{}
	order := make([]key, 0, len(segments))
	for _, segment := range segments {
		k := key{sourceKind: segment.SourceKind, sourceRef: segment.SourceRef}
		entry, ok := index[k]
		if !ok {
			entry = &WorkspaceSegmentSourceRecord{
				SourceKind:  segment.SourceKind,
				SourceRef:   segment.SourceRef,
				SourceTitle: segment.SourceTitle,
				ContentType: segment.ContentType,
			}
			index[k] = entry
			order = append(order, k)
		}
		entry.SegmentCount++
		if segment.IsRoot {
			entry.RootSegmentRef = segment.SegmentRef
		}
		if !segment.IsRoot {
			entry.HasRichSegments = true
		}
	}
	out := make([]WorkspaceSegmentSourceRecord, 0, len(order))
	for _, k := range order {
		out = append(out, *index[k])
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceKind != out[j].SourceKind {
			return out[i].SourceKind < out[j].SourceKind
		}
		return out[i].SourceRef < out[j].SourceRef
	})
	return out
}

func buildWorkspaceDocSegments(workspaceID string, doc WorkspaceDocRecord) []WorkspaceSegmentRecord {
	docKey := strings.TrimSpace(doc.DocKey)
	rootRef := buildWorkspaceDocSegmentRef(workspaceID, docKey, "root")
	content := normalizeSegmentText(doc.Content)
	lines := strings.Split(content, "\n")
	out := []WorkspaceSegmentRecord{{
		SegmentRef:  rootRef,
		SourceKind:  "workspace_doc",
		SourceRef:   docKey,
		SourceTitle: firstNonEmpty(strings.TrimSpace(doc.Title), docKey),
		SegmentKind: "root",
		Title:       firstNonEmpty(strings.TrimSpace(doc.Title), docKey),
		Summary:     clipSummary(firstNonEmpty(markdownFirstBodySnippet(lines), strings.TrimSpace(doc.Title), docKey), 180),
		ContentType: "text/markdown",
		Ordinal:     0,
		StartLine:   1,
		EndLine:     len(lines),
		IsRoot:      true,
	}}
	headings := extractMarkdownHeadingSegments(lines)
	if len(headings) == 0 {
		return out
	}
	slugs := map[string]int{}
	for idx, heading := range headings {
		endLine := len(lines)
		if idx+1 < len(headings) {
			endLine = headings[idx+1].line - 1
		}
		slug := workspaceSegmentSlug(heading.title)
		slugs[slug]++
		if slugs[slug] > 1 {
			slug = fmt.Sprintf("%s-%d", slug, slugs[slug])
		}
		out = append(out, WorkspaceSegmentRecord{
			SegmentRef:   buildWorkspaceDocSegmentRef(workspaceID, docKey, slug),
			SourceKind:   "workspace_doc",
			SourceRef:    docKey,
			SourceTitle:  firstNonEmpty(strings.TrimSpace(doc.Title), docKey),
			SegmentKind:  "heading",
			Title:        heading.title,
			Summary:      clipSummary(markdownSegmentSummary(lines, heading.line, endLine), 180),
			ContentType:  "text/markdown",
			HeadingLevel: heading.level,
			Ordinal:      idx + 1,
			StartLine:    heading.line,
			EndLine:      endLine,
		})
	}
	return out
}

func buildWorkspaceArtifactSegments(workspaceID string, artifact WorkspaceArtifactRecord) []WorkspaceSegmentRecord {
	artifactRef := strings.TrimSpace(artifact.ArtifactRef)
	title := firstNonEmpty(strings.TrimSpace(artifact.Title), artifactRef, strings.TrimSpace(artifact.ArtifactID))
	contentType := firstNonEmpty(strings.TrimSpace(artifact.ContentType), "application/octet-stream")
	rootRef := buildWorkspaceArtifactSegmentRef(workspaceID, artifactRef, "root")
	textBody := workspaceArtifactTextBody(artifact)
	lines := []string{}
	if textBody != "" {
		lines = strings.Split(normalizeSegmentText(textBody), "\n")
	}
	summary := title
	if len(lines) > 0 {
		summary = firstNonEmpty(markdownFirstBodySnippet(lines), title)
	}
	out := []WorkspaceSegmentRecord{{
		SegmentRef:  rootRef,
		SourceKind:  "workspace_artifact",
		SourceRef:   artifactRef,
		SourceTitle: title,
		SegmentKind: "root",
		Title:       title,
		Summary:     clipSummary(summary, 180),
		ContentType: contentType,
		Ordinal:     0,
		StartLine:   1,
		EndLine:     maxInt(len(lines), 1),
		IsRoot:      true,
	}}
	if len(lines) == 0 {
		return out
	}
	headings := extractMarkdownHeadingSegments(lines)
	if len(headings) > 0 {
		slugs := map[string]int{}
		for idx, heading := range headings {
			endLine := len(lines)
			if idx+1 < len(headings) {
				endLine = headings[idx+1].line - 1
			}
			slug := workspaceSegmentSlug(heading.title)
			slugs[slug]++
			if slugs[slug] > 1 {
				slug = fmt.Sprintf("%s-%d", slug, slugs[slug])
			}
			out = append(out, WorkspaceSegmentRecord{
				SegmentRef:   buildWorkspaceArtifactSegmentRef(workspaceID, artifactRef, slug),
				SourceKind:   "workspace_artifact",
				SourceRef:    artifactRef,
				SourceTitle:  title,
				SegmentKind:  "heading",
				Title:        heading.title,
				Summary:      clipSummary(markdownSegmentSummary(lines, heading.line, endLine), 180),
				ContentType:  contentType,
				HeadingLevel: heading.level,
				Ordinal:      idx + 1,
				StartLine:    heading.line,
				EndLine:      endLine,
			})
		}
		return out
	}
	return out
}

func extractMarkdownHeadingSegments(lines []string) []workspaceHeadingSegment {
	out := []workspaceHeadingSegment{}
	inFence := false
	for idx, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		level, title, ok := markdownHeadingFromLine(trimmed)
		if !ok {
			continue
		}
		out = append(out, workspaceHeadingSegment{
			level: level,
			title: title,
			line:  idx + 1,
		})
	}
	return out
}

func markdownHeadingFromLine(line string) (int, string, bool) {
	if !strings.HasPrefix(line, "#") {
		return 0, "", false
	}
	level := 0
	for level < len(line) && line[level] == '#' {
		level++
	}
	if level == 0 || level > 6 {
		return 0, "", false
	}
	if level >= len(line) || line[level] != ' ' {
		return 0, "", false
	}
	title := strings.TrimSpace(line[level:])
	if title == "" {
		return 0, "", false
	}
	return level, title, true
}

func markdownFirstBodySnippet(lines []string) string {
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if _, _, ok := markdownHeadingFromLine(trimmed); ok {
			continue
		}
		return trimmed
	}
	return ""
}

func markdownSegmentSummary(lines []string, startLine, endLine int) string {
	if startLine < 1 {
		startLine = 1
	}
	if endLine > len(lines) {
		endLine = len(lines)
	}
	for idx := startLine; idx <= endLine; idx++ {
		trimmed := strings.TrimSpace(lines[idx-1])
		if trimmed == "" {
			continue
		}
		if _, _, ok := markdownHeadingFromLine(trimmed); ok && idx == startLine {
			continue
		}
		return trimmed
	}
	if startLine >= 1 && startLine <= len(lines) {
		_, title, ok := markdownHeadingFromLine(strings.TrimSpace(lines[startLine-1]))
		if ok {
			return title
		}
	}
	return ""
}

func buildWorkspaceDocSegmentRef(workspaceID, docKey, suffix string) string {
	return "workspace_doc:" + strings.TrimSpace(workspaceID) + "/" + strings.TrimSpace(docKey) + "#" + strings.TrimSpace(suffix)
}

func buildWorkspaceArtifactSegmentRef(workspaceID, artifactRef, suffix string) string {
	return "artifact:" + strings.TrimSpace(workspaceID) + "/" + strings.TrimSpace(artifactRef) + "#" + strings.TrimSpace(suffix)
}

func parseWorkspaceSegmentRef(workspaceID, segmentRef string) (string, string, error) {
	segmentRef = strings.TrimSpace(segmentRef)
	workspaceID = strings.TrimSpace(workspaceID)
	switch {
	case strings.HasPrefix(segmentRef, "workspace_doc:"+workspaceID+"/"):
		rest := strings.TrimPrefix(segmentRef, "workspace_doc:"+workspaceID+"/")
		parts := strings.SplitN(rest, "#", 2)
		if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
			return "", "", fmt.Errorf("invalid segment_ref: %s", segmentRef)
		}
		return "workspace_doc", strings.TrimSpace(parts[0]), nil
	case strings.HasPrefix(segmentRef, "artifact:"+workspaceID+"/"):
		rest := strings.TrimPrefix(segmentRef, "artifact:"+workspaceID+"/")
		parts := strings.SplitN(rest, "#", 2)
		if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
			return "", "", fmt.Errorf("invalid segment_ref: %s", segmentRef)
		}
		return "workspace_artifact", strings.TrimSpace(parts[0]), nil
	default:
		return "", "", fmt.Errorf("invalid segment_ref: %s", segmentRef)
	}
}

func workspaceArtifactTextBody(record WorkspaceArtifactRecord) string {
	contentType := strings.ToLower(strings.TrimSpace(record.ContentType))
	if !(strings.HasPrefix(contentType, "text/") || strings.Contains(contentType, "markdown") || strings.Contains(contentType, "json")) {
		return ""
	}
	return clipSummary(workspaceArtifactMetadataPrimaryText(record.MetadataJSON), 8000)
}

func workspaceArtifactMetadataPrimaryText(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return ""
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return ""
	}
	for _, candidate := range workspaceArtifactMetadataTextCandidates(decoded, 0) {
		if strings.TrimSpace(candidate) != "" {
			return strings.TrimSpace(candidate)
		}
	}
	return ""
}

func workspaceArtifactMetadataTextCandidates(value any, depth int) []string {
	if depth > 4 || value == nil {
		return nil
	}
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case map[string]any:
		out := []string{}
		for _, key := range []string{"markdown", "content", "text", "body"} {
			if raw, ok := typed[key]; ok {
				out = append(out, workspaceArtifactMetadataTextCandidates(raw, depth+1)...)
			}
		}
		return out
	case []any:
		return nil
	default:
		return nil
	}
}

func workspaceSegmentSlug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "section"
	}
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(r)
			lastDash = false
		case !lastDash:
			b.WriteByte('-')
			lastDash = true
		}
	}
	slug := strings.Trim(b.String(), "-")
	if slug == "" {
		return "section"
	}
	return slug
}

func normalizeSegmentText(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	return strings.TrimSpace(value)
}
