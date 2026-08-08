package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestBuildWorkspaceSegmentReportExtractsDocHeadingSegmentsAndRootFallback(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-workspace-segments-docs"
	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook",
		Content:     "# Incident\nDeploy failed.\n\n## Fix Plan\nPatch and verify.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert headed workspace doc: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "note",
		Title:       "Short Note",
		Content:     "single line note",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert plain workspace doc: %v", err)
	}

	headed, err := store.BuildWorkspaceSegmentReport(ctx, sqlite.WorkspaceSegmentFilter{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build headed workspace segment report: %v", err)
	}
	if len(headed.Sources) != 1 || headed.Sources[0].SourceKind != "workspace_doc" || !headed.Sources[0].HasRichSegments {
		t.Fatalf("expected headed doc source to expose rich segments, got %+v", headed.Sources)
	}
	if headed.TimeAuthority.WorkspaceID != workspaceID || headed.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected workspace segment report to expose time authority, got %+v", headed.TimeAuthority)
	}
	if headed.GeneratedAt != headed.TimeAuthority.ReferenceAt {
		t.Fatalf("expected workspace segment report generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", headed.GeneratedAt, headed.TimeAuthority.ReferenceAt)
	}
	if len(headed.Segments) < 2 {
		t.Fatalf("expected headed doc to expose root + concrete segments, got %+v", headed.Segments)
	}
	headedSegment := requireNonRootWorkspaceSegment(t, headed.Segments, "workspace_doc")
	if headedSegment.GeneratedAt != headed.TimeAuthority.ReferenceAt {
		t.Fatalf("expected workspace segment generated_at to mirror time authority reference_at, got %+v", headedSegment)
	}
	if headedSegment.SegmentKind != "heading" || headedSegment.Title == "" {
		t.Fatalf("expected concrete heading segment, got %+v", headedSegment)
	}
	gotSegment, err := store.GetWorkspaceSegment(ctx, workspaceID, headedSegment.SegmentRef)
	if err != nil {
		t.Fatalf("get headed workspace segment: %v", err)
	}
	if gotSegment.SegmentRef != headedSegment.SegmentRef || gotSegment.SourceRef != "runbook" {
		t.Fatalf("expected get workspace segment to return headed segment, got %+v", gotSegment)
	}
	if gotSegment.GeneratedAt == "" {
		t.Fatalf("expected get workspace segment to keep generated_at populated, got %+v", gotSegment)
	}

	plain, err := store.BuildWorkspaceSegmentReport(ctx, sqlite.WorkspaceSegmentFilter{
		WorkspaceID: workspaceID,
		DocKey:      "note",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build plain workspace segment report: %v", err)
	}
	if len(plain.Sources) != 1 || plain.Sources[0].HasRichSegments {
		t.Fatalf("expected plain doc source to stay root-only, got %+v", plain.Sources)
	}
	if len(plain.Segments) != 1 || !plain.Segments[0].IsRoot || plain.Segments[0].SegmentKind != "root" {
		t.Fatalf("expected plain doc to fall back to root segment only, got %+v", plain.Segments)
	}
}

func TestBuildWorkspaceSegmentReportExtractsTextArtifactSegmentsAndBinaryRootFallback(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-workspace-segments-artifacts"
	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")

	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:   "artifact-text",
		WorkspaceID:  workspaceID,
		Title:        "Text Artifact",
		ArtifactRef:  "artifact://workspace-segments-text",
		Kind:         "document",
		ContentType:  "text/markdown",
		CreatedBy:    "agent-a",
		MetadataJSON: mustJSONForTest(t, map[string]any{"content": "# Timeline\n\nDeploy blocked.\n\n## Fix\nConfirm operator approval."}),
	}); err != nil {
		t.Fatalf("create text artifact: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:   "artifact-binary",
		WorkspaceID:  workspaceID,
		Title:        "Binary Artifact",
		ArtifactRef:  "artifact://workspace-segments-binary",
		Kind:         "blob",
		ContentType:  "application/octet-stream",
		CreatedBy:    "agent-a",
		MetadataJSON: mustJSONForTest(t, map[string]any{"bytes": 42}),
	}); err != nil {
		t.Fatalf("create binary artifact: %v", err)
	}

	textReport, err := store.BuildWorkspaceSegmentReport(ctx, sqlite.WorkspaceSegmentFilter{
		WorkspaceID: workspaceID,
		ArtifactRef: "artifact://workspace-segments-text",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build text artifact segment report: %v", err)
	}
	if len(textReport.Sources) != 1 || textReport.Sources[0].SourceKind != "workspace_artifact" || !textReport.Sources[0].HasRichSegments {
		t.Fatalf("expected text artifact source to expose rich segments, got %+v", textReport.Sources)
	}
	if textReport.GeneratedAt != textReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected text artifact report generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", textReport.GeneratedAt, textReport.TimeAuthority.ReferenceAt)
	}
	if len(textReport.Segments) < 2 {
		t.Fatalf("expected text artifact to expose root + concrete segments, got %+v", textReport.Segments)
	}
	textSegment := requireNonRootWorkspaceSegment(t, textReport.Segments, "workspace_artifact")
	if textSegment.GeneratedAt != textReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected text artifact segment generated_at to mirror time authority reference_at, got %+v", textSegment)
	}
	if textSegment.SourceRef != "artifact://workspace-segments-text" || textSegment.SegmentKind == "root" {
		t.Fatalf("expected concrete text artifact segment, got %+v", textSegment)
	}

	binaryReport, err := store.BuildWorkspaceSegmentReport(ctx, sqlite.WorkspaceSegmentFilter{
		WorkspaceID: workspaceID,
		ArtifactRef: "artifact://workspace-segments-binary",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build binary artifact segment report: %v", err)
	}
	if len(binaryReport.Sources) != 1 || binaryReport.Sources[0].HasRichSegments {
		t.Fatalf("expected binary artifact source to stay root-only, got %+v", binaryReport.Sources)
	}
	if len(binaryReport.Segments) != 1 || !binaryReport.Segments[0].IsRoot || binaryReport.Segments[0].SegmentKind != "root" {
		t.Fatalf("expected binary artifact to fall back to root segment only, got %+v", binaryReport.Segments)
	}
}

func TestBuildWorkspaceSegmentReportKeepsNonHeadingTextArtifactsRootOnly(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-workspace-segments-text-root-only"
	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")

	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:   "artifact-text-root-only",
		WorkspaceID:  workspaceID,
		Title:        "Loose Notes",
		ArtifactRef:  "artifact://workspace-segments-text-root-only",
		Kind:         "document",
		ContentType:  "text/plain",
		CreatedBy:    "agent-a",
		MetadataJSON: mustJSONForTest(t, map[string]any{"content": "first paragraph\n\nsecond paragraph without headings"}),
	}); err != nil {
		t.Fatalf("create plain text artifact: %v", err)
	}

	report, err := store.BuildWorkspaceSegmentReport(ctx, sqlite.WorkspaceSegmentFilter{
		WorkspaceID: workspaceID,
		ArtifactRef: "artifact://workspace-segments-text-root-only",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build plain text artifact segment report: %v", err)
	}
	if len(report.Sources) != 1 || report.Sources[0].HasRichSegments {
		t.Fatalf("expected plain text artifact without headings to stay root-only, got %+v", report.Sources)
	}
	if len(report.Segments) != 1 || !report.Segments[0].IsRoot || report.Segments[0].SegmentKind != "root" {
		t.Fatalf("expected plain text artifact to keep only a root segment, got %+v", report.Segments)
	}
}

func requireNonRootWorkspaceSegment(t *testing.T, items []sqlite.WorkspaceSegmentRecord, sourceKind string) sqlite.WorkspaceSegmentRecord {
	t.Helper()
	for _, item := range items {
		if item.SourceKind == sourceKind && !item.IsRoot {
			return item
		}
	}
	t.Fatalf("non-root workspace segment for %s not found in %+v", sourceKind, items)
	return sqlite.WorkspaceSegmentRecord{}
}
