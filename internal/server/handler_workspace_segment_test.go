package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceSegmentRPCSurface(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-workspace-segment-rpc"
	createServerTaskClassWorkspace(t, ctx, store, workspaceID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook",
		Content:     "# Incident\nDeploy failed.\n\n## Fix Plan\nPatch and verify.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert rpc workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:   "artifact-rpc-text",
		WorkspaceID:  workspaceID,
		Title:        "Text Artifact",
		ArtifactRef:  "artifact://workspace-segment-rpc-text",
		Kind:         "document",
		ContentType:  "text/markdown",
		CreatedBy:    "agent-a",
		MetadataJSON: serverJSONStringForSegments(t, map[string]any{"content": "# Timeline\n\nDeploy blocked.\n\n## Fix\nConfirm operator approval."}),
	}); err != nil {
		t.Fatalf("create rpc text artifact: %v", err)
	}

	rawDoc, err := json.Marshal(workspaceSegmentListParams{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal workspace.segment.list doc params: %v", err)
	}
	result, rpcErr := h.workspaceSegmentList(ctx, rawDoc)
	if rpcErr != nil {
		t.Fatalf("workspaceSegmentList doc rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspace.segment.list doc result type %T", result)
	}
	docReport, ok := payload["report"].(sqlite.WorkspaceSegmentReport)
	if !ok {
		t.Fatalf("unexpected workspace.segment.list doc report type %T", payload["report"])
	}
	if docReport.TimeAuthority.WorkspaceID != workspaceID || docReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected workspace.segment.list doc report to expose workspace time authority, got %+v", docReport.TimeAuthority)
	}
	if docReport.GeneratedAt != docReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected workspace.segment.list doc report generated_at to mirror reference_at, got generated_at=%q reference_at=%q", docReport.GeneratedAt, docReport.TimeAuthority.ReferenceAt)
	}
	docSegment := requireServerNonRootWorkspaceSegment(t, docReport.Segments, "workspace_doc")
	if docSegment.GeneratedAt != docReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected workspace.segment.list doc segment generated_at to mirror reference_at, got %+v", docSegment)
	}

	rawArtifact, err := json.Marshal(workspaceSegmentListParams{
		WorkspaceID: workspaceID,
		ArtifactRef: "artifact://workspace-segment-rpc-text",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal workspace.segment.list artifact params: %v", err)
	}
	result, rpcErr = h.workspaceSegmentList(ctx, rawArtifact)
	if rpcErr != nil {
		t.Fatalf("workspaceSegmentList artifact rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspace.segment.list artifact result type %T", result)
	}
	artifactReport, ok := payload["report"].(sqlite.WorkspaceSegmentReport)
	if !ok {
		t.Fatalf("unexpected workspace.segment.list artifact report type %T", payload["report"])
	}
	if len(artifactReport.Segments) < 2 {
		t.Fatalf("expected artifact list rpc to expose root + concrete segments, got %+v", artifactReport.Segments)
	}
	if artifactReport.GeneratedAt != artifactReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected workspace.segment.list artifact report generated_at to mirror reference_at, got generated_at=%q reference_at=%q", artifactReport.GeneratedAt, artifactReport.TimeAuthority.ReferenceAt)
	}
	artifactSegment := requireServerNonRootWorkspaceSegment(t, artifactReport.Segments, "workspace_artifact")
	if artifactSegment.GeneratedAt != artifactReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected workspace.segment.list artifact segment generated_at to mirror reference_at, got %+v", artifactSegment)
	}

	rawGet, err := json.Marshal(workspaceSegmentGetParams{
		WorkspaceID: workspaceID,
		SegmentRef:  docSegment.SegmentRef,
	})
	if err != nil {
		t.Fatalf("marshal workspace.segment.get params: %v", err)
	}
	result, rpcErr = h.workspaceSegmentGet(ctx, rawGet)
	if rpcErr != nil {
		t.Fatalf("workspaceSegmentGet rpc error: %+v", rpcErr)
	}
	getPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspace.segment.get result type %T", result)
	}
	authority, ok := getPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok {
		t.Fatalf("unexpected workspace.segment.get time_authority type %T", getPayload["time_authority"])
	}
	if authority.WorkspaceID != workspaceID || authority.ReferenceAt == "" {
		t.Fatalf("expected workspace.segment.get to expose workspace time authority, got %+v", authority)
	}
	segment, ok := getPayload["segment"].(sqlite.WorkspaceSegmentRecord)
	if !ok {
		t.Fatalf("unexpected workspace.segment.get payload type %T", getPayload["segment"])
	}
	if segment.SegmentRef != docSegment.SegmentRef || segment.SourceRef != "runbook" {
		t.Fatalf("expected workspace.segment.get to return headed doc segment, got %+v", segment)
	}
	if segment.GeneratedAt != authority.ReferenceAt {
		t.Fatalf("expected workspace.segment.get segment generated_at to mirror reference_at, got %+v", segment)
	}
	if artifactSegment.SegmentRef == "" {
		t.Fatalf("expected non-root artifact segment from rpc surface, got %+v", artifactReport.Segments)
	}
}

func TestWorkspaceSegmentRPCRejectsInvalidParams(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	tests := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params any
	}{
		{name: "list-missing-workspace", call: h.workspaceSegmentList, params: workspaceSegmentListParams{DocKey: "runbook"}},
		{name: "list-mutually-exclusive", call: h.workspaceSegmentList, params: workspaceSegmentListParams{WorkspaceID: "ws-test", DocKey: "runbook", ArtifactRef: "artifact://both"}},
		{name: "list-segment-ref-with-doc", call: h.workspaceSegmentList, params: workspaceSegmentListParams{WorkspaceID: "ws-test", SegmentRef: "segment:ws-test:doc:runbook#root", DocKey: "runbook"}},
		{name: "list-segment-ref-with-artifact", call: h.workspaceSegmentList, params: workspaceSegmentListParams{WorkspaceID: "ws-test", SegmentRef: "segment:ws-test:artifact:artifact://note#root", ArtifactRef: "artifact://note"}},
		{name: "get-missing-segment-ref", call: h.workspaceSegmentGet, params: workspaceSegmentGetParams{WorkspaceID: "ws-test"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			if _, rpcErr := tc.call(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params error, got %+v", rpcErr)
			}
		})
	}
}

func requireServerNonRootWorkspaceSegment(t *testing.T, items []sqlite.WorkspaceSegmentRecord, sourceKind string) sqlite.WorkspaceSegmentRecord {
	t.Helper()
	for _, item := range items {
		if item.SourceKind == sourceKind && !item.IsRoot {
			return item
		}
	}
	t.Fatalf("non-root workspace segment for %s not found in %+v", sourceKind, items)
	return sqlite.WorkspaceSegmentRecord{}
}

func serverJSONStringForSegments(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal segment metadata: %v", err)
	}
	return string(raw)
}
