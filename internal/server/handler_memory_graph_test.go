package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryGraphListAndGet(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-memory-graph"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Graph",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Prefer canonical truth",
		Body:        "Canonical runtime truth stays above cached memory.",
		Summary:     "Prefer canonical truth.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryGraphListParams{
		WorkspaceID: workspaceID,
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryGraphList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphList rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	listAuthority, ok := payload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || listAuthority.WorkspaceID != workspaceID || listAuthority.ReferenceAt == "" {
		t.Fatalf("expected memory graph list time authority, got %+v", payload["time_authority"])
	}
	items, ok := payload["items"].([]sqlite.MemoryGraphNodeRecord)
	if !ok {
		t.Fatalf("unexpected items type %T", payload["items"])
	}
	if len(items) == 0 {
		t.Fatalf("expected memory graph nodes, got %+v", payload)
	}

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	raw, err = json.Marshal(workspaceMemoryGraphGetParams{
		WorkspaceID: workspaceID,
		MemoryID:    nodeID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	detailRaw, rpcErr := h.workspaceMemoryGraphGet(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphGet rpc error: %+v", rpcErr)
	}
	detail := detailRaw.(sqlite.MemoryGraphNodeDetail)
	if detail.TimeAuthority.WorkspaceID != workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected memory graph detail time authority, got %+v", detail.TimeAuthority)
	}
	if detail.Node.MemoryID != nodeID || detail.Node.OriginID != record.MemoryID {
		t.Fatalf("unexpected detail payload: %+v", detail)
	}
}

func TestWorkspaceMemoryGraphGetSurfacesAnchorStateFields(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-memory-graph-anchor"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Graph Anchor",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "procedure",
		Title:       "Guard deploy with doctor",
		Body:        "Procedural memory should surface anchor-state fields through RPC.",
		Summary:     "Guard deploy with doctor.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.7,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	nodeID := "memnode:workspace_memory:" + record.MemoryID
	if err := store.TouchMemoryNodeTrusted(ctx, workspaceID, nodeID, 0.2, sqlite.DefaultRMPSalienceConfig()); err != nil {
		t.Fatalf("trusted touch: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryGraphGetParams{
		WorkspaceID: workspaceID,
		MemoryID:    nodeID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	detailRaw, rpcErr := h.workspaceMemoryGraphGet(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphGet rpc error: %+v", rpcErr)
	}
	detail := detailRaw.(sqlite.MemoryGraphNodeDetail)
	if detail.Node.SemanticLineageID != "workspace_memory:"+record.MemoryID || !detail.Node.Protect || detail.Node.Revision < 1 {
		t.Fatalf("expected anchor-state identity/protect/revision fields, got %+v", detail.Node)
	}
	if detail.Node.LastAnyAccess == nil || detail.Node.LastTrustedAccess == nil || detail.Node.TLife <= 0 {
		t.Fatalf("expected salience-backed anchor-state access fields, got %+v", detail.Node)
	}
	if detail.Node.RetentionBand == "" || detail.Node.RetentionHotUntil == nil || detail.Node.RetentionWarmUntil == nil || detail.Node.RetentionExpiresAt == nil {
		t.Fatalf("expected retention fields through memory graph RPC, got %+v", detail.Node)
	}
}

func TestWorkspaceMemoryGraphGetSurfacesDriftReport(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-graph-drift"
		docKey      = "runbook-handler-drift"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Graph Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nInitial deployment procedure.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert initial workspace doc: %v", err)
	}
	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Prefer canonical runbook",
		Body:        "Canonical runbook truth stays above stale cache entries.",
		Summary:     "Prefer canonical runbook.",
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	nodeID := "memnode:workspace_memory:" + record.MemoryID

	raw, err := json.Marshal(workspaceMemoryGraphGetParams{
		WorkspaceID: workspaceID,
		MemoryID:    nodeID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	detailRaw, rpcErr := h.workspaceMemoryGraphGet(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphGet rpc error: %+v", rpcErr)
	}
	detail := detailRaw.(sqlite.MemoryGraphNodeDetail)
	if detail.TimeAuthority.WorkspaceID != workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected current drift detail time authority, got %+v", detail.TimeAuthority)
	}
	if detail.DriftReport == nil || detail.DriftReport.Status != "CURRENT" || detail.Node.Drift != 0 {
		t.Fatalf("expected current drift report before doc update, got node=%+v report=%+v", detail.Node, detail.DriftReport)
	}
	if detail.DriftReport.TimeAuthority.WorkspaceID != workspaceID || detail.DriftReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected current drift report time authority, got %+v", detail.DriftReport.TimeAuthority)
	}

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nUpdated deployment procedure with rollback.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert updated workspace doc: %v", err)
	}

	detailRaw, rpcErr = h.workspaceMemoryGraphGet(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphGet after update rpc error: %+v", rpcErr)
	}
	detail = detailRaw.(sqlite.MemoryGraphNodeDetail)
	if detail.DriftReport == nil || detail.DriftReport.Status != "STALE" {
		t.Fatalf("expected stale drift report after doc update, got %+v", detail.DriftReport)
	}
	if detail.DriftReport.TimeAuthority.WorkspaceID != workspaceID || detail.DriftReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected stale drift report time authority, got %+v", detail.DriftReport.TimeAuthority)
	}
	if detail.DriftReport.DriftedRefCount == 0 || detail.Node.Drift <= 0 {
		t.Fatalf("expected non-zero drift after doc update, got node=%+v report=%+v", detail.Node, detail.DriftReport)
	}
}

func TestWorkspaceMemoryGraphSyncBackfillsNodes(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-memory-graph-sync"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Graph Sync",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Keep dissent",
		Body:        "Dissent must survive compaction.",
		Summary:     "Keep dissent.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-dissent",
		ClaimType:   "lesson",
		Status:      "active",
		Subject:     "Dissent",
		Body:        "Dissent must survive compaction.",
		Summary:     "Preserve dissent.",
		Confidence:  0.7,
		SourceKind:  "manual",
		SourceID:    "developer",
		MemoryID:    record.MemoryID,
	}); err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_edges; DELETE FROM memory_node_versions; DELETE FROM memory_node_refs; DELETE FROM memory_nodes;`); err != nil {
		t.Fatalf("clear graph tables: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryGraphSyncParams{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("marshal sync params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryGraphSync(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphSync rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	if payload["status"] != "SYNCED" {
		t.Fatalf("unexpected sync payload: %+v", payload)
	}

	items, err := store.ListMemoryGraphNodes(ctx, sqlite.MemoryGraphNodeFilter{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("list graph nodes: %v", err)
	}
	if len(items) < 2 {
		t.Fatalf("expected synced nodes, got %+v", items)
	}
}

func TestWorkspaceMemoryGraphRepairRepairsMissingCompatibilityAnchor(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-memory-graph-repair"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Graph Repair",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Repair derived anchor explicitly",
		Body:        "Derived compatibility repair should remain explicit.",
		Summary:     "Repair derived anchor explicitly.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`,
		workspaceID,
		"memnode:workspace_memory:"+record.MemoryID,
	); err != nil {
		t.Fatalf("delete expected compatibility anchor: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryGraphRepairParams{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
	})
	if err != nil {
		t.Fatalf("marshal repair params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryGraphRepair(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphRepair rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	if payload["status"] != "REPAIRED" {
		t.Fatalf("unexpected repair payload: %+v", payload)
	}
	repair, ok := payload["repair"].(sqlite.WorkspaceMemoryProjectionRepairResult)
	if !ok {
		t.Fatalf("unexpected repair type %T", payload["repair"])
	}
	if repair.Repaired != 1 || repair.Examined != 1 {
		t.Fatalf("expected one repaired compatibility anchor, got %+v", repair)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, "memnode:workspace_memory:"+record.MemoryID)
	if err != nil {
		t.Fatalf("get repaired graph node: %v", err)
	}
	if detail.Node.OriginID != record.MemoryID || detail.Node.Summary != record.Summary {
		t.Fatalf("unexpected repaired graph node %+v", detail.Node)
	}
}

func TestWorkspaceMemoryGraphGetIncludesClaimRelationEdgesAndVersions(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-memory-graph-relations"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Graph Relations",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	for _, claimID := range []string{"claim-support", "claim-validate", "claim-contradict"} {
		if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
			WorkspaceID: workspaceID,
			ClaimID:     claimID,
			ClaimType:   "FACT",
			Subject:     claimID,
			Body:        "Body for " + claimID,
			SourceKind:  "manual",
			SourceID:    "developer",
		}); err != nil {
			t.Fatalf("record target claim %s: %v", claimID, err)
		}
	}

	sourceClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID:      workspaceID,
		ClaimID:          "claim-source",
		ClaimType:        "DECISION",
		Status:           "CONFIRMED",
		Subject:          "Use canonical replay",
		Body:             "Canonical replay remains the read authority.",
		Summary:          "Use canonical replay.",
		SourceKind:       "manual",
		SourceID:         "developer",
		ConflictsClaimID: "claim-contradict",
		Evidence: []string{
			"supports:claim-support",
			"validated_by:claim-validate",
		},
	})
	if err != nil {
		t.Fatalf("record source claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceMemoryGraphListParams{
		WorkspaceID: workspaceID,
		OriginKind:  "knowledge_claim",
		MemoryType:  "DECISION",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryGraphList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphList rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	items, ok := payload["items"].([]sqlite.MemoryGraphNodeRecord)
	if !ok {
		t.Fatalf("unexpected items type %T", payload["items"])
	}
	if len(items) != 1 || items[0].OriginID != sourceClaim.ClaimID {
		t.Fatalf("unexpected list payload %+v", payload)
	}
	if items[0].MemoryType != "DECISION_RECORD" || items[0].CompatType != "DECISION" {
		t.Fatalf("expected canonical decision record with compat decision, got %+v", items[0])
	}

	nodeID := "memnode:knowledge_claim:" + sourceClaim.ClaimID
	raw, err = json.Marshal(workspaceMemoryGraphGetParams{
		WorkspaceID: workspaceID,
		MemoryID:    nodeID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	detailRaw, rpcErr := h.workspaceMemoryGraphGet(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphGet rpc error: %+v", rpcErr)
	}
	detail := detailRaw.(sqlite.MemoryGraphNodeDetail)
	if detail.Node.MemoryID != nodeID || detail.Node.OriginID != sourceClaim.ClaimID {
		t.Fatalf("unexpected detail payload: %+v", detail)
	}
	if detail.Node.MemoryType != "DECISION_RECORD" || detail.Node.CompatType != "DECISION" {
		t.Fatalf("expected canonical decision record detail with compat decision, got %+v", detail.Node)
	}

	expectedEdges := map[string]string{
		"SUPPORTS":     "memnode:knowledge_claim:claim-support",
		"VALIDATED_BY": "memnode:knowledge_claim:claim-validate",
		"CONTRADICTS":  "memnode:knowledge_claim:claim-contradict",
	}
	foundEdges := make(map[string]string, len(detail.OutboundEdges))
	for _, edge := range detail.OutboundEdges {
		if _, ok := expectedEdges[edge.EdgeType]; ok {
			foundEdges[edge.EdgeType] = edge.ToMemoryID
		}
	}
	if len(foundEdges) != len(expectedEdges) {
		t.Fatalf("expected relation edges %+v, got %+v", expectedEdges, detail.OutboundEdges)
	}
	for edgeType, toID := range expectedEdges {
		if foundEdges[edgeType] != toID {
			t.Fatalf("missing %s edge to %s in %+v", edgeType, toID, detail.OutboundEdges)
		}
	}

	relationVersionCount := 0
	for _, version := range detail.Versions {
		if version.RefKind == "knowledge_claim_relation" {
			relationVersionCount++
		}
	}
	if relationVersionCount != 3 {
		t.Fatalf("expected 3 relation-backed versions, got %+v", detail.Versions)
	}
}

func TestWorkspaceMemoryGraphListRejectsInvalidEnumFilters(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	tests := []workspaceMemoryGraphListParams{
		{WorkspaceID: "ws-invalid-graph-filter", MemoryLayer: "NOT_A_LAYER"},
		{WorkspaceID: "ws-invalid-graph-filter", Visibility: "NOT_A_VISIBILITY"},
		{WorkspaceID: "ws-invalid-graph-filter", EpistemicStatus: "NOT_A_STATUS"},
		{WorkspaceID: "ws-invalid-graph-filter", LifecycleState: "NOT_A_LIFECYCLE"},
	}
	for _, tc := range tests {
		raw, err := json.Marshal(tc)
		if err != nil {
			t.Fatalf("marshal invalid graph params: %v", err)
		}
		if _, rpcErr := h.workspaceMemoryGraphList(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
			t.Fatalf("expected invalid params for %+v, got %+v", tc, rpcErr)
		}
	}
}

func TestWorkspaceMemoryGraphClassifiesValidationAndInternalErrors(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	rawMissing, err := json.Marshal(workspaceMemoryGraphGetParams{
		WorkspaceID: "ws-missing-graph-node",
		MemoryID:    "memnode:workspace_memory:missing",
	})
	if err != nil {
		t.Fatalf("marshal missing graph get params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryGraphGet(ctx, rawMissing); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for missing graph node, got %+v", rpcErr)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	rawList, err := json.Marshal(workspaceMemoryGraphListParams{
		WorkspaceID: "ws-memory-graph-internal",
	})
	if err != nil {
		t.Fatalf("marshal graph list params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryGraphList(ctx, rawList); rpcErr == nil || rpcErr.Code != errCodeInternal {
		t.Fatalf("expected internal error for closed graph store, got %+v", rpcErr)
	}
}

func TestWorkspaceMemoryGraphKnowledgeClaimProjectionLagIsMachineReadable(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-memory-graph-claim-projection-lag"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Graph Claim Projection Lag",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "decision",
		Status:      "active",
		Subject:     "Keep pending projection visible",
		Body:        "Graph RPC should surface canonical knowledge claim existence without pretending the derived node is current.",
		Summary:     "Pending graph projection stays machine-readable.",
		Confidence:  0.75,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	listRaw, err := json.Marshal(workspaceMemoryGraphListParams{
		WorkspaceID: workspaceID,
		OriginKind:  "knowledge_claim",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	listAny, rpcErr := h.workspaceMemoryGraphList(ctx, listRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphList rpc error: %+v", rpcErr)
	}
	listPayload := listAny.(map[string]any)
	items, ok := listPayload["items"].([]sqlite.MemoryGraphNodeRecord)
	if !ok {
		t.Fatalf("unexpected list items type %T", listPayload["items"])
	}
	if len(items) != 0 {
		t.Fatalf("expected no derived graph nodes before reconcile, got %+v", items)
	}
	boundary, ok := listPayload["boundary_contract"].(sqlite.MemoryShapeBoundaryContract)
	if !ok {
		t.Fatalf("expected typed list boundary contract, got %+v", listPayload["boundary_contract"])
	}
	if boundary.ProjectionCoverage != "PARTIAL" || boundary.ProjectionLagState != "degraded" || boundary.ProjectionPendingCount < 1 {
		t.Fatalf("expected partial degraded graph list boundary, got %+v", boundary)
	}

	nodeID := "memnode:knowledge_claim:" + claim.ClaimID
	getRaw, err := json.Marshal(workspaceMemoryGraphGetParams{
		WorkspaceID: workspaceID,
		MemoryID:    nodeID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryGraphGet(ctx, getRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params with projection details before reconcile, got %+v", rpcErr)
	} else {
		details, ok := rpcErr.Details.(map[string]any)
		if !ok {
			t.Fatalf("expected structured projection details, got %+v", rpcErr.Details)
		}
		if details["projection_missing_reason"] != "KNOWLEDGE_CLAIM_PROJECTION_PENDING" {
			t.Fatalf("expected pending projection reason, got %+v", details)
		}
		if details["canonical_authority"] != "knowledge_claim" || details["surface_authority"] != "compatibility_only" || details["compatibility_only"] != true {
			t.Fatalf("expected compatibility-only projection details, got %+v", details)
		}
		detailBoundary, ok := details["boundary_contract"].(sqlite.MemoryShapeBoundaryContract)
		if !ok {
			t.Fatalf("expected typed boundary contract in details, got %+v", details["boundary_contract"])
		}
		if detailBoundary.ProjectionCoverage != "PARTIAL" || detailBoundary.ProjectionLagState != "degraded" || detailBoundary.ProjectionPendingCount < 1 {
			t.Fatalf("expected partial degraded detail boundary, got %+v", detailBoundary)
		}
	}

	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	detailAny, rpcErr := h.workspaceMemoryGraphGet(ctx, getRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphGet after reconcile rpc error: %+v", rpcErr)
	}
	detail := detailAny.(sqlite.MemoryGraphNodeDetail)
	if detail.Node.MemoryID != nodeID || detail.Node.OriginID != claim.ClaimID {
		t.Fatalf("expected reconciled knowledge claim node, got %+v", detail.Node)
	}
	if detail.BoundaryContract.ProjectionCoverage != "CURRENT" || detail.BoundaryContract.ProjectionLagState != "ok" {
		t.Fatalf("expected current boundary after reconcile, got %+v", detail.BoundaryContract)
	}
}
