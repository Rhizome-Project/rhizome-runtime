package sqlite

import (
	"context"
	"testing"
)

func TestMemoryGraphListPreservesEffectiveDriftWhenNoComparableRefsExist(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-graph-unresolved-drift"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Unresolved Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	nodeID := memoryGraphNodeID("proto_cluster", "cluster-unresolved")
	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := store.upsertMemoryGraphNodeTx(ctx, tx, MemoryGraphNodeInput{
		MemoryID:        nodeID,
		WorkspaceID:     workspaceID,
		MemoryType:      "CLUSTER_BRIEF",
		Visibility:      "CLUSTER",
		MemoryLayer:     "SEMANTIC",
		EpistemicStatus: "SUPPORTED",
		LifecycleState:  "ACTIVE",
		OriginKind:      "proto_cluster",
		OriginID:        "cluster-unresolved",
		SourceKind:      "test",
		SourceID:        "cluster-unresolved",
		Title:           "Unresolved cluster",
		Summary:         "Existing drift should survive unresolved-only grounding.",
		Drift:           0.37,
		CreatedAt:       "2026-03-23T12:00:00Z",
		UpdatedAt:       "2026-03-23T12:00:00Z",
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert unresolved cluster node: %v", err)
	}
	if err := store.replaceMemoryGraphNodeVersionsTx(ctx, tx, nodeID, workspaceID, []MemoryGraphNodeVersionInput{
		{MemoryID: nodeID, WorkspaceID: workspaceID, RefKind: "proto_cluster", RefID: "cluster-unresolved", VersionToken: "epoch-1", Weight: 0.9},
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("replace unresolved versions: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	items, err := store.ListMemoryGraphNodes(ctx, MemoryGraphNodeFilter{
		WorkspaceID: workspaceID,
		MemoryType:  "CLUSTER_BRIEF",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory graph nodes: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one unresolved cluster node, got %+v", items)
	}
	if items[0].Drift != 0.37 {
		t.Fatalf("expected effective drift to preserve existing 0.37, got %+v", items[0])
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get unresolved cluster node: %v", err)
	}
	report, err := store.buildMemoryGraphDriftReport(ctx, workspaceID, detail.Versions)
	if err != nil {
		t.Fatalf("build drift report: %v", err)
	}
	if report.Status != "UNRESOLVED" || report.Drift != 0 || report.ComparedRefCount != 0 || report.UnresolvedRefCount != 1 {
		t.Fatalf("unexpected unresolved drift report %+v", report)
	}
}

func TestMemoryGraphManualGroundingKeepsAboutSegmentAndBelongsToClusterEdges(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-graph-grounding"
		docKey      = "grounding-runbook"
		clusterID   = "cluster-grounding-a"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Grounding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Grounding Runbook",
		Content:     "# Incident\nDeploy stalled.\n\n## Recovery Plan\nReset the canary and verify replay.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}

	report, err := store.BuildWorkspaceSegmentReport(ctx, WorkspaceSegmentFilter{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build workspace segment report: %v", err)
	}
	segment := requireGroundingNonRootSegment(t, report.Segments, "workspace_doc")
	doc, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc: %v", err)
	}

	sourceNodeID := memoryGraphNodeID("knowledge_claim", "claim-grounded")
	segmentNodeID := memoryGraphNodeID("workspace_segment", segment.SegmentRef)
	clusterNodeID := memoryGraphNodeID("proto_cluster", clusterID)

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := store.upsertMemoryGraphNodeTx(ctx, tx, MemoryGraphNodeInput{
		MemoryID:        sourceNodeID,
		WorkspaceID:     workspaceID,
		MemoryType:      "DECISION",
		Visibility:      "WORKSPACE",
		MemoryLayer:     "SEMANTIC",
		EpistemicStatus: "SUPPORTED",
		LifecycleState:  "ACTIVE",
		OriginKind:      "knowledge_claim",
		OriginID:        "claim-grounded",
		SourceKind:      "test",
		SourceID:        "claim-grounded",
		Title:           "Grounded claim",
		Summary:         "Grounded against one concrete segment and one cluster.",
		CreatedAt:       doc.UpdatedAt,
		UpdatedAt:       doc.UpdatedAt,
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert source node: %v", err)
	}
	if _, err := store.upsertMemoryGraphNodeTx(ctx, tx, MemoryGraphNodeInput{
		MemoryID:        segmentNodeID,
		WorkspaceID:     workspaceID,
		MemoryType:      "BRIDGE_NOTE",
		CompatType:      "SEGMENT_ANCHOR",
		Visibility:      "WORKSPACE",
		MemoryLayer:     "SEMANTIC",
		EpistemicStatus: "SUPPORTED",
		LifecycleState:  "ACTIVE",
		OriginKind:      "workspace_segment",
		OriginID:        segment.SegmentRef,
		SourceKind:      segment.SourceKind,
		SourceID:        segment.SourceRef,
		Title:           segment.Title,
		Summary:         segment.Summary,
		Drift:           0.18,
		CreatedAt:       doc.UpdatedAt,
		UpdatedAt:       doc.UpdatedAt,
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert segment node: %v", err)
	}
	if err := store.replaceMemoryGraphNodeRefsTx(ctx, tx, segmentNodeID, workspaceID, []MemoryGraphNodeRefInput{
		{MemoryID: segmentNodeID, WorkspaceID: workspaceID, RefKind: "workspace_doc", RefID: docKey, RefRole: "source", Weight: 1, MetadataJSON: "{}"},
		{MemoryID: segmentNodeID, WorkspaceID: workspaceID, RefKind: "segment_ref", RefID: segment.SegmentRef, RefRole: "anchor", Weight: 1, MetadataJSON: "{}"},
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("replace segment refs: %v", err)
	}
	if err := store.replaceMemoryGraphNodeVersionsTx(ctx, tx, segmentNodeID, workspaceID, []MemoryGraphNodeVersionInput{
		{MemoryID: segmentNodeID, WorkspaceID: workspaceID, RefKind: "workspace_doc", RefID: docKey, VersionToken: doc.SHA, Weight: 1},
		{MemoryID: segmentNodeID, WorkspaceID: workspaceID, RefKind: "workspace_segment", RefID: segment.SegmentRef, VersionToken: doc.UpdatedAt, Weight: 0.9},
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("replace segment versions: %v", err)
	}
	if _, err := store.upsertMemoryGraphNodeTx(ctx, tx, MemoryGraphNodeInput{
		MemoryID:        clusterNodeID,
		WorkspaceID:     workspaceID,
		MemoryType:      "CLUSTER_BRIEF",
		Visibility:      "CLUSTER",
		MemoryLayer:     "SEMANTIC",
		EpistemicStatus: "SUPPORTED",
		LifecycleState:  "DORMANT",
		OriginKind:      "proto_cluster",
		OriginID:        clusterID,
		SourceKind:      "test",
		SourceID:        clusterID,
		Title:           "Grounding cluster",
		Summary:         "Cluster-level locality anchor for grounded evidence.",
		Drift:           0.42,
		CreatedAt:       doc.UpdatedAt,
		UpdatedAt:       doc.UpdatedAt,
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert cluster node: %v", err)
	}
	if err := store.replaceMemoryGraphNodeVersionsTx(ctx, tx, clusterNodeID, workspaceID, []MemoryGraphNodeVersionInput{
		{MemoryID: clusterNodeID, WorkspaceID: workspaceID, RefKind: "proto_cluster", RefID: clusterID, VersionToken: "epoch-1", Weight: 1},
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("replace cluster versions: %v", err)
	}
	if err := store.replaceMemoryGraphEdgesForSourceTx(ctx, tx, workspaceID, "artifact_grounding", "claim-grounded", []MemoryGraphEdgeInput{
		{
			WorkspaceID:  workspaceID,
			FromMemoryID: sourceNodeID,
			ToMemoryID:   segmentNodeID,
			EdgeType:     "ABOUT_SEGMENT",
			SourceKind:   "artifact_grounding",
			SourceID:     "claim-grounded",
			Weight:       1,
			MetadataJSON: `{"segment_ref":"` + segment.SegmentRef + `"}`,
		},
		{
			WorkspaceID:  workspaceID,
			FromMemoryID: sourceNodeID,
			ToMemoryID:   clusterNodeID,
			EdgeType:     "BELONGS_TO_CLUSTER",
			SourceKind:   "artifact_grounding",
			SourceID:     "claim-grounded",
			Weight:       0.85,
			MetadataJSON: `{"proto_cluster_id":"` + clusterID + `"}`,
		},
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("replace grounding edges: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, sourceNodeID)
	if err != nil {
		t.Fatalf("get grounded node: %v", err)
	}
	if !hasGroundingEdge(detail.OutboundEdges, "ABOUT_SEGMENT", segmentNodeID) {
		t.Fatalf("expected ABOUT_SEGMENT edge, got %+v", detail.OutboundEdges)
	}
	if !hasGroundingEdge(detail.OutboundEdges, "BELONGS_TO_CLUSTER", clusterNodeID) {
		t.Fatalf("expected BELONGS_TO_CLUSTER edge, got %+v", detail.OutboundEdges)
	}

	segmentDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, segmentNodeID)
	if err != nil {
		t.Fatalf("get segment anchor node: %v", err)
	}
	if segmentDetail.Node.OriginKind != "workspace_segment" || segmentDetail.Node.OriginID != segment.SegmentRef {
		t.Fatalf("unexpected segment node %+v", segmentDetail.Node)
	}
	if !hasGroundingVersion(segmentDetail.Versions, "workspace_doc", docKey, doc.SHA) {
		t.Fatalf("expected workspace_doc sha version on segment node, got %+v", segmentDetail.Versions)
	}

	clusterDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, clusterNodeID)
	if err != nil {
		t.Fatalf("get cluster node: %v", err)
	}
	if clusterDetail.Node.MemoryType != "CLUSTER_BRIEF" || clusterDetail.Node.Visibility != "CLUSTER" || clusterDetail.Node.LifecycleState != "DORMANT" {
		t.Fatalf("unexpected cluster node %+v", clusterDetail.Node)
	}
	if clusterDetail.Node.Drift != 0.42 {
		t.Fatalf("expected persisted drift 0.42, got %+v", clusterDetail.Node)
	}
}

func TestMemoryGraphWorkspaceMemoryDocGroundingDetectsDocDriftAfterDocUpdate(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-graph-doc-drift"
		docKey      = "runbook-doc-drift"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Doc Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Incident\nDeploy blocked.\n\nMitigate carefully.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	initialDoc, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get initial workspace doc: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Grounded lesson",
		Body:        "This lesson is grounded in the operator runbook.",
		Summary:     "Grounded lesson.",
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.6,
		Confidence:  0.7,
	})
	if err != nil {
		t.Fatalf("record grounded workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	rootSegmentRef := buildWorkspaceDocSegmentRef(workspaceID, docKey, "root")
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get grounded workspace memory node: %v", err)
	}
	if !hasGroundingVersion(detail.Versions, "workspace_doc", docKey, initialDoc.SHA) {
		t.Fatalf("expected workspace_doc grounding version, got %+v", detail.Versions)
	}
	if !hasGroundingRef(detail.Refs, "segment_ref", rootSegmentRef) {
		t.Fatalf("expected coarse root segment ref, got %+v", detail.Refs)
	}

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Incident\nDeploy blocked.\n\nMitigate carefully.\n\nUpdate: rotate the canary.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("update workspace doc: %v", err)
	}
	updatedDoc, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get updated workspace doc: %v", err)
	}
	if updatedDoc.SHA == initialDoc.SHA {
		t.Fatalf("expected updated doc sha to change, got initial=%s updated=%s", initialDoc.SHA, updatedDoc.SHA)
	}

	detail, err = store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get drifted workspace memory node: %v", err)
	}
	report, err := store.buildMemoryGraphDriftReport(ctx, workspaceID, detail.Versions)
	if err != nil {
		t.Fatalf("build workspace memory drift report: %v", err)
	}
	if report.Status != "STALE" || report.ComparedRefCount != 2 || report.DriftedRefCount != 1 || report.Drift != 1 {
		t.Fatalf("unexpected workspace memory drift report %+v", report)
	}
	if item := requireGroundingVersionStatus(t, report, "workspace_doc", docKey); item.State != "STALE" || item.CurrentVersionToken != updatedDoc.SHA {
		t.Fatalf("unexpected workspace_doc drift status %+v", item)
	}
	if hasGroundingVersionStatus(report, "segment_ref", rootSegmentRef) {
		t.Fatalf("did not expect coarse root segment version status in drift report, got %+v", report.Items)
	}

	items, err := store.ListMemoryGraphNodes(ctx, MemoryGraphNodeFilter{
		WorkspaceID: workspaceID,
		OriginKind:  "workspace_memory",
		OriginID:    record.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace memory nodes after doc drift: %v", err)
	}
	if len(items) != 1 || items[0].Drift != 1 {
		t.Fatalf("expected effective drift 1 after doc update, got %+v", items)
	}
}

func TestMemoryGraphKnowledgeClaimRelationGroundingDetectsTargetClaimDrift(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-graph-relation-grounding"
		docKey      = "runbook-relation-grounding"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Relation Grounding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Policy\nUse replay as the canonical explanation path.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	doc, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc: %v", err)
	}

	targetClaim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-target",
		ClaimType:   "FACT",
		Subject:     "Canonical replay",
		Body:        "Replay remains canonical.",
		Summary:     "Replay remains canonical.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record target claim: %v", err)
	}
	sourceClaim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-source",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Grounded replay policy",
		Body:        "This decision is grounded in the runbook and supports the target claim.",
		Summary:     "Grounded replay policy.",
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Evidence:    []string{"supports:" + targetClaim.ClaimID},
	})
	if err != nil {
		t.Fatalf("record source claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	relations, err := store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID: workspaceID,
		FromClaimID: sourceClaim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list claim relations: %v", err)
	}
	if len(relations) != 1 {
		t.Fatalf("expected one relation row, got %+v", relations)
	}
	relation := relations[0]

	nodeID := memoryGraphNodeID("knowledge_claim", sourceClaim.ClaimID)
	rootSegmentRef := buildWorkspaceDocSegmentRef(workspaceID, docKey, "root")
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get grounded claim node: %v", err)
	}
	if !hasGroundingEdge(detail.OutboundEdges, "SUPPORTS", memoryGraphNodeID("knowledge_claim", targetClaim.ClaimID)) {
		t.Fatalf("expected SUPPORTS edge, got %+v", detail.OutboundEdges)
	}
	if !hasGroundingVersion(detail.Versions, "knowledge_claim_relation", relation.RelationID, relation.UpdatedAt) {
		t.Fatalf("expected relation-backed version, got %+v", detail.Versions)
	}
	if !hasGroundingVersion(detail.Versions, "knowledge_claim", targetClaim.ClaimID, targetClaim.UpdatedAt) {
		t.Fatalf("expected grounded target-claim version, got %+v", detail.Versions)
	}
	if !hasGroundingVersion(detail.Versions, "workspace_doc", docKey, doc.SHA) {
		t.Fatalf("expected grounded doc version, got %+v", detail.Versions)
	}
	if !hasGroundingRef(detail.Refs, "segment_ref", rootSegmentRef) {
		t.Fatalf("expected grounded coarse segment ref, got %+v", detail.Refs)
	}

	updatedTarget, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     targetClaim.ClaimID,
		ClaimType:   "FACT",
		Subject:     "Canonical replay",
		Body:        "Replay remains canonical after the operator clarification.",
		Summary:     "Replay remains canonical after clarification.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("update target claim: %v", err)
	}
	if updatedTarget.UpdatedAt == targetClaim.UpdatedAt {
		t.Fatalf("expected target claim updated_at to change, got before=%s after=%s", targetClaim.UpdatedAt, updatedTarget.UpdatedAt)
	}

	detail, err = store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get drifted claim node: %v", err)
	}
	report, err := store.buildMemoryGraphDriftReport(ctx, workspaceID, detail.Versions)
	if err != nil {
		t.Fatalf("build relation-grounded drift report: %v", err)
	}
	if report.Status != "STALE" || report.Drift != 0.9 {
		t.Fatalf("expected stale relation-grounded drift 0.9, got %+v", report)
	}
	if item := requireGroundingVersionStatus(t, report, "knowledge_claim", targetClaim.ClaimID); item.State != "STALE" || item.CurrentVersionToken != updatedTarget.UpdatedAt {
		t.Fatalf("unexpected grounded target-claim status %+v", item)
	}
	if item := requireGroundingVersionStatus(t, report, "knowledge_claim_relation", relation.RelationID); item.State != "CURRENT" {
		t.Fatalf("expected relation row itself to stay current, got %+v", item)
	}
}

func TestMemoryGraphEpisodePackGroundingAddsDocArtifactVersionsAndEdges(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-graph-episode-pack-grounding"
		agentID     = "agent-episode-pack-grounding"
		sessionID   = "sess-episode-pack-grounding"
		docKey      = "episode-pack-runbook"
		artifactRef = "artifact://episode-pack-grounding"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Episode Pack Grounding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Episode Pack Grounding Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		StartedAt:   "2026-03-23T12:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Timeline\nGround the compaction pack against this doc.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	doc, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc: %v", err)
	}
	artifact, err := store.RecordWorkspaceArtifact(ctx, WorkspaceArtifactInput{
		ArtifactID:   "artifact-episode-pack-grounding",
		WorkspaceID:  workspaceID,
		Title:        "Grounding Artifact",
		ArtifactRef:  artifactRef,
		Kind:         "document",
		ContentType:  "text/markdown",
		CreatedBy:    agentID,
		MetadataJSON: `{"content":"# Notes\nGround the episode pack against this artifact."}`,
	})
	if err != nil {
		t.Fatalf("record workspace artifact: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	pack, err := store.upsertEpisodePackTx(ctx, tx, EpisodePackRecord{
		PackID:              "pack-grounding",
		PackKey:             "episode-pack-grounding-key",
		WorkspaceID:         workspaceID,
		PackType:            episodePackTypeCompaction,
		PackMode:            episodePackModeFallback,
		SchemaVersion:       episodePackSchemaVersion,
		SessionID:           sessionID,
		LineageSessionID:    sessionID,
		AgentID:             agentID,
		TriggerKind:         "token_budget_exceeded",
		SourceWindowStart:   0,
		SourceWindowEnd:     2,
		SourceWindowDigest:  "digest-grounding",
		SummaryText:         "Fallback pack summary.",
		SummaryDigest:       "summary-digest-grounding",
		NarrativeSummary:    "Grounded episode pack.",
		DissentState:        episodePackDissentNone,
		ProvenanceRefs:      []string{"workspace_doc:" + docKey, "artifact_ref:" + artifactRef},
		MessageCountBefore:  4,
		MessageCountAfter:   2,
		MessageTokensBefore: 100,
		MessageTokensAfter:  50,
		TotalInputTokens:    120,
		TotalOutputTokens:   60,
		CreatedAt:           "2026-03-23T12:00:00Z",
		UpdatedAt:           "2026-03-23T12:00:00Z",
	})
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert grounded episode pack: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit episode pack tx: %v", err)
	}
	if _, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 10); err != nil {
		t.Fatalf("reconcile episode pack projections: %v", err)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, pack.CanonicalMemoryID)
	if err != nil {
		t.Fatalf("get grounded episode pack node: %v", err)
	}
	docRootRef := buildWorkspaceDocSegmentRef(workspaceID, docKey, "root")
	artifactRootRef := buildWorkspaceArtifactSegmentRef(workspaceID, artifactRef, "root")
	if !hasGroundingVersion(detail.Versions, "workspace_doc", docKey, doc.SHA) {
		t.Fatalf("expected grounded doc version, got %+v", detail.Versions)
	}
	if !hasGroundingRef(detail.Refs, "segment_ref", docRootRef) {
		t.Fatalf("expected grounded doc segment ref, got %+v", detail.Refs)
	}
	if !hasGroundingVersion(detail.Versions, "artifact_ref", artifactRef, workspaceArtifactVersionToken(artifact)) {
		t.Fatalf("expected grounded artifact version, got %+v", detail.Versions)
	}
	if !hasGroundingRef(detail.Refs, "segment_ref", artifactRootRef) {
		t.Fatalf("expected grounded artifact segment ref, got %+v", detail.Refs)
	}
}

func TestMemoryGraphListFiltersManualClusterGroundingStatus(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-graph-grounding-filter"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Grounding Filter",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	for _, tc := range []struct {
		clusterID  string
		lifecycle  string
		visibility string
		drift      float64
	}{
		{clusterID: "cluster-active", lifecycle: "ACTIVE", visibility: "WORKSPACE", drift: 0.12},
		{clusterID: "cluster-dormant", lifecycle: "DORMANT", visibility: "CLUSTER", drift: 0.57},
	} {
		if _, err := store.upsertMemoryGraphNodeTx(ctx, tx, MemoryGraphNodeInput{
			MemoryID:        memoryGraphNodeID("proto_cluster", tc.clusterID),
			WorkspaceID:     workspaceID,
			MemoryType:      "CLUSTER_BRIEF",
			Visibility:      tc.visibility,
			MemoryLayer:     "SEMANTIC",
			EpistemicStatus: "SUPPORTED",
			LifecycleState:  tc.lifecycle,
			OriginKind:      "proto_cluster",
			OriginID:        tc.clusterID,
			SourceKind:      "test",
			SourceID:        tc.clusterID,
			Title:           tc.clusterID,
			Summary:         tc.clusterID,
			Drift:           tc.drift,
			CreatedAt:       "2026-03-23T12:00:00Z",
			UpdatedAt:       "2026-03-23T12:00:00Z",
		}); err != nil {
			_ = tx.Rollback()
			t.Fatalf("upsert cluster %s: %v", tc.clusterID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	items, err := store.ListMemoryGraphNodes(ctx, MemoryGraphNodeFilter{
		WorkspaceID:    workspaceID,
		MemoryType:     "CLUSTER_BRIEF",
		Visibility:     "cluster",
		LifecycleState: "dormant",
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("list filtered cluster nodes: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected exactly one filtered cluster node, got %+v", items)
	}
	if items[0].OriginID != "cluster-dormant" || items[0].Drift != 0.57 {
		t.Fatalf("unexpected filtered cluster node %+v", items[0])
	}
}

func TestMemoryGraphWorkspaceDocGroundingReportsDriftAfterDocUpdate(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-graph-doc-drift"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nInitial deployment procedure.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert initial workspace doc: %v", err)
	}
	initialDoc, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get initial workspace doc: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Follow the runbook",
		Body:        "The operator should follow the workspace runbook.",
		Summary:     "Follow the runbook.",
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.8,
		Confidence:  0.85,
	})
	if err != nil {
		t.Fatalf("record grounded workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get grounded memory node: %v", err)
	}
	rootSegmentRef := buildWorkspaceDocSegmentRef(workspaceID, docKey, "root")
	if !hasGroundingVersion(detail.Versions, "workspace_doc", docKey, initialDoc.SHA) {
		t.Fatalf("expected workspace_doc grounding version, got %+v", detail.Versions)
	}
	if !hasGroundingRef(detail.Refs, "segment_ref", rootSegmentRef) {
		t.Fatalf("expected coarse root segment ref, got %+v", detail.Refs)
	}
	if detail.Node.Drift != 0 || detail.DriftReport == nil || detail.DriftReport.Status != "CURRENT" {
		t.Fatalf("expected current drift report, got node=%+v report=%+v", detail.Node, detail.DriftReport)
	}

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nUpdated deployment procedure with rollback.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert updated workspace doc: %v", err)
	}
	updatedDoc, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get updated workspace doc: %v", err)
	}
	if updatedDoc.SHA == initialDoc.SHA {
		t.Fatalf("expected updated workspace doc SHA to change")
	}

	detail, err = store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get stale grounded memory node: %v", err)
	}
	if detail.DriftReport == nil || detail.DriftReport.Status != "STALE" {
		t.Fatalf("expected stale drift report, got %+v", detail.DriftReport)
	}
	if detail.DriftReport.DriftedRefCount == 0 || detail.Node.Drift <= 0 {
		t.Fatalf("expected non-zero drift after doc update, got node=%+v report=%+v", detail.Node, detail.DriftReport)
	}

	items, err := store.ListMemoryGraphNodes(ctx, MemoryGraphNodeFilter{
		WorkspaceID: workspaceID,
		MemoryType:  "DECISION",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory graph nodes after doc update: %v", err)
	}
	found := false
	for _, item := range items {
		if item.MemoryID == nodeID && item.Drift > 0 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected listed workspace memory node to surface non-zero drift, got %+v", items)
	}
}

func TestMemoryGraphClaimGroundingAvoidsSyntheticProtoClusterAuthority(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-graph-no-cluster-authority"
		docKey      = "incident-brief"
		agentID     = "agent-grounding"
		sessionID   = "sess-grounding"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph No Cluster Authority",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Grounding Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		StartedAt:   "2026-03-23T12:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Incident Brief",
		Content:     "# Incident\nPrimary evidence for the claim.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}

	backing, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "fact",
		Title:       "Artifact-backed memory",
		Body:        "Grounded in the incident brief.",
		Summary:     "Grounded in incident brief.",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.7,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record backing workspace memory: %v", err)
	}

	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-grounded-no-cluster",
		ClaimType:   "fact",
		Status:      "confirmed",
		Subject:     "Incident brief stays the evidence source",
		Body:        "This claim should inherit artifact grounding without synthetic cluster authority.",
		Summary:     "No synthetic cluster authority.",
		Confidence:  0.9,
		SourceKind:  "manual",
		SourceID:    "developer",
		MemoryID:    backing.MemoryID,
		SessionID:   sessionID,
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("knowledge_claim", claim.ClaimID))
	if err != nil {
		t.Fatalf("get knowledge claim node: %v", err)
	}
	if !hasGroundingVersion(detail.Versions, "workspace_doc", docKey, mustWorkspaceDocSHA(t, store, ctx, workspaceID, docKey)) {
		t.Fatalf("expected inherited workspace doc grounding version, got %+v", detail.Versions)
	}
	if hasRefKind(detail.Refs, "proto_cluster") {
		t.Fatalf("expected no synthetic proto_cluster refs, got %+v", detail.Refs)
	}
	if hasEdgeType(detail.OutboundEdges, "BELONGS_TO_CLUSTER") {
		t.Fatalf("expected no synthetic BELONGS_TO_CLUSTER edge, got %+v", detail.OutboundEdges)
	}
}

func requireGroundingNonRootSegment(t *testing.T, items []WorkspaceSegmentRecord, sourceKind string) WorkspaceSegmentRecord {
	t.Helper()
	for _, item := range items {
		if item.SourceKind == sourceKind && !item.IsRoot {
			return item
		}
	}
	t.Fatalf("non-root workspace segment for %s not found in %+v", sourceKind, items)
	return WorkspaceSegmentRecord{}
}

func hasGroundingEdge(edges []MemoryGraphEdgeRecord, edgeType, toMemoryID string) bool {
	for _, edge := range edges {
		if edge.EdgeType == edgeType && edge.ToMemoryID == toMemoryID {
			return true
		}
	}
	return false
}

func hasGroundingVersion(versions []MemoryGraphNodeVersionRecord, refKind, refID, versionToken string) bool {
	for _, version := range versions {
		if version.RefKind == refKind && version.RefID == refID && version.VersionToken == versionToken {
			return true
		}
	}
	return false
}

func requireGroundingVersionStatus(t *testing.T, report MemoryGraphDriftReport, refKind, refID string) MemoryGraphVersionStatus {
	t.Helper()
	for _, item := range report.Items {
		if item.RefKind == refKind && item.RefID == refID {
			return item
		}
	}
	t.Fatalf("version status for %s/%s not found in %+v", refKind, refID, report.Items)
	return MemoryGraphVersionStatus{}
}

func hasGroundingVersionStatus(report MemoryGraphDriftReport, refKind, refID string) bool {
	for _, item := range report.Items {
		if item.RefKind == refKind && item.RefID == refID {
			return true
		}
	}
	return false
}

func hasGroundingRef(refs []MemoryGraphNodeRefRecord, refKind, refID string) bool {
	for _, ref := range refs {
		if ref.RefKind == refKind && ref.RefID == refID {
			return true
		}
	}
	return false
}

func hasRefKind(refs []MemoryGraphNodeRefRecord, refKind string) bool {
	for _, ref := range refs {
		if ref.RefKind == refKind {
			return true
		}
	}
	return false
}

func hasEdgeType(edges []MemoryGraphEdgeRecord, edgeType string) bool {
	for _, edge := range edges {
		if edge.EdgeType == edgeType {
			return true
		}
	}
	return false
}

func mustWorkspaceDocSHA(t *testing.T, store *Store, ctx context.Context, workspaceID, docKey string) string {
	t.Helper()
	record, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc %s/%s: %v", workspaceID, docKey, err)
	}
	return record.SHA
}
