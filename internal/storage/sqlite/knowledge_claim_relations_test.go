package sqlite

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
)

func TestKnowledgeClaimRelationsDeriveFromFieldsAndEvidence(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-claim-relations-derived"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Relations Derived",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	targetIDs := []string{
		"claim-target-supports",
		"claim-target-validated-by",
		"claim-target-blocks",
		"claim-target-resolves",
		"claim-target-supersedes",
		"claim-target-contradicts",
	}
	for _, claimID := range targetIDs {
		if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
			WorkspaceID: workspaceID,
			ClaimID:     claimID,
			ClaimType:   "FACT",
			Subject:     "Target " + claimID,
			Body:        "Body for " + claimID,
			SourceKind:  "manual",
			SourceID:    "developer",
		}); err != nil {
			t.Fatalf("record target claim %s: %v", claimID, err)
		}
	}

	sourceClaim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID:       workspaceID,
		ClaimID:           "claim-source",
		ClaimType:         "FACT",
		Subject:           "Source claim",
		Body:              "Source claim body",
		SourceKind:        "manual",
		SourceID:          "developer",
		SupersedesClaimID: "claim-target-supersedes",
		ConflictsClaimID:  "claim-target-contradicts",
		Evidence: []string{
			"supports:claim-target-supports",
			"validated_by:claim-target-validated-by",
			"blocks:claim-target-blocks",
			"resolves:claim-target-resolves",
			"supersedes:claim-target-supersedes",
			"contradicts:claim-target-contradicts",
			"supports:claim-target-supports",
			"unknown_prefix:claim-ignore",
		},
	})
	if err != nil {
		t.Fatalf("record source claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	relations, err := store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID: workspaceID,
		FromClaimID: sourceClaim.ClaimID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list relations: %v", err)
	}
	if len(relations) != 6 {
		t.Fatalf("expected 6 derived relations, got %+v", relations)
	}

	type relationExpectation struct {
		toClaimID    string
		sourceKind   string
		weight       float64
		relationType string
	}
	expected := map[string]relationExpectation{
		"SUPPORTS": {
			toClaimID:    "claim-target-supports",
			sourceKind:   "knowledge_claim_evidence",
			weight:       0.9,
			relationType: "SUPPORTS",
		},
		"VALIDATED_BY": {
			toClaimID:    "claim-target-validated-by",
			sourceKind:   "knowledge_claim_evidence",
			weight:       0.95,
			relationType: "VALIDATED_BY",
		},
		"BLOCKS": {
			toClaimID:    "claim-target-blocks",
			sourceKind:   "knowledge_claim_evidence",
			weight:       0.9,
			relationType: "BLOCKS",
		},
		"RESOLVES": {
			toClaimID:    "claim-target-resolves",
			sourceKind:   "knowledge_claim_evidence",
			weight:       0.9,
			relationType: "RESOLVES",
		},
		"SUPERSEDES": {
			toClaimID:    "claim-target-supersedes",
			sourceKind:   "knowledge_claim_field",
			weight:       1,
			relationType: "SUPERSEDES",
		},
		"CONTRADICTS": {
			toClaimID:    "claim-target-contradicts",
			sourceKind:   "knowledge_claim_field",
			weight:       1,
			relationType: "CONTRADICTS",
		},
	}

	for _, relation := range relations {
		want, ok := expected[relation.RelationType]
		if !ok {
			t.Fatalf("unexpected relation type %+v", relation)
		}
		if relation.ToClaimID != want.toClaimID || relation.SourceKind != want.sourceKind || relation.SourceID != sourceClaim.ClaimID {
			t.Fatalf("unexpected relation payload %+v", relation)
		}
		if relation.Weight != want.weight {
			t.Fatalf("unexpected relation weight %+v", relation)
		}
		expectedID := stableKnowledgeClaimRelationID(workspaceID, sourceClaim.ClaimID, want.relationType, want.toClaimID)
		if relation.RelationID != expectedID {
			t.Fatalf("unexpected stable relation id %+v", relation)
		}
	}
}

func TestKnowledgeClaimRelationListFiltering(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-claim-relations-filter"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Relations Filter",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	for _, claimID := range []string{"claim-alpha", "claim-beta", "claim-gamma", "claim-delta"} {
		if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
			WorkspaceID: workspaceID,
			ClaimID:     claimID,
			ClaimType:   "FACT",
			Subject:     claimID,
			Body:        "Body for " + claimID,
			SourceKind:  "manual",
			SourceID:    "developer",
		}); err != nil {
			t.Fatalf("record claim %s: %v", claimID, err)
		}
	}

	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-source-a",
		ClaimType:   "FACT",
		Subject:     "Source A",
		Body:        "Source A body",
		SourceKind:  "manual",
		SourceID:    "developer",
		Evidence: []string{
			"supports:claim-alpha",
			"blocks:claim-beta",
		},
	}); err != nil {
		t.Fatalf("record source claim A: %v", err)
	}

	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-source-b",
		ClaimType:   "FACT",
		Subject:     "Source B",
		Body:        "Source B body",
		SourceKind:  "manual",
		SourceID:    "developer",
		Evidence: []string{
			"supports:claim-alpha",
			"resolves:claim-delta",
		},
	}); err != nil {
		t.Fatalf("record source claim B: %v", err)
	}

	allSupports, err := store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID:  workspaceID,
		RelationType: "supports",
		Limit:        20,
	})
	if err != nil {
		t.Fatalf("list supports relations: %v", err)
	}
	if len(allSupports) != 2 {
		t.Fatalf("expected 2 supports relations, got %+v", allSupports)
	}

	filteredByTo, err := store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID: workspaceID,
		ToClaimID:   "claim-alpha",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list relations by to_claim_id: %v", err)
	}
	if len(filteredByTo) != 2 {
		t.Fatalf("expected 2 inbound relations to claim-alpha, got %+v", filteredByTo)
	}

	filteredByFrom, err := store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID: workspaceID,
		FromClaimID: "claim-source-a",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list relations by from_claim_id: %v", err)
	}
	if len(filteredByFrom) != 2 {
		t.Fatalf("expected 2 outbound relations from claim-source-a, got %+v", filteredByFrom)
	}

	filteredByClaim, err := store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-source-b",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list relations by claim_id: %v", err)
	}
	if len(filteredByClaim) != 2 {
		t.Fatalf("expected 2 relations touching claim-source-b, got %+v", filteredByClaim)
	}

	if _, err := store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID:  workspaceID,
		RelationType: "not-a-relation",
	}); err == nil {
		t.Fatalf("expected invalid relation type error")
	}
}

func TestKnowledgeClaimGraphUsesRelationRowsForEdgesAndVersions(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-claim-relations-graph"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Relations Graph",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	backingMemory, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Use runtime replay",
		Body:        "Runtime replay remains canonical.",
		Summary:     "Use runtime replay.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record backing memory: %v", err)
	}

	for _, claimID := range []string{"claim-support", "claim-validate", "claim-block"} {
		if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
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

	sourceClaim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-graph-source",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Graph source claim",
		Body:        "Source claim for graph relations.",
		Summary:     "Graph source.",
		Confidence:  0.88,
		SourceKind:  "manual",
		SourceID:    "developer",
		MemoryID:    backingMemory.MemoryID,
		Evidence: []string{
			"supports:claim-support",
			"validated_by:claim-validate",
			"blocks:claim-block",
		},
	})
	if err != nil {
		t.Fatalf("record source claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	relations, err := store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID: workspaceID,
		FromClaimID: sourceClaim.ClaimID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list relations: %v", err)
	}
	if len(relations) != 3 {
		t.Fatalf("expected 3 relation rows, got %+v", relations)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("knowledge_claim", sourceClaim.ClaimID))
	if err != nil {
		t.Fatalf("get memory graph node: %v", err)
	}
	if detail.Node.MemoryType != "DECISION_RECORD" || detail.Node.CompatType != "DECISION" || detail.Node.EpistemicStatus != "VERIFIED" {
		t.Fatalf("unexpected graph node %+v", detail.Node)
	}

	type edgeKey struct {
		edgeType string
		toID     string
	}
	outbound := make(map[edgeKey]MemoryGraphEdgeRecord, len(detail.OutboundEdges))
	for _, edge := range detail.OutboundEdges {
		outbound[edgeKey{edgeType: edge.EdgeType, toID: edge.ToMemoryID}] = edge
	}

	derivedKey := edgeKey{edgeType: "DERIVED_FROM", toID: memoryGraphNodeID("workspace_memory", backingMemory.MemoryID)}
	if _, ok := outbound[derivedKey]; !ok {
		t.Fatalf("expected DERIVED_FROM edge, got %+v", detail.OutboundEdges)
	}

	for _, relation := range relations {
		key := edgeKey{edgeType: relation.RelationType, toID: memoryGraphNodeID("knowledge_claim", relation.ToClaimID)}
		edge, ok := outbound[key]
		if !ok {
			t.Fatalf("missing relation edge for %+v in %+v", relation, detail.OutboundEdges)
		}
		if edge.Weight != relation.Weight {
			t.Fatalf("edge weight mismatch for %+v vs %+v", edge, relation)
		}
		var metadata map[string]any
		if err := json.Unmarshal([]byte(edge.MetadataJSON), &metadata); err != nil {
			t.Fatalf("decode edge metadata: %v", err)
		}
		if metadata["relation_id"] != relation.RelationID || metadata["source_kind"] != relation.SourceKind || metadata["source_id"] != relation.SourceID {
			t.Fatalf("unexpected edge metadata %+v for relation %+v", metadata, relation)
		}
	}

	versionKinds := make([]string, 0, len(detail.Versions))
	relationVersions := make(map[string]MemoryGraphNodeVersionRecord)
	for _, version := range detail.Versions {
		versionKinds = append(versionKinds, version.RefKind)
		if version.RefKind == "knowledge_claim_relation" {
			relationVersions[version.RefID] = version
		}
	}
	sort.Strings(versionKinds)
	if !containsString(versionKinds, "knowledge_claim") || !containsString(versionKinds, "workspace_memory") {
		t.Fatalf("expected canonical knowledge_claim/workspace_memory versions, got %+v", detail.Versions)
	}
	for _, relation := range relations {
		version, ok := relationVersions[relation.RelationID]
		if !ok {
			t.Fatalf("missing relation-backed version for %+v in %+v", relation, detail.Versions)
		}
		if version.Weight != relation.Weight {
			t.Fatalf("unexpected relation version weight %+v for relation %+v", version, relation)
		}
	}
}

func TestKnowledgeClaimDissentGraphPreservesContradictionSemantics(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-claim-dissent-graph"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Dissent Graph",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	targetClaim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-canonical",
		ClaimType:   "FACT",
		Subject:     "Canonical claim",
		Body:        "Canonical operational fact.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record target claim: %v", err)
	}

	now := "2026-03-23T12:00:00Z"
	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	dissentRecord := KnowledgeClaimRecord{
		ClaimID:          "claim-dissent",
		WorkspaceID:      workspaceID,
		ClaimType:        "DISSENT",
		Status:           "ACTIVE",
		Subject:          "Operator veto",
		Body:             "Explicit dissent against the canonical claim.",
		Summary:          "Operator dissent.",
		Confidence:       0.61,
		SourceKind:       "manual",
		SourceID:         "developer",
		ConflictsClaimID: targetClaim.ClaimID,
	}
	if _, _, _, err := store.upsertKnowledgeClaimTx(ctx, tx, dissentRecord, now); err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert dissent claim: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit dissent claim: %v", err)
	}

	stored, err := store.GetKnowledgeClaim(ctx, workspaceID, dissentRecord.ClaimID)
	if err != nil {
		t.Fatalf("get dissent claim: %v", err)
	}
	if stored.ClaimType != "DISSENT" || stored.ConflictsClaimID != targetClaim.ClaimID {
		t.Fatalf("unexpected stored dissent claim %+v", stored)
	}

	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_edges; DELETE FROM memory_node_versions; DELETE FROM memory_node_refs; DELETE FROM memory_nodes;`); err != nil {
		t.Fatalf("clear graph tables: %v", err)
	}
	if _, err := store.SyncMemoryGraphWorkspace(ctx, workspaceID); err != nil {
		t.Fatalf("sync memory graph workspace: %v", err)
	}

	relations, err := store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID: workspaceID,
		FromClaimID: dissentRecord.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list dissent relations: %v", err)
	}
	if len(relations) != 1 || relations[0].RelationType != "CONTRADICTS" || relations[0].ToClaimID != targetClaim.ClaimID {
		t.Fatalf("unexpected dissent relation rows %+v", relations)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("knowledge_claim", dissentRecord.ClaimID))
	if err != nil {
		t.Fatalf("get dissent graph node: %v", err)
	}
	if detail.Node.MemoryType != "DISSENT" || detail.Node.ClaimModality != "observed" {
		t.Fatalf("unexpected dissent graph node %+v", detail.Node)
	}
	foundContradiction := false
	for _, edge := range detail.OutboundEdges {
		if edge.EdgeType == "CONTRADICTS" && edge.ToMemoryID == memoryGraphNodeID("knowledge_claim", targetClaim.ClaimID) {
			foundContradiction = true
			break
		}
	}
	if !foundContradiction {
		t.Fatalf("expected contradiction edge for dissent claim, got %+v", detail.OutboundEdges)
	}
}
