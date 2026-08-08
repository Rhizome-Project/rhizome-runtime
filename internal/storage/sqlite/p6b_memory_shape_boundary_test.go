package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestMemoryGraphSurfacesBoundaryContractAndCompatibilityAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-p6b-memory-shape-boundary"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P6B Memory Shape Boundary",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Derived graph stays compatibility-only",
		Body:        "The graph should remain compatibility-only over canonical workspace memory.",
		Summary:     "Compatibility-only graph",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	now := time.Now().UTC()
	hotAt := now.Add(30 * time.Minute).Format(time.RFC3339Nano)
	warmAt := now.Add(90 * time.Minute).Format(time.RFC3339Nano)
	gcAt := now.Add(3 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(memory_id) DO UPDATE SET
			a_i=excluded.a_i,
			t_i_star=excluded.t_i_star,
			t_i_acc=excluded.t_i_acc,
			n_i=excluded.n_i,
			q_i=excluded.q_i,
			h_i=excluded.h_i,
			t_hot=excluded.t_hot,
			t_warm=excluded.t_warm,
			t_gc=excluded.t_gc,
			updated_at=excluded.updated_at
	`, nodeID, workspaceID, 0.8, now.Add(-2*time.Hour).Format(time.RFC3339Nano), now.Add(-30*time.Minute).Format(time.RFC3339Nano), 3, 0.2, 7200.0, hotAt, warmAt, gcAt, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}

	items, err := store.ListMemoryGraphNodes(ctx, MemoryGraphNodeFilter{
		WorkspaceID: workspaceID,
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory graph nodes: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one compatibility node, got %+v", items)
	}
	if items[0].CanonicalAuthority != "workspace_memory" || items[0].SurfaceAuthority != "compatibility_only" || items[0].SurfaceRole != "derived_compatibility_projection" || !items[0].CompatibilityOnly {
		t.Fatalf("expected list node boundary contract, got %+v", items[0])
	}
	assertRetentionTemporalContracts(t, items[0].TemporalContracts)

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get memory graph node: %v", err)
	}
	if detail.BoundaryContract.CanonicalShape != memoryShapeCanonicalRetained || detail.BoundaryContract.SurfaceAuthority != memoryShapeSurfaceAuthorityCompatOnly || detail.BoundaryContract.SurfaceRole != memoryShapeSurfaceRoleDerivedProjection {
		t.Fatalf("expected detail boundary contract, got %+v", detail.BoundaryContract)
	}
	if detail.Node.CanonicalAuthority != "workspace_memory" || detail.Node.SurfaceAuthority != "compatibility_only" || !detail.Node.CompatibilityOnly {
		t.Fatalf("expected detail node compatibility authority, got %+v", detail.Node)
	}
	assertRetentionTemporalContracts(t, detail.Node.TemporalContracts)

	searchResult, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "compatibility-only graph",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search memory nodes: %v", err)
	}
	if searchResult.BoundaryContract.CanonicalShape != memoryShapeCanonicalRetained || searchResult.BoundaryContract.SurfaceAuthority != memoryShapeSurfaceAuthorityCompatOnly || searchResult.BoundaryContract.SurfaceRole != memoryShapeSurfaceRoleDerivedProjection {
		t.Fatalf("expected search boundary contract, got %+v", searchResult.BoundaryContract)
	}
	if len(searchResult.Hits) != 1 {
		t.Fatalf("expected one search hit, got %+v", searchResult)
	}
	hit := searchResult.Hits[0]
	if hit.CanonicalAuthority != "workspace_memory" || hit.SurfaceAuthority != "compatibility_only" || hit.SurfaceRole != "derived_compatibility_projection" || !hit.CompatibilityOnly {
		t.Fatalf("expected search hit compatibility authority, got %+v", hit)
	}
	assertRetentionTemporalContracts(t, hit.TemporalContracts)
}

func assertRetentionTemporalContracts(t *testing.T, contracts []TemporalHorizonContract) {
	t.Helper()
	if len(contracts) == 0 {
		t.Fatalf("expected retention temporal contracts, got %+v", contracts)
	}
	for _, contract := range contracts {
		if contract.SchemaVersion != temporalContractSchemaVersion ||
			contract.Domain != "retention" ||
			contract.Basis != temporalBasisWallClock ||
			contract.Mapping != temporalMappingExactWallClock ||
			!contract.WallClockComparable {
			t.Fatalf("expected retention wall-clock temporal contract, got %+v", contract)
		}
		if contract.ReferenceAt == "" || contract.TargetAt == "" {
			t.Fatalf("expected retention temporal contract to carry reference/target anchors, got %+v", contract)
		}
	}
}
