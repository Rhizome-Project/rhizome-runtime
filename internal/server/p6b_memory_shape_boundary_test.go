package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestMemoryGraphAndNodeSearchRPCSurfaceBoundaryContract(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-p6b-memory-shape-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P6B Memory Shape RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "RPC graph stays compatibility-only",
		Body:        "RPC graph stays compatibility-only over the retained canonical shape.",
		Summary:     "RPC compatibility boundary",
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

	listRaw, err := json.Marshal(workspaceMemoryGraphListParams{
		WorkspaceID: workspaceID,
		OriginKind:  "workspace_memory",
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
	boundary, ok := listPayload["boundary_contract"].(sqlite.MemoryShapeBoundaryContract)
	if !ok {
		t.Fatalf("expected typed list boundary contract, got %+v", listPayload["boundary_contract"])
	}
	expectedBoundary := sqlite.DefaultMemoryShapeBoundaryContract()
	if boundary != expectedBoundary {
		t.Fatalf("expected list boundary %+v, got %+v", expectedBoundary, boundary)
	}
	items, ok := listPayload["items"].([]sqlite.MemoryGraphNodeRecord)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one list node, got %+v", listPayload)
	}
	if items[0].CanonicalAuthority != "workspace_memory" || items[0].SurfaceAuthority != "compatibility_only" || !items[0].CompatibilityOnly {
		t.Fatalf("expected list node compatibility authority, got %+v", items[0])
	}
	assertHandlerRetentionTemporalContracts(t, items[0].TemporalContracts)

	detailRaw, err := json.Marshal(workspaceMemoryGraphGetParams{
		WorkspaceID: workspaceID,
		MemoryID:    nodeID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	detailAny, rpcErr := h.workspaceMemoryGraphGet(ctx, detailRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphGet rpc error: %+v", rpcErr)
	}
	detail := detailAny.(sqlite.MemoryGraphNodeDetail)
	if detail.BoundaryContract != expectedBoundary {
		t.Fatalf("expected detail boundary %+v, got %+v", expectedBoundary, detail.BoundaryContract)
	}
	if detail.Node.CanonicalAuthority != "workspace_memory" || detail.Node.SurfaceAuthority != "compatibility_only" || !detail.Node.CompatibilityOnly {
		t.Fatalf("expected detail node compatibility authority, got %+v", detail.Node)
	}
	assertHandlerRetentionTemporalContracts(t, detail.Node.TemporalContracts)

	searchRaw, err := json.Marshal(workspaceMemoryNodeSearchParams{
		WorkspaceID: workspaceID,
		Query:       "compatibility-only over the retained canonical shape",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal search params: %v", err)
	}
	searchAny, rpcErr := h.workspaceMemoryNodeSearch(ctx, searchRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeSearch rpc error: %+v", rpcErr)
	}
	searchResult := searchAny.(sqlite.MemoryNodeSearchResult)
	if searchResult.BoundaryContract != expectedBoundary {
		t.Fatalf("expected search boundary %+v, got %+v", expectedBoundary, searchResult.BoundaryContract)
	}
	if len(searchResult.Hits) != 1 {
		t.Fatalf("expected one search hit, got %+v", searchResult)
	}
	if searchResult.Hits[0].CanonicalAuthority != "workspace_memory" || searchResult.Hits[0].SurfaceAuthority != "compatibility_only" || !searchResult.Hits[0].CompatibilityOnly {
		t.Fatalf("expected search hit compatibility authority, got %+v", searchResult.Hits[0])
	}
	assertHandlerRetentionTemporalContracts(t, searchResult.Hits[0].TemporalContracts)
}

func assertHandlerRetentionTemporalContracts(t *testing.T, contracts []sqlite.TemporalHorizonContract) {
	t.Helper()
	if len(contracts) == 0 {
		t.Fatalf("expected handler retention temporal contracts, got %+v", contracts)
	}
	for _, contract := range contracts {
		if contract.Domain != "retention" ||
			contract.Basis != "wall_clock" ||
			contract.Mapping != "exact_wall_clock" ||
			!contract.WallClockComparable {
			t.Fatalf("expected handler retention wall-clock contract, got %+v", contract)
		}
	}
}
