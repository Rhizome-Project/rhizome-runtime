package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryNodeSearchRPCSurface(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-memory-node-search"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Search",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Canonical rollout truth",
		Body:        "Canonical rollout truth should survive search compaction.",
		Summary:     "Canonical rollout truth.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.8,
		Confidence:  0.9,
	}); err != nil {
		t.Fatalf("record memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceMemoryNodeSearchParams{
		WorkspaceID: workspaceID,
		Query:       "rollout truth",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryNodeSearch(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeSearch rpc error: %+v", rpcErr)
	}
	searchResult, ok := result.(sqlite.MemoryNodeSearchResult)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if searchResult.TimeAuthority.WorkspaceID != workspaceID || searchResult.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected search result time authority, got %+v", searchResult.TimeAuthority)
	}
	if searchResult.GeneratedAt != searchResult.TimeAuthority.ReferenceAt {
		t.Fatalf("expected search result generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", searchResult.GeneratedAt, searchResult.TimeAuthority.ReferenceAt)
	}
	if searchResult.Count == 0 || len(searchResult.Hits) == 0 {
		t.Fatalf("expected search hits, got %+v", searchResult)
	}
	if searchResult.Hits[0].Snippet == "" || searchResult.Hits[0].RefCount == 0 {
		t.Fatalf("expected compact hit diagnostics, got %+v", searchResult.Hits[0])
	}
}

func TestWorkspaceMemoryNodeSearchRPCSurfacesDriftStateAfterSourceUpdate(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-node-search-drift"
		docKey      = "runbook-handler-search-drift"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Node Search Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nCanonical rollout truth.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert initial workspace doc: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Canonical rollout truth",
		Body:        "Canonical rollout truth should survive search compaction.",
		Summary:     "Canonical rollout truth.",
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.8,
		Confidence:  0.9,
	}); err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	searchRaw, err := json.Marshal(workspaceMemoryNodeSearchParams{
		WorkspaceID: workspaceID,
		Query:       "rollout truth",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal search params: %v", err)
	}
	beforeResult, rpcErr := h.workspaceMemoryNodeSearch(ctx, searchRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeSearch rpc error: %+v", rpcErr)
	}
	before, ok := beforeResult.(sqlite.MemoryNodeSearchResult)
	if !ok {
		t.Fatalf("unexpected search result type %T", beforeResult)
	}
	if before.Count != 1 || len(before.Hits) != 1 || before.Hits[0].DriftState != "CURRENT" {
		t.Fatalf("expected current drift state before source update, got %+v", before)
	}

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nUpdated rollout truth with rollback steps.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert updated workspace doc: %v", err)
	}

	afterResult, rpcErr := h.workspaceMemoryNodeSearch(ctx, searchRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeSearch after update rpc error: %+v", rpcErr)
	}
	after, ok := afterResult.(sqlite.MemoryNodeSearchResult)
	if !ok {
		t.Fatalf("unexpected search result type %T", afterResult)
	}
	if after.Count != 1 || len(after.Hits) != 1 {
		t.Fatalf("expected one search hit after source update, got %+v", after)
	}
	if after.Hits[0].DriftState != "STALE" || after.Hits[0].DriftScore <= 0 {
		t.Fatalf("expected stale drift state after source update, got %+v", after.Hits[0])
	}
}

func TestWorkspaceMemoryNodeSearchRPCRejectsInvalidParams(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	cases := []workspaceMemoryNodeSearchParams{
		{Query: "missing workspace"},
		{WorkspaceID: "ws-invalid"},
		{WorkspaceID: "ws-invalid", MemoryLayer: "NOT_A_LAYER"},
	}
	for idx, params := range cases {
		raw, err := json.Marshal(params)
		if err != nil {
			t.Fatalf("marshal params %d: %v", idx, err)
		}
		if _, rpcErr := h.workspaceMemoryNodeSearch(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
			t.Fatalf("expected invalid params for case %d, got %+v", idx, rpcErr)
		}
	}
}

func TestWorkspaceMemoryNodeSearchRPCSurfacesAnchorStateFieldsWithoutBreakingCompactHitContract(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-memory-node-search-anchor-state"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Node Search Anchor State",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Anchor-state search",
		Body:        "Anchor-state fields should surface through RPC without breaking compact hits.",
		Summary:     "Anchor-state search.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	lineageID := "lineage:" + record.MemoryID
	lastAnyAccess := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	lastTrustedAccess := time.Now().UTC().Add(-70 * time.Minute).Format(time.RFC3339Nano)
	tLife := 43200.0
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE memory_nodes
		   SET semantic_lineage_id = ?, revision = ?, protect = ?, unresolved = ?
		 WHERE workspace_id = ? AND memory_id = ?
	`, lineageID, 11, 1, 1, workspaceID, nodeID); err != nil {
		t.Fatalf("seed memory node anchor-state fields: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(memory_id) DO UPDATE SET
			t_i_star = excluded.t_i_star,
			t_i_acc = excluded.t_i_acc,
			h_i = excluded.h_i,
			t_hot = excluded.t_hot,
			t_warm = excluded.t_warm,
			t_gc = excluded.t_gc,
			updated_at = excluded.updated_at
	`, nodeID, workspaceID, 0.85, lastTrustedAccess, lastAnyAccess, 5, 0.3, tLife, lastAnyAccess, lastAnyAccess, lastAnyAccess, lastAnyAccess); err != nil {
		t.Fatalf("seed memory node salience: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryNodeSearchParams{
		WorkspaceID: workspaceID,
		Query:       "Anchor-state search",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryNodeSearch(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeSearch rpc error: %+v", rpcErr)
	}
	searchResult, ok := result.(sqlite.MemoryNodeSearchResult)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if searchResult.TimeAuthority.WorkspaceID != workspaceID || searchResult.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected search result time authority, got %+v", searchResult.TimeAuthority)
	}
	if searchResult.Count != 1 || len(searchResult.Hits) != 1 {
		t.Fatalf("expected one search hit, got %+v", searchResult)
	}
	hit := searchResult.Hits[0]
	if hit.Snippet == "" || hit.RefCount == 0 || hit.MemoryID != nodeID {
		t.Fatalf("expected compact hit contract to stay intact, got %+v", hit)
	}

	hitJSON, err := json.Marshal(hit)
	if err != nil {
		t.Fatalf("marshal rpc search hit: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(hitJSON, &payload); err != nil {
		t.Fatalf("decode rpc search hit: %v", err)
	}

	if payload["semantic_lineage_id"] != lineageID {
		t.Fatalf("expected semantic_lineage_id %q, got payload %+v", lineageID, payload)
	}
	if got, ok := payload["revision"].(float64); !ok || int(got) != 11 {
		t.Fatalf("expected revision 11, got payload %+v", payload)
	}
	if got, ok := payload["protect"].(bool); !ok || !got {
		t.Fatalf("expected protect=true, got payload %+v", payload)
	}
	if got, ok := payload["unresolved"].(bool); !ok || !got {
		t.Fatalf("expected unresolved=true, got payload %+v", payload)
	}
	if payload["last_any_access"] != lastAnyAccess {
		t.Fatalf("expected last_any_access %q, got payload %+v", lastAnyAccess, payload)
	}
	if payload["last_trusted_access"] != lastTrustedAccess {
		t.Fatalf("expected last_trusted_access %q, got payload %+v", lastTrustedAccess, payload)
	}
	if got, ok := payload["t_life"].(float64); !ok || got != tLife {
		t.Fatalf("expected t_life %v, got payload %+v", tLife, payload)
	}
}
