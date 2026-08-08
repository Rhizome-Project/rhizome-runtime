package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryNodeTouchTrustedAndUntrusted(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-handler-memory-touch"
	ctx := testAuthContext(workspaceID, "human", "developer")
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Touch",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "FACT",
		Title:       "Touch target",
		Body:        "Touch target seed",
		Summary:     "Touch target seed",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.6,
		Confidence:  0.7,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	seedNow := time.Now().UTC()
	seedStar := seedNow.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	seedAcc := seedNow.Add(-90 * time.Minute).Format(time.RFC3339Nano)
	seedH := 7200.0
	seedA := 0.75
	seedQ := 0.4
	seedN := 2
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, nodeID, workspaceID, seedA, seedStar, seedAcc, seedN, seedQ, seedH, seedStar, seedStar, seedStar, seedAcc); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}
	baselineDetail := mustWorkspaceMemoryGraphDetail(t, h, ctx, workspaceID, nodeID)

	trustedRaw, err := json.Marshal(workspaceMemoryNodeTouchParams{
		WorkspaceID: workspaceID,
		NodeID:      nodeID,
		Trusted:     true,
		Actor:       "developer",
	})
	if err != nil {
		t.Fatalf("marshal trusted touch params: %v", err)
	}
	trustedResult, rpcErr := h.workspaceMemoryNodeTouch(ctx, trustedRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeTouch trusted rpc error: %+v", rpcErr)
	}
	trustedResponse, ok := trustedResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected trusted response type %T", trustedResult)
	}
	trustedEventID, ok := trustedResponse["event_id"].(string)
	if !ok || trustedEventID == "" {
		t.Fatalf("expected trusted event_id in response, got %+v", trustedResponse)
	}
	trustedLive := nextEventOfType(t, ch, "workspace.memory.node.touched")
	trustedEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.node_touched",
		EntityType:  "memory_node",
		EntityID:    nodeID,
	})
	if trustedEvent.EventID != trustedEventID {
		t.Fatalf("trusted response event_id %q does not match persisted event %+v", trustedEventID, trustedEvent)
	}
	assertLiveEventMirrorsRuntimeEvent(t, trustedLive, trustedEvent, "workspace.memory.node.touched")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, trustedLive.PayloadJSON), trustedEvent.PayloadJSON)
	assertServerRuntimeEventAuthorityMetadata(t, trustedEvent, authority)
	assertServerWorkspaceMemoryRuntimePromptContext(t, trustedEvent, "workspace.memory.node.touch", workspaceID, nodeID, "human", "developer", map[string]string{
		"node_id":     nodeID,
		"trusted":     "true",
		"touch_mode":  "trusted",
		"memory_type": baselineDetail.Node.MemoryType,
		"origin_kind": "workspace_memory",
		"origin_id":   record.MemoryID,
		"actor_type":  "human",
		"actor_id":    "developer",
	})

	trusted, ok, err := getServerSalienceRecord(t, store, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get trusted salience record: %v", err)
	}
	if !ok {
		t.Fatal("expected salience record after trusted touch")
	}
	if trusted.N_i != seedN+1 {
		t.Fatalf("expected trusted touch count %d, got %d", seedN+1, trusted.N_i)
	}
	trustedStar, err := time.Parse(time.RFC3339Nano, trusted.T_i_star)
	if err != nil {
		t.Fatalf("parse trusted t_i_star: %v", err)
	}
	if trustedStar.Before(seedNow.Add(-250 * time.Millisecond)) {
		t.Fatalf("expected trusted touch to refresh anchor state near now, got %s before %s", trustedStar, seedNow)
	}

	untrustedSeedNow := time.Now().UTC()
	untrustedSeedStar := untrustedSeedNow.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	untrustedSeedAcc := untrustedSeedNow.Add(-90 * time.Minute).Format(time.RFC3339Nano)
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
	`, nodeID, workspaceID, seedA, untrustedSeedStar, untrustedSeedAcc, seedN, seedQ, seedH, untrustedSeedStar, untrustedSeedStar, untrustedSeedStar, untrustedSeedAcc); err != nil {
		t.Fatalf("reseed salience row: %v", err)
	}

	untrustedRaw, err := json.Marshal(workspaceMemoryNodeTouchParams{
		WorkspaceID: workspaceID,
		NodeID:      nodeID,
		Trusted:     false,
		Actor:       "developer",
	})
	if err != nil {
		t.Fatalf("marshal untrusted touch params: %v", err)
	}
	untrustedResult, rpcErr := h.workspaceMemoryNodeTouch(ctx, untrustedRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeTouch untrusted rpc error: %+v", rpcErr)
	}
	untrustedResponse, ok := untrustedResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected untrusted response type %T", untrustedResult)
	}
	untrustedEventID, ok := untrustedResponse["event_id"].(string)
	if !ok || untrustedEventID == "" {
		t.Fatalf("expected untrusted event_id in response, got %+v", untrustedResponse)
	}
	untrustedLive := nextEventOfType(t, ch, "workspace.memory.node.touched")
	untrustedEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.node_touched",
		EntityType:  "memory_node",
		EntityID:    nodeID,
	})
	if untrustedEvent.EventID != untrustedEventID {
		t.Fatalf("untrusted response event_id %q does not match persisted event %+v", untrustedEventID, untrustedEvent)
	}
	assertLiveEventMirrorsRuntimeEvent(t, untrustedLive, untrustedEvent, "workspace.memory.node.touched")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, untrustedLive.PayloadJSON), untrustedEvent.PayloadJSON)
	assertServerRuntimeEventAuthorityMetadata(t, untrustedEvent, authority)
	assertServerWorkspaceMemoryRuntimePromptContext(t, untrustedEvent, "workspace.memory.node.touch", workspaceID, nodeID, "human", "developer", map[string]string{
		"node_id":     nodeID,
		"trusted":     "false",
		"touch_mode":  "untrusted",
		"memory_type": baselineDetail.Node.MemoryType,
		"origin_kind": "workspace_memory",
		"origin_id":   record.MemoryID,
		"actor_type":  "human",
		"actor_id":    "developer",
	})

	untrusted, ok, err := getServerSalienceRecord(t, store, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get untrusted salience record: %v", err)
	}
	if !ok {
		t.Fatal("expected salience record after untrusted touch")
	}
	if untrusted.A_i != seedA {
		t.Fatalf("expected untrusted a_i to stay unchanged, got %f want %f", untrusted.A_i, seedA)
	}
	if untrusted.T_i_star != untrustedSeedStar {
		t.Fatalf("expected untrusted t_i_star to stay unchanged, got %s want %s", untrusted.T_i_star, untrustedSeedStar)
	}
	if untrusted.N_i != seedN {
		t.Fatalf("expected untrusted n_i to stay unchanged, got %d want %d", untrusted.N_i, seedN)
	}
	if untrusted.Q_i != seedQ {
		t.Fatalf("expected untrusted q_i to stay unchanged, got %f want %f", untrusted.Q_i, seedQ)
	}
	if untrusted.T_i_acc != untrustedSeedAcc {
		t.Fatalf("expected untrusted t_i_acc to stay unchanged, got %s want %s", untrusted.T_i_acc, untrustedSeedAcc)
	}
	if untrusted.H_i != seedH {
		t.Fatalf("expected untrusted h_i to stay unchanged, got %f want %f", untrusted.H_i, seedH)
	}
	if untrusted.UpdatedAt == untrustedSeedAcc {
		t.Fatal("expected untrusted touch to leave an audit/update trace without refreshing anchor-state clocks")
	}
}

func TestWorkspaceMemoryNodeTouchRejectsMissingPrincipalWithoutSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-memory-touch-no-principal"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Touch No Principal",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "FACT",
		Title:       "Unauthorized touch target",
		Body:        "Unauthorized touch should not mutate salience or write runtime events.",
		Summary:     "Unauthorized touch target",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.6,
		Confidence:  0.7,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	nodeID := "memnode:workspace_memory:" + record.MemoryID
	seedNow := time.Now().UTC()
	seedStar := seedNow.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	seedAcc := seedNow.Add(-90 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, 0.75, ?, ?, 2, 0.4, 7200, ?, ?, ?, ?)
	`, nodeID, workspaceID, seedStar, seedAcc, seedStar, seedStar, seedStar, seedAcc); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryNodeTouchParams{
		WorkspaceID: workspaceID,
		NodeID:      nodeID,
		Trusted:     true,
		Actor:       "developer",
	})
	if err != nil {
		t.Fatalf("marshal touch params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryNodeTouch(context.Background(), raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected missing principal permission reject, got result=%+v err=%+v", result, rpcErr)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.node_touched",
		EntityType:  "memory_node",
		EntityID:    nodeID,
	})
	if err != nil {
		t.Fatalf("list node touch runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no node touch runtime events after auth reject, got %+v", events)
	}
	got, ok, err := getServerSalienceRecord(t, store, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get salience record after auth reject: %v", err)
	}
	if !ok {
		t.Fatal("expected salience row to remain after auth reject")
	}
	if got.T_i_star != seedStar || got.T_i_acc != seedAcc || got.N_i != 2 || got.Q_i != 0.4 || got.UpdatedAt != seedAcc {
		t.Fatalf("expected salience to remain unchanged after auth reject, got %+v", got)
	}
}

func TestWorkspaceMemoryNodeTouchRPCPreservesProtectedAnchorSurface(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-handler-memory-touch-anchor"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Touch Anchor",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "PROCEDURE",
		Title:       "Protected anchor touch target",
		Body:        "Trusted and risky touch should diverge on surfaced protected anchor state.",
		Summary:     "Protected anchor touch target",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.7,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record protected memory: %v", err)
	}

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	seedNow := time.Now().UTC()
	seedStar := seedNow.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	seedAcc := seedNow.Add(-90 * time.Minute).Format(time.RFC3339Nano)
	seedH := 7200.0
	seedA := 0.75
	seedQ := 0.4
	seedN := 2
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
	`, nodeID, workspaceID, seedA, seedStar, seedAcc, seedN, seedQ, seedH, seedStar, seedStar, seedStar, seedAcc); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}

	baselineDetail := mustWorkspaceMemoryGraphDetail(t, h, ctx, workspaceID, nodeID)
	if !baselineDetail.Node.Protect {
		t.Fatalf("expected protected anchor baseline, got %+v", baselineDetail.Node)
	}
	if baselineDetail.Node.SemanticLineageID != "workspace_memory:"+record.MemoryID || baselineDetail.Node.Revision != 1 {
		t.Fatalf("unexpected baseline anchor identity %+v", baselineDetail.Node)
	}
	if baselineDetail.Node.LastAnyAccess == nil || *baselineDetail.Node.LastAnyAccess != seedAcc {
		t.Fatalf("expected baseline last_any_access %q, got %+v", seedAcc, baselineDetail.Node)
	}
	if baselineDetail.Node.LastTrustedAccess == nil || *baselineDetail.Node.LastTrustedAccess != seedStar {
		t.Fatalf("expected baseline last_trusted_access %q, got %+v", seedStar, baselineDetail.Node)
	}
	if baselineDetail.Node.TLife != seedH {
		t.Fatalf("expected baseline t_life %v, got %+v", seedH, baselineDetail.Node)
	}

	untrustedRaw, err := json.Marshal(workspaceMemoryNodeTouchParams{
		WorkspaceID: workspaceID,
		NodeID:      nodeID,
		Trusted:     false,
		Actor:       "developer",
	})
	if err != nil {
		t.Fatalf("marshal untrusted touch params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryNodeTouch(ctx, untrustedRaw); rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeTouch untrusted rpc error: %+v", rpcErr)
	}
	untrustedDetail := mustWorkspaceMemoryGraphDetail(t, h, ctx, workspaceID, nodeID)
	if !untrustedDetail.Node.Protect {
		t.Fatalf("expected risky touch to preserve protect=true, got %+v", untrustedDetail.Node)
	}
	if untrustedDetail.Node.SemanticLineageID != baselineDetail.Node.SemanticLineageID || untrustedDetail.Node.Revision != baselineDetail.Node.Revision {
		t.Fatalf("expected risky touch to preserve anchor identity, got %+v", untrustedDetail.Node)
	}
	if untrustedDetail.Node.LastTrustedAccess == nil || *untrustedDetail.Node.LastTrustedAccess != seedStar {
		t.Fatalf("expected risky touch not to refresh last_trusted_access, got %+v", untrustedDetail.Node)
	}
	if untrustedDetail.Node.LastAnyAccess == nil || *untrustedDetail.Node.LastAnyAccess != seedAcc {
		t.Fatalf("expected risky touch not to refresh surfaced last_any_access, got %+v", untrustedDetail.Node)
	}
	if untrustedDetail.Node.TLife != seedH {
		t.Fatalf("expected risky touch not to refresh t_life, got %+v", untrustedDetail.Node)
	}

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
	`, nodeID, workspaceID, seedA, seedStar, seedAcc, seedN, seedQ, seedH, seedStar, seedStar, seedStar, seedAcc); err != nil {
		t.Fatalf("reseed salience row: %v", err)
	}

	trustedRaw, err := json.Marshal(workspaceMemoryNodeTouchParams{
		WorkspaceID: workspaceID,
		NodeID:      nodeID,
		Trusted:     true,
		Actor:       "developer",
	})
	if err != nil {
		t.Fatalf("marshal trusted touch params: %v", err)
	}
	beforeTrusted := time.Now().UTC()
	if _, rpcErr := h.workspaceMemoryNodeTouch(ctx, trustedRaw); rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeTouch trusted rpc error: %+v", rpcErr)
	}
	afterTrusted := time.Now().UTC()

	trustedDetail := mustWorkspaceMemoryGraphDetail(t, h, ctx, workspaceID, nodeID)
	if !trustedDetail.Node.Protect {
		t.Fatalf("expected trusted touch to preserve protect=true, got %+v", trustedDetail.Node)
	}
	if trustedDetail.Node.SemanticLineageID != baselineDetail.Node.SemanticLineageID || trustedDetail.Node.Revision != baselineDetail.Node.Revision {
		t.Fatalf("expected trusted touch to preserve anchor identity, got %+v", trustedDetail.Node)
	}
	if trustedDetail.Node.LastTrustedAccess == nil || trustedDetail.Node.LastAnyAccess == nil {
		t.Fatalf("expected trusted touch to surface refreshed access fields, got %+v", trustedDetail.Node)
	}
	trustedStar, err := time.Parse(time.RFC3339Nano, *trustedDetail.Node.LastTrustedAccess)
	if err != nil {
		t.Fatalf("parse trusted last_trusted_access: %v", err)
	}
	trustedAny, err := time.Parse(time.RFC3339Nano, *trustedDetail.Node.LastAnyAccess)
	if err != nil {
		t.Fatalf("parse trusted last_any_access: %v", err)
	}
	if trustedStar.Before(beforeTrusted.Add(-250*time.Millisecond)) || trustedStar.After(afterTrusted.Add(250*time.Millisecond)) {
		t.Fatalf("expected trusted touch to refresh last_trusted_access near now, got %+v", trustedDetail.Node)
	}
	if trustedAny.Before(beforeTrusted.Add(-250*time.Millisecond)) || trustedAny.After(afterTrusted.Add(250*time.Millisecond)) {
		t.Fatalf("expected trusted touch to refresh last_any_access near now, got %+v", trustedDetail.Node)
	}
	if trustedDetail.Node.TLife <= seedH {
		t.Fatalf("expected trusted touch to refresh t_life beyond %v, got %+v", seedH, trustedDetail.Node)
	}
}

func getServerSalienceRecord(t *testing.T, store *sqlite.Store, workspaceID, memoryID string) (sqlite.MemoryNodeSalienceRecord, bool, error) {
	t.Helper()

	batch, err := store.GetMemoryNodeSalienceBatch(context.Background(), workspaceID, []string{memoryID})
	if err != nil {
		return sqlite.MemoryNodeSalienceRecord{}, false, err
	}
	rec, ok := batch[memoryID]
	return rec, ok, nil
}

func mustWorkspaceMemoryGraphDetail(t *testing.T, h *Handler, ctx context.Context, workspaceID, nodeID string) sqlite.MemoryGraphNodeDetail {
	t.Helper()

	raw, err := json.Marshal(workspaceMemoryGraphGetParams{
		WorkspaceID: workspaceID,
		MemoryID:    nodeID,
	})
	if err != nil {
		t.Fatalf("marshal graph get params: %v", err)
	}
	detailRaw, rpcErr := h.workspaceMemoryGraphGet(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphGet rpc error: %+v", rpcErr)
	}
	detail, ok := detailRaw.(sqlite.MemoryGraphNodeDetail)
	if !ok {
		t.Fatalf("unexpected graph get result type %T", detailRaw)
	}
	return detail
}
