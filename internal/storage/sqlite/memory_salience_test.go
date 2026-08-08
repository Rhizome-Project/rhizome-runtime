package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestTouchMemoryNodeTrustedClampsRiskAgentAndRefreshesAnchorState(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-trusted"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Trusted",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "FACT",
		Title:       "Trusted touch",
		Body:        "Trusted touch seed",
		Summary:     "Trusted touch seed",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.6,
		Confidence:  0.7,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)

	seedNow := time.Now().UTC()
	seedStar := seedNow.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	seedAcc := seedNow.Add(-90 * time.Minute).Format(time.RFC3339Nano)
	seedH := 7200.0
	seedA := 0.75
	seedQ := 0.4
	seedN := 2
	seedHot := rmpComputeThresholdTime(seedStar, seedH, seedA, DefaultRMPSalienceConfig().ThetaHot)
	seedWarm := rmpComputeThresholdTime(seedStar, seedH, seedA, DefaultRMPSalienceConfig().ThetaWarm)
	seedGc := rmpComputeThresholdTime(seedStar, seedH, seedA, DefaultRMPSalienceConfig().ThetaGc)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, nodeID, workspaceID, seedA, seedStar, seedAcc, seedN, seedQ, seedH, seedHot, seedWarm, seedGc, seedAcc); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}

	before := time.Now().UTC()
	if err := store.TouchMemoryNodeTrusted(ctx, workspaceID, nodeID, -0.5, DefaultRMPSalienceConfig()); err != nil {
		t.Fatalf("trusted touch: %v", err)
	}
	after := time.Now().UTC()

	got, ok, err := getSalienceRecord(t, store, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get salience record: %v", err)
	}
	if !ok {
		t.Fatal("expected salience record after trusted touch")
	}
	if got.N_i != seedN+1 {
		t.Fatalf("expected trusted touch count %d, got %d", seedN+1, got.N_i)
	}
	if got.Q_i == seedQ {
		t.Fatal("expected q_i to change after trusted touch")
	}
	newStar, err := time.Parse(time.RFC3339Nano, got.T_i_star)
	if err != nil {
		t.Fatalf("parse t_i_star: %v", err)
	}
	if newStar.Before(before.Add(-250 * time.Millisecond)) {
		t.Fatalf("expected t_i_star to refresh near trusted touch time, got %s before %s", newStar, before)
	}
	if newStar.After(after.Add(250 * time.Millisecond)) {
		t.Fatalf("expected clamped trusted touch not to move anchor into the future, got %s after %s", newStar, after)
	}
}

func TestTouchMemoryNodeUntrustedDoesNotRefreshAnchorState(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-untrusted"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Untrusted",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "FACT",
		Title:       "Untrusted touch",
		Body:        "Untrusted touch seed",
		Summary:     "Untrusted touch seed",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.6,
		Confidence:  0.7,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)

	seedNow := time.Now().UTC()
	seedStar := seedNow.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	seedAcc := seedNow.Add(-90 * time.Minute).Format(time.RFC3339Nano)
	seedH := 7200.0
	seedA := 0.75
	seedQ := 0.4
	seedN := 2
	seedHot := rmpComputeThresholdTime(seedStar, seedH, seedA, DefaultRMPSalienceConfig().ThetaHot)
	seedWarm := rmpComputeThresholdTime(seedStar, seedH, seedA, DefaultRMPSalienceConfig().ThetaWarm)
	seedGc := rmpComputeThresholdTime(seedStar, seedH, seedA, DefaultRMPSalienceConfig().ThetaGc)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, nodeID, workspaceID, seedA, seedStar, seedAcc, seedN, seedQ, seedH, seedHot, seedWarm, seedGc, seedAcc); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}

	if err := store.TouchMemoryNodeUntrusted(ctx, workspaceID, nodeID); err != nil {
		t.Fatalf("untrusted touch: %v", err)
	}

	got, ok, err := getSalienceRecord(t, store, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get salience record: %v", err)
	}
	if !ok {
		t.Fatal("expected salience record after untrusted touch")
	}
	if got.A_i != seedA {
		t.Fatalf("expected a_i to stay unchanged, got %f want %f", got.A_i, seedA)
	}
	if got.T_i_star != seedStar {
		t.Fatalf("expected t_i_star to stay unchanged, got %s want %s", got.T_i_star, seedStar)
	}
	if got.N_i != seedN {
		t.Fatalf("expected n_i to stay unchanged, got %d want %d", got.N_i, seedN)
	}
	if got.Q_i != seedQ {
		t.Fatalf("expected q_i to stay unchanged, got %f want %f", got.Q_i, seedQ)
	}
	if got.T_i_acc != seedAcc {
		t.Fatalf("expected untrusted touch to leave t_i_acc unchanged, got %s want %s", got.T_i_acc, seedAcc)
	}
	if got.H_i != seedH {
		t.Fatalf("expected h_i to stay unchanged, got %f want %f", got.H_i, seedH)
	}
	if got.UpdatedAt == seedAcc {
		t.Fatal("expected untrusted touch to leave an audit/update trace without refreshing anchor-state clocks")
	}
}

func TestTouchMemoryNodeWithEventCarriesAuthorityAndPromptContext(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-touch-event"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Touch Event",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "FACT",
		Title:       "Touch event target",
		Body:        "Touch event should carry authority and prompt context.",
		Summary:     "Touch event target",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.6,
		Confidence:  0.7,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get memory graph node: %v", err)
	}

	envelope := BuildWorkspaceMemoryPromptContextEnvelope("workspace.memory.node.touch", "server_rpc", workspaceID, "human", "developer")
	result, err := store.TouchMemoryNodeWithEvent(ctx, MemoryNodeTouchInput{
		WorkspaceID:           workspaceID,
		NodeID:                nodeID,
		Trusted:               true,
		SalienceConfig:        DefaultRMPSalienceConfig(),
		ActorType:             "human",
		ActorID:               "developer",
		PromptContextEnvelope: envelope,
	})
	if err != nil {
		t.Fatalf("touch memory node with event: %v", err)
	}
	if result.Salience.MemoryID != nodeID || result.Salience.WorkspaceID != workspaceID || result.Salience.T_i_acc == "" {
		t.Fatalf("unexpected salience result %+v", result.Salience)
	}
	event := result.RuntimeEvent
	if event.EventType != "workspace_memory.node_touched" || event.EntityType != "memory_node" || event.EntityID != nodeID {
		t.Fatalf("unexpected runtime event %+v", event)
	}
	if event.ActorType != "human" || event.ActorID != "developer" {
		t.Fatalf("expected human/developer actor binding, got %+v", event)
	}
	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.node_touched",
		EntityType:  "memory_node",
		EntityID:    nodeID,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one persisted node touch event %+v, got %+v", event, events)
	}

	payload := decodeMemoryNodeTouchPayload(t, event.PayloadJSON)
	promptEnvelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in payload %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_workspace_memory_write",
		"surface":                            "workspace.memory.node.touch",
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"memory_id":                          nodeID,
		"node_id":                            nodeID,
		"trusted":                            "true",
		"touch_mode":                         "trusted",
		"memory_type":                        detail.Node.MemoryType,
		"origin_kind":                        "workspace_memory",
		"origin_id":                          record.MemoryID,
		"actor_type":                         "human",
		"actor_id":                           "developer",
		"principal_type":                     "human",
		"principal_id":                       "developer",
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	for key, want := range expected {
		got, ok := promptEnvelope[key].(string)
		if !ok || got != want {
			t.Fatalf("prompt_context_envelope[%s] = %#v, want %q in %+v", key, promptEnvelope[key], want, promptEnvelope)
		}
	}
}

func TestTouchMemoryNodeWithEventRejectsForgedPromptContextWithoutSideEffects(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-touch-forged-context"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Touch Forged Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "FACT",
		Title:       "Forged context target",
		Body:        "Forged prompt context must roll back salience and runtime event writes.",
		Summary:     "Forged context target",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.6,
		Confidence:  0.7,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	forged := BuildWorkspaceMemoryPromptContextEnvelope("workspace.memory.node.touch", "server_rpc", workspaceID, "human", "developer")
	forged["node_id"] = "wrong-node"

	_, err = store.TouchMemoryNodeWithEvent(ctx, MemoryNodeTouchInput{
		WorkspaceID:           workspaceID,
		NodeID:                nodeID,
		Trusted:               true,
		SalienceConfig:        DefaultRMPSalienceConfig(),
		ActorType:             "human",
		ActorID:               "developer",
		PromptContextEnvelope: forged,
	})
	if err == nil || !strings.Contains(err.Error(), "node_id") {
		t.Fatalf("expected forged prompt context node_id reject, got %v", err)
	}
	events, listErr := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.node_touched",
		EntityType:  "memory_node",
		EntityID:    nodeID,
	})
	if listErr != nil {
		t.Fatalf("list runtime events after forged reject: %v", listErr)
	}
	if len(events) != 0 {
		t.Fatalf("expected no node touch runtime events after forged reject, got %+v", events)
	}
	if _, ok, salienceErr := getSalienceRecord(t, store, workspaceID, nodeID); salienceErr != nil || ok {
		t.Fatalf("expected trusted touch salience insert to roll back after forged reject, ok=%v err=%v", ok, salienceErr)
	}
}

func TestTouchMemoryNodeTrustedAndUntrustedPreserveProtectedAnchorSurface(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-protected-anchor"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Protected Anchor",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "PROCEDURE",
		Title:       "Protected anchor touch target",
		Body:        "Trusted and risky touch paths should diverge on protected anchor-state surfaces.",
		Summary:     "Protected anchor touch target",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.7,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record protected workspace memory: %v", err)
	}
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)

	seedNow := time.Now().UTC()
	seedStar := seedNow.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	seedAcc := seedNow.Add(-90 * time.Minute).Format(time.RFC3339Nano)
	seedH := 7200.0
	seedA := 0.75
	seedQ := 0.4
	seedN := 2
	seedHot := rmpComputeThresholdTime(seedStar, seedH, seedA, DefaultRMPSalienceConfig().ThetaHot)
	seedWarm := rmpComputeThresholdTime(seedStar, seedH, seedA, DefaultRMPSalienceConfig().ThetaWarm)
	seedGc := rmpComputeThresholdTime(seedStar, seedH, seedA, DefaultRMPSalienceConfig().ThetaGc)
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
	`, nodeID, workspaceID, seedA, seedStar, seedAcc, seedN, seedQ, seedH, seedHot, seedWarm, seedGc, seedAcc); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}

	baseline, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get baseline memory graph node: %v", err)
	}
	if !baseline.Node.Protect {
		t.Fatalf("expected protected procedural anchor, got %+v", baseline.Node)
	}
	if baseline.Node.SemanticLineageID != "workspace_memory:"+record.MemoryID || baseline.Node.Revision != 1 {
		t.Fatalf("unexpected baseline anchor identity %+v", baseline.Node)
	}
	if baseline.Node.LastAnyAccess == nil || *baseline.Node.LastAnyAccess != seedAcc {
		t.Fatalf("expected baseline last_any_access %q, got %+v", seedAcc, baseline.Node)
	}
	if baseline.Node.LastTrustedAccess == nil || *baseline.Node.LastTrustedAccess != seedStar {
		t.Fatalf("expected baseline last_trusted_access %q, got %+v", seedStar, baseline.Node)
	}
	if baseline.Node.TLife != seedH {
		t.Fatalf("expected baseline t_life %v, got %+v", seedH, baseline.Node)
	}

	if err := store.TouchMemoryNodeUntrusted(ctx, workspaceID, nodeID); err != nil {
		t.Fatalf("untrusted touch: %v", err)
	}
	untrustedDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get untrusted memory graph node: %v", err)
	}
	if !untrustedDetail.Node.Protect {
		t.Fatalf("expected risky touch to preserve protect=true, got %+v", untrustedDetail.Node)
	}
	if untrustedDetail.Node.SemanticLineageID != baseline.Node.SemanticLineageID || untrustedDetail.Node.Revision != baseline.Node.Revision {
		t.Fatalf("expected risky touch to preserve anchor identity, got %+v", untrustedDetail.Node)
	}
	if untrustedDetail.Node.LastTrustedAccess == nil || *untrustedDetail.Node.LastTrustedAccess != seedStar {
		t.Fatalf("expected risky touch not to refresh last_trusted_access, got %+v", untrustedDetail.Node)
	}
	if untrustedDetail.Node.LastAnyAccess == nil || *untrustedDetail.Node.LastAnyAccess != seedAcc {
		t.Fatalf("expected risky touch not to refresh surfaced last_any_access, got %+v", untrustedDetail.Node)
	}
	if untrustedDetail.Node.TLife != seedH {
		t.Fatalf("expected risky touch not to refresh surfaced t_life, got %+v", untrustedDetail.Node)
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
	`, nodeID, workspaceID, seedA, seedStar, seedAcc, seedN, seedQ, seedH, seedHot, seedWarm, seedGc, seedAcc); err != nil {
		t.Fatalf("reseed salience row: %v", err)
	}

	beforeTrusted := time.Now().UTC()
	if err := store.TouchMemoryNodeTrusted(ctx, workspaceID, nodeID, 0.0, DefaultRMPSalienceConfig()); err != nil {
		t.Fatalf("trusted touch: %v", err)
	}
	afterTrusted := time.Now().UTC()
	trustedDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get trusted memory graph node: %v", err)
	}
	if !trustedDetail.Node.Protect {
		t.Fatalf("expected trusted touch to preserve protect=true, got %+v", trustedDetail.Node)
	}
	if trustedDetail.Node.SemanticLineageID != baseline.Node.SemanticLineageID || trustedDetail.Node.Revision != baseline.Node.Revision {
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

func TestTouchMemoryNodeTrustedRecomputesRetentionThresholdsAndUntrustedLeavesThemStable(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-retention-thresholds"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Retention Thresholds",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "FACT",
		Title:       "Retention threshold target",
		Body:        "Retention thresholds should recompute only on trusted touch.",
		Summary:     "Retention thresholds should recompute only on trusted touch.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.6,
		Confidence:  0.7,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)

	seedNow := time.Now().UTC()
	seedStar := seedNow.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	seedAcc := seedNow.Add(-90 * time.Minute).Format(time.RFC3339Nano)
	seedH := 7200.0
	seedA := 0.75
	seedQ := 0.4
	seedN := 2
	cfg := DefaultRMPSalienceConfig()
	seedHot := rmpComputeThresholdTime(seedStar, seedH, seedA, cfg.ThetaHot)
	seedWarm := rmpComputeThresholdTime(seedStar, seedH, seedA, cfg.ThetaWarm)
	seedGc := rmpComputeThresholdTime(seedStar, seedH, seedA, cfg.ThetaGc)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, nodeID, workspaceID, seedA, seedStar, seedAcc, seedN, seedQ, seedH, seedHot, seedWarm, seedGc, seedAcc); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}

	if err := store.TouchMemoryNodeUntrusted(ctx, workspaceID, nodeID); err != nil {
		t.Fatalf("untrusted touch: %v", err)
	}
	afterUntrusted, ok, err := getSalienceRecord(t, store, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get salience record after untrusted touch: %v", err)
	}
	if !ok {
		t.Fatal("expected salience record after untrusted touch")
	}
	if afterUntrusted.T_hot != seedHot || afterUntrusted.T_warm != seedWarm || afterUntrusted.T_gc != seedGc {
		t.Fatalf("expected untrusted touch to leave retention thresholds unchanged, got %+v", afterUntrusted)
	}

	if err := store.TouchMemoryNodeTrusted(ctx, workspaceID, nodeID, 0.0, cfg); err != nil {
		t.Fatalf("trusted touch: %v", err)
	}
	afterTrusted, ok, err := getSalienceRecord(t, store, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get salience record after trusted touch: %v", err)
	}
	if !ok {
		t.Fatal("expected salience record after trusted touch")
	}

	wantHot := rmpComputeThresholdTime(afterTrusted.T_i_star, afterTrusted.H_i, afterTrusted.A_i, cfg.ThetaHot)
	wantWarm := rmpComputeThresholdTime(afterTrusted.T_i_star, afterTrusted.H_i, afterTrusted.A_i, cfg.ThetaWarm)
	wantGc := rmpComputeThresholdTime(afterTrusted.T_i_star, afterTrusted.H_i, afterTrusted.A_i, cfg.ThetaGc)
	if afterTrusted.T_hot != wantHot || afterTrusted.T_warm != wantWarm || afterTrusted.T_gc != wantGc {
		t.Fatalf("expected trusted touch to recompute retention thresholds, got %+v want hot=%s warm=%s gc=%s", afterTrusted, wantHot, wantWarm, wantGc)
	}
	if afterTrusted.T_hot == seedHot && afterTrusted.T_warm == seedWarm && afterTrusted.T_gc == seedGc {
		t.Fatalf("expected trusted touch to advance at least one retention threshold, got %+v", afterTrusted)
	}
}

func TestRMPRunBatchedPruningArchivesOnlyPastGcActiveNodes(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-batched-pruning"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Batched Pruning",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	expiredRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Expired memory",
		Body:        "Past-gc active memory should prune.",
		Summary:     "Past-gc active memory should prune.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record expired workspace memory: %v", err)
	}
	freshRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Fresh memory",
		Body:        "Future-gc active memory should survive.",
		Summary:     "Future-gc active memory should survive.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record fresh workspace memory: %v", err)
	}
	archivedRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Archived memory",
		Body:        "Already archived memory should not be reselected.",
		Summary:     "Already archived memory should not be reselected.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record archived workspace memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    archivedRecord.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "seed archived state",
	}); err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}

	now := time.Now().UTC()
	expiredNodeID := memoryGraphNodeID("workspace_memory", expiredRecord.MemoryID)
	freshNodeID := memoryGraphNodeID("workspace_memory", freshRecord.MemoryID)
	archivedNodeID := memoryGraphNodeID("workspace_memory", archivedRecord.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES
			(?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?),
			(?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?),
			(?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?)
	`, expiredNodeID, workspaceID,
		now.Add(-5*time.Hour).Format(time.RFC3339Nano),
		now.Add(-4*time.Hour).Format(time.RFC3339Nano),
		now.Add(-3*time.Hour).Format(time.RFC3339Nano),
		now.Add(-2*time.Hour).Format(time.RFC3339Nano),
		now.Add(-time.Minute).Format(time.RFC3339Nano),
		now.Add(-4*time.Hour).Format(time.RFC3339Nano),
		freshNodeID, workspaceID,
		now.Add(-2*time.Hour).Format(time.RFC3339Nano),
		now.Add(-90*time.Minute).Format(time.RFC3339Nano),
		now.Add(time.Hour).Format(time.RFC3339Nano),
		now.Add(2*time.Hour).Format(time.RFC3339Nano),
		now.Add(3*time.Hour).Format(time.RFC3339Nano),
		now.Add(-90*time.Minute).Format(time.RFC3339Nano),
		archivedNodeID, workspaceID,
		now.Add(-5*time.Hour).Format(time.RFC3339Nano),
		now.Add(-4*time.Hour).Format(time.RFC3339Nano),
		now.Add(-3*time.Hour).Format(time.RFC3339Nano),
		now.Add(-2*time.Hour).Format(time.RFC3339Nano),
		now.Add(-time.Minute).Format(time.RFC3339Nano),
		now.Add(-4*time.Hour).Format(time.RFC3339Nano),
	); err != nil {
		t.Fatalf("seed salience rows for pruning: %v", err)
	}

	pruned, err := store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("run batched pruning: %v", err)
	}
	if len(pruned) != 1 || pruned[0] != expiredNodeID {
		t.Fatalf("expected only expired active node to prune, got %+v", pruned)
	}

	expiredDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, expiredNodeID)
	if err != nil {
		t.Fatalf("get expired pruned node: %v", err)
	}
	if expiredDetail.Node.LifecycleState != "ARCHIVED" || expiredDetail.Node.ArchivedAt == nil {
		t.Fatalf("expected expired node to be archived by pruning, got %+v", expiredDetail.Node)
	}
	if expiredDetail.Node.ArchivedReason != rmpArchivedReasonExpired {
		t.Fatalf("expected expired node prune trace %q, got %+v", rmpArchivedReasonExpired, expiredDetail.Node)
	}
	expiredRecordAfter, err := store.GetWorkspaceMemory(ctx, workspaceID, expiredRecord.MemoryID)
	if err != nil {
		t.Fatalf("get expired workspace memory after prune: %v", err)
	}
	if expiredRecordAfter.ArchivedAt == nil || strings.TrimSpace(expiredRecordAfter.ArchivedReason) != rmpArchivedReasonExpired {
		t.Fatalf("expected canonical workspace memory archive trace after prune, got %+v", expiredRecordAfter)
	}
	activeItems, err := store.ListWorkspaceMemory(ctx, WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list active workspace memory after prune: %v", err)
	}
	for _, item := range activeItems {
		if item.MemoryID == expiredRecord.MemoryID {
			t.Fatalf("did not expect auto-pruned memory in active workspace list, got %+v", activeItems)
		}
	}
	allItems, err := store.ListWorkspaceMemory(ctx, WorkspaceMemoryFilter{
		WorkspaceID:     workspaceID,
		IncludeArchived: true,
		Limit:           20,
	})
	if err != nil {
		t.Fatalf("list archived workspace memory after prune: %v", err)
	}
	foundArchived := false
	for _, item := range allItems {
		if item.MemoryID != expiredRecord.MemoryID {
			continue
		}
		foundArchived = true
		if item.ArchivedAt == nil || strings.TrimSpace(item.ArchivedReason) != rmpArchivedReasonExpired {
			t.Fatalf("expected archived workspace memory list trace after prune, got %+v", item)
		}
	}
	if !foundArchived {
		t.Fatalf("expected archived workspace memory to remain queryable with include_archived, got %+v", allItems)
	}

	freshDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, freshNodeID)
	if err != nil {
		t.Fatalf("get fresh node: %v", err)
	}
	if freshDetail.Node.LifecycleState != "ACTIVE" || freshDetail.Node.ArchivedAt != nil {
		t.Fatalf("expected future-gc node to remain active, got %+v", freshDetail.Node)
	}

	archivedDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, archivedNodeID)
	if err != nil {
		t.Fatalf("get already archived node: %v", err)
	}
	if archivedDetail.Node.LifecycleState != "ARCHIVED" || archivedDetail.Node.ArchivedAt == nil {
		t.Fatalf("expected previously archived node to stay archived, got %+v", archivedDetail.Node)
	}
	if archivedDetail.Node.ArchivedReason != "seed archived state" {
		t.Fatalf("expected existing archive reason to stay intact, got %+v", archivedDetail.Node)
	}
}

func TestRMPRunBatchedPruningArchivesDormantAndSupersededPastGcNodes(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-batched-pruning-lifecycle-span"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Batched Pruning Lifecycle Span",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	activeRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Active stale lesson",
		Body:        "Active expired lesson should prune.",
		Summary:     "Active stale lesson.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record active workspace memory: %v", err)
	}
	dormantRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Dormant stale lesson",
		Body:        "Dormant expired lesson should also prune.",
		Summary:     "Dormant stale lesson.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record dormant workspace memory: %v", err)
	}
	supersededRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Superseded stale lesson",
		Body:        "Superseded expired lesson should also prune.",
		Summary:     "Superseded stale lesson.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record superseded workspace memory: %v", err)
	}

	activeNodeID := memoryGraphNodeID("workspace_memory", activeRecord.MemoryID)
	dormantNodeID := memoryGraphNodeID("workspace_memory", dormantRecord.MemoryID)
	supersededNodeID := memoryGraphNodeID("workspace_memory", supersededRecord.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE memory_nodes
		   SET lifecycle_state = CASE memory_id
		     WHEN ? THEN 'DORMANT'
		     WHEN ? THEN 'SUPERSEDED'
		     ELSE lifecycle_state
		   END
		 WHERE workspace_id = ? AND memory_id IN (?, ?)
	`, dormantNodeID, supersededNodeID, workspaceID, dormantNodeID, supersededNodeID); err != nil {
		t.Fatalf("seed dormant/superseded lifecycle states: %v", err)
	}

	now := time.Now().UTC()
	pastStar := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	pastAcc := now.Add(-4 * time.Hour).Format(time.RFC3339Nano)
	pastHot := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	pastWarm := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	pastGc := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES
			(?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?),
			(?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?),
			(?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?)
	`, activeNodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc,
		dormantNodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc,
		supersededNodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc); err != nil {
		t.Fatalf("seed salience rows for lifecycle-span pruning: %v", err)
	}

	pruned, err := store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("run batched pruning: %v", err)
	}
	gotSet := map[string]struct{}{}
	for _, id := range pruned {
		gotSet[id] = struct{}{}
	}
	for _, expectedID := range []string{activeNodeID, dormantNodeID, supersededNodeID} {
		if _, ok := gotSet[expectedID]; !ok {
			t.Fatalf("expected lifecycle-span prune set to include %s, got %+v", expectedID, pruned)
		}
	}
	if len(gotSet) != 3 {
		t.Fatalf("expected exactly three pruned nodes, got %+v", pruned)
	}

	for _, nodeID := range []string{activeNodeID, dormantNodeID, supersededNodeID} {
		detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
		if err != nil {
			t.Fatalf("get lifecycle-span pruned node %s: %v", nodeID, err)
		}
		if detail.Node.LifecycleState != "ARCHIVED" || detail.Node.ArchivedAt == nil {
			t.Fatalf("expected node %s to become archived, got %+v", nodeID, detail.Node)
		}
		if detail.Node.ArchivedReason != rmpArchivedReasonExpired {
			t.Fatalf("expected node %s prune trace %q, got %+v", nodeID, rmpArchivedReasonExpired, detail.Node)
		}
	}
	for _, memoryID := range []string{activeRecord.MemoryID, dormantRecord.MemoryID, supersededRecord.MemoryID} {
		record, err := store.GetWorkspaceMemory(ctx, workspaceID, memoryID)
		if err != nil {
			t.Fatalf("get pruned workspace memory %s: %v", memoryID, err)
		}
		if record.ArchivedAt == nil || strings.TrimSpace(record.ArchivedReason) != rmpArchivedReasonExpired {
			t.Fatalf("expected canonical workspace memory archive trace for %s, got %+v", memoryID, record)
		}
	}
}

func TestRMPRunBatchedPruningSkipsProtectedAndDissentNodesEvenWhenPastGc(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-batched-pruning-safety"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Batched Pruning Safety",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ordinaryRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Ordinary stale lesson",
		Body:        "An ordinary stale lesson may be pruned once gc threshold passes.",
		Summary:     "Ordinary stale lesson.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record ordinary workspace memory: %v", err)
	}
	protectedRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "procedure",
		Title:       "Protected rollback procedure",
		Body:        "Protected procedure should not be auto-pruned purely on gc age.",
		Summary:     "Protected rollback procedure.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record protected workspace memory: %v", err)
	}
	dissentClaim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-pruning-dissent",
		ClaimType:   "dissent",
		Status:      "draft",
		Subject:     "Rollback-first branch",
		Body:        "Dissenting or alternative branch memory should stay available beyond naive gc pruning.",
		Summary:     "Keep dissent branch visible.",
		Confidence:  0.7,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record dissent claim: %v", err)
	}
	dissentMarkerClaim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-pruning-dissent-marker",
		ClaimType:   "dissent_marker",
		Status:      "draft",
		Subject:     "Rollback disagreement exists",
		Body:        "Dissent marker should stay available beyond naive gc pruning.",
		Summary:     "Keep dissent marker visible.",
		Confidence:  0.7,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record dissent marker claim: %v", err)
	}
	dissentContentClaim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-pruning-dissent-content",
		ClaimType:   "dissent_content",
		Status:      "draft",
		Subject:     "Rollback counter-argument",
		Body:        "Dissent content should stay available beyond naive gc pruning.",
		Summary:     "Keep dissent content visible.",
		Confidence:  0.7,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record dissent content claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	now := time.Now().UTC()
	ordinaryNodeID := memoryGraphNodeID("workspace_memory", ordinaryRecord.MemoryID)
	protectedNodeID := memoryGraphNodeID("workspace_memory", protectedRecord.MemoryID)
	dissentNodeID := memoryGraphNodeID("knowledge_claim", dissentClaim.ClaimID)
	dissentMarkerNodeID := memoryGraphNodeID("knowledge_claim", dissentMarkerClaim.ClaimID)
	dissentContentNodeID := memoryGraphNodeID("knowledge_claim", dissentContentClaim.ClaimID)
	pastStar := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	pastAcc := now.Add(-4 * time.Hour).Format(time.RFC3339Nano)
	pastHot := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	pastWarm := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	pastGc := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES
			(?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?),
			(?, ?, 0.8, ?, ?, 5, 0.2, 7200, ?, ?, ?, ?),
			(?, ?, 0.7, ?, ?, 4, 0.2, 5400, ?, ?, ?, ?),
			(?, ?, 0.7, ?, ?, 4, 0.2, 5400, ?, ?, ?, ?),
			(?, ?, 0.7, ?, ?, 4, 0.2, 5400, ?, ?, ?, ?)
	`, ordinaryNodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc,
		protectedNodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc,
		dissentNodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc,
		dissentMarkerNodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc,
		dissentContentNodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc); err != nil {
		t.Fatalf("seed salience rows for pruning safety: %v", err)
	}

	pruned, err := store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("run batched pruning: %v", err)
	}
	if len(pruned) != 1 || pruned[0] != ordinaryNodeID {
		t.Fatalf("expected only ordinary stale node to prune, got %+v", pruned)
	}

	ordinaryDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, ordinaryNodeID)
	if err != nil {
		t.Fatalf("get ordinary pruned node: %v", err)
	}
	if ordinaryDetail.Node.LifecycleState != "ARCHIVED" || ordinaryDetail.Node.ArchivedAt == nil {
		t.Fatalf("expected ordinary node to be archived by pruning, got %+v", ordinaryDetail.Node)
	}

	protectedDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, protectedNodeID)
	if err != nil {
		t.Fatalf("get protected node: %v", err)
	}
	if !protectedDetail.Node.Protect {
		t.Fatalf("expected procedure node to remain protect=true, got %+v", protectedDetail.Node)
	}
	if protectedDetail.Node.LifecycleState != "ACTIVE" || protectedDetail.Node.ArchivedAt != nil {
		t.Fatalf("expected protected node to stay active despite past gc, got %+v", protectedDetail.Node)
	}
	if protectedDetail.Node.RetentionBand != "PRUNABLE" || protectedDetail.Node.RetentionPrunable || protectedDetail.Node.RetentionGuardReason != "PROTECT" {
		t.Fatalf("expected protected node retention guard surface, got %+v", protectedDetail.Node)
	}

	dissentDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, dissentNodeID)
	if err != nil {
		t.Fatalf("get dissent node: %v", err)
	}
	if dissentDetail.Node.MemoryType != "DISSENT" {
		t.Fatalf("expected dissent node type to stay DISSENT, got %+v", dissentDetail.Node)
	}
	if dissentDetail.Node.LifecycleState != "ACTIVE" || dissentDetail.Node.ArchivedAt != nil {
		t.Fatalf("expected dissent node to stay active despite past gc, got %+v", dissentDetail.Node)
	}
	if dissentDetail.Node.RetentionBand != "PRUNABLE" || dissentDetail.Node.RetentionPrunable || dissentDetail.Node.RetentionGuardReason != "DISSENT" {
		t.Fatalf("expected dissent node retention guard surface, got %+v", dissentDetail.Node)
	}

	dissentMarkerDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, dissentMarkerNodeID)
	if err != nil {
		t.Fatalf("get dissent marker node: %v", err)
	}
	if dissentMarkerDetail.Node.MemoryType != "DISSENT_MARKER" {
		t.Fatalf("expected dissent marker node type to stay DISSENT_MARKER, got %+v", dissentMarkerDetail.Node)
	}
	if dissentMarkerDetail.Node.LifecycleState != "ACTIVE" || dissentMarkerDetail.Node.ArchivedAt != nil {
		t.Fatalf("expected dissent marker node to stay active despite past gc, got %+v", dissentMarkerDetail.Node)
	}
	if dissentMarkerDetail.Node.RetentionBand != "PRUNABLE" || dissentMarkerDetail.Node.RetentionPrunable || dissentMarkerDetail.Node.RetentionGuardReason != "DISSENT_MARKER" {
		t.Fatalf("expected dissent marker retention guard surface, got %+v", dissentMarkerDetail.Node)
	}

	dissentContentDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, dissentContentNodeID)
	if err != nil {
		t.Fatalf("get dissent content node: %v", err)
	}
	if dissentContentDetail.Node.MemoryType != "DISSENT_CONTENT" {
		t.Fatalf("expected dissent content node type to stay DISSENT_CONTENT, got %+v", dissentContentDetail.Node)
	}
	if dissentContentDetail.Node.LifecycleState != "ACTIVE" || dissentContentDetail.Node.ArchivedAt != nil {
		t.Fatalf("expected dissent content node to stay active despite past gc, got %+v", dissentContentDetail.Node)
	}
	if dissentContentDetail.Node.RetentionBand != "PRUNABLE" || dissentContentDetail.Node.RetentionPrunable || dissentContentDetail.Node.RetentionGuardReason != "DISSENT_CONTENT" {
		t.Fatalf("expected dissent content retention guard surface, got %+v", dissentContentDetail.Node)
	}
}

func TestRMPRunBatchedPruningSkipsUnresolvedNodesEvenWhenPastGc(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-batched-pruning-unresolved"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Batched Pruning Unresolved",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ordinaryRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Ordinary stale lesson",
		Body:        "Ordinary stale lesson should still prune.",
		Summary:     "Ordinary stale lesson.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record ordinary workspace memory: %v", err)
	}
	unresolvedRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "update_digest",
		Title:       "Handoff pending",
		Body:        "Unresolved handoff memory should not auto-prune purely on gc age.",
		Summary:     "Handoff pending.",
		SourceKind:  "session_event",
		SourceID:    "sess-pruning-unresolved",
	})
	if err != nil {
		t.Fatalf("record unresolved workspace memory: %v", err)
	}

	now := time.Now().UTC()
	ordinaryNodeID := memoryGraphNodeID("workspace_memory", ordinaryRecord.MemoryID)
	unresolvedNodeID := memoryGraphNodeID("workspace_memory", unresolvedRecord.MemoryID)
	pastStar := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	pastAcc := now.Add(-4 * time.Hour).Format(time.RFC3339Nano)
	pastHot := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	pastWarm := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	pastGc := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES
			(?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?),
			(?, ?, 0.6, ?, ?, 3, 0.2, 3600, ?, ?, ?, ?)
	`, ordinaryNodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc,
		unresolvedNodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc); err != nil {
		t.Fatalf("seed salience rows for unresolved pruning safety: %v", err)
	}

	pruned, err := store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("run batched pruning: %v", err)
	}
	if len(pruned) != 1 || pruned[0] != ordinaryNodeID {
		t.Fatalf("expected only ordinary stale node to prune, got %+v", pruned)
	}

	ordinaryDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, ordinaryNodeID)
	if err != nil {
		t.Fatalf("get ordinary pruned node: %v", err)
	}
	if ordinaryDetail.Node.LifecycleState != "ARCHIVED" || ordinaryDetail.Node.ArchivedAt == nil {
		t.Fatalf("expected ordinary node to be archived by pruning, got %+v", ordinaryDetail.Node)
	}

	unresolvedDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, unresolvedNodeID)
	if err != nil {
		t.Fatalf("get unresolved node: %v", err)
	}
	if !unresolvedDetail.Node.Unresolved {
		t.Fatalf("expected unresolved node to stay unresolved, got %+v", unresolvedDetail.Node)
	}
	if unresolvedDetail.Node.LifecycleState != "ACTIVE" || unresolvedDetail.Node.ArchivedAt != nil {
		t.Fatalf("expected unresolved node to stay active despite past gc, got %+v", unresolvedDetail.Node)
	}
	if unresolvedDetail.Node.RetentionBand != "PRUNABLE" || unresolvedDetail.Node.RetentionPrunable || unresolvedDetail.Node.RetentionGuardReason != "UNRESOLVED" {
		t.Fatalf("expected unresolved node retention guard surface, got %+v", unresolvedDetail.Node)
	}
}

func TestRMPRunBatchedPruningAutoPrunedWorkspaceMemoryCanBeRestored(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-salience-batched-pruning-restore"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Salience Batched Pruning Restore",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Recoverable stale lesson",
		Body:        "Auto-pruned workspace memory should remain recoverable on the same identity.",
		Summary:     "Recoverable stale lesson.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	now := time.Now().UTC()
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	pastStar := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	pastAcc := now.Add(-4 * time.Hour).Format(time.RFC3339Nano)
	pastHot := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	pastWarm := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	pastGc := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?)
	`, nodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc); err != nil {
		t.Fatalf("seed salience row for recoverable prune: %v", err)
	}

	pruned, err := store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("run batched pruning: %v", err)
	}
	if len(pruned) != 1 || pruned[0] != nodeID {
		t.Fatalf("expected recoverable node to auto-prune, got %+v", pruned)
	}

	archivedRecord, err := store.GetWorkspaceMemory(ctx, workspaceID, record.MemoryID)
	if err != nil {
		t.Fatalf("get auto-pruned workspace memory: %v", err)
	}
	if archivedRecord.ArchivedAt == nil || strings.TrimSpace(archivedRecord.ArchivedReason) != rmpArchivedReasonExpired {
		t.Fatalf("expected auto-pruned raw record archive trace, got %+v", archivedRecord)
	}
	archivedDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get auto-pruned graph node: %v", err)
	}
	if archivedDetail.Node.LifecycleState != "ARCHIVED" || archivedDetail.Node.ArchivedAt == nil || archivedDetail.Node.ArchivedReason != rmpArchivedReasonExpired {
		t.Fatalf("expected auto-pruned graph detail archive trace, got %+v", archivedDetail.Node)
	}

	restored, err := store.RestoreWorkspaceMemory(ctx, WorkspaceMemoryRestoreInput{
		WorkspaceID:    workspaceID,
		MemoryID:       record.MemoryID,
		RestoredBy:     "developer",
		RecoveryReason: "rmp_gc_reactivated",
	})
	if err != nil {
		t.Fatalf("restore auto-pruned workspace memory: %v", err)
	}
	if restored.MemoryID != record.MemoryID {
		t.Fatalf("expected restore to preserve memory id %s, got %+v", record.MemoryID, restored)
	}
	if restored.ArchivedAt != nil || restored.ArchivedReason != "" || restored.RecoveryReason != "rmp_gc_reactivated" {
		t.Fatalf("expected restore to clear archive fields and surface recovery trace, got %+v", restored)
	}

	restoredDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get restored graph node: %v", err)
	}
	if restoredDetail.Node.LifecycleState != "ACTIVE" || restoredDetail.Node.ArchivedAt != nil {
		t.Fatalf("expected restored graph node to reactivate, got %+v", restoredDetail.Node)
	}
	if restoredDetail.Node.RecoveryReason != "rmp_gc_reactivated" {
		t.Fatalf("expected restored graph node to surface recovery trace, got %+v", restoredDetail.Node)
	}
	if restoredDetail.Node.SemanticLineageID != "workspace_memory:"+record.MemoryID {
		t.Fatalf("expected restore to preserve semantic lineage, got %+v", restoredDetail.Node)
	}

	activeItems, err := store.ListWorkspaceMemory(ctx, WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list active workspace memory after restore: %v", err)
	}
	foundActive := false
	for _, item := range activeItems {
		if item.MemoryID != record.MemoryID {
			continue
		}
		foundActive = true
		if item.ArchivedAt != nil || item.RecoveryReason != "rmp_gc_reactivated" {
			t.Fatalf("expected restored active item to clear archive fields and keep recovery trace, got %+v", item)
		}
	}
	if !foundActive {
		t.Fatalf("expected restored memory to reappear in active workspace list, got %+v", activeItems)
	}
}

func getSalienceRecord(t *testing.T, store *Store, workspaceID, memoryID string) (MemoryNodeSalienceRecord, bool, error) {
	t.Helper()

	batch, err := store.GetMemoryNodeSalienceBatch(context.Background(), workspaceID, []string{memoryID})
	if err != nil {
		return MemoryNodeSalienceRecord{}, false, err
	}
	rec, ok := batch[memoryID]
	return rec, ok, nil
}

func decodeMemoryNodeTouchPayload(t *testing.T, raw string) map[string]any {
	t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("decode memory node touch payload %q: %v", raw, err)
	}
	return decoded
}
