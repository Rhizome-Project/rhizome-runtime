package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestProceduralAndAntiProcedureSurfaceEquivalentProtectRetentionGuards(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-procedural-retention-guards"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Procedural Retention Guards",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	procedure, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-procedural-guard-procedure",
		MemoryType:  "PROCEDURE",
		Title:       "Deploy procedure",
		Body:        "Procedural memory should stay protected even after retention expiry.",
		Summary:     "Deploy procedure.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record procedure memory: %v", err)
	}
	antiProcedure, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-procedural-guard-anti-procedure",
		MemoryType:  "ANTI_PROCEDURE",
		Title:       "Forbidden deploy shortcut",
		Body:        "Anti-procedural memory should inherit the same protected retention guard.",
		Summary:     "Forbidden deploy shortcut.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record anti-procedure memory: %v", err)
	}

	now := time.Now().UTC()
	pastStar := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	pastAcc := now.Add(-4 * time.Hour).Format(time.RFC3339Nano)
	pastHot := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	pastWarm := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	pastGc := now.Add(-time.Minute).Format(time.RFC3339Nano)

	seedPastGcSalienceForMemoryNode(t, ctx, store, workspaceID, memoryGraphNodeID("workspace_memory", procedure.MemoryID), pastStar, pastAcc, pastHot, pastWarm, pastGc)
	seedPastGcSalienceForMemoryNode(t, ctx, store, workspaceID, memoryGraphNodeID("workspace_memory", antiProcedure.MemoryID), pastStar, pastAcc, pastHot, pastWarm, pastGc)

	procedureDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("workspace_memory", procedure.MemoryID))
	if err != nil {
		t.Fatalf("get procedure graph node: %v", err)
	}
	antiProcedureDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("workspace_memory", antiProcedure.MemoryID))
	if err != nil {
		t.Fatalf("get anti-procedure graph node: %v", err)
	}

	if !procedureDetail.Node.Protect || !antiProcedureDetail.Node.Protect {
		t.Fatalf("expected both procedural variants to surface protect=true, procedure=%+v anti=%+v", procedureDetail.Node, antiProcedureDetail.Node)
	}
	if procedureDetail.Node.MemoryLayer != "PROCEDURAL" || antiProcedureDetail.Node.MemoryLayer != "PROCEDURAL" {
		t.Fatalf("expected both procedural variants to stay in PROCEDURAL layer, procedure=%+v anti=%+v", procedureDetail.Node, antiProcedureDetail.Node)
	}
	if procedureDetail.Node.RetentionBand != "PRUNABLE" || antiProcedureDetail.Node.RetentionBand != "PRUNABLE" {
		t.Fatalf("expected past-gc procedural variants to surface PRUNABLE retention band, procedure=%+v anti=%+v", procedureDetail.Node, antiProcedureDetail.Node)
	}
	if procedureDetail.Node.RetentionPrunable || antiProcedureDetail.Node.RetentionPrunable {
		t.Fatalf("expected protected procedural variants to stay non-prunable, procedure=%+v anti=%+v", procedureDetail.Node, antiProcedureDetail.Node)
	}
	if procedureDetail.Node.RetentionGuardReason != "PROTECT" || antiProcedureDetail.Node.RetentionGuardReason != "PROTECT" {
		t.Fatalf("expected protected retention guard reason for both procedural variants, procedure=%+v anti=%+v", procedureDetail.Node, antiProcedureDetail.Node)
	}
}

func TestRMPRunBatchedPruningSkipsAntiProcedureLikeProcedureWhenPastGc(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-procedural-pruning-parity"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Procedural Pruning Parity",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ordinary, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-procedural-pruning-ordinary",
		MemoryType:  "LESSON",
		Title:       "Ordinary stale lesson",
		Body:        "Ordinary stale lesson should still auto-prune.",
		Summary:     "Ordinary stale lesson.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record ordinary memory: %v", err)
	}
	procedure, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-procedural-pruning-procedure",
		MemoryType:  "PROCEDURE",
		Title:       "Deploy procedure",
		Body:        "Protected procedural memory should not auto-prune.",
		Summary:     "Deploy procedure.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record procedure memory: %v", err)
	}
	antiProcedure, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-procedural-pruning-anti-procedure",
		MemoryType:  "ANTI_PROCEDURE",
		Title:       "Forbidden deploy shortcut",
		Body:        "Protected anti-procedural memory should not auto-prune either.",
		Summary:     "Forbidden deploy shortcut.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record anti-procedure memory: %v", err)
	}

	now := time.Now().UTC()
	pastStar := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	pastAcc := now.Add(-4 * time.Hour).Format(time.RFC3339Nano)
	pastHot := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	pastWarm := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	pastGc := now.Add(-time.Minute).Format(time.RFC3339Nano)

	seedPastGcSalienceForMemoryNode(t, ctx, store, workspaceID, memoryGraphNodeID("workspace_memory", ordinary.MemoryID), pastStar, pastAcc, pastHot, pastWarm, pastGc)
	seedPastGcSalienceForMemoryNode(t, ctx, store, workspaceID, memoryGraphNodeID("workspace_memory", procedure.MemoryID), pastStar, pastAcc, pastHot, pastWarm, pastGc)
	seedPastGcSalienceForMemoryNode(t, ctx, store, workspaceID, memoryGraphNodeID("workspace_memory", antiProcedure.MemoryID), pastStar, pastAcc, pastHot, pastWarm, pastGc)

	pruned, err := store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("run batched pruning: %v", err)
	}
	ordinaryNodeID := memoryGraphNodeID("workspace_memory", ordinary.MemoryID)
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

	procedureDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("workspace_memory", procedure.MemoryID))
	if err != nil {
		t.Fatalf("get procedure node: %v", err)
	}
	antiProcedureDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("workspace_memory", antiProcedure.MemoryID))
	if err != nil {
		t.Fatalf("get anti-procedure node: %v", err)
	}
	if procedureDetail.Node.LifecycleState != "ACTIVE" || procedureDetail.Node.ArchivedAt != nil {
		t.Fatalf("expected procedure to stay active despite past gc, got %+v", procedureDetail.Node)
	}
	if antiProcedureDetail.Node.LifecycleState != "ACTIVE" || antiProcedureDetail.Node.ArchivedAt != nil {
		t.Fatalf("expected anti-procedure to stay active despite past gc, got %+v", antiProcedureDetail.Node)
	}
	if procedureDetail.Node.RetentionGuardReason != "PROTECT" || antiProcedureDetail.Node.RetentionGuardReason != "PROTECT" {
		t.Fatalf("expected protect retention guard on both procedural variants after pruning, procedure=%+v anti=%+v", procedureDetail.Node, antiProcedureDetail.Node)
	}

	procedureRecord, err := store.GetWorkspaceMemory(ctx, workspaceID, procedure.MemoryID)
	if err != nil {
		t.Fatalf("get procedure workspace memory: %v", err)
	}
	antiProcedureRecord, err := store.GetWorkspaceMemory(ctx, workspaceID, antiProcedure.MemoryID)
	if err != nil {
		t.Fatalf("get anti-procedure workspace memory: %v", err)
	}
	if procedureRecord.ArchivedAt != nil || antiProcedureRecord.ArchivedAt != nil {
		t.Fatalf("expected protected procedural raw records to stay unarchived, procedure=%+v anti=%+v", procedureRecord, antiProcedureRecord)
	}
}

func seedPastGcSalienceForMemoryNode(t *testing.T, ctx context.Context, store *Store, workspaceID, nodeID, pastStar, pastAcc, pastHot, pastWarm, pastGc string) {
	t.Helper()
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, 0.6, ?, ?, 2, 0.2, 3600, ?, ?, ?, ?)
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
	`, nodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc); err != nil {
		t.Fatalf("seed salience row for %s: %v", nodeID, err)
	}
}
