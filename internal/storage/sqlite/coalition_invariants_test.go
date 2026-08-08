package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCoalitionJoinOfferRejectedFirstAttachDisbandsEmptyCoalition(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-empty-cleanup"
		tensionID   = "tension-coalition-empty-cleanup"
		taskID      = "task-coalition-empty-cleanup"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-fit", "agent-other")

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE workspace_tensions
		SET task_ids_json = ?, agent_ids_json = ?, session_ids_json = ?, updated_at = ?
		WHERE workspace_id = ? AND tension_id = ?`,
		`[]`,
		`["agent-other"]`,
		`["sess-other"]`,
		now,
		workspaceID,
		tensionID,
	); err != nil {
		t.Fatalf("tighten tension anchors: %v", err)
	}

	_, err := store.CoalitionJoinOffer(ctx, CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      tensionID,
		AgentID:     "agent-fit",
		Role:        "PRIMARY",
	})
	if !errors.Is(err, ErrCoalitionAttachmentRejected) {
		t.Fatalf("expected rejected attachment envelope error, got %v", err)
	}

	var liveCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM workspace_coalitions WHERE workspace_id = ? AND tension_id = ? AND status IN ('FORMING', 'ACTIVE')`,
		workspaceID,
		tensionID,
	).Scan(&liveCount); err != nil {
		t.Fatalf("count live coalitions: %v", err)
	}
	if liveCount != 0 {
		t.Fatalf("expected rejected first attach to leave no live coalition rows, got %d", liveCount)
	}

	var disbandedCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM workspace_coalitions WHERE workspace_id = ? AND tension_id = ? AND status = 'DISBANDED'`,
		workspaceID,
		tensionID,
	).Scan(&disbandedCount); err != nil {
		t.Fatalf("count disbanded coalitions: %v", err)
	}
	if disbandedCount != 1 {
		t.Fatalf("expected rejected first attach to leave one disbanded cleanup row, got %d", disbandedCount)
	}
}

func TestGetCoalitionByWorkspacePropagatesUnexpectedTensionLookupFailure(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-error-propagation"
		tensionID   = "tension-coalition-error-propagation"
		taskID      = "task-coalition-error-propagation"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-propagation")

	if _, err := store.CoalitionJoinOffer(ctx, CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-propagation",
		Role:        "PRIMARY",
	}); err != nil {
		t.Fatalf("seed coalition offer: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `ALTER TABLE workspace_tensions RENAME TO workspace_tensions_broken`); err != nil {
		t.Fatalf("rename workspace_tensions: %v", err)
	}

	if _, err := store.GetCoalitionByWorkspace(ctx, workspaceID); err == nil {
		t.Fatalf("expected coalition status read to propagate tension lookup failure")
	}
}

func TestGetCoalitionByWorkspaceSurfacesIntegrityDriftWithoutMutatingDuplicateRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-integrity-duplicate"
		tensionID          = "tension-coalition-integrity-duplicate"
		taskID             = "task-coalition-integrity-duplicate"
		canonicalID        = "coalition-integrity-canonical"
		duplicateID        = "coalition-integrity-shadow"
		canonicalCreatedAt = "2026-04-09T08:00:00Z"
		duplicateCreatedAt = "2026-04-09T08:05:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-alpha", "agent-beta")

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, canonicalID, "ACTIVE", 4, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-alpha", "GENERATOR", 0.88, 0.42, 4, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-beta", "NEAR_REVIEWER", 0.81, 0.39, 4, canonicalCreatedAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, duplicateID, "FORMING", 5, duplicateCreatedAt)

	status, err := store.GetCoalitionByWorkspace(ctx, workspaceID)
	if err != nil {
		t.Fatalf("get coalition by workspace: %v", err)
	}

	coalitions, ok := status["coalitions"].([]WorkspaceCoalition)
	if !ok || len(coalitions) != 1 || coalitions[0].CoalitionID != canonicalID {
		t.Fatalf("expected one canonical coalition in read surface, got %+v", status["coalitions"])
	}

	integrity, ok := status["integrity"].(WorkspaceCoalitionIntegrityReport)
	if !ok {
		t.Fatalf("expected integrity report, got %T", status["integrity"])
	}
	if integrity.State != WorkspaceCoalitionIntegrityDrift {
		t.Fatalf("expected duplicate live coalition rows to surface drift, got %+v", integrity)
	}
	if len(integrity.Items) != 1 {
		t.Fatalf("expected one integrity item, got %+v", integrity.Items)
	}
	item := integrity.Items[0]
	if item.CanonicalCoalitionID != canonicalID {
		t.Fatalf("expected integrity item to point at canonical coalition %s, got %+v", canonicalID, item)
	}
	if !coalitionIntegrityIssuePresent(item.Issues, "DUPLICATE_LIVE_COALITIONS") {
		t.Fatalf("expected duplicate-live issue, got %+v", item.Issues)
	}
	if len(item.ShadowCoalitionIDs) != 1 || item.ShadowCoalitionIDs[0] != duplicateID {
		t.Fatalf("expected shadow coalition id %s, got %+v", duplicateID, item.ShadowCoalitionIDs)
	}

	assertCoalitionStatuses(t, ctx, store, workspaceID, map[string]string{
		canonicalID: "ACTIVE",
		duplicateID: "FORMING",
	})
}

func TestGetCoalitionByWorkspaceRepeatedReadsKeepIntegrityDriftAndShadowRowUntouched(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-integrity-repeated"
		tensionID          = "tension-coalition-integrity-repeated"
		taskID             = "task-coalition-integrity-repeated"
		canonicalID        = "coalition-integrity-repeated-canonical"
		duplicateID        = "coalition-integrity-repeated-shadow"
		canonicalCreatedAt = "2026-04-09T16:00:00Z"
		duplicateCreatedAt = "2026-04-09T16:05:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-alpha", "agent-beta")

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, canonicalID, "ACTIVE", 4, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-alpha", "GENERATOR", 0.88, 0.42, 4, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-beta", "NEAR_REVIEWER", 0.81, 0.39, 4, canonicalCreatedAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, duplicateID, "FORMING", 5, duplicateCreatedAt)

	canonicalBefore := loadCoalitionStatusSnapshot(t, ctx, store, workspaceID, canonicalID)
	shadowBefore := loadCoalitionStatusSnapshot(t, ctx, store, workspaceID, duplicateID)

	for readIdx := 0; readIdx < 2; readIdx++ {
		status, err := store.GetCoalitionByWorkspace(ctx, workspaceID)
		if err != nil {
			t.Fatalf("get coalition by workspace on read %d: %v", readIdx+1, err)
		}

		coalitions, ok := status["coalitions"].([]WorkspaceCoalition)
		if !ok || len(coalitions) != 1 || coalitions[0].CoalitionID != canonicalID {
			t.Fatalf("expected canonical coalition on read %d, got %+v", readIdx+1, status["coalitions"])
		}

		integrity, ok := status["integrity"].(WorkspaceCoalitionIntegrityReport)
		if !ok {
			t.Fatalf("expected integrity report on read %d, got %T", readIdx+1, status["integrity"])
		}
		if integrity.State != WorkspaceCoalitionIntegrityDrift {
			t.Fatalf("expected integrity drift on read %d, got %+v", readIdx+1, integrity)
		}
		if len(integrity.Items) != 1 {
			t.Fatalf("expected one integrity item on read %d, got %+v", readIdx+1, integrity.Items)
		}
		item := integrity.Items[0]
		if item.CanonicalCoalitionID != canonicalID {
			t.Fatalf("expected canonical integrity item %s on read %d, got %+v", canonicalID, readIdx+1, item)
		}
		if len(item.ShadowCoalitionIDs) != 1 || item.ShadowCoalitionIDs[0] != duplicateID {
			t.Fatalf("expected shadow coalition %s on read %d, got %+v", duplicateID, readIdx+1, item.ShadowCoalitionIDs)
		}
	}

	canonicalAfter := loadCoalitionStatusSnapshot(t, ctx, store, workspaceID, canonicalID)
	shadowAfter := loadCoalitionStatusSnapshot(t, ctx, store, workspaceID, duplicateID)
	if canonicalAfter != canonicalBefore {
		t.Fatalf("expected repeated coalition.status reads to avoid mutating canonical row, before=%+v after=%+v", canonicalBefore, canonicalAfter)
	}
	if shadowAfter != shadowBefore {
		t.Fatalf("expected repeated coalition.status reads to avoid mutating shadow row, before=%+v after=%+v", shadowBefore, shadowAfter)
	}
}

func TestEvaluateWorkspaceCoalitionIntegrityDetectsEmptyCanonicalCoalition(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-integrity-empty"
		tensionID   = "tension-coalition-integrity-empty"
		taskID      = "task-coalition-integrity-empty"
		coalitionID = "coalition-integrity-empty"
		createdAt   = "2026-04-09T09:00:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, coalitionID, "FORMING", 3, createdAt)

	report, err := store.EvaluateWorkspaceCoalitionIntegrity(ctx, workspaceID)
	if err != nil {
		t.Fatalf("evaluate workspace coalition integrity: %v", err)
	}
	if report.State != WorkspaceCoalitionIntegrityDrift {
		t.Fatalf("expected empty live coalition to surface drift, got %+v", report)
	}
	if len(report.Items) != 1 {
		t.Fatalf("expected one integrity item, got %+v", report.Items)
	}
	item := report.Items[0]
	if item.CanonicalCoalitionID != coalitionID {
		t.Fatalf("expected integrity item to point at canonical coalition %s, got %+v", coalitionID, item)
	}
	if !coalitionIntegrityIssuePresent(item.Issues, "EMPTY_LIVE_COALITION") {
		t.Fatalf("expected empty-live issue, got %+v", item.Issues)
	}
}

func coalitionIntegrityIssuePresent(items []WorkspaceCoalitionIntegrityIssue, code string) bool {
	for _, item := range items {
		if item.Code == code {
			return true
		}
	}
	return false
}
