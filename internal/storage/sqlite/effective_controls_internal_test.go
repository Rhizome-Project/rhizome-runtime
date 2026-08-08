package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestLoadEffectiveControlsTreatsMalformedGeneratedAtAsPending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	workspaceID := "ws-effective-malformed-generated-at"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "codex",
		Status:      "active",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    "cluster-alpha",
		Epoch:             11,
		TTLSeconds:        600,
		CandidateControls: sqlite.ControlSuggestedControls{FanoutCap: 4, ReviewDepth: 1, ContextCap: 6, BridgeQuota: 4, MergeThreshold: 5, PriorityFocus: "throughput"},
		AdvisoryControls:  sqlite.ControlSuggestedControls{FanoutCap: 3, ReviewDepth: 1, ContextCap: 5, BridgeQuota: 3, MergeThreshold: 4, PriorityFocus: "review"},
		EffectiveControls: sqlite.ControlSuggestedControls{FanoutCap: 2, ReviewDepth: 1, ContextCap: 4, BridgeQuota: 2, MergeThreshold: 3, PriorityFocus: "safety"},
		GeneratedAt:       "2026-04-08T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("persist effective controls: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `UPDATE workspace_effective_controls SET generated_at = ? WHERE workspace_id = ? AND proto_cluster_id = ?`, "not-a-timestamp", workspaceID, "cluster-alpha"); err != nil {
		t.Fatalf("corrupt generated_at: %v", err)
	}

	loaded, live, err := store.LoadEffectiveControls(ctx, workspaceID, "cluster-alpha", "2026-04-08T10:05:00Z")
	if err != nil {
		t.Fatalf("load malformed effective controls: %v", err)
	}
	if live || !loaded.Pending {
		t.Fatalf("expected malformed generated_at to fail closed as pending, got live=%v record=%+v original=%+v", live, loaded, record)
	}
	if loaded.GeneratedAt != "not-a-timestamp" {
		t.Fatalf("expected corrupted generated_at to round-trip for inspection, got %+v", loaded)
	}
}
