package server

import (
	"context"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"testing"
)

func TestWorkspaceControlEpochTick_CoalitionSweep(t *testing.T) {
	ctx := context.Background()
	store := newServerTestStore(t)
	defer store.Close()

	h := NewHandler(store)

	workspaceID := "ws-epoch-test"
	err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Test Workspace",
		Description: "Test Description",
		CreatedBy:   "test-user",
	})
	if err != nil {
		t.Fatalf("failed to create workspace: %v", err)
	}

	// Create a tension to trigger a sweep
	_, err = store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, created_at, updated_at)
		VALUES ('tension-epoch-1', ?, 'cluster-1', 'general', 'ACTIVE', 'PENDING', datetime('now'), datetime('now'))
	`, workspaceID)
	if err != nil {
		t.Fatalf("failed to insert tension: %v", err)
	}

	params := []byte(`{"workspace_id":"ws-epoch-test"}`)

	// Call the epoch tick which should advance epoch and sweep coalitions
	res, rpcErr := h.workspaceControlEpochTick(ctx, params)
	if rpcErr != nil {
		t.Fatalf("workspaceControlEpochTick failed: %v", rpcErr)
	}

	respMap, ok := res.(map[string]any)
	if !ok || respMap["status"] != "ok" {
		t.Fatalf("unexpected response: %v", res)
	}
	preview, ok := respMap["control_policy_preview"].(sqlite.ControlPolicyPreview)
	if !ok {
		t.Fatalf("expected control_policy_preview in response, got %+v", res)
	}
	if preview.LiveApplied {
		t.Fatalf("expected epoch tick preview to stay non-actuating, got %+v", preview)
	}

	// Verify a coalition was created
	var count int
	err = store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM workspace_coalitions WHERE tension_id = 'tension-epoch-1'").Scan(&count)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 coalition to be formed, got %d", count)
	}

	// Re-running the sweep should reuse the canonical coalition path rather than
	// creating a parallel live coalition row.
	res, rpcErr = h.workspaceControlEpochTick(ctx, params)
	if rpcErr != nil {
		t.Fatalf("workspaceControlEpochTick rerun failed: %v", rpcErr)
	}
	respMap, ok = res.(map[string]any)
	if !ok || respMap["status"] != "ok" {
		t.Fatalf("unexpected rerun response: %v", res)
	}

	err = store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM workspace_coalitions WHERE tension_id = 'tension-epoch-1' AND status IN ('FORMING','ACTIVE')").Scan(&count)
	if err != nil {
		t.Fatalf("query rerun failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected epoch sweep rerun to stay idempotent, got %d live coalitions", count)
	}

	check, err := store.CheckCapabilityPolicy(ctx, sqlite.CapabilityCheckInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "agent.fork",
	})
	if err != nil {
		t.Fatalf("check capability policy after epoch tick: %v", err)
	}
	if check.Verdict != "ALLOW" {
		t.Fatalf("expected epoch tick preview not to mutate capability policy, got %+v", check)
	}
}
