package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// seedAuthorityHandoffCarrier seeds a dedicated authority/handoff carrier task
// (no live claim, no runnable session by default) plus, optionally, a durable
// claim_admitted handoff agent_update with the given created_at age.
//
// claimStatus == "" means a claimless carrier (no task_claims row). Otherwise a
// task_claims row is inserted with that status owned by ownerAgentID.
func seedAuthorityHandoffCarrier(
	t *testing.T,
	ctx context.Context,
	store *Store,
	workspaceID, projectID, taskID, ownerAgentID, claimStatus, signalCreatedAt, signalUpdateID string,
) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// Lightweight workspace + project seed. We avoid seedBranchNameResolverProject
	// here because that helper also inserts project_repositories rows whose repo_id
	// is globally UNIQUE, which collides across subtests sharing the test store.
	if _, err := store.DB().ExecContext(ctx, `
INSERT OR IGNORE INTO workspaces(workspace_id, title, description, created_by, status, created_at, updated_at)
VALUES (?, ?, '', 'developer', 'ACTIVE', ?, ?)`,
		workspaceID, workspaceID, now, now,
	); err != nil {
		t.Fatalf("insert workspace: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT OR IGNORE INTO projects(project_id, workspace_id, title, description, status, created_by, created_at, updated_at)
VALUES (?, ?, ?, '', 'ACTIVE', 'developer', ?, ?)`,
		projectID, workspaceID, projectID, now, now,
	); err != nil {
		t.Fatalf("insert project: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO tasks(task_id, owner_user_id, title, description, status, priority, created_at, updated_at, task_kind, task_template, project_id, requires_project_gate, tags_json)
VALUES (?, 'developer', ?, '', ?, 'high', ?, ?, 'COORDINATION', 'generic', ?, 1, '["project-role-scope"]')`,
		taskID, taskID, model.TaskStatusRunning, now, now, projectID,
	); err != nil {
		t.Fatalf("insert carrier task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT OR IGNORE INTO agents(workspace_id, agent_id, owner_user_id, display_name, role, status, protocol_version, capabilities_json, summary, created_at, updated_at, last_seen_at)
VALUES (?, ?, 'developer', ?, 'strategist', 'ACTIVE', 'rhizome.v1', '{}', '', ?, ?, ?)`,
		workspaceID, ownerAgentID, ownerAgentID, now, now, now,
	); err != nil {
		t.Fatalf("insert owner agent: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT OR IGNORE INTO workspace_tasks(workspace_id, task_id, linked_by, created_at)
VALUES (?, ?, 'test', ?)`,
		workspaceID, taskID, now,
	); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}
	if strings.TrimSpace(claimStatus) != "" {
		if _, err := store.DB().ExecContext(ctx, `
INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, project_role_id, repo_id, checkout_id, branch_id, write_scope_json, updated_at)
VALUES (?, ?, ?, ?, 'seed claim', ?, NULL, '', '', '', '', '', ?)`,
			taskID, workspaceID, ownerAgentID, claimStatus, now, now,
		); err != nil {
			t.Fatalf("insert task claim: %v", err)
		}
	}
	if strings.TrimSpace(signalUpdateID) != "" {
		payload := mustJSON(map[string]any{
			"delegation_state": "claim_admitted",
			"request_id":       "req-" + signalUpdateID,
			"from_agent_id":    "peer",
			"to_agent_id":      ownerAgentID,
			"task_id":          taskID,
			"request_kind":     "authority_transition",
			"coverage_state":   "authority_task_claim_admitted",
		})
		if _, err := store.DB().ExecContext(ctx, `
INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
VALUES (?, ?, ?, 'coordination', 'authority transition claim_admitted', ?, 0, ?)`,
			signalUpdateID, workspaceID, ownerAgentID, payload, signalCreatedAt,
		); err != nil {
			t.Fatalf("insert handoff signal: %v", err)
		}
	}
}

func seedAuthorityHandoffRunnableSession(t *testing.T, ctx context.Context, store *Store, workspaceID, taskID, agentID, sessionID string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO agent_sessions(session_id, agent_id, workspace_id, task_id, status, started_at, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		sessionID, agentID, workspaceID, taskID, model.SessionStatusActive, now, now,
	); err != nil {
		t.Fatalf("insert runnable session: %v", err)
	}
}

func authorityHandoffClaimRow(t *testing.T, ctx context.Context, store *Store, workspaceID, taskID string) (agentID, claimStatus, summary string) {
	t.Helper()
	if err := store.DB().QueryRowContext(ctx,
		`SELECT agent_id, claim_status, summary FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID, taskID,
	).Scan(&agentID, &claimStatus, &summary); err != nil {
		t.Fatalf("query carrier claim row: %v", err)
	}
	return strings.TrimSpace(agentID), strings.TrimSpace(claimStatus), summary
}

func TestStaleAuthorityHandoffCarrierPromotedToBlocker(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-authority-handoff-block"
		projectID   = "project-authority-handoff-block"
		taskID      = "task-role-scope-authority-handoff-block"
		ownerAgent  = "delta"
		updateID    = "update-authority-handoff-block"
	)
	// Stale signal older than localOwnershipReclaimGrace; carrier was admitted then
	// RELEASED (the realistic claim_admitted-then-stalled case), so it has a real
	// last-owner agent the BLOCKED claim is FK-safely attributed to.
	staleAt := time.Now().UTC().Add(-localOwnershipReclaimGrace - time.Minute).Format(time.RFC3339Nano)
	seedAuthorityHandoffCarrier(t, ctx, store, workspaceID, projectID, taskID, ownerAgent, model.TaskClaimStatusReleased, staleAt, updateID)

	result, err := store.ReconcileStaleAuthorityHandoffCarriers(ctx, ReconcileStaleAuthorityHandoffCarriersInput{
		ActorID:     authorityHandoffWatchdogActorID,
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reconcile authority handoff carriers: %v", err)
	}
	if result.Scanned != 1 || result.Changed != 1 || result.Skipped != 0 || result.Problems != 0 {
		t.Fatalf("unexpected reconcile result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].Action != "BLOCKED" {
		t.Fatalf("unexpected items: %+v", result.Items)
	}

	agentID, claimStatus, summary := authorityHandoffClaimRow(t, ctx, store, workspaceID, taskID)
	if claimStatus != model.TaskClaimStatusBlocked {
		t.Fatalf("expected claim_status BLOCKED, got %q", claimStatus)
	}
	// BLOCKED claim is attributed to the carrier's last owner (FK-safe; no fabricated agent).
	if agentID != ownerAgent {
		t.Fatalf("expected last-owner agent_id %q, got %q", ownerAgent, agentID)
	}
	if !strings.Contains(summary, authorityHandoffFollowThroughBlockerSchema) {
		t.Fatalf("expected evidence schema in claim summary, got %q", summary)
	}

	// agent contract: the read-model must expose ClaimStatus == "BLOCKED".
	record, err := store.getWorkspaceTaskRecord(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("read carrier task record: %v", err)
	}
	if record.ClaimStatus == nil || strings.TrimSpace(*record.ClaimStatus) != model.TaskClaimStatusBlocked {
		t.Fatalf("expected read-model ClaimStatus BLOCKED, got %v", record.ClaimStatus)
	}
}

func TestStaleAuthorityHandoffClaimlessCarrierSkipped(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-authority-handoff-claimless"
		projectID   = "project-authority-handoff-claimless"
		taskID      = "task-role-scope-authority-handoff-claimless"
		ownerAgent  = "delta"
		updateID    = "update-authority-handoff-claimless"
	)
	staleAt := time.Now().UTC().Add(-localOwnershipReclaimGrace - time.Minute).Format(time.RFC3339Nano)
	// Claimless carrier (no task_claims row, no prior owner): the watchdog must SKIP
	// it rather than fabricate a sentinel agent (which would pollute roster-registry
	// parity). Real claim_admitted targets always have a row.
	seedAuthorityHandoffCarrier(t, ctx, store, workspaceID, projectID, taskID, ownerAgent, "", staleAt, updateID)

	result, err := store.ReconcileStaleAuthorityHandoffCarriers(ctx, ReconcileStaleAuthorityHandoffCarriersInput{
		ActorID:     authorityHandoffWatchdogActorID,
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reconcile claimless carrier: %v", err)
	}
	if result.Changed != 0 || result.Skipped != 1 {
		t.Fatalf("expected claimless carrier skipped (changed=0 skipped=1), got %+v", result)
	}
	if len(result.Items) != 1 || !strings.Contains(result.Items[0].SkipReason, "claimless") {
		t.Fatalf("expected claimless skip reason, got %+v", result.Items)
	}
	var claimCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimCount); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if claimCount != 0 {
		t.Fatalf("claimless carrier must remain claimless (no fabricated claim), got %d", claimCount)
	}
}

func TestStaleAuthorityHandoffWatchdogIdempotent(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-authority-handoff-idempotent"
		projectID   = "project-authority-handoff-idempotent"
		taskID      = "task-role-scope-authority-handoff-idempotent"
		ownerAgent  = "delta"
		updateID    = "update-authority-handoff-idempotent"
	)
	staleAt := time.Now().UTC().Add(-localOwnershipReclaimGrace - time.Minute).Format(time.RFC3339Nano)
	seedAuthorityHandoffCarrier(t, ctx, store, workspaceID, projectID, taskID, ownerAgent, model.TaskClaimStatusReleased, staleAt, updateID)

	first, err := store.ReconcileStaleAuthorityHandoffCarriers(ctx, ReconcileStaleAuthorityHandoffCarriersInput{
		ActorID:     authorityHandoffWatchdogActorID,
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if first.Changed != 1 {
		t.Fatalf("expected first pass to block 1, got %+v", first)
	}
	_, firstStatus, firstSummary := authorityHandoffClaimRow(t, ctx, store, workspaceID, taskID)

	second, err := store.ReconcileStaleAuthorityHandoffCarriers(ctx, ReconcileStaleAuthorityHandoffCarriersInput{
		ActorID:     authorityHandoffWatchdogActorID,
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	// Already-BLOCKED carrier is dedup-excluded by the candidate query, so the
	// second pass neither scans nor changes it.
	if second.Changed != 0 {
		t.Fatalf("expected second pass to change 0, got %+v", second)
	}
	_, secondStatus, secondSummary := authorityHandoffClaimRow(t, ctx, store, workspaceID, taskID)
	if secondStatus != firstStatus || secondStatus != model.TaskClaimStatusBlocked {
		t.Fatalf("claim status changed across idempotent runs: %q -> %q", firstStatus, secondStatus)
	}
	if secondSummary != firstSummary {
		t.Fatalf("claim summary changed across idempotent runs")
	}
}

func TestStaleAuthorityHandoffWatchdogSkipsLiveCarrier(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	// (a) Carrier with a fresh (newer-than-grace) handoff signal -> not a candidate.
	t.Run("fresh_signal", func(t *testing.T) {
		const (
			workspaceID = "ws-authority-handoff-fresh"
			projectID   = "project-authority-handoff-fresh"
			taskID      = "task-role-scope-authority-handoff-fresh"
			ownerAgent  = "delta"
			updateID    = "update-authority-handoff-fresh"
		)
		freshAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
		seedAuthorityHandoffCarrier(t, ctx, store, workspaceID, projectID, taskID, ownerAgent, "", freshAt, updateID)

		result, err := store.ReconcileStaleAuthorityHandoffCarriers(ctx, ReconcileStaleAuthorityHandoffCarriersInput{
			ActorID:     authorityHandoffWatchdogActorID,
			ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
		})
		if err != nil {
			t.Fatalf("reconcile fresh-signal carrier: %v", err)
		}
		for _, item := range result.Items {
			if item.TaskID == taskID {
				t.Fatalf("fresh-signal carrier should not be a candidate, got item %+v", item)
			}
		}
		// No task_claims row should have been created.
		var claimCount int
		if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimCount); err != nil {
			t.Fatalf("count claims: %v", err)
		}
		if claimCount != 0 {
			t.Fatalf("fresh-signal carrier should remain claimless, got %d claims", claimCount)
		}
	})

	// (b) Carrier with an active CLAIMED claim + runnable session -> untouched.
	t.Run("active_claim_and_session", func(t *testing.T) {
		const (
			workspaceID = "ws-authority-handoff-active"
			projectID   = "project-authority-handoff-active"
			taskID      = "task-role-scope-authority-handoff-active"
			ownerAgent  = "delta"
			updateID    = "update-authority-handoff-active"
			sessionID   = "sess-authority-handoff-active"
		)
		staleAt := time.Now().UTC().Add(-localOwnershipReclaimGrace - time.Minute).Format(time.RFC3339Nano)
		seedAuthorityHandoffCarrier(t, ctx, store, workspaceID, projectID, taskID, ownerAgent, model.TaskClaimStatusClaimed, staleAt, updateID)
		seedAuthorityHandoffRunnableSession(t, ctx, store, workspaceID, taskID, ownerAgent, sessionID)

		result, err := store.ReconcileStaleAuthorityHandoffCarriers(ctx, ReconcileStaleAuthorityHandoffCarriersInput{
			ActorID:     authorityHandoffWatchdogActorID,
			ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
		})
		if err != nil {
			t.Fatalf("reconcile active carrier: %v", err)
		}
		for _, item := range result.Items {
			if item.TaskID == taskID && item.Action == "BLOCKED" {
				t.Fatalf("active CLAIMED+session carrier must not be blocked, got %+v", item)
			}
		}
		_, claimStatus, _ := authorityHandoffClaimRow(t, ctx, store, workspaceID, taskID)
		if claimStatus != model.TaskClaimStatusClaimed {
			t.Fatalf("active carrier claim must remain CLAIMED, got %q", claimStatus)
		}
	})
}
