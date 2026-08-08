package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectStrategicLeadLeaseMaintenanceRenewsFreshExpiredLead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-lead-lease-maintenance"
		projectID   = "project-lead-lease-maintenance"
		leadID      = "beta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID})
	createProjectLeadLeaseMaintenanceProject(t, ctx, store, workspaceID, projectID, leadID)

	role, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "initial lead claim",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	})
	if err != nil {
		t.Fatalf("claim lead: %v", err)
	}
	referenceAt := time.Now().UTC().Round(0)
	expiredAt := referenceAt.Add(-time.Minute).Format(time.RFC3339Nano)
	freshSeenAt := referenceAt.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_agent_roles SET lease_expires_at = ?, updated_at = ? WHERE role_id = ?`,
		expiredAt, expiredAt, role.RoleID); err != nil {
		t.Fatalf("expire lead lease: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE agents SET last_seen_at = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?`,
		freshSeenAt, freshSeenAt, workspaceID, leadID); err != nil {
		t.Fatalf("freshen lead agent: %v", err)
	}

	result, err := store.ReconcileProjectStrategicLeadLeases(ctx, sqlite.ProjectStrategicLeadLeaseMaintenanceInput{
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		Limit:       8,
	})
	if err != nil {
		t.Fatalf("reconcile lead leases: %v", err)
	}
	if result.Scanned != 1 || result.Renewed != 1 || result.Problems != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	active, ok, err := store.GetActiveProjectStrategicLead(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get active lead: %v", err)
	}
	if !ok || active.AgentID != leadID {
		t.Fatalf("expected fresh lead to be active, ok=%v role=%+v", ok, active)
	}
	leaseExpiresAt, err := time.Parse(time.RFC3339Nano, active.LeaseExpiresAt)
	if err != nil {
		t.Fatalf("parse renewed lease: %v", err)
	}
	if !leaseExpiresAt.After(referenceAt) {
		t.Fatalf("expected renewed lease after reference_at, got %s ref %s", active.LeaseExpiresAt, referenceAt.Format(time.RFC3339Nano))
	}
}

func TestProjectStrategicLeadLeaseMaintenanceSkipsStaleExpiredLead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-lead-lease-maintenance-stale"
		projectID   = "project-lead-lease-maintenance-stale"
		leadID      = "beta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID})
	createProjectLeadLeaseMaintenanceProject(t, ctx, store, workspaceID, projectID, leadID)

	role, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "initial lead claim",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	})
	if err != nil {
		t.Fatalf("claim lead: %v", err)
	}
	referenceAt := time.Now().UTC().Round(0)
	expiredAt := referenceAt.Add(-time.Minute).Format(time.RFC3339Nano)
	staleSeenAt := referenceAt.Add(-30 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_agent_roles SET lease_expires_at = ?, updated_at = ? WHERE role_id = ?`,
		expiredAt, expiredAt, role.RoleID); err != nil {
		t.Fatalf("expire lead lease: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE agents SET last_seen_at = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?`,
		staleSeenAt, staleSeenAt, workspaceID, leadID); err != nil {
		t.Fatalf("stale lead agent: %v", err)
	}

	result, err := store.ReconcileProjectStrategicLeadLeases(ctx, sqlite.ProjectStrategicLeadLeaseMaintenanceInput{
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		Limit:       8,
	})
	if err != nil {
		t.Fatalf("reconcile lead leases: %v", err)
	}
	if result.Scanned != 1 || result.Renewed != 0 || result.Skipped != 1 || result.Problems != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if _, ok, err := store.GetActiveProjectStrategicLead(ctx, workspaceID, projectID); err != nil {
		t.Fatalf("get active lead: %v", err)
	} else if ok {
		t.Fatal("stale expired lead should not be renewed")
	}
}

func createProjectLeadLeaseMaintenanceProject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID string) {
	t.Helper()
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       projectID,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
}
