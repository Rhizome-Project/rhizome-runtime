package sqlite_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestEnsureLocalWorkspaceAuthorityClaimsMissingAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-local-authority-claim"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Local Authority Claim",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	result, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "operator",
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("ensure local workspace authority: %v", err)
	}
	if result.Action != "CLAIMED" {
		t.Fatalf("expected CLAIMED action, got %+v", result)
	}
	if result.RuntimeEvent == nil || result.RuntimeEvent.EventType != sqlite.AuthorityEventGranted {
		t.Fatalf("expected authority.granted runtime event, got %+v", result.RuntimeEvent)
	}
	if result.Status.Authority == nil {
		t.Fatalf("expected authority status snapshot to include authority row, got %+v", result.Status)
	}
	if !result.Status.LocalHolder || !result.Status.LeaseLive {
		t.Fatalf("expected local live holder after claim, got %+v", result.Status)
	}
	if got := result.Status.LeaseState; got != "healthy" && got != "renew_due" {
		t.Fatalf("expected healthy/renew_due lease state after claim, got %+v", result.Status)
	}
}

func TestEnsureLocalWorkspaceAuthorityRenewsExpiringLocalLease(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-local-authority-renew"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Local Authority Renew",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	renewDueAt := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE workspace_authority
   SET lease_expires_at = ?, updated_at = ?
 WHERE workspace_id = ? AND scope = ?`,
		renewDueAt,
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		"workspace",
	); err != nil {
		t.Fatalf("mark authority lease renew-due: %v", err)
	}

	result, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "operator",
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("ensure local workspace authority: %v", err)
	}
	if result.Action != "RENEWED" {
		t.Fatalf("expected RENEWED action, got %+v", result)
	}
	if result.RuntimeEvent == nil || result.RuntimeEvent.EventType != sqlite.AuthorityEventRenewed {
		t.Fatalf("expected authority.renewed runtime event, got %+v", result.RuntimeEvent)
	}
	if result.Status.Authority == nil {
		t.Fatalf("expected authority status snapshot to include authority row, got %+v", result.Status)
	}
	if result.Status.Authority.Term != current.Term || result.Status.Authority.LeaseToken != current.LeaseToken {
		t.Fatalf("expected renew to preserve term/token, before=%+v after=%+v", current, result.Status.Authority)
	}
	if result.Status.Authority.LeaseExpiresAt == renewDueAt {
		t.Fatalf("expected lease_expires_at to advance beyond renew-due timestamp, got %+v", result.Status.Authority)
	}
}

func TestEnsureLocalWorkspaceAuthorityRequiresForceBreakForForeignLiveHolder(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-local-authority-foreign-live"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Local Authority Foreign Live",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-7001")

	if _, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "operator",
		ActorID:     "tests",
	}); err == nil {
		t.Fatal("expected foreign live holder to reject ensure-local")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject == nil || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected authority_stale reject, got %v", err)
	}

	forced, err := store.ForceBreakWorkspaceAuthority(ctx, sqlite.ForceBreakWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "operator",
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("force-break workspace authority: %v", err)
	}
	if forced.Action != "EXPIRED" {
		t.Fatalf("expected EXPIRED action, got %+v", forced)
	}
	if forced.RuntimeEvent == nil || forced.RuntimeEvent.EventType != sqlite.AuthorityEventExpired {
		t.Fatalf("expected authority.expired runtime event, got %+v", forced.RuntimeEvent)
	}

	reclaimed, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "operator",
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("reclaim after force-break: %v", err)
	}
	if reclaimed.Action != "RECLAIMED" {
		t.Fatalf("expected RECLAIMED action after force-break, got %+v", reclaimed)
	}
	if reclaimed.Status.Authority == nil || !reclaimed.Status.LocalHolder || !reclaimed.Status.LeaseLive {
		t.Fatalf("expected reclaimed local authority, got %+v", reclaimed.Status)
	}
}

func TestForceBreakWorkspaceAuthorityReturnsMissingWhenAbsent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-local-authority-force-break-missing"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Force Break Missing",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	result, err := store.ForceBreakWorkspaceAuthority(ctx, sqlite.ForceBreakWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "operator",
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("force-break missing authority: %v", err)
	}
	if result.Action != "MISSING" {
		t.Fatalf("expected MISSING action, got %+v", result)
	}
	if result.RuntimeEvent != nil {
		t.Fatalf("expected no runtime event for missing force-break, got %+v", result.RuntimeEvent)
	}
	if result.Status.LeaseState != "missing" {
		t.Fatalf("expected missing lease state, got %+v", result.Status)
	}
}

func TestRunLocalWorkspaceAuthorityLeaseMaintenanceRenewsRenewDueLease(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-lease-maintenance-renew"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Lease Maintenance Renew",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	renewDueAt := time.Now().UTC().Add(90 * time.Second).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE workspace_authority
   SET lease_expires_at = ?, updated_at = ?
 WHERE workspace_id = ? AND scope = ?`,
		renewDueAt,
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		"workspace",
	); err != nil {
		t.Fatalf("mark authority lease renew-due: %v", err)
	}

	result, err := store.RunLocalWorkspaceAuthorityLeaseMaintenance(ctx, sqlite.LocalWorkspaceAuthorityLeaseMaintenanceInput{
		Scope:     "workspace",
		ActorType: "system",
		ActorID:   "tests",
	})
	if err != nil {
		t.Fatalf("run local workspace authority lease maintenance: %v", err)
	}
	if result.Renewed != 1 || result.Problems != 0 {
		t.Fatalf("expected one renewed lease and no problems, got %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].Action != "RENEWED" {
		t.Fatalf("expected single RENEWED maintenance item, got %+v", result.Items)
	}
	if result.Items[0].RuntimeEvent == nil || result.Items[0].RuntimeEvent.EventType != sqlite.AuthorityEventRenewed {
		t.Fatalf("expected authority.renewed runtime event, got %+v", result.Items[0].RuntimeEvent)
	}
	if result.Items[0].Authority == nil || result.Items[0].Authority.LeaseToken != current.LeaseToken || result.Items[0].Authority.Term != current.Term {
		t.Fatalf("expected renewed authority to preserve token/term, before=%+v item=%+v", current, result.Items[0])
	}
	if result.Items[0].Authority.LeaseExpiresAt == renewDueAt {
		t.Fatalf("expected renewed authority to advance lease expiry, got %+v", result.Items[0].Authority)
	}
}

func TestRunLocalWorkspaceAuthorityLeaseMaintenanceLeavesGraceWindowUnexpired(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-lease-maintenance-grace"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Lease Maintenance Grace",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID); current.Status != sqlite.WorkspaceAuthorityStatusActive {
		t.Fatalf("expected active local authority seed, got %+v", current)
	}
	graceLeaseAt := time.Now().UTC().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE workspace_authority
   SET lease_expires_at = ?, updated_at = ?
 WHERE workspace_id = ? AND scope = ?`,
		graceLeaseAt,
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		"workspace",
	); err != nil {
		t.Fatalf("mark authority lease inside grace: %v", err)
	}

	result, err := store.RunLocalWorkspaceAuthorityLeaseMaintenance(ctx, sqlite.LocalWorkspaceAuthorityLeaseMaintenanceInput{
		Scope:     "workspace",
		ActorType: "system",
		ActorID:   "tests",
	})
	if err != nil {
		t.Fatalf("run local workspace authority lease maintenance: %v", err)
	}
	if result.Grace != 1 || result.Expired != 0 || result.Problems != 0 {
		t.Fatalf("expected one grace lease and no expiry/problems, got %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].Action != "GRACE" {
		t.Fatalf("expected single GRACE maintenance item, got %+v", result.Items)
	}
	reloaded, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("reload workspace authority after grace maintenance: %v", err)
	}
	if reloaded.Status != sqlite.WorkspaceAuthorityStatusActive {
		t.Fatalf("expected grace-window lease to remain ACTIVE, got %+v", reloaded)
	}
}

func TestRunLocalWorkspaceAuthorityLeaseMaintenanceExpiresLeasePastGrace(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-lease-maintenance-expire"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Lease Maintenance Expire",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID); current.Status != sqlite.WorkspaceAuthorityStatusActive {
		t.Fatalf("expected active local authority seed, got %+v", current)
	}
	expiredAt := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE workspace_authority
   SET lease_expires_at = ?, updated_at = ?
 WHERE workspace_id = ? AND scope = ?`,
		expiredAt,
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		"workspace",
	); err != nil {
		t.Fatalf("mark authority lease stale beyond grace: %v", err)
	}

	result, err := store.RunLocalWorkspaceAuthorityLeaseMaintenance(ctx, sqlite.LocalWorkspaceAuthorityLeaseMaintenanceInput{
		Scope:     "workspace",
		ActorType: "system",
		ActorID:   "tests",
	})
	if err != nil {
		t.Fatalf("run local workspace authority lease maintenance: %v", err)
	}
	if result.Expired != 1 || result.Problems != 0 {
		t.Fatalf("expected one expired lease and no problems, got %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].Action != "EXPIRED" {
		t.Fatalf("expected single EXPIRED maintenance item, got %+v", result.Items)
	}
	if result.Items[0].RuntimeEvent == nil || result.Items[0].RuntimeEvent.EventType != sqlite.AuthorityEventExpired {
		t.Fatalf("expected authority.expired runtime event, got %+v", result.Items[0].RuntimeEvent)
	}
	reloaded, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("reload workspace authority after expiry maintenance: %v", err)
	}
	if reloaded.Status != sqlite.WorkspaceAuthorityStatusExpired {
		t.Fatalf("expected stale lease to be EXPIRED after maintenance, got %+v", reloaded)
	}
}

func TestWorkspaceAuthorityRenewVsExpireRaceLeavesSingleCanonicalOutcome(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-sh1a-renew-expire-race"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1A Renew Expire Race",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)

	var (
		renewErr  error
		expireErr error
		wg        sync.WaitGroup
		start     = make(chan struct{})
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, _, renewErr = store.RenewWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityRenewInput{
			WorkspaceID:           workspaceID,
			Scope:                 "workspace",
			HolderAuthorityNodeID: current.HolderAuthorityNodeID,
			LeaseToken:            current.LeaseToken,
			Term:                  current.Term,
			LeaseExpiresAt:        time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339Nano),
			CommitWatermark:       current.CommitWatermark,
			AppliedWatermark:      current.AppliedWatermark,
			ActorType:             "system",
			ActorID:               "tests",
			ReferenceAt:           referenceAt,
		})
	}()
	go func() {
		defer wg.Done()
		<-start
		_, _, expireErr = store.ExpireWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityExpireInput{
			WorkspaceID:           workspaceID,
			Scope:                 "workspace",
			HolderAuthorityNodeID: current.HolderAuthorityNodeID,
			LeaseToken:            current.LeaseToken,
			Term:                  current.Term,
			CommitWatermark:       current.CommitWatermark,
			AppliedWatermark:      current.AppliedWatermark,
			ActorType:             "system",
			ActorID:               "tests",
			ReferenceAt:           referenceAt,
		})
	}()
	close(start)
	wg.Wait()

	successes := 0
	if renewErr == nil {
		successes++
	}
	if expireErr == nil {
		successes++
	}
	if successes != 1 {
		t.Fatalf("expected exactly one canonical outcome from renew/expire race, renewErr=%v expireErr=%v", renewErr, expireErr)
	}
	if renewErr != nil {
		if reject, ok := sqlite.AsAuthorityReject(renewErr); !ok || reject == nil {
			t.Fatalf("expected renew loser to be typed authority reject, got %v", renewErr)
		}
	}
	if expireErr != nil {
		if reject, ok := sqlite.AsAuthorityReject(expireErr); !ok || reject == nil {
			t.Fatalf("expected expire loser to be typed authority reject, got %v", expireErr)
		}
	}

	reloaded, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("reload workspace authority after renew/expire race: %v", err)
	}
	switch {
	case renewErr == nil:
		if reloaded.Status != sqlite.WorkspaceAuthorityStatusActive || reloaded.LeaseToken != current.LeaseToken || reloaded.Term != current.Term {
			t.Fatalf("expected renewed authority to remain canonical, got %+v", reloaded)
		}
	case expireErr == nil:
		if reloaded.Status != sqlite.WorkspaceAuthorityStatusExpired || reloaded.LeaseToken != current.LeaseToken || reloaded.Term != current.Term {
			t.Fatalf("expected expired authority to remain canonical, got %+v", reloaded)
		}
	}
}
