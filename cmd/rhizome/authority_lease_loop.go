package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

const (
	loopNameAuthorityLease = "authority_lease"
)

type localAuthorityLeaseMaintenancePassResult struct {
	LeaseMaintenance  sqlite.LocalWorkspaceAuthorityLeaseMaintenanceResult `json:"lease_maintenance"`
	SessionReclaim    sqlite.LocalSessionOwnershipReclaimResult            `json:"session_reclaim"`
	OrphanClaim       sqlite.LocalTaskClaimOwnershipReclaimResult          `json:"orphan_claim_reclaim"`
	ProjectLeadLease  sqlite.ProjectStrategicLeadLeaseMaintenanceResult    `json:"project_lead_lease_maintenance"`
	ClaimLiberation   sqlite.ClaimLiberationReconcileResult                `json:"claim_liberation"`
	AuthorityHandoff  sqlite.ReconcileStaleAuthorityHandoffCarriersResult  `json:"authority_handoff_watchdog"`
	TerminalSessionOQ sqlite.TerminalSessionOperatorQueueReconcileResult   `json:"terminal_session_operator_queue"`
}

func startLocalAuthorityLeaseLoop(ctx context.Context, store *sqlite.Store, readiness *ReadinessRegistry, supervisor *serveLoopSupervisor) {
	if readiness != nil {
		readiness.SetState(loopNameAuthorityLease, LoopRunning)
	}

	supervisor.Go(func() {
		backoff := 1 * time.Second
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[FATAL] authority lease loop panic recovered: %v", r)
						if readiness != nil {
							readiness.SetError(loopNameAuthorityLease, fmt.Errorf("panic: %v", r))
						}
					}
				}()
				if readiness != nil {
					readiness.SetState(loopNameAuthorityLease, LoopRunning)
				}
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				for {
					if err := runLocalAuthorityLeaseMaintenanceOnce(ctx, store, readiness); err != nil {
						log.Printf("[authority-lease] error: %v", err)
						if readiness != nil {
							readiness.SetError(loopNameAuthorityLease, err)
						}
						return
					}
					select {
					case <-ctx.Done():
						if readiness != nil {
							readiness.SetState(loopNameAuthorityLease, LoopStopped)
						}
						return
					case <-ticker.C:
					}
				}
			}()
			select {
			case <-ctx.Done():
				if readiness != nil {
					readiness.SetState(loopNameAuthorityLease, LoopStopped)
				}
				return
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
		}
	})
}

func runLocalAuthorityLeaseMaintenancePass(ctx context.Context, store *sqlite.Store) (localAuthorityLeaseMaintenancePassResult, error) {
	result, err := store.RunLocalWorkspaceAuthorityLeaseMaintenance(ctx, sqlite.LocalWorkspaceAuthorityLeaseMaintenanceInput{
		Scope:     "workspace",
		ActorType: "system",
		ActorID:   "local_lease_manager",
	})
	if err != nil {
		return localAuthorityLeaseMaintenancePassResult{}, err
	}
	reclaimResult, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: result.ReferenceAt,
	})
	if err != nil {
		return localAuthorityLeaseMaintenancePassResult{}, err
	}
	orphanClaimResult, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: result.ReferenceAt,
	})
	if err != nil {
		return localAuthorityLeaseMaintenancePassResult{}, err
	}
	projectLeadLeaseResult, err := store.ReconcileProjectStrategicLeadLeases(ctx, sqlite.ProjectStrategicLeadLeaseMaintenanceInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "project_lead_lease_maintainer",
		ReferenceAt: result.ReferenceAt,
		Limit:       32,
	})
	if err != nil {
		return localAuthorityLeaseMaintenancePassResult{}, err
	}
	claimLiberationResult, err := store.ReconcileStaleProjectClaimLiberationNarrow(ctx, sqlite.ClaimLiberationReconcileInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "claim_liberation_watchdog",
		ReferenceAt: result.ReferenceAt,
		Limit:       32,
	})
	if err != nil {
		return localAuthorityLeaseMaintenancePassResult{}, err
	}
	authorityHandoffResult, err := store.ReconcileStaleAuthorityHandoffCarriers(ctx, sqlite.ReconcileStaleAuthorityHandoffCarriersInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "authority_handoff_watchdog",
		ReferenceAt: result.ReferenceAt,
		Limit:       32,
	})
	if err != nil {
		return localAuthorityLeaseMaintenancePassResult{}, err
	}
	terminalSessionQueueResult, err := store.ReconcileTerminalSessionOperatorQueues(ctx, sqlite.TerminalSessionOperatorQueueReconcileInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "terminal_session_queue_reconciler",
		ReferenceAt: result.ReferenceAt,
		Limit:       32,
	})
	if err != nil {
		return localAuthorityLeaseMaintenancePassResult{}, err
	}
	return localAuthorityLeaseMaintenancePassResult{
		LeaseMaintenance:  result,
		SessionReclaim:    reclaimResult,
		OrphanClaim:       orphanClaimResult,
		ProjectLeadLease:  projectLeadLeaseResult,
		ClaimLiberation:   claimLiberationResult,
		AuthorityHandoff:  authorityHandoffResult,
		TerminalSessionOQ: terminalSessionQueueResult,
	}, nil
}

func runLocalAuthorityLeaseMaintenanceOnce(ctx context.Context, store *sqlite.Store, readiness *ReadinessRegistry) error {
	pass, err := runLocalAuthorityLeaseMaintenancePass(ctx, store)
	if err != nil {
		return err
	}
	if readiness != nil {
		readiness.SetState(loopNameAuthorityLease, LoopRunning)
	}
	result := pass.LeaseMaintenance
	if result.Renewed > 0 || result.Expired > 0 || result.Grace > 0 {
		log.Printf("[authority-lease] renewed=%d expired=%d grace=%d problems=%d", result.Renewed, result.Expired, result.Grace, result.Problems)
	}
	if result.Problems > 0 {
		return fmt.Errorf("local authority lease maintenance reported %d workspace issue(s)", result.Problems)
	}
	reclaimResult := pass.SessionReclaim
	if reclaimResult.SessionsEnded > 0 || reclaimResult.SessionQueuesResolved > 0 || reclaimResult.TaskClaimsReleased > 0 {
		log.Printf("[authority-lease] reclaimed_sessions=%d resolved_session_queues=%d released_claims=%d skipped_recent=%d problems=%d", reclaimResult.SessionsEnded, reclaimResult.SessionQueuesResolved, reclaimResult.TaskClaimsReleased, reclaimResult.SkippedRecent, reclaimResult.Problems)
	}
	if reclaimResult.Problems > 0 {
		return fmt.Errorf("local ownership reclaim reported %d issue(s)", reclaimResult.Problems)
	}
	orphanClaimResult := pass.OrphanClaim
	if orphanClaimResult.TaskClaimsReleased > 0 || orphanClaimResult.SkippedRecent > 0 || orphanClaimResult.SkippedActiveSession > 0 || orphanClaimResult.SkippedActiveExecution > 0 {
		log.Printf("[authority-lease] reclaimed_orphan_claims=%d skipped_recent=%d skipped_active_session=%d skipped_active_execution=%d problems=%d", orphanClaimResult.TaskClaimsReleased, orphanClaimResult.SkippedRecent, orphanClaimResult.SkippedActiveSession, orphanClaimResult.SkippedActiveExecution, orphanClaimResult.Problems)
	}
	if orphanClaimResult.Problems > 0 {
		return fmt.Errorf("orphan claimed task reclaim reported %d issue(s)", orphanClaimResult.Problems)
	}
	projectLeadLeaseResult := pass.ProjectLeadLease
	if projectLeadLeaseResult.Renewed > 0 || projectLeadLeaseResult.Skipped > 0 || projectLeadLeaseResult.Problems > 0 {
		log.Printf("[authority-lease] project_lead_lease_scanned=%d renewed=%d skipped=%d problems=%d", projectLeadLeaseResult.Scanned, projectLeadLeaseResult.Renewed, projectLeadLeaseResult.Skipped, projectLeadLeaseResult.Problems)
	}
	if projectLeadLeaseResult.Problems > 0 {
		return fmt.Errorf("project lead lease maintenance reported %d issue(s)", projectLeadLeaseResult.Problems)
	}
	claimLiberationResult := pass.ClaimLiberation
	if claimLiberationResult.Narrowed > 0 || claimLiberationResult.Skipped > 0 || claimLiberationResult.Problems > 0 {
		log.Printf("[authority-lease] claim_liberation_scanned=%d narrowed=%d skipped=%d problems=%d", claimLiberationResult.Scanned, claimLiberationResult.Narrowed, claimLiberationResult.Skipped, claimLiberationResult.Problems)
	}
	if claimLiberationResult.Problems > 0 {
		return fmt.Errorf("claim liberation reported %d issue(s)", claimLiberationResult.Problems)
	}
	authorityHandoffResult := pass.AuthorityHandoff
	if authorityHandoffResult.Changed > 0 || authorityHandoffResult.Skipped > 0 || authorityHandoffResult.Problems > 0 {
		log.Printf("[authority-lease] authority_handoff_scanned=%d changed=%d skipped=%d problems=%d", authorityHandoffResult.Scanned, authorityHandoffResult.Changed, authorityHandoffResult.Skipped, authorityHandoffResult.Problems)
	}
	if authorityHandoffResult.Problems > 0 {
		return fmt.Errorf("authority handoff watchdog reported %d issue(s)", authorityHandoffResult.Problems)
	}
	terminalSessionQueueResult := pass.TerminalSessionOQ
	if terminalSessionQueueResult.Resolved > 0 || terminalSessionQueueResult.Skipped > 0 || terminalSessionQueueResult.Problems > 0 {
		log.Printf("[authority-lease] terminal_session_operator_queue_scanned=%d resolved=%d skipped=%d problems=%d", terminalSessionQueueResult.Scanned, terminalSessionQueueResult.Resolved, terminalSessionQueueResult.Skipped, terminalSessionQueueResult.Problems)
	}
	if terminalSessionQueueResult.Problems > 0 {
		return fmt.Errorf("terminal session operator queue reconcile reported %d issue(s)", terminalSessionQueueResult.Problems)
	}
	return nil
}
