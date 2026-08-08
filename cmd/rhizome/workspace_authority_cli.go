package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceAuthorityCLIRecord struct {
	WorkspaceID           string                          `json:"workspace_id"`
	Scope                 string                          `json:"scope"`
	HolderAuthorityNodeID string                          `json:"holder_authority_node_id,omitempty"`
	LeaseTokenFingerprint string                          `json:"lease_token_fingerprint,omitempty"`
	Term                  int64                           `json:"term"`
	LeaseExpiresAt        string                          `json:"lease_expires_at,omitempty"`
	CommitWatermark       int64                           `json:"commit_watermark"`
	AppliedWatermark      int64                           `json:"applied_watermark"`
	Status                sqlite.WorkspaceAuthorityStatus `json:"status"`
	UpdatedAt             string                          `json:"updated_at"`
}

type workspaceAuthorityCLIStatus struct {
	WorkspaceID string                       `json:"workspace_id"`
	Scope       string                       `json:"scope"`
	ReferenceAt string                       `json:"reference_at"`
	LocalNode   sqlite.RuntimeNodeRecord     `json:"local_node"`
	Authority   *workspaceAuthorityCLIRecord `json:"authority,omitempty"`
	JournalHead int64                        `json:"journal_head"`
	LocalHolder bool                         `json:"local_holder"`
	LeaseLive   bool                         `json:"lease_live"`
	LeaseState  string                       `json:"lease_state"`
}

type workspaceAuthorityCLIEvent struct {
	EventID                        string `json:"event_id"`
	DedupKey                       string `json:"dedup_key,omitempty"`
	WorkspaceID                    string `json:"workspace_id"`
	EventType                      string `json:"event_type"`
	EntityType                     string `json:"entity_type"`
	EntityID                       string `json:"entity_id"`
	ActorType                      string `json:"actor_type,omitempty"`
	ActorID                        string `json:"actor_id,omitempty"`
	AgentID                        string `json:"agent_id,omitempty"`
	SessionID                      string `json:"session_id,omitempty"`
	TaskID                         string `json:"task_id,omitempty"`
	RootCauseID                    string `json:"root_cause_id,omitempty"`
	ProvenanceGroupID              string `json:"provenance_group_id,omitempty"`
	ParentRefsJSON                 string `json:"parent_refs_json,omitempty"`
	CreatedAt                      string `json:"created_at"`
	AuthorityHolderNodeID          string `json:"authority_holder_node_id,omitempty"`
	AuthorityTerm                  int64  `json:"authority_term,omitempty"`
	AuthorityLeaseTokenFingerprint string `json:"authority_lease_token_fingerprint,omitempty"`
	IngestSeq                      int64  `json:"ingest_seq,omitempty"`
}

type workspaceAuthorityCLIActionResult struct {
	Action       string                      `json:"action"`
	Status       workspaceAuthorityCLIStatus `json:"status"`
	RuntimeEvent *workspaceAuthorityCLIEvent `json:"runtime_event,omitempty"`
}

type workspaceAuthorityCLILeaseMaintenanceItem struct {
	WorkspaceID  string                       `json:"workspace_id"`
	LeaseState   string                       `json:"lease_state"`
	Action       string                       `json:"action"`
	Authority    *workspaceAuthorityCLIRecord `json:"authority,omitempty"`
	RuntimeEvent *workspaceAuthorityCLIEvent  `json:"runtime_event,omitempty"`
	Error        string                       `json:"error,omitempty"`
}

type workspaceAuthorityCLILeaseMaintenanceResult struct {
	Scope       string                                      `json:"scope"`
	ReferenceAt string                                      `json:"reference_at"`
	Healthy     int                                         `json:"healthy"`
	Renewed     int                                         `json:"renewed"`
	Grace       int                                         `json:"grace"`
	Expired     int                                         `json:"expired"`
	Problems    int                                         `json:"problems"`
	Items       []workspaceAuthorityCLILeaseMaintenanceItem `json:"items,omitempty"`
}

type workspaceAuthorityCLIMaintenancePassResult struct {
	LeaseMaintenance  workspaceAuthorityCLILeaseMaintenanceResult         `json:"lease_maintenance"`
	SessionReclaim    sqlite.LocalSessionOwnershipReclaimResult           `json:"session_reclaim"`
	OrphanClaim       sqlite.LocalTaskClaimOwnershipReclaimResult         `json:"orphan_claim_reclaim"`
	ProjectLeadLease  sqlite.ProjectStrategicLeadLeaseMaintenanceResult   `json:"project_lead_lease_maintenance"`
	ClaimLiberation   sqlite.ClaimLiberationReconcileResult               `json:"claim_liberation"`
	AuthorityHandoff  sqlite.ReconcileStaleAuthorityHandoffCarriersResult `json:"authority_handoff_watchdog"`
	TerminalSessionOQ sqlite.TerminalSessionOperatorQueueReconcileResult  `json:"terminal_session_operator_queue"`
}

func runWorkspaceAuthority(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace authority subcommand")
	}
	switch args[0] {
	case "status":
		return runWorkspaceAuthorityStatus(args[1:])
	case "ensure-local":
		return runWorkspaceAuthorityEnsureLocal(args[1:])
	case "force-break":
		return runWorkspaceAuthorityForceBreak(args[1:])
	case "maintain-once":
		return runWorkspaceAuthorityMaintainOnce(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace authority subcommand: %s", args[0])
	}
}

func runWorkspaceAuthorityStatus(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace authority status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	scope := fs.String("scope", "workspace", "Authority scope")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	status, err := store.GetLocalWorkspaceAuthorityStatus(ctx, *workspaceID, *scope)
	if err != nil {
		return fmt.Errorf("get local workspace authority status: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"status":   safeWorkspaceAuthorityCLIStatus(status),
	})
}

func runWorkspaceAuthorityEnsureLocal(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace authority ensure-local", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	scope := fs.String("scope", "workspace", "Authority scope")
	actorType := fs.String("actor-type", "operator", "Actor type: operator|system")
	actorID := fs.String("actor-id", "", "Actor identifier recorded on authority lifecycle events")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	result, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: *workspaceID,
		Scope:       *scope,
		ActorType:   *actorType,
		ActorID:     *actorID,
	})
	if err != nil {
		return fmt.Errorf("ensure local workspace authority: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"result":   safeWorkspaceAuthorityCLIActionResult(result.Action, result.Status, result.RuntimeEvent),
	})
}

func runWorkspaceAuthorityForceBreak(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace authority force-break", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	scope := fs.String("scope", "workspace", "Authority scope")
	actorType := fs.String("actor-type", "operator", "Actor type: operator|system")
	actorID := fs.String("actor-id", "", "Actor identifier recorded on the force-break event")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	result, err := store.ForceBreakWorkspaceAuthority(ctx, sqlite.ForceBreakWorkspaceAuthorityInput{
		WorkspaceID: *workspaceID,
		Scope:       *scope,
		ActorType:   *actorType,
		ActorID:     *actorID,
	})
	if err != nil {
		return fmt.Errorf("force-break workspace authority: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"result":   safeWorkspaceAuthorityCLIActionResult(result.Action, result.Status, result.RuntimeEvent),
	})
}

func runWorkspaceAuthorityMaintainOnce(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace authority maintain-once", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	pass, err := runLocalAuthorityLeaseMaintenancePass(ctx, store)
	if err != nil {
		return fmt.Errorf("run workspace authority maintenance pass: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"result":   safeWorkspaceAuthorityCLIMaintenancePass(pass),
	})
}

func safeWorkspaceAuthorityCLIActionResult(action string, status sqlite.LocalWorkspaceAuthorityStatus, event *sqlite.RuntimeEventRecord) workspaceAuthorityCLIActionResult {
	leaseToken := ""
	if status.Authority != nil {
		leaseToken = status.Authority.LeaseToken
	}
	return workspaceAuthorityCLIActionResult{
		Action:       action,
		Status:       safeWorkspaceAuthorityCLIStatus(status),
		RuntimeEvent: safeWorkspaceAuthorityCLIEvent(event, leaseToken),
	}
}

func safeWorkspaceAuthorityCLIStatus(status sqlite.LocalWorkspaceAuthorityStatus) workspaceAuthorityCLIStatus {
	return workspaceAuthorityCLIStatus{
		WorkspaceID: status.WorkspaceID,
		Scope:       status.Scope,
		ReferenceAt: status.ReferenceAt,
		LocalNode:   status.LocalNode,
		Authority:   safeWorkspaceAuthorityCLIRecord(status.Authority),
		JournalHead: status.JournalHead,
		LocalHolder: status.LocalHolder,
		LeaseLive:   status.LeaseLive,
		LeaseState:  status.LeaseState,
	}
}

func safeWorkspaceAuthorityCLIRecord(record *sqlite.WorkspaceAuthorityRecord) *workspaceAuthorityCLIRecord {
	if record == nil {
		return nil
	}
	return &workspaceAuthorityCLIRecord{
		WorkspaceID:           record.WorkspaceID,
		Scope:                 record.Scope,
		HolderAuthorityNodeID: record.HolderAuthorityNodeID,
		LeaseTokenFingerprint: workspaceAuthorityCLILeaseTokenFingerprint(record.LeaseToken),
		Term:                  record.Term,
		LeaseExpiresAt:        record.LeaseExpiresAt,
		CommitWatermark:       record.CommitWatermark,
		AppliedWatermark:      record.AppliedWatermark,
		Status:                record.Status,
		UpdatedAt:             record.UpdatedAt,
	}
}

func safeWorkspaceAuthorityCLIEvent(event *sqlite.RuntimeEventRecord, fallbackLeaseToken string) *workspaceAuthorityCLIEvent {
	if event == nil {
		return nil
	}
	fingerprint := ""
	if strings.TrimSpace(fallbackLeaseToken) != "" {
		fingerprint = workspaceAuthorityCLILeaseTokenFingerprint(fallbackLeaseToken)
	} else {
		fingerprint = canonicalWorkspaceAuthorityCLIFingerprint(event.AuthorityLeaseTokenFingerprint)
	}
	return &workspaceAuthorityCLIEvent{
		EventID:                        event.EventID,
		DedupKey:                       event.DedupKey,
		WorkspaceID:                    event.WorkspaceID,
		EventType:                      event.EventType,
		EntityType:                     event.EntityType,
		EntityID:                       event.EntityID,
		ActorType:                      event.ActorType,
		ActorID:                        event.ActorID,
		AgentID:                        event.AgentID,
		SessionID:                      event.SessionID,
		TaskID:                         event.TaskID,
		RootCauseID:                    event.RootCauseID,
		ProvenanceGroupID:              event.ProvenanceGroupID,
		ParentRefsJSON:                 event.ParentRefsJSON,
		CreatedAt:                      event.CreatedAt,
		AuthorityHolderNodeID:          event.AuthorityHolderNodeID,
		AuthorityTerm:                  event.AuthorityTerm,
		AuthorityLeaseTokenFingerprint: fingerprint,
		IngestSeq:                      event.IngestSeq,
	}
}

func safeWorkspaceAuthorityCLIMaintenancePass(pass localAuthorityLeaseMaintenancePassResult) workspaceAuthorityCLIMaintenancePassResult {
	leaseMaintenance := workspaceAuthorityCLILeaseMaintenanceResult{
		Scope:       pass.LeaseMaintenance.Scope,
		ReferenceAt: pass.LeaseMaintenance.ReferenceAt,
		Healthy:     pass.LeaseMaintenance.Healthy,
		Renewed:     pass.LeaseMaintenance.Renewed,
		Grace:       pass.LeaseMaintenance.Grace,
		Expired:     pass.LeaseMaintenance.Expired,
		Problems:    pass.LeaseMaintenance.Problems,
	}
	if len(pass.LeaseMaintenance.Items) > 0 {
		leaseMaintenance.Items = make([]workspaceAuthorityCLILeaseMaintenanceItem, 0, len(pass.LeaseMaintenance.Items))
		for _, item := range pass.LeaseMaintenance.Items {
			leaseToken := ""
			if item.Authority != nil {
				leaseToken = item.Authority.LeaseToken
			}
			leaseMaintenance.Items = append(leaseMaintenance.Items, workspaceAuthorityCLILeaseMaintenanceItem{
				WorkspaceID:  item.WorkspaceID,
				LeaseState:   item.LeaseState,
				Action:       item.Action,
				Authority:    safeWorkspaceAuthorityCLIRecord(item.Authority),
				RuntimeEvent: safeWorkspaceAuthorityCLIEvent(item.RuntimeEvent, leaseToken),
				Error:        item.Error,
			})
		}
	}
	return workspaceAuthorityCLIMaintenancePassResult{
		LeaseMaintenance:  leaseMaintenance,
		SessionReclaim:    pass.SessionReclaim,
		OrphanClaim:       pass.OrphanClaim,
		ProjectLeadLease:  pass.ProjectLeadLease,
		ClaimLiberation:   pass.ClaimLiberation,
		AuthorityHandoff:  pass.AuthorityHandoff,
		TerminalSessionOQ: pass.TerminalSessionOQ,
	}
}

func workspaceAuthorityCLILeaseTokenFingerprint(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("sha256:%x", sum[:8])
}

func canonicalWorkspaceAuthorityCLIFingerprint(value string) string {
	value = strings.TrimSpace(value)
	if len(value) != len("sha256:")+16 || !strings.HasPrefix(value, "sha256:") {
		return ""
	}
	for _, char := range value[len("sha256:"):] {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return ""
		}
	}
	return value
}
