package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const localAuthorityExpiryGrace = 2 * time.Minute

type LocalWorkspaceAuthorityLeaseMaintenanceInput struct {
	Scope     string `json:"scope,omitempty"`
	ActorType string `json:"actor_type,omitempty"`
	ActorID   string `json:"actor_id,omitempty"`
}

type LocalWorkspaceAuthorityLeaseMaintenanceItem struct {
	WorkspaceID  string                    `json:"workspace_id"`
	LeaseState   string                    `json:"lease_state"`
	Action       string                    `json:"action"`
	Authority    *WorkspaceAuthorityRecord `json:"authority,omitempty"`
	RuntimeEvent *RuntimeEventRecord       `json:"runtime_event,omitempty"`
	Error        string                    `json:"error,omitempty"`
}

type LocalWorkspaceAuthorityLeaseMaintenanceResult struct {
	Scope       string                                        `json:"scope"`
	ReferenceAt string                                        `json:"reference_at"`
	Healthy     int                                           `json:"healthy"`
	Renewed     int                                           `json:"renewed"`
	Grace       int                                           `json:"grace"`
	Expired     int                                           `json:"expired"`
	Problems    int                                           `json:"problems"`
	Items       []LocalWorkspaceAuthorityLeaseMaintenanceItem `json:"items,omitempty"`
}

type AuthorityLeaseDiagnosticsItem struct {
	WorkspaceID       string `json:"workspace_id"`
	Scope             string `json:"scope"`
	LeaseState        string `json:"lease_state"`
	LeaseLive         bool   `json:"lease_live"`
	LocalHolder       bool   `json:"local_holder"`
	HolderAuthorityID string `json:"holder_authority_node_id,omitempty"`
	Term              int64  `json:"term"`
	LeaseExpiresAt    string `json:"lease_expires_at,omitempty"`
	CommitWatermark   int64  `json:"commit_watermark"`
	AppliedWatermark  int64  `json:"applied_watermark"`
	AuthorityStatus   string `json:"authority_status,omitempty"`
	UpdatedAt         string `json:"updated_at,omitempty"`
	Error             string `json:"error,omitempty"`
}

type AuthorityLeaseDiagnostics struct {
	State                string                          `json:"state,omitempty"`
	Message              string                          `json:"message,omitempty"`
	Scope                string                          `json:"scope,omitempty"`
	ReferenceAt          string                          `json:"reference_at,omitempty"`
	LocalAuthorityNodeID string                          `json:"local_authority_node_id,omitempty"`
	TotalHeld            int                             `json:"total_held"`
	ForeignLive          int                             `json:"foreign_live"`
	Healthy              int                             `json:"healthy"`
	RenewDue             int                             `json:"renew_due"`
	Grace                int                             `json:"grace"`
	Stale                int                             `json:"stale"`
	Problems             int                             `json:"problems"`
	Items                []AuthorityLeaseDiagnosticsItem `json:"items,omitempty"`
}

func (s *Store) CurrentAuthorityLeaseDiagnostics(ctx context.Context, scope string) AuthorityLeaseDiagnostics {
	scope = normalizeWorkspaceAuthorityScope(scope)
	referenceAt := time.Now().UTC()
	diag := AuthorityLeaseDiagnostics{
		State:       "ok",
		Message:     "no workspace authority rows observed for this scope",
		Scope:       scope,
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		Items:       make([]AuthorityLeaseDiagnosticsItem, 0, 4),
	}
	if s == nil {
		diag.State = "missing"
		diag.Message = "sqlite store unavailable"
		return diag
	}
	if strings.TrimSpace(s.authorityIdentityPath) == "" {
		diag.State = "error"
		diag.Message = "authority identity path is not configured"
		return diag
	}

	authorityNodeID, err := readAuthorityNodeID(s.authorityIdentityPath)
	if err != nil {
		diag.State = "error"
		diag.Message = err.Error()
		if errors.Is(err, os.ErrNotExist) {
			diag.State = "missing"
		}
		return diag
	}
	diag.LocalAuthorityNodeID = authorityNodeID

	records, err := s.listWorkspaceAuthorityRecords(ctx, scope)
	if err != nil {
		diag.State = "error"
		diag.Message = fmt.Sprintf("list workspace authority records: %v", err)
		return diag
	}
	if len(records) == 0 {
		return diag
	}

	for _, current := range records {
		localHolder := strings.TrimSpace(current.HolderAuthorityNodeID) == authorityNodeID
		item := AuthorityLeaseDiagnosticsItem{
			WorkspaceID:       current.WorkspaceID,
			Scope:             current.Scope,
			LeaseState:        "missing",
			LocalHolder:       localHolder,
			HolderAuthorityID: current.HolderAuthorityNodeID,
			Term:              current.Term,
			LeaseExpiresAt:    current.LeaseExpiresAt,
			CommitWatermark:   current.CommitWatermark,
			AppliedWatermark:  current.AppliedWatermark,
			AuthorityStatus:   string(current.Status),
			UpdatedAt:         current.UpdatedAt,
		}

		if current.Status == WorkspaceAuthorityStatusExpired {
			item.LeaseState = "expired"
			diag.Items = append(diag.Items, item)
			continue
		}

		state, live, stateErr := evaluateLocalWorkspaceAuthorityLeaseState(current, authorityNodeID, referenceAt)
		item.LeaseLive = live
		item.LeaseState = state
		if stateErr != nil {
			item.Error = stateErr.Error()
			item.LeaseState = "invalid"
			diag.Problems++
			diag.Items = append(diag.Items, item)
			continue
		}

		switch state {
		case "healthy":
			if localHolder {
				diag.TotalHeld++
				diag.Healthy++
			}
		case "renew_due":
			if localHolder {
				diag.TotalHeld++
				diag.RenewDue++
			}
		case "stale":
			if localHolder {
				diag.TotalHeld++
			}
			leaseExpiresAt, parseErr := parseAuthorityTimestamp("workspace_authority.lease_expires_at", current.LeaseExpiresAt, true)
			if parseErr != nil {
				item.Error = parseErr.Error()
				item.LeaseState = "invalid"
				diag.Problems++
				break
			}
			if referenceAt.Before(leaseExpiresAt.Add(localAuthorityExpiryGrace)) {
				item.LeaseState = "grace"
				diag.Grace++
			} else {
				diag.Stale++
			}
		case "foreign_live":
			diag.ForeignLive++
		case "orphaned_holder":
			// CA-25: holder node row was deleted; the local node self-heals this on the
			// next EnsureLocalWorkspaceAuthority (adopts via term-bump). Surface it as a
			// degraded-but-recoverable problem rather than a hard operator-recovery wedge.
			if item.Error == "" {
				item.Error = "workspace authority holder node is missing (orphaned lease); local node will adopt it on next ensure"
			}
			diag.Problems++
		case "incomplete", "invalid", "transferring":
			if item.Error == "" {
				item.Error = "workspace authority requires operator recovery before automatic lease maintenance can proceed"
			}
			diag.Problems++
		default:
			item.Error = fmt.Sprintf("unsupported workspace authority lease_state %q", state)
			diag.Problems++
		}

		diag.Items = append(diag.Items, item)
	}

	switch {
	case diag.Problems > 0 || diag.Stale > 0 || diag.Grace > 0 || diag.ForeignLive > 0:
		diag.State = "degraded"
		diag.Message = authorityLeaseDiagnosticsMessage(diag)
	case diag.RenewDue > 0:
		diag.Message = authorityLeaseDiagnosticsMessage(diag)
	default:
		diag.Message = "workspace authority lease diagnostics are healthy"
	}
	return diag
}

func (s *Store) CurrentLocalAuthorityLeaseDiagnostics(ctx context.Context, scope string) AuthorityLeaseDiagnostics {
	return s.CurrentAuthorityLeaseDiagnostics(ctx, scope)
}

func authorityLeaseDiagnosticsMessage(diag AuthorityLeaseDiagnostics) string {
	parts := make([]string, 0, 4)
	if diag.Problems > 0 {
		parts = append(parts, fmt.Sprintf("problems=%d", diag.Problems))
	}
	if diag.ForeignLive > 0 {
		parts = append(parts, fmt.Sprintf("foreign_live=%d", diag.ForeignLive))
	}
	if diag.Stale > 0 {
		parts = append(parts, fmt.Sprintf("stale=%d", diag.Stale))
	}
	if diag.Grace > 0 {
		parts = append(parts, fmt.Sprintf("grace=%d", diag.Grace))
	}
	if diag.RenewDue > 0 {
		parts = append(parts, fmt.Sprintf("renew_due=%d", diag.RenewDue))
	}
	if len(parts) == 0 {
		if diag.TotalHeld == 0 && diag.ForeignLive == 0 {
			return "no workspace authority rows observed for this scope"
		}
		return "workspace authority lease diagnostics are healthy"
	}
	return "workspace authority lease diagnostics: " + strings.Join(parts, ", ")
}

func (s *Store) listWorkspaceAuthorityRecords(ctx context.Context, scope string) ([]WorkspaceAuthorityRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at,
       commit_watermark, applied_watermark, status, updated_at
  FROM workspace_authority
 WHERE scope = ?
 ORDER BY workspace_id`,
		normalizeWorkspaceAuthorityScope(scope),
	)
	if err != nil {
		return nil, fmt.Errorf("list workspace authority records: %w", err)
	}
	defer rows.Close()

	records := make([]WorkspaceAuthorityRecord, 0, 8)
	for rows.Next() {
		var record WorkspaceAuthorityRecord
		if err := scanWorkspaceAuthority(rows, &record); err != nil {
			return nil, fmt.Errorf("scan workspace authority record: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace authority records: %w", err)
	}
	return records, nil
}

func (s *Store) listLocalWorkspaceAuthorityRecords(ctx context.Context, scope, holderAuthorityNodeID string) ([]WorkspaceAuthorityRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at,
       commit_watermark, applied_watermark, status, updated_at
  FROM workspace_authority
 WHERE scope = ?
   AND holder_authority_node_id = ?
 ORDER BY workspace_id`,
		normalizeWorkspaceAuthorityScope(scope),
		strings.TrimSpace(holderAuthorityNodeID),
	)
	if err != nil {
		return nil, fmt.Errorf("list local workspace authority records: %w", err)
	}
	defer rows.Close()

	records := make([]WorkspaceAuthorityRecord, 0, 8)
	for rows.Next() {
		var record WorkspaceAuthorityRecord
		if err := scanWorkspaceAuthority(rows, &record); err != nil {
			return nil, fmt.Errorf("scan local workspace authority record: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate local workspace authority records: %w", err)
	}
	return records, nil
}

func (s *Store) RunLocalWorkspaceAuthorityLeaseMaintenance(ctx context.Context, input LocalWorkspaceAuthorityLeaseMaintenanceInput) (LocalWorkspaceAuthorityLeaseMaintenanceResult, error) {
	scope := normalizeWorkspaceAuthorityScope(input.Scope)
	actorType, actorID, err := normalizeLocalAuthorityActor(input.ActorType, input.ActorID)
	if err != nil {
		return LocalWorkspaceAuthorityLeaseMaintenanceResult{}, err
	}
	if actorType == authorityActorTypeSystem && strings.TrimSpace(actorID) == "authority_spine" {
		actorID = "local_lease_manager"
	}

	referenceAt := time.Now().UTC()
	node, err := s.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		return LocalWorkspaceAuthorityLeaseMaintenanceResult{}, fmt.Errorf("ensure local authority node: %w", err)
	}
	records, err := s.listLocalWorkspaceAuthorityRecords(ctx, scope, node.AuthorityNodeID)
	if err != nil {
		return LocalWorkspaceAuthorityLeaseMaintenanceResult{}, err
	}

	result := LocalWorkspaceAuthorityLeaseMaintenanceResult{
		Scope:       scope,
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		Items:       make([]LocalWorkspaceAuthorityLeaseMaintenanceItem, 0, len(records)),
	}

	for _, current := range records {
		item := LocalWorkspaceAuthorityLeaseMaintenanceItem{
			WorkspaceID: current.WorkspaceID,
			Authority:   &current,
		}
		if current.Status == WorkspaceAuthorityStatusExpired {
			item.LeaseState = "expired"
			item.Action = "ALREADY_EXPIRED"
			result.Items = append(result.Items, item)
			continue
		}

		state, _, stateErr := evaluateLocalWorkspaceAuthorityLeaseState(current, node.AuthorityNodeID, referenceAt)
		item.LeaseState = state
		if stateErr != nil {
			item.Action = "PROBLEM"
			item.Error = stateErr.Error()
			result.Problems++
			result.Items = append(result.Items, item)
			continue
		}

		switch state {
		case "healthy":
			item.Action = "UNCHANGED"
			result.Healthy++
		case "renew_due":
			updated, runtimeEvent, renewErr := s.RenewWorkspaceAuthority(ctx, WorkspaceAuthorityRenewInput{
				WorkspaceID:           current.WorkspaceID,
				Scope:                 current.Scope,
				HolderAuthorityNodeID: current.HolderAuthorityNodeID,
				LeaseToken:            current.LeaseToken,
				Term:                  current.Term,
				LeaseExpiresAt:        referenceAt.Add(localAuthorityLeaseTTL).Format(time.RFC3339Nano),
				CommitWatermark:       current.CommitWatermark,
				AppliedWatermark:      current.AppliedWatermark,
				ActorType:             actorType,
				ActorID:               actorID,
				ReferenceAt:           result.ReferenceAt,
			})
			if renewErr != nil {
				item.Action = "PROBLEM"
				item.Error = renewErr.Error()
				result.Problems++
				break
			}
			item.Action = "RENEWED"
			item.Authority = &updated
			item.RuntimeEvent = &runtimeEvent
			result.Renewed++
		case "stale":
			leaseExpiresAt, parseErr := parseAuthorityTimestamp("workspace_authority.lease_expires_at", current.LeaseExpiresAt, true)
			if parseErr != nil {
				item.Action = "PROBLEM"
				item.Error = parseErr.Error()
				result.Problems++
				break
			}
			if referenceAt.Before(leaseExpiresAt.Add(localAuthorityExpiryGrace)) {
				item.Action = "GRACE"
				result.Grace++
				break
			}
			updated, runtimeEvent, expireErr := s.ExpireWorkspaceAuthority(ctx, WorkspaceAuthorityExpireInput{
				WorkspaceID:           current.WorkspaceID,
				Scope:                 current.Scope,
				HolderAuthorityNodeID: current.HolderAuthorityNodeID,
				LeaseToken:            current.LeaseToken,
				Term:                  current.Term,
				CommitWatermark:       current.CommitWatermark,
				AppliedWatermark:      current.AppliedWatermark,
				ActorType:             actorType,
				ActorID:               actorID,
				ReferenceAt:           result.ReferenceAt,
			})
			if expireErr != nil {
				item.Action = "PROBLEM"
				item.Error = expireErr.Error()
				result.Problems++
				break
			}
			item.Action = "EXPIRED"
			item.Authority = &updated
			item.RuntimeEvent = &runtimeEvent
			result.Expired++
		case "foreign_live":
			item.Action = "PROBLEM"
			item.Error = "local lease maintenance selected non-local workspace authority row"
			result.Problems++
		case "orphaned_holder":
			// CA-25: a holderless row is not local-held, so it normally won't be listed
			// here; handle defensively. Recovery is via EnsureLocalWorkspaceAuthority's
			// adopt path, not this maintenance pass.
			item.Action = "PROBLEM"
			item.Error = "workspace authority holder node is missing (orphaned lease); local node will adopt it on next ensure"
			result.Problems++
		case "incomplete", "invalid", "transferring":
			item.Action = "PROBLEM"
			item.Error = "workspace authority requires operator recovery before automatic lease maintenance can proceed"
			result.Problems++
		default:
			item.Action = "PROBLEM"
			item.Error = fmt.Sprintf("unsupported workspace authority lease_state %q", state)
			result.Problems++
		}

		result.Items = append(result.Items, item)
	}

	return result, nil
}
