package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	controlCommandEventType            = "control.command.requested"
	controlCommandEntityType           = "control_command"
	controlCommandTypedEventType       = "CONTROL_COMMAND_REQUEST"
	ControlCommandExcludeAgentTension  = "tension.exclude_agent"
	ControlCommandRefreshKernel        = "agent.control.refresh_kernel"
	ControlCommandFlushCache           = "agent.control.flush_cache"
	ControlCommandClusterFreeze        = "cluster.freeze"
	ControlCommandWorkspaceThrottle    = "workspace.throttle"
	ControlCommandClusterModeSwitch    = "cluster.mode_switch"
	defaultSystemControlCommandActorID = "system"
	controlActorTypeOperator           = "operator"
	controlActorTypeSystem             = "system"
	controlOwnershipRoleNone           = "none"
	controlOwnershipRoleActuatorOwner  = "actuator_owner"
	controlOwnershipRoleAdvisoryOnly   = "advisory_only"
	controlOwnershipRoleSignalSource   = "bounded_signal_source"
	controlApplyModeInlineJournaled    = "inline_journaled"
	controlApplyModeJournalOnly        = "journal_request_only"
)

type ControlCommandOwnership struct {
	ActuatorOwner string `json:"actuator_owner"`
	RRP           string `json:"rrp"`
	RMP           string `json:"rmp"`
	RSP           string `json:"rsp"`
	ApplyMode     string `json:"apply_mode"`
	Summary       string `json:"summary,omitempty"`
}

type controlCommandContract struct {
	DefaultScope string
	Ownership    ControlCommandOwnership
}

type ControlCommandInput struct {
	CommandID             string
	WorkspaceID           string
	CommandType           string
	Scope                 string
	ProtoClusterID        string
	TensionID             string
	AgentID               string
	TargetMode            string
	TTLSeconds            int
	Reason                string
	RequestedBy           string
	ActorType             string
	ParentRefs            []string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ControlCommandRecord struct {
	CommandID      string                  `json:"command_id"`
	WorkspaceID    string                  `json:"workspace_id"`
	CommandType    string                  `json:"command_type"`
	Scope          string                  `json:"scope"`
	ProtoClusterID string                  `json:"proto_cluster_id,omitempty"`
	TensionID      string                  `json:"tension_id,omitempty"`
	AgentID        string                  `json:"agent_id,omitempty"`
	TargetMode     string                  `json:"target_mode,omitempty"`
	TTLSeconds     int                     `json:"ttl_seconds,omitempty"`
	Reason         string                  `json:"reason,omitempty"`
	RequestedBy    string                  `json:"requested_by"`
	ActorType      string                  `json:"actor_type"`
	Ownership      ControlCommandOwnership `json:"ownership"`
	RequestedAt    string                  `json:"requested_at"`
	AppliedInline  bool                    `json:"applied_inline"`
	ExpiresAt      string                  `json:"expires_at,omitempty"`
	ParentRefs     []string                `json:"parent_refs,omitempty"`
}

type controlCommandExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func normalizeControlCommandInput(input ControlCommandInput) (ControlCommandRecord, error) {
	record := ControlCommandRecord{
		CommandID:      strings.TrimSpace(input.CommandID),
		WorkspaceID:    strings.TrimSpace(input.WorkspaceID),
		CommandType:    strings.ToLower(strings.TrimSpace(input.CommandType)),
		Scope:          strings.TrimSpace(input.Scope),
		ProtoClusterID: strings.TrimSpace(input.ProtoClusterID),
		TensionID:      strings.TrimSpace(input.TensionID),
		AgentID:        strings.TrimSpace(input.AgentID),
		TargetMode:     normalizeClusterControlMode(input.TargetMode),
		TTLSeconds:     input.TTLSeconds,
		Reason:         strings.TrimSpace(input.Reason),
		RequestedBy:    strings.TrimSpace(input.RequestedBy),
		ActorType:      strings.ToLower(strings.TrimSpace(input.ActorType)),
		ParentRefs:     uniqueSortedStrings(input.ParentRefs),
	}
	if record.WorkspaceID == "" {
		return ControlCommandRecord{}, errors.New("workspace_id is required")
	}
	if record.CommandType == "" {
		return ControlCommandRecord{}, errors.New("command_type is required")
	}
	if record.RequestedBy == "" {
		return ControlCommandRecord{}, errors.New("requested_by is required")
	}
	if record.ActorType == "" {
		record.ActorType = controlActorTypeOperator
	}
	if record.ActorType != controlActorTypeOperator && record.ActorType != controlActorTypeSystem {
		return ControlCommandRecord{}, errors.New("actor_type must be operator or system for control commands")
	}
	contract, ok := controlCommandContractForType(record.CommandType)
	if !ok {
		return ControlCommandRecord{}, fmt.Errorf("unsupported control command_type %q", record.CommandType)
	}
	record.Ownership = contract.Ownership
	if record.CommandID == "" {
		record.CommandID = nextID("ctrlcmd")
	}
	switch record.CommandType {
	case ControlCommandExcludeAgentTension:
		record.Scope = firstNonEmpty(record.Scope, contract.DefaultScope)
		if record.TensionID == "" {
			return ControlCommandRecord{}, errors.New("tension_id is required for tension.exclude_agent")
		}
		if record.AgentID == "" {
			return ControlCommandRecord{}, errors.New("agent_id is required for tension.exclude_agent")
		}
		if record.TTLSeconds <= 0 {
			return ControlCommandRecord{}, errors.New("ttl_seconds must be > 0 for tension.exclude_agent")
		}
	case ControlCommandRefreshKernel, ControlCommandFlushCache:
		record.Scope = firstNonEmpty(record.Scope, contract.DefaultScope)
		if record.AgentID == "" {
			return ControlCommandRecord{}, errors.New("agent_id is required for agent-scoped control commands")
		}
	case ControlCommandClusterFreeze:
		record.Scope = firstNonEmpty(record.Scope, contract.DefaultScope)
		if record.ProtoClusterID == "" {
			return ControlCommandRecord{}, errors.New("proto_cluster_id is required for cluster.freeze")
		}
	case ControlCommandWorkspaceThrottle:
		record.Scope = firstNonEmpty(record.Scope, contract.DefaultScope)
	case ControlCommandClusterModeSwitch:
		record.Scope = firstNonEmpty(record.Scope, contract.DefaultScope)
		if record.ProtoClusterID == "" {
			return ControlCommandRecord{}, errors.New("proto_cluster_id is required for cluster.mode_switch")
		}
		if strings.TrimSpace(input.TargetMode) == "" || !isKnownClusterControlMode(input.TargetMode) {
			return ControlCommandRecord{}, errors.New("target_mode is required for cluster.mode_switch")
		}
	}
	return record, nil
}

func (s *Store) RequestControlCommand(ctx context.Context, input ControlCommandInput) (ControlCommandRecord, error) {
	record, _, err := s.RequestControlCommandWithEvent(ctx, input)
	return record, err
}

func (s *Store) RequestControlCommandWithEvent(ctx context.Context, input ControlCommandInput) (ControlCommandRecord, RuntimeEventRecord, error) {
	record, err := normalizeControlCommandInput(input)
	if err != nil {
		return ControlCommandRecord{}, RuntimeEventRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	record.RequestedAt = now
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ControlCommandRecord{}, RuntimeEventRecord{}, err
	}
	parentRefsJSON := ""
	if len(record.ParentRefs) > 0 {
		parentRefsJSON, err = normalizeRuntimeEventParentRefs(mustJSON(record.ParentRefs))
		if err != nil {
			return ControlCommandRecord{}, RuntimeEventRecord{}, fmt.Errorf("normalize control command parent refs: %w", err)
		}
	}

	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
			return err
		}

		switch record.CommandType {
		case ControlCommandExcludeAgentTension:
			referenceAt, refErr := s.workspaceReferenceTimestamp(ctx, record.WorkspaceID, now)
			if refErr != nil {
				return refErr
			}
			referenceAt = strings.TrimSpace(referenceAt)
			if referenceAt == "" {
				referenceAt = now
			}
			record.ExpiresAt = memoryInvalidationTimestampAdd(referenceAt, time.Duration(record.TTLSeconds)*time.Second)
			record.AppliedInline = true
			if err := upsertTensionExclusionTx(ctx, tx, record.WorkspaceID, record.TensionID, record.AgentID, record.ExpiresAt, record.Reason, referenceAt); err != nil {
				return err
			}
		default:
			record.AppliedInline = false
		}

		payload := map[string]any{
			"command_id":       record.CommandID,
			"workspace_id":     record.WorkspaceID,
			"command_type":     record.CommandType,
			"scope":            record.Scope,
			"reason":           record.Reason,
			"requested_by":     record.RequestedBy,
			"actor_type":       record.ActorType,
			"ownership":        record.Ownership,
			"requested_at":     record.RequestedAt,
			"applied_inline":   record.AppliedInline,
			"typed_event_type": controlCommandTypedEventType,
			"event_kind":       controlCommandEventType,
			"event_type":       controlCommandEventType,
			"entity_type":      controlCommandEntityType,
			"entity_id":        record.CommandID,
		}
		if record.ProtoClusterID != "" {
			payload["proto_cluster_id"] = record.ProtoClusterID
		}
		if record.TensionID != "" {
			payload["tension_id"] = record.TensionID
		}
		if record.AgentID != "" {
			payload["agent_id"] = record.AgentID
		}
		if record.TargetMode != "" {
			payload["target_mode"] = record.TargetMode
		}
		if record.TTLSeconds > 0 {
			payload["ttl_seconds"] = record.TTLSeconds
		}
		if record.ExpiresAt != "" {
			payload["expires_at"] = record.ExpiresAt
		}
		if len(record.ParentRefs) > 0 {
			payload["parent_refs"] = append([]string(nil), record.ParentRefs...)
		}
		fields := map[string]string{
			"workspace_id":   record.WorkspaceID,
			"command_id":     record.CommandID,
			"command_type":   record.CommandType,
			"scope":          record.Scope,
			"requested_by":   record.RequestedBy,
			"actor_type":     record.ActorType,
			"requested_at":   record.RequestedAt,
			"applied_inline": fmt.Sprintf("%t", record.AppliedInline),
			"event_type":     controlCommandEventType,
			"entity_type":    controlCommandEntityType,
			"entity_id":      record.CommandID,
		}
		if record.ProtoClusterID != "" {
			fields["proto_cluster_id"] = record.ProtoClusterID
		}
		if record.TensionID != "" {
			fields["tension_id"] = record.TensionID
		}
		if record.AgentID != "" {
			fields["agent_id"] = record.AgentID
		}
		if record.TargetMode != "" {
			fields["target_mode"] = record.TargetMode
		}
		if record.TTLSeconds > 0 {
			fields["ttl_seconds"] = fmt.Sprintf("%d", record.TTLSeconds)
		}
		if record.ExpiresAt != "" {
			fields["expires_at"] = record.ExpiresAt
		}
		if len(record.ParentRefs) > 0 {
			fields["parent_refs_json"] = mustJSON(record.ParentRefs)
		}
		var attachErr error
		payload, attachErr = attachControlCommandPromptContextEnvelope(payload, input.PromptContextEnvelope, controlCommandPromptContextSurface(input.PromptContextSurface), fields)
		if attachErr != nil {
			return attachErr
		}

		appended, appendErr := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:        nextID("rtev"),
			WorkspaceID:    record.WorkspaceID,
			EventType:      controlCommandEventType,
			EntityType:     controlCommandEntityType,
			EntityID:       record.CommandID,
			ActorType:      record.ActorType,
			ActorID:        record.RequestedBy,
			ParentRefsJSON: parentRefsJSON,
			PayloadJSON:    mustJSON(payload),
			CreatedAt:      now,
		})
		if appendErr != nil {
			return appendErr
		}
		event = appended
		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, record.WorkspaceID); err != nil {
			return fmt.Errorf("touch workspace after control command: %w", err)
		}
		return nil
	}); err != nil {
		return ControlCommandRecord{}, RuntimeEventRecord{}, err
	}
	return record, event, nil
}

func controlCommandPromptContextSurface(surface string) string {
	if surface = strings.TrimSpace(surface); surface != "" {
		return surface
	}
	return "workspace.control.command.request"
}

func upsertTensionExclusionTx(ctx context.Context, tx controlCommandExecer, workspaceID, tensionID, agentID, expiresAt, reason, referenceAt string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	tensionID = strings.TrimSpace(tensionID)
	agentID = strings.TrimSpace(agentID)
	reason = strings.TrimSpace(reason)
	expiresAt = strings.TrimSpace(expiresAt)
	referenceAt = strings.TrimSpace(referenceAt)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if tensionID == "" {
		return errors.New("tension_id is required")
	}
	if agentID == "" {
		return errors.New("agent_id is required")
	}
	if expiresAt == "" {
		return errors.New("expires_at is required")
	}
	if referenceAt == "" {
		return errors.New("reference_at is required")
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO workspace_tension_exclusions (workspace_id, tension_id, agent_id, expires_at, reason, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(workspace_id, tension_id, agent_id)
		DO UPDATE SET expires_at = excluded.expires_at, reason = excluded.reason, updated_at = excluded.updated_at
	`, workspaceID, tensionID, agentID, expiresAt, reason, referenceAt, referenceAt); err != nil {
		return fmt.Errorf("upsert workspace tension exclusion: %w", err)
	}
	return nil
}

func controlCommandContractForType(commandType string) (controlCommandContract, bool) {
	switch strings.ToLower(strings.TrimSpace(commandType)) {
	case ControlCommandExcludeAgentTension:
		return controlCommandContract{
			DefaultScope: "tension",
			Ownership: ControlCommandOwnership{
				ActuatorOwner: "RRP",
				RRP:           controlOwnershipRoleActuatorOwner,
				RMP:           controlOwnershipRoleNone,
				RSP:           controlOwnershipRoleSignalSource,
				ApplyMode:     controlApplyModeInlineJournaled,
				Summary:       "RRP owns bounded runtime exclusion; RSP may only contribute bounded signals through the canonical command path.",
			},
		}, true
	case ControlCommandRefreshKernel:
		return controlCommandContract{
			DefaultScope: "agent",
			Ownership: ControlCommandOwnership{
				ActuatorOwner: "RMP",
				RRP:           controlOwnershipRoleNone,
				RMP:           controlOwnershipRoleActuatorOwner,
				RSP:           controlOwnershipRoleAdvisoryOnly,
				ApplyMode:     controlApplyModeJournalOnly,
				Summary:       "RMP owns memory-refresh actuation; RSP remains advisory-only.",
			},
		}, true
	case ControlCommandFlushCache:
		return controlCommandContract{
			DefaultScope: "agent",
			Ownership: ControlCommandOwnership{
				ActuatorOwner: "RMP",
				RRP:           controlOwnershipRoleNone,
				RMP:           controlOwnershipRoleActuatorOwner,
				RSP:           controlOwnershipRoleAdvisoryOnly,
				ApplyMode:     controlApplyModeJournalOnly,
				Summary:       "RMP owns cache-flush actuation; RSP remains advisory-only.",
			},
		}, true
	case ControlCommandClusterFreeze:
		return controlCommandContract{
			DefaultScope: "cluster",
			Ownership: ControlCommandOwnership{
				ActuatorOwner: "RRP",
				RRP:           controlOwnershipRoleActuatorOwner,
				RMP:           controlOwnershipRoleNone,
				RSP:           controlOwnershipRoleAdvisoryOnly,
				ApplyMode:     controlApplyModeJournalOnly,
				Summary:       "RRP owns cluster-freeze actuation; RSP may only advise through bounded read-side signals.",
			},
		}, true
	case ControlCommandWorkspaceThrottle:
		return controlCommandContract{
			DefaultScope: "workspace",
			Ownership: ControlCommandOwnership{
				ActuatorOwner: "RRP",
				RRP:           controlOwnershipRoleActuatorOwner,
				RMP:           controlOwnershipRoleNone,
				RSP:           controlOwnershipRoleAdvisoryOnly,
				ApplyMode:     controlApplyModeJournalOnly,
				Summary:       "RRP owns workspace-throttle actuation; RSP may only advise through bounded read-side signals.",
			},
		}, true
	case ControlCommandClusterModeSwitch:
		return controlCommandContract{
			DefaultScope: "cluster",
			Ownership: ControlCommandOwnership{
				ActuatorOwner: "RRP",
				RRP:           controlOwnershipRoleActuatorOwner,
				RMP:           controlOwnershipRoleNone,
				RSP:           controlOwnershipRoleAdvisoryOnly,
				ApplyMode:     controlApplyModeJournalOnly,
				Summary:       "RRP owns cluster-mode switching; RSP may only advise through bounded read-side signals.",
			},
		}, true
	default:
		return controlCommandContract{}, false
	}
}
