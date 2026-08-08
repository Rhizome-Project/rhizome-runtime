package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/fssecure"
)

const localAuthorityNodeKind = "sqlite_local_store"

const (
	authorityEventEntityType      = "workspace_authority"
	authorityEventTypedEventType  = "WORKSPACE_AUTHORITY_EVENT"
	authorityActorTypeSystem      = "system"
	authorityActorTypeOperator    = "operator"
	authorityScopeWorkspace       = "workspace"
	AuthorityEventGranted         = "authority.granted"
	AuthorityEventRenewed         = "authority.renewed"
	AuthorityEventExpired         = "authority.expired"
	AuthorityEventTransferStarted = "authority.transfer_started"
	AuthorityEventTransferred     = "authority.transferred"
	AuthorityEventRejected        = "authority.rejected"
	AuthorityEventRejoinRejected  = "authority.rejoin_rejected"
	localAuthorityLeaseTTL        = 10 * time.Minute
	localAuthorityRenewLead       = 3 * time.Minute
)

type RuntimeNodeStatus string

const (
	RuntimeNodeStatusOnline  RuntimeNodeStatus = "ONLINE"
	RuntimeNodeStatusOffline RuntimeNodeStatus = "OFFLINE"
)

type RuntimeNodeRecord struct {
	AuthorityNodeID string            `json:"authority_node_id"`
	NodeKind        string            `json:"node_kind"`
	HostLabel       string            `json:"host_label"`
	BootInstanceID  string            `json:"boot_instance_id"`
	RegisteredAt    string            `json:"registered_at"`
	LastSeenAt      string            `json:"last_seen_at"`
	Status          RuntimeNodeStatus `json:"status"`
}

type WorkspaceAuthorityStatus string

const (
	WorkspaceAuthorityStatusActive       WorkspaceAuthorityStatus = "ACTIVE"
	WorkspaceAuthorityStatusExpired      WorkspaceAuthorityStatus = "EXPIRED"
	WorkspaceAuthorityStatusTransferring WorkspaceAuthorityStatus = "TRANSFERRING"
	WorkspaceAuthorityStatusRejected     WorkspaceAuthorityStatus = "REJECTED"
	WorkspaceAuthorityStatusRecovering   WorkspaceAuthorityStatus = "RECOVERING"
)

type WorkspaceAuthorityRecord struct {
	WorkspaceID           string                   `json:"workspace_id"`
	Scope                 string                   `json:"scope"`
	HolderAuthorityNodeID string                   `json:"holder_authority_node_id,omitempty"`
	LeaseToken            string                   `json:"lease_token,omitempty"`
	Term                  int64                    `json:"term"`
	LeaseExpiresAt        string                   `json:"lease_expires_at,omitempty"`
	CommitWatermark       int64                    `json:"commit_watermark"`
	AppliedWatermark      int64                    `json:"applied_watermark"`
	Status                WorkspaceAuthorityStatus `json:"status"`
	UpdatedAt             string                   `json:"updated_at"`
}

type AuthorityNodeDiagnostics struct {
	State           string `json:"state,omitempty"`
	Message         string `json:"message,omitempty"`
	AuthorityNodeID string `json:"authority_node_id,omitempty"`
	NodeKind        string `json:"node_kind,omitempty"`
	HostLabel       string `json:"host_label,omitempty"`
	BootInstanceID  string `json:"boot_instance_id,omitempty"`
	Status          string `json:"status,omitempty"`
}

type AuthorityEventInput struct {
	EventID                       string
	WorkspaceID                   string
	Scope                         string
	EventType                     string
	HolderAuthorityNodeID         string
	PreviousHolderAuthorityNodeID string
	LeaseToken                    string
	Term                          int64
	LeaseExpiresAt                string
	CommitWatermark               int64
	AppliedWatermark              int64
	Status                        WorkspaceAuthorityStatus
	ActorType                     string
	ActorID                       string
	RejectCode                    string
	RejectMessage                 string
	ExpectedAuthorityNodeID       string
	ExpectedTerm                  int64
	ReferenceAt                   string
	ParentRefs                    []string
}

type WorkspaceAuthorityFenceInput struct {
	WorkspaceID                   string
	Scope                         string
	ExpectedHolderAuthorityNodeID string
	ExpectedLeaseToken            string
	ExpectedTerm                  int64
	ReferenceAt                   string
}

type WorkspaceAuthorityClaimInput struct {
	WorkspaceID           string
	Scope                 string
	HolderAuthorityNodeID string
	LeaseToken            string
	Term                  int64
	LeaseExpiresAt        string
	CommitWatermark       int64
	AppliedWatermark      int64
	ActorType             string
	ActorID               string
	ReferenceAt           string
}

type WorkspaceAuthorityRenewInput struct {
	WorkspaceID           string
	Scope                 string
	HolderAuthorityNodeID string
	LeaseToken            string
	Term                  int64
	LeaseExpiresAt        string
	CommitWatermark       int64
	AppliedWatermark      int64
	ActorType             string
	ActorID               string
	ReferenceAt           string
}

type WorkspaceAuthorityExpireInput struct {
	WorkspaceID           string
	Scope                 string
	HolderAuthorityNodeID string
	LeaseToken            string
	Term                  int64
	CommitWatermark       int64
	AppliedWatermark      int64
	ActorType             string
	ActorID               string
	ReferenceAt           string
}

type WorkspaceAuthorityTransferInput struct {
	WorkspaceID                  string
	Scope                        string
	CurrentHolderAuthorityNodeID string
	CurrentLeaseToken            string
	CurrentTerm                  int64
	NewHolderAuthorityNodeID     string
	NewLeaseToken                string
	NewTerm                      int64
	LeaseExpiresAt               string
	CommitWatermark              int64
	AppliedWatermark             int64
	ActorType                    string
	ActorID                      string
	ReferenceAt                  string
}

type LocalWorkspaceAuthorityStatus struct {
	WorkspaceID string                    `json:"workspace_id"`
	Scope       string                    `json:"scope"`
	ReferenceAt string                    `json:"reference_at"`
	LocalNode   RuntimeNodeRecord         `json:"local_node"`
	Authority   *WorkspaceAuthorityRecord `json:"authority,omitempty"`
	JournalHead int64                     `json:"journal_head"`
	LocalHolder bool                      `json:"local_holder"`
	LeaseLive   bool                      `json:"lease_live"`
	LeaseState  string                    `json:"lease_state"`
}

type EnsureLocalWorkspaceAuthorityInput struct {
	WorkspaceID string `json:"workspace_id"`
	Scope       string `json:"scope,omitempty"`
	ActorType   string `json:"actor_type,omitempty"`
	ActorID     string `json:"actor_id,omitempty"`
}

type EnsureLocalWorkspaceAuthorityResult struct {
	Action       string                        `json:"action"`
	Status       LocalWorkspaceAuthorityStatus `json:"status"`
	RuntimeEvent *RuntimeEventRecord           `json:"runtime_event,omitempty"`
}

type ForceBreakWorkspaceAuthorityInput struct {
	WorkspaceID string `json:"workspace_id"`
	Scope       string `json:"scope,omitempty"`
	ActorType   string `json:"actor_type,omitempty"`
	ActorID     string `json:"actor_id,omitempty"`
}

type ForceBreakWorkspaceAuthorityResult struct {
	Action       string                        `json:"action"`
	Status       LocalWorkspaceAuthorityStatus `json:"status"`
	RuntimeEvent *RuntimeEventRecord           `json:"runtime_event,omitempty"`
}

type WorkspaceFencedWriteTxFunc func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error

func authorityIdentityPathForDB(dbPath string) string {
	dir := filepath.Dir(strings.TrimSpace(dbPath))
	if dir == "" || dir == "." {
		return ".rhizome-authority-node-id"
	}
	return filepath.Join(dir, ".rhizome-authority-node-id")
}

func authorityEventEntityID(workspaceID, scope string) string {
	workspaceID = strings.TrimSpace(workspaceID)
	scope = strings.TrimSpace(scope)
	if scope == "" {
		scope = authorityScopeWorkspace
	}
	return workspaceID + "/" + scope
}

func expectedAuthorityEventStatus(eventType string) (WorkspaceAuthorityStatus, error) {
	switch strings.TrimSpace(eventType) {
	case AuthorityEventGranted, AuthorityEventRenewed, AuthorityEventTransferred:
		return WorkspaceAuthorityStatusActive, nil
	case AuthorityEventTransferStarted:
		return WorkspaceAuthorityStatusTransferring, nil
	case AuthorityEventExpired:
		return WorkspaceAuthorityStatusExpired, nil
	case AuthorityEventRejected, AuthorityEventRejoinRejected:
		return WorkspaceAuthorityStatusRejected, nil
	default:
		return "", fmt.Errorf("unsupported authority event_type %q", eventType)
	}
}

func validateAuthorityEventTimestamp(fieldName, raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if _, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return nil
	}
	if _, err := time.Parse(time.RFC3339, raw); err == nil {
		return nil
	}
	return fmt.Errorf("%s must be RFC3339/RFC3339Nano, got %q", fieldName, raw)
}

func normalizeAuthorityEventInput(input AuthorityEventInput) (AuthorityEventInput, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.Scope = strings.TrimSpace(input.Scope)
	input.EventType = strings.TrimSpace(input.EventType)
	input.HolderAuthorityNodeID = strings.TrimSpace(input.HolderAuthorityNodeID)
	input.PreviousHolderAuthorityNodeID = strings.TrimSpace(input.PreviousHolderAuthorityNodeID)
	input.LeaseToken = strings.TrimSpace(input.LeaseToken)
	input.LeaseExpiresAt = strings.TrimSpace(input.LeaseExpiresAt)
	input.ActorType = strings.ToLower(strings.TrimSpace(input.ActorType))
	input.ActorID = strings.TrimSpace(input.ActorID)
	input.RejectCode = strings.TrimSpace(input.RejectCode)
	input.RejectMessage = strings.TrimSpace(input.RejectMessage)
	input.ExpectedAuthorityNodeID = strings.TrimSpace(input.ExpectedAuthorityNodeID)
	input.ReferenceAt = strings.TrimSpace(input.ReferenceAt)
	input.ParentRefs = uniqueSortedStrings(input.ParentRefs)
	if input.WorkspaceID == "" {
		return AuthorityEventInput{}, errors.New("workspace_id is required")
	}
	if input.Scope == "" {
		input.Scope = authorityScopeWorkspace
	}
	expectedStatus, err := expectedAuthorityEventStatus(input.EventType)
	if err != nil {
		return AuthorityEventInput{}, err
	}
	if input.HolderAuthorityNodeID != "" {
		if err := validateAuthorityNodeID(input.HolderAuthorityNodeID); err != nil {
			return AuthorityEventInput{}, fmt.Errorf("holder_authority_node_id is invalid: %w", err)
		}
	}
	if input.PreviousHolderAuthorityNodeID != "" {
		if err := validateAuthorityNodeID(input.PreviousHolderAuthorityNodeID); err != nil {
			return AuthorityEventInput{}, fmt.Errorf("previous_holder_authority_node_id is invalid: %w", err)
		}
	}
	if input.ExpectedAuthorityNodeID != "" {
		if err := validateAuthorityNodeID(input.ExpectedAuthorityNodeID); err != nil {
			return AuthorityEventInput{}, fmt.Errorf("expected_authority_node_id is invalid: %w", err)
		}
	}
	if err := validateAuthorityEventTimestamp("lease_expires_at", input.LeaseExpiresAt); err != nil {
		return AuthorityEventInput{}, err
	}
	if err := validateAuthorityEventTimestamp("reference_at", input.ReferenceAt); err != nil {
		return AuthorityEventInput{}, err
	}
	if input.ActorType == "" {
		input.ActorType = authorityActorTypeSystem
	}
	if input.ActorType != authorityActorTypeSystem && input.ActorType != authorityActorTypeOperator {
		return AuthorityEventInput{}, errors.New("actor_type must be operator or system for authority events")
	}
	if input.ActorID == "" {
		input.ActorID = "authority_spine"
	}
	switch input.EventType {
	case AuthorityEventGranted, AuthorityEventRenewed, AuthorityEventExpired, AuthorityEventTransferStarted, AuthorityEventTransferred:
		if input.HolderAuthorityNodeID == "" {
			return AuthorityEventInput{}, errors.New("holder_authority_node_id is required for authority lifecycle events")
		}
		if input.Term <= 0 {
			return AuthorityEventInput{}, errors.New("term must be > 0 for authority lifecycle events")
		}
	case AuthorityEventRejected, AuthorityEventRejoinRejected:
		if input.RejectCode == "" {
			return AuthorityEventInput{}, errors.New("reject_code is required for authority reject events")
		}
		if input.ExpectedAuthorityNodeID == "" {
			return AuthorityEventInput{}, errors.New("expected_authority_node_id is required for authority reject events")
		}
		if input.ExpectedTerm <= 0 {
			return AuthorityEventInput{}, errors.New("expected_term must be > 0 for authority reject events")
		}
		if input.ReferenceAt == "" {
			return AuthorityEventInput{}, errors.New("reference_at is required for authority reject events")
		}
	}
	if input.Status == "" {
		input.Status = expectedStatus
	} else if input.Status != expectedStatus {
		return AuthorityEventInput{}, fmt.Errorf("status %q does not match canonical status %q for %s", input.Status, expectedStatus, input.EventType)
	}
	return input, nil
}

func (s *Store) RecordAuthorityEvent(ctx context.Context, input AuthorityEventInput) (RuntimeEventRecord, error) {
	input, err := normalizeAuthorityEventInput(input)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin authority event tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	runtimeEvent, err := s.recordAuthorityEventTx(ctx, tx, input, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit authority event tx: %w", err)
	}
	return runtimeEvent, nil
}

func (s *Store) recordAuthorityEventTx(ctx context.Context, tx *sql.Tx, input AuthorityEventInput, now string) (RuntimeEventRecord, error) {
	parentRefsJSON := ""
	var err error
	if len(input.ParentRefs) > 0 {
		parentRefsJSON, err = normalizeRuntimeEventParentRefs(mustJSON(input.ParentRefs))
		if err != nil {
			return RuntimeEventRecord{}, fmt.Errorf("normalize authority parent refs: %w", err)
		}
	}
	payload := map[string]any{
		"typed_event_type":           authorityEventTypedEventType,
		"event_kind":                 input.EventType,
		"workspace_id":               input.WorkspaceID,
		"scope":                      input.Scope,
		"holder_authority_node_id":   input.HolderAuthorityNodeID,
		"lease_token":                input.LeaseToken,
		"term":                       input.Term,
		"lease_expires_at":           input.LeaseExpiresAt,
		"commit_watermark":           input.CommitWatermark,
		"applied_watermark":          input.AppliedWatermark,
		"status":                     string(input.Status),
		"actor_type":                 input.ActorType,
		"actor_id":                   input.ActorID,
		"reject_code":                input.RejectCode,
		"reject_message":             input.RejectMessage,
		"expected_authority_node_id": input.ExpectedAuthorityNodeID,
		"expected_term":              input.ExpectedTerm,
		"reference_at":               input.ReferenceAt,
	}
	if input.PreviousHolderAuthorityNodeID != "" {
		payload["previous_holder_authority_node_id"] = input.PreviousHolderAuthorityNodeID
	}
	if len(input.ParentRefs) > 0 {
		payload["parent_refs"] = append([]string(nil), input.ParentRefs...)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
		return RuntimeEventRecord{}, err
	}
	appendInput := RuntimeEventInput{
		EventID:        firstNonEmpty(strings.TrimSpace(input.EventID), nextID("rtev")),
		WorkspaceID:    input.WorkspaceID,
		EventType:      input.EventType,
		EntityType:     authorityEventEntityType,
		EntityID:       authorityEventEntityID(input.WorkspaceID, input.Scope),
		ActorType:      input.ActorType,
		ActorID:        input.ActorID,
		ParentRefsJSON: parentRefsJSON,
		PayloadJSON:    mustJSON(payload),
		CreatedAt:      now,
	}
	runtimeEvent, err := s.appendAuthorityEventRuntimeRowTx(ctx, tx, input, appendInput)
	if err != nil {
		return RuntimeEventRecord{}, err
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:     nextID("audit"),
		EventType:   input.EventType,
		EntityType:  authorityEventEntityType,
		EntityID:    authorityEventEntityID(input.WorkspaceID, input.Scope),
		ActorID:     input.ActorID,
		PayloadJSON: mustJSON(payload),
	}); err != nil {
		return RuntimeEventRecord{}, err
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, input.WorkspaceID); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("touch workspace after authority event: %w", err)
	}
	return runtimeEvent, nil
}

func authorityEventUsesTransitionMetadata(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case AuthorityEventGranted, AuthorityEventRenewed, AuthorityEventExpired, AuthorityEventTransferStarted, AuthorityEventTransferred:
		return true
	default:
		return false
	}
}

func authorityTransitionRuntimeEventInput(event AuthorityEventInput, appendInput RuntimeEventInput) (RuntimeEventInput, error) {
	if !authorityEventUsesTransitionMetadata(event.EventType) {
		return RuntimeEventInput{}, fmt.Errorf("authority event %q is not a transition-backed authority runtime row", event.EventType)
	}
	holder := strings.TrimSpace(event.HolderAuthorityNodeID)
	if holder == "" {
		return RuntimeEventInput{}, errors.New("authority transition runtime event requires holder_authority_node_id")
	}
	if event.Term <= 0 {
		return RuntimeEventInput{}, errors.New("authority transition runtime event requires positive term")
	}
	fingerprint := authorityLeaseTokenFingerprint(event.LeaseToken)
	if fingerprint == "" {
		return RuntimeEventInput{}, errors.New("authority transition runtime event requires lease token fingerprint")
	}
	appendInput.AuthorityHolderNodeID = holder
	appendInput.AuthorityTerm = event.Term
	appendInput.AuthorityLeaseTokenFingerprint = fingerprint
	return appendInput, nil
}

func (s *Store) appendAuthorityEventRuntimeRowTx(ctx context.Context, tx *sql.Tx, event AuthorityEventInput, appendInput RuntimeEventInput) (RuntimeEventRecord, error) {
	if !authorityEventUsesTransitionMetadata(event.EventType) {
		return s.appendRuntimeEventTx(ctx, tx, appendInput)
	}
	appendInput, err := authorityTransitionRuntimeEventInput(event, appendInput)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return s.appendRuntimeEventTx(ctx, tx, appendInput)
}

func authorityEventRuntimeMetadata(input AuthorityEventInput) (string, int64, string) {
	switch strings.TrimSpace(input.EventType) {
	case AuthorityEventGranted, AuthorityEventRenewed, AuthorityEventExpired, AuthorityEventTransferStarted, AuthorityEventTransferred:
		return strings.TrimSpace(input.HolderAuthorityNodeID), input.Term, authorityLeaseTokenFingerprint(input.LeaseToken)
	default:
		return "", 0, ""
	}
}

type workspaceAuthorityScanner interface {
	Scan(dest ...any) error
}

func scanWorkspaceAuthority(scanner workspaceAuthorityScanner, record *WorkspaceAuthorityRecord) error {
	var holderAuthorityNodeID sql.NullString
	var leaseToken sql.NullString
	var leaseExpiresAt sql.NullString
	if err := scanner.Scan(
		&record.WorkspaceID,
		&record.Scope,
		&holderAuthorityNodeID,
		&leaseToken,
		&record.Term,
		&leaseExpiresAt,
		&record.CommitWatermark,
		&record.AppliedWatermark,
		&record.Status,
		&record.UpdatedAt,
	); err != nil {
		return err
	}
	if holderAuthorityNodeID.Valid {
		record.HolderAuthorityNodeID = holderAuthorityNodeID.String
	}
	if leaseToken.Valid {
		record.LeaseToken = leaseToken.String
	}
	if leaseExpiresAt.Valid {
		record.LeaseExpiresAt = leaseExpiresAt.String
	}
	return nil
}

func normalizeWorkspaceAuthorityScope(scope string) string {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return authorityScopeWorkspace
	}
	return scope
}

func authorityRejectEventType(code AuthorityRejectCode) string {
	switch code {
	case AuthorityRejectRejoinRejected:
		return AuthorityEventRejoinRejected
	default:
		return AuthorityEventRejected
	}
}

func authorityRejectEventInput(reject *AuthorityRejectError, input WorkspaceAuthorityFenceInput) AuthorityEventInput {
	event := AuthorityEventInput{
		WorkspaceID:             firstNonEmpty(strings.TrimSpace(reject.WorkspaceID), strings.TrimSpace(input.WorkspaceID)),
		Scope:                   normalizeWorkspaceAuthorityScope(firstNonEmpty(strings.TrimSpace(reject.Scope), strings.TrimSpace(input.Scope))),
		EventType:               authorityRejectEventType(reject.RejectCode),
		HolderAuthorityNodeID:   strings.TrimSpace(reject.HolderAuthorityNodeID),
		LeaseToken:              strings.TrimSpace(input.ExpectedLeaseToken),
		Term:                    reject.Term,
		LeaseExpiresAt:          strings.TrimSpace(reject.LeaseExpiresAt),
		CommitWatermark:         reject.CommitWatermark,
		AppliedWatermark:        reject.AppliedWatermark,
		Status:                  WorkspaceAuthorityStatusRejected,
		ActorType:               authorityActorTypeSystem,
		ActorID:                 "authority_spine",
		RejectCode:              string(reject.RejectCode),
		RejectMessage:           strings.TrimSpace(reject.RejectMessage),
		ExpectedAuthorityNodeID: firstNonEmpty(strings.TrimSpace(reject.ExpectedAuthorityNodeID), strings.TrimSpace(input.ExpectedHolderAuthorityNodeID)),
		ExpectedTerm:            input.ExpectedTerm,
		ReferenceAt:             firstNonEmpty(strings.TrimSpace(reject.ReferenceAt), strings.TrimSpace(input.ReferenceAt)),
	}
	if event.ExpectedTerm <= 0 {
		event.ExpectedTerm = reject.ExpectedTerm
	}
	return event
}

func (s *Store) journalWorkspaceAuthorityReject(ctx context.Context, err error, input WorkspaceAuthorityFenceInput) error {
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		return err
	}
	eventInput := authorityRejectEventInput(reject, input)
	if _, normalizeErr := normalizeAuthorityEventInput(eventInput); normalizeErr != nil {
		return err
	}
	if _, journalErr := s.RecordAuthorityEvent(ctx, eventInput); journalErr != nil {
		return fmt.Errorf("%w: journal authority reject event: %v", err, journalErr)
	}
	return err
}

func parseAuthorityTimestamp(fieldName, raw string, required bool) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if required {
			return time.Time{}, fmt.Errorf("%s is required", fieldName)
		}
		return time.Time{}, nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("%s must be RFC3339/RFC3339Nano, got %q", fieldName, raw)
}

func authorityWatermarksMonotonic(current, next WorkspaceAuthorityRecord) error {
	if next.CommitWatermark < current.CommitWatermark {
		return fmt.Errorf("commit_watermark %d regresses existing authority watermark %d", next.CommitWatermark, current.CommitWatermark)
	}
	if next.AppliedWatermark < current.AppliedWatermark {
		return fmt.Errorf("applied_watermark %d regresses existing authority watermark %d", next.AppliedWatermark, current.AppliedWatermark)
	}
	return nil
}

func (s *Store) workspaceRuntimeJournalHeadTx(ctx context.Context, tx *sql.Tx, workspaceID string) (int64, error) {
	var head int64
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&head); err != nil {
		return 0, fmt.Errorf("read workspace runtime journal head: %w", err)
	}
	return head, nil
}

func (s *Store) workspaceRuntimeJournalHead(ctx context.Context, workspaceID string) (int64, error) {
	var head int64
	if err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		strings.TrimSpace(workspaceID),
	).Scan(&head); err != nil {
		return 0, fmt.Errorf("read workspace runtime journal head: %w", err)
	}
	return head, nil
}

func workspaceAuthorityIsLiveAt(record WorkspaceAuthorityRecord, referenceAt time.Time) (bool, error) {
	if record.Status != WorkspaceAuthorityStatusActive {
		return false, nil
	}
	// CA-25: a lease with no holder cannot be live -- the holder's runtime_nodes row
	// was deleted (FK ON DELETE SET NULL) leaving an ACTIVE row that no node holds.
	// Treating it as not-live lets a fresh term-bump claim adopt the orphaned lease
	// instead of being rejected with "authority already active".
	if strings.TrimSpace(record.HolderAuthorityNodeID) == "" {
		return false, nil
	}
	leaseExpiresAt, err := parseAuthorityTimestamp("workspace_authority.lease_expires_at", record.LeaseExpiresAt, true)
	if err != nil {
		return false, err
	}
	return leaseExpiresAt.After(referenceAt), nil
}

func validateWorkspaceAuthorityNewLeaseLive(leaseExpiresAt, serverReferenceAt time.Time) error {
	leaseExpiresAt = leaseExpiresAt.UTC()
	serverReferenceAt = serverReferenceAt.UTC()
	if leaseExpiresAt.After(serverReferenceAt) {
		return nil
	}
	return authorityRejectError(
		AuthorityRejectLeaseExpired,
		"workspace authority new lease_expires_at is not live at server time",
		WorkspaceAuthorityRecord{},
		authorityRejectContext{
			ReferenceAt:    serverReferenceAt.Format(time.RFC3339Nano),
			LeaseExpiresAt: leaseExpiresAt.Format(time.RFC3339Nano),
		},
	)
}

func nextWorkspaceAuthorityTransitionTimestamp(currentUpdatedAt string, referenceAt time.Time) string {
	next := time.Now().UTC()
	if current, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(currentUpdatedAt)); err == nil {
		current = current.UTC()
		if !next.After(current) {
			next = current.Add(time.Nanosecond)
		}
	}
	referenceAt = referenceAt.UTC()
	if !next.After(referenceAt) {
		next = referenceAt.Add(time.Nanosecond)
	}
	return next.Format(time.RFC3339Nano)
}

func normalizeWorkspaceAuthorityFenceInput(input WorkspaceAuthorityFenceInput) (WorkspaceAuthorityFenceInput, time.Time, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	input.ExpectedHolderAuthorityNodeID = strings.TrimSpace(input.ExpectedHolderAuthorityNodeID)
	input.ExpectedLeaseToken = strings.TrimSpace(input.ExpectedLeaseToken)
	if input.WorkspaceID == "" {
		return WorkspaceAuthorityFenceInput{}, time.Time{}, errors.New("workspace_id is required")
	}
	if input.ExpectedHolderAuthorityNodeID == "" {
		return WorkspaceAuthorityFenceInput{}, time.Time{}, errors.New("expected_holder_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.ExpectedHolderAuthorityNodeID); err != nil {
		return WorkspaceAuthorityFenceInput{}, time.Time{}, fmt.Errorf("expected_holder_authority_node_id is invalid: %w", err)
	}
	if input.ExpectedLeaseToken == "" {
		return WorkspaceAuthorityFenceInput{}, time.Time{}, errors.New("expected_lease_token is required")
	}
	if input.ExpectedTerm <= 0 {
		return WorkspaceAuthorityFenceInput{}, time.Time{}, errors.New("expected_term must be > 0")
	}
	referenceAt, err := parseAuthorityTimestamp("reference_at", input.ReferenceAt, true)
	if err != nil {
		return WorkspaceAuthorityFenceInput{}, time.Time{}, err
	}
	referenceAt = effectiveWorkspaceAuthorityFenceReferenceAt(referenceAt)
	input.ReferenceAt = referenceAt.Format(time.RFC3339Nano)
	return input, referenceAt, nil
}

func effectiveWorkspaceAuthorityFenceReferenceAt(referenceAt time.Time) time.Time {
	referenceAt = referenceAt.UTC()
	now := time.Now().UTC()
	if referenceAt.Before(now) {
		return now
	}
	return referenceAt
}

func normalizeWorkspaceAuthorityClaimInput(input WorkspaceAuthorityClaimInput) (WorkspaceAuthorityClaimInput, time.Time, time.Time, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	input.HolderAuthorityNodeID = strings.TrimSpace(input.HolderAuthorityNodeID)
	input.LeaseToken = strings.TrimSpace(input.LeaseToken)
	input.ActorType = strings.TrimSpace(input.ActorType)
	input.ActorID = strings.TrimSpace(input.ActorID)
	if input.WorkspaceID == "" {
		return WorkspaceAuthorityClaimInput{}, time.Time{}, time.Time{}, errors.New("workspace_id is required")
	}
	if input.HolderAuthorityNodeID == "" {
		return WorkspaceAuthorityClaimInput{}, time.Time{}, time.Time{}, errors.New("holder_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.HolderAuthorityNodeID); err != nil {
		return WorkspaceAuthorityClaimInput{}, time.Time{}, time.Time{}, fmt.Errorf("holder_authority_node_id is invalid: %w", err)
	}
	if input.LeaseToken == "" {
		return WorkspaceAuthorityClaimInput{}, time.Time{}, time.Time{}, errors.New("lease_token is required")
	}
	if input.Term <= 0 {
		return WorkspaceAuthorityClaimInput{}, time.Time{}, time.Time{}, errors.New("term must be > 0")
	}
	leaseExpiresAt, err := parseAuthorityTimestamp("lease_expires_at", input.LeaseExpiresAt, true)
	if err != nil {
		return WorkspaceAuthorityClaimInput{}, time.Time{}, time.Time{}, err
	}
	referenceAt, err := parseAuthorityTimestamp("reference_at", input.ReferenceAt, true)
	if err != nil {
		return WorkspaceAuthorityClaimInput{}, time.Time{}, time.Time{}, err
	}
	return input, leaseExpiresAt, referenceAt, nil
}

func normalizeWorkspaceAuthorityRenewInput(input WorkspaceAuthorityRenewInput) (WorkspaceAuthorityRenewInput, time.Time, time.Time, error) {
	claimInput, leaseExpiresAt, referenceAt, err := normalizeWorkspaceAuthorityClaimInput(WorkspaceAuthorityClaimInput{
		WorkspaceID:           input.WorkspaceID,
		Scope:                 input.Scope,
		HolderAuthorityNodeID: input.HolderAuthorityNodeID,
		LeaseToken:            input.LeaseToken,
		Term:                  input.Term,
		LeaseExpiresAt:        input.LeaseExpiresAt,
		CommitWatermark:       input.CommitWatermark,
		AppliedWatermark:      input.AppliedWatermark,
		ActorType:             input.ActorType,
		ActorID:               input.ActorID,
		ReferenceAt:           input.ReferenceAt,
	})
	if err != nil {
		return WorkspaceAuthorityRenewInput{}, time.Time{}, time.Time{}, err
	}
	return WorkspaceAuthorityRenewInput{
		WorkspaceID:           claimInput.WorkspaceID,
		Scope:                 claimInput.Scope,
		HolderAuthorityNodeID: claimInput.HolderAuthorityNodeID,
		LeaseToken:            claimInput.LeaseToken,
		Term:                  claimInput.Term,
		LeaseExpiresAt:        claimInput.LeaseExpiresAt,
		CommitWatermark:       input.CommitWatermark,
		AppliedWatermark:      input.AppliedWatermark,
		ActorType:             claimInput.ActorType,
		ActorID:               claimInput.ActorID,
		ReferenceAt:           claimInput.ReferenceAt,
	}, leaseExpiresAt, referenceAt, nil
}

func normalizeWorkspaceAuthorityExpireInput(input WorkspaceAuthorityExpireInput) (WorkspaceAuthorityExpireInput, time.Time, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	input.HolderAuthorityNodeID = strings.TrimSpace(input.HolderAuthorityNodeID)
	input.LeaseToken = strings.TrimSpace(input.LeaseToken)
	input.ActorType = strings.TrimSpace(input.ActorType)
	input.ActorID = strings.TrimSpace(input.ActorID)
	if input.WorkspaceID == "" {
		return WorkspaceAuthorityExpireInput{}, time.Time{}, errors.New("workspace_id is required")
	}
	if input.HolderAuthorityNodeID == "" {
		return WorkspaceAuthorityExpireInput{}, time.Time{}, errors.New("holder_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.HolderAuthorityNodeID); err != nil {
		return WorkspaceAuthorityExpireInput{}, time.Time{}, fmt.Errorf("holder_authority_node_id is invalid: %w", err)
	}
	if input.LeaseToken == "" {
		return WorkspaceAuthorityExpireInput{}, time.Time{}, errors.New("lease_token is required")
	}
	if input.Term <= 0 {
		return WorkspaceAuthorityExpireInput{}, time.Time{}, errors.New("term must be > 0")
	}
	referenceAt, err := parseAuthorityTimestamp("reference_at", input.ReferenceAt, true)
	if err != nil {
		return WorkspaceAuthorityExpireInput{}, time.Time{}, err
	}
	return input, referenceAt, nil
}

func normalizeWorkspaceAuthorityTransferInput(input WorkspaceAuthorityTransferInput) (WorkspaceAuthorityTransferInput, time.Time, time.Time, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	input.CurrentHolderAuthorityNodeID = strings.TrimSpace(input.CurrentHolderAuthorityNodeID)
	input.CurrentLeaseToken = strings.TrimSpace(input.CurrentLeaseToken)
	input.NewHolderAuthorityNodeID = strings.TrimSpace(input.NewHolderAuthorityNodeID)
	input.NewLeaseToken = strings.TrimSpace(input.NewLeaseToken)
	input.ActorType = strings.TrimSpace(input.ActorType)
	input.ActorID = strings.TrimSpace(input.ActorID)
	if input.WorkspaceID == "" {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, errors.New("workspace_id is required")
	}
	if input.CurrentHolderAuthorityNodeID == "" {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, errors.New("current_holder_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.CurrentHolderAuthorityNodeID); err != nil {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, fmt.Errorf("current_holder_authority_node_id is invalid: %w", err)
	}
	if input.CurrentLeaseToken == "" {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, errors.New("current_lease_token is required")
	}
	if input.CurrentTerm <= 0 {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, errors.New("current_term must be > 0")
	}
	if input.NewHolderAuthorityNodeID == "" {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, errors.New("new_holder_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.NewHolderAuthorityNodeID); err != nil {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, fmt.Errorf("new_holder_authority_node_id is invalid: %w", err)
	}
	if input.NewHolderAuthorityNodeID == input.CurrentHolderAuthorityNodeID {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, errors.New("transfer requires a distinct new_holder_authority_node_id")
	}
	if input.NewLeaseToken == "" {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, errors.New("new_lease_token is required")
	}
	if input.NewTerm <= input.CurrentTerm {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, errors.New("new_term must be greater than current_term")
	}
	leaseExpiresAt, err := parseAuthorityTimestamp("lease_expires_at", input.LeaseExpiresAt, true)
	if err != nil {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, err
	}
	referenceAt, err := parseAuthorityTimestamp("reference_at", input.ReferenceAt, true)
	if err != nil {
		return WorkspaceAuthorityTransferInput{}, time.Time{}, time.Time{}, err
	}
	return input, leaseExpiresAt, referenceAt, nil
}

func (s *Store) getWorkspaceAuthorityTx(ctx context.Context, tx *sql.Tx, workspaceID, scope string) (WorkspaceAuthorityRecord, error) {
	record := WorkspaceAuthorityRecord{}
	if err := scanWorkspaceAuthority(
		tx.QueryRowContext(
			ctx,
			`SELECT workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at, commit_watermark, applied_watermark, status, updated_at
			   FROM workspace_authority
			  WHERE workspace_id = ? AND scope = ?`,
			strings.TrimSpace(workspaceID),
			normalizeWorkspaceAuthorityScope(scope),
		),
		&record,
	); err != nil {
		return WorkspaceAuthorityRecord{}, err
	}
	return record, nil
}

func (s *Store) GetWorkspaceAuthority(ctx context.Context, workspaceID, scope string) (WorkspaceAuthorityRecord, error) {
	record := WorkspaceAuthorityRecord{}
	if err := scanWorkspaceAuthority(
		s.db.QueryRowContext(
			ctx,
			`SELECT workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at, commit_watermark, applied_watermark, status, updated_at
			   FROM workspace_authority
			  WHERE workspace_id = ? AND scope = ?`,
			strings.TrimSpace(workspaceID),
			normalizeWorkspaceAuthorityScope(scope),
		),
		&record,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceAuthorityRecord{}, sql.ErrNoRows
		}
		return WorkspaceAuthorityRecord{}, fmt.Errorf("get workspace authority: %w", err)
	}
	return record, nil
}

func normalizeLocalAuthorityActor(actorType, actorID string) (string, string, error) {
	actorType = strings.ToLower(strings.TrimSpace(actorType))
	actorID = strings.TrimSpace(actorID)
	if actorType == "" {
		actorType = authorityActorTypeOperator
	}
	switch actorType {
	case authorityActorTypeOperator:
		if actorID == "" {
			actorID = "operator"
		}
	case authorityActorTypeSystem:
		if actorID == "" {
			actorID = "authority_spine"
		}
	default:
		return "", "", errors.New("actor_type must be operator or system")
	}
	return actorType, actorID, nil
}

func evaluateLocalWorkspaceAuthorityLeaseState(record WorkspaceAuthorityRecord, localAuthorityNodeID string, referenceAt time.Time) (string, bool, error) {
	// CA-25: distinguish the orphaned-holder wedge from a generically incomplete row.
	// Deleting the holder's runtime_nodes row (operator cleanup / future
	// deregistration) sets holder_authority_node_id to NULL via the FK ON DELETE SET
	// NULL while lease_token/term/status are preserved -- so the row still reads as a
	// live ACTIVE lease that no node holds. Classified as "incomplete", it routed to
	// "force-break required", but ForceBreak itself rejects incomplete rows, leaving
	// the workspace permanently wedged. Surface it as a distinct, self-recoverable
	// state so the local node can adopt it (EnsureLocal) without operator action.
	if strings.TrimSpace(record.HolderAuthorityNodeID) == "" &&
		strings.TrimSpace(record.LeaseToken) != "" && record.Term > 0 &&
		record.Status == WorkspaceAuthorityStatusActive {
		return "orphaned_holder", false, nil
	}
	if strings.TrimSpace(record.HolderAuthorityNodeID) == "" || strings.TrimSpace(record.LeaseToken) == "" || record.Term <= 0 {
		return "incomplete", false, nil
	}
	if record.Status == WorkspaceAuthorityStatusTransferring {
		return "transferring", false, nil
	}
	live, err := workspaceAuthorityIsLiveAt(record, referenceAt)
	if err != nil {
		return "invalid", false, err
	}
	if live {
		if strings.TrimSpace(record.HolderAuthorityNodeID) != strings.TrimSpace(localAuthorityNodeID) {
			return "foreign_live", true, nil
		}
		leaseExpiresAt, err := parseAuthorityTimestamp("workspace_authority.lease_expires_at", record.LeaseExpiresAt, true)
		if err != nil {
			return "invalid", false, err
		}
		if leaseExpiresAt.Sub(referenceAt) <= localAuthorityRenewLead {
			return "renew_due", true, nil
		}
		return "healthy", true, nil
	}
	return "stale", false, nil
}

func (s *Store) GetLocalWorkspaceAuthorityStatus(ctx context.Context, workspaceID, scope string) (LocalWorkspaceAuthorityStatus, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	scope = normalizeWorkspaceAuthorityScope(scope)
	if workspaceID == "" {
		return LocalWorkspaceAuthorityStatus{}, errors.New("workspace_id is required")
	}

	referenceAt := time.Now().UTC()
	node, err := s.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		return LocalWorkspaceAuthorityStatus{}, fmt.Errorf("ensure local authority node: %w", err)
	}
	journalHead, err := s.workspaceRuntimeJournalHead(ctx, workspaceID)
	if err != nil {
		return LocalWorkspaceAuthorityStatus{}, err
	}

	status := LocalWorkspaceAuthorityStatus{
		WorkspaceID: workspaceID,
		Scope:       scope,
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		LocalNode:   node,
		JournalHead: journalHead,
		LeaseState:  "missing",
	}

	record, err := s.GetWorkspaceAuthority(ctx, workspaceID, scope)
	if errors.Is(err, sql.ErrNoRows) {
		return status, nil
	}
	if err != nil {
		return LocalWorkspaceAuthorityStatus{}, fmt.Errorf("get workspace authority status: %w", err)
	}

	state, live, err := evaluateLocalWorkspaceAuthorityLeaseState(record, node.AuthorityNodeID, referenceAt)
	if err != nil {
		return LocalWorkspaceAuthorityStatus{}, fmt.Errorf("evaluate workspace authority status: %w", err)
	}
	status.Authority = &record
	status.LocalHolder = strings.TrimSpace(record.HolderAuthorityNodeID) == strings.TrimSpace(node.AuthorityNodeID)
	status.LeaseLive = live
	status.LeaseState = state
	return status, nil
}

func (s *Store) currentLocalWorkspaceAuthorityFenceInput(ctx context.Context, workspaceID, scope, referenceAt string) (WorkspaceAuthorityFenceInput, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	scope = normalizeWorkspaceAuthorityScope(scope)
	referenceAt = strings.TrimSpace(referenceAt)
	if workspaceID == "" {
		return WorkspaceAuthorityFenceInput{}, errors.New("workspace_id is required")
	}
	if referenceAt == "" {
		referenceAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	node, err := s.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		return WorkspaceAuthorityFenceInput{}, fmt.Errorf("ensure local authority node: %w", err)
	}
	record, err := s.GetWorkspaceAuthority(ctx, workspaceID, scope)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkspaceAuthorityFenceInput{}, authorityRejectError(
			AuthorityRejectMissing,
			"workspace authority is missing",
			WorkspaceAuthorityRecord{},
			authorityRejectContext{
				WorkspaceID:             workspaceID,
				Scope:                   scope,
				ExpectedAuthorityNodeID: node.AuthorityNodeID,
				ReferenceAt:             referenceAt,
			},
		)
	}
	if err != nil {
		return WorkspaceAuthorityFenceInput{}, fmt.Errorf("get workspace authority for local fence: %w", err)
	}
	if strings.TrimSpace(record.LeaseToken) == "" || record.Term <= 0 {
		return WorkspaceAuthorityFenceInput{}, authorityRejectError(
			AuthorityRejectStale,
			"workspace authority row is incomplete for fenced write",
			record,
			authorityRejectContext{
				WorkspaceID:             workspaceID,
				Scope:                   scope,
				ExpectedAuthorityNodeID: node.AuthorityNodeID,
				ExpectedTerm:            record.Term,
				ReferenceAt:             referenceAt,
			},
		)
	}
	return WorkspaceAuthorityFenceInput{
		WorkspaceID:                   workspaceID,
		Scope:                         scope,
		ExpectedHolderAuthorityNodeID: node.AuthorityNodeID,
		ExpectedLeaseToken:            record.LeaseToken,
		ExpectedTerm:                  record.Term,
		ReferenceAt:                   referenceAt,
	}, nil
}

func (s *Store) currentLocalWorkspaceAuthorityFenceInputTx(ctx context.Context, tx *sql.Tx, workspaceID, scope, referenceAt string) (WorkspaceAuthorityFenceInput, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	scope = normalizeWorkspaceAuthorityScope(scope)
	referenceAt = strings.TrimSpace(referenceAt)
	if tx == nil {
		return WorkspaceAuthorityFenceInput{}, errors.New("workspace authority tx is required")
	}
	if workspaceID == "" {
		return WorkspaceAuthorityFenceInput{}, errors.New("workspace_id is required")
	}
	if referenceAt == "" {
		referenceAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if strings.TrimSpace(s.authorityIdentityPath) == "" {
		return WorkspaceAuthorityFenceInput{}, errors.New("authority identity path is not configured")
	}

	authorityNodeID, err := readAuthorityNodeID(s.authorityIdentityPath)
	if errors.Is(err, os.ErrNotExist) {
		return WorkspaceAuthorityFenceInput{}, authorityRejectError(
			AuthorityRejectMissing,
			"local authority node identity is missing",
			WorkspaceAuthorityRecord{},
			authorityRejectContext{
				WorkspaceID: workspaceID,
				Scope:       scope,
				ReferenceAt: referenceAt,
			},
		)
	}
	if err != nil {
		return WorkspaceAuthorityFenceInput{}, fmt.Errorf("read local authority node id: %w", err)
	}

	record, err := s.getWorkspaceAuthorityTx(ctx, tx, workspaceID, scope)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkspaceAuthorityFenceInput{}, authorityRejectError(
			AuthorityRejectMissing,
			"workspace authority is missing",
			WorkspaceAuthorityRecord{},
			authorityRejectContext{
				WorkspaceID:             workspaceID,
				Scope:                   scope,
				ExpectedAuthorityNodeID: authorityNodeID,
				ReferenceAt:             referenceAt,
			},
		)
	}
	if err != nil {
		return WorkspaceAuthorityFenceInput{}, fmt.Errorf("get workspace authority for local tx fence: %w", err)
	}
	if strings.TrimSpace(record.LeaseToken) == "" || record.Term <= 0 {
		return WorkspaceAuthorityFenceInput{}, authorityRejectError(
			AuthorityRejectStale,
			"workspace authority row is incomplete for fenced write",
			record,
			authorityRejectContext{
				WorkspaceID:             workspaceID,
				Scope:                   scope,
				ExpectedAuthorityNodeID: authorityNodeID,
				ExpectedTerm:            record.Term,
				ReferenceAt:             referenceAt,
			},
		)
	}
	return WorkspaceAuthorityFenceInput{
		WorkspaceID:                   workspaceID,
		Scope:                         scope,
		ExpectedHolderAuthorityNodeID: authorityNodeID,
		ExpectedLeaseToken:            record.LeaseToken,
		ExpectedTerm:                  record.Term,
		ReferenceAt:                   referenceAt,
	}, nil
}

func workspaceAuthorityFenceInputFromRecord(authority WorkspaceAuthorityRecord, referenceAt string) (WorkspaceAuthorityFenceInput, error) {
	workspaceID := strings.TrimSpace(authority.WorkspaceID)
	scope := normalizeWorkspaceAuthorityScope(authority.Scope)
	holderAuthorityNodeID := strings.TrimSpace(authority.HolderAuthorityNodeID)
	leaseToken := strings.TrimSpace(authority.LeaseToken)
	referenceAt = strings.TrimSpace(referenceAt)
	if referenceAt == "" {
		referenceAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if workspaceID == "" {
		return WorkspaceAuthorityFenceInput{}, errors.New("workspace authority record is missing workspace_id")
	}
	if holderAuthorityNodeID == "" {
		return WorkspaceAuthorityFenceInput{}, errors.New("workspace authority record is missing holder_authority_node_id")
	}
	if err := validateAuthorityNodeID(holderAuthorityNodeID); err != nil {
		return WorkspaceAuthorityFenceInput{}, fmt.Errorf("workspace authority holder_authority_node_id is invalid: %w", err)
	}
	if leaseToken == "" {
		return WorkspaceAuthorityFenceInput{}, errors.New("workspace authority record is missing lease_token")
	}
	if authority.Term <= 0 {
		return WorkspaceAuthorityFenceInput{}, errors.New("workspace authority record is missing valid term")
	}
	return WorkspaceAuthorityFenceInput{
		WorkspaceID:                   workspaceID,
		Scope:                         scope,
		ExpectedHolderAuthorityNodeID: holderAuthorityNodeID,
		ExpectedLeaseToken:            leaseToken,
		ExpectedTerm:                  authority.Term,
		ReferenceAt:                   referenceAt,
	}, nil
}

func (s *Store) upsertWorkspaceAuthorityTx(ctx context.Context, tx *sql.Tx, record WorkspaceAuthorityRecord) error {
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO workspace_authority(
		    workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at, commit_watermark, applied_watermark, status, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		  ON CONFLICT(workspace_id, scope) DO UPDATE SET
		    holder_authority_node_id = excluded.holder_authority_node_id,
		    lease_token = excluded.lease_token,
		    term = excluded.term,
		    lease_expires_at = excluded.lease_expires_at,
		    -- CA-26: enforce watermark monotonicity at the DB level (not only via the
		    -- Go authorityWatermarksMonotonic guard) so any unguarded/raw upsert or
		    -- future caller cannot regress the commit/applied watermark, which would
		    -- corrupt split-brain reconciliation ordering. Legitimate callers already
		    -- pass non-decreasing watermarks, so max() is a no-op for them.
		    commit_watermark = max(commit_watermark, excluded.commit_watermark),
		    applied_watermark = max(applied_watermark, excluded.applied_watermark),
		    status = excluded.status,
		    updated_at = excluded.updated_at`,
		record.WorkspaceID,
		record.Scope,
		blankStringOrNil(record.HolderAuthorityNodeID),
		blankStringOrNil(record.LeaseToken),
		record.Term,
		blankStringOrNil(record.LeaseExpiresAt),
		record.CommitWatermark,
		record.AppliedWatermark,
		string(record.Status),
		record.UpdatedAt,
	); err != nil {
		return fmt.Errorf("upsert workspace authority: %w", err)
	}
	return nil
}

func (s *Store) ensureRuntimeNodeExistsTx(ctx context.Context, tx *sql.Tx, authorityNodeID string) error {
	authorityNodeID = strings.TrimSpace(authorityNodeID)
	if authorityNodeID == "" {
		return errors.New("authority_node_id is required")
	}
	var dummy string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT authority_node_id FROM runtime_nodes WHERE authority_node_id = ? LIMIT 1`,
		authorityNodeID,
	).Scan(&dummy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("runtime node %q is not registered", authorityNodeID)
		}
		return fmt.Errorf("lookup runtime node %q: %w", authorityNodeID, err)
	}
	return nil
}

func (s *Store) runtimeNodeStatusTx(ctx context.Context, tx *sql.Tx, authorityNodeID string) (RuntimeNodeStatus, error) {
	authorityNodeID = strings.TrimSpace(authorityNodeID)
	if authorityNodeID == "" {
		return "", errors.New("authority_node_id is required")
	}
	var status string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT status FROM runtime_nodes WHERE authority_node_id = ? LIMIT 1`,
		authorityNodeID,
	).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("runtime node %q is not registered", authorityNodeID)
		}
		return "", fmt.Errorf("lookup runtime node %q status: %w", authorityNodeID, err)
	}
	return RuntimeNodeStatus(strings.TrimSpace(status)), nil
}

func (s *Store) checkWorkspaceAuthorityFenceTx(ctx context.Context, tx *sql.Tx, input WorkspaceAuthorityFenceInput) (WorkspaceAuthorityRecord, error) {
	input, referenceAt, err := normalizeWorkspaceAuthorityFenceInput(input)
	if err != nil {
		return WorkspaceAuthorityRecord{}, err
	}
	record, err := s.getWorkspaceAuthorityTx(ctx, tx, input.WorkspaceID, input.Scope)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkspaceAuthorityRecord{}, authorityRejectError(
			AuthorityRejectMissing,
			"workspace authority is missing",
			WorkspaceAuthorityRecord{},
			authorityRejectContext{
				WorkspaceID:             input.WorkspaceID,
				Scope:                   input.Scope,
				ExpectedAuthorityNodeID: input.ExpectedHolderAuthorityNodeID,
				ExpectedTerm:            input.ExpectedTerm,
				LeaseToken:              input.ExpectedLeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	if err != nil {
		return WorkspaceAuthorityRecord{}, fmt.Errorf("read workspace authority for fence: %w", err)
	}
	if record.Status != WorkspaceAuthorityStatusActive {
		rejectCode := AuthorityRejectStale
		switch record.Status {
		case WorkspaceAuthorityStatusExpired:
			rejectCode = AuthorityRejectLeaseExpired
		case WorkspaceAuthorityStatusRejected:
			rejectCode = AuthorityRejectRejoinRejected
		}
		return WorkspaceAuthorityRecord{}, authorityRejectError(
			rejectCode,
			fmt.Sprintf("workspace authority status is %s, expected ACTIVE", record.Status),
			record,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.ExpectedHolderAuthorityNodeID,
				ExpectedTerm:            input.ExpectedTerm,
				LeaseToken:              input.ExpectedLeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	if record.HolderAuthorityNodeID != input.ExpectedHolderAuthorityNodeID {
		return WorkspaceAuthorityRecord{}, authorityRejectError(
			AuthorityRejectStale,
			fmt.Sprintf("workspace authority holder %q does not match expected holder %q", record.HolderAuthorityNodeID, input.ExpectedHolderAuthorityNodeID),
			record,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.ExpectedHolderAuthorityNodeID,
				ExpectedTerm:            input.ExpectedTerm,
				LeaseToken:              input.ExpectedLeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.HolderAuthorityNodeID); err != nil {
		return WorkspaceAuthorityRecord{}, authorityRejectError(
			AuthorityRejectMissing,
			err.Error(),
			record,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.ExpectedHolderAuthorityNodeID,
				ExpectedTerm:            input.ExpectedTerm,
				LeaseToken:              input.ExpectedLeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	holderStatus, err := s.runtimeNodeStatusTx(ctx, tx, record.HolderAuthorityNodeID)
	if err != nil {
		return WorkspaceAuthorityRecord{}, authorityRejectError(
			AuthorityRejectMissing,
			err.Error(),
			record,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.ExpectedHolderAuthorityNodeID,
				ExpectedTerm:            input.ExpectedTerm,
				LeaseToken:              input.ExpectedLeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	if holderStatus != RuntimeNodeStatusOnline {
		return WorkspaceAuthorityRecord{}, authorityRejectError(
			AuthorityRejectStale,
			fmt.Sprintf("runtime node %q is %s, expected ONLINE", record.HolderAuthorityNodeID, holderStatus),
			record,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.ExpectedHolderAuthorityNodeID,
				ExpectedTerm:            input.ExpectedTerm,
				LeaseToken:              input.ExpectedLeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	if record.LeaseToken != input.ExpectedLeaseToken {
		return WorkspaceAuthorityRecord{}, authorityRejectError(
			AuthorityRejectRejoinRejected,
			"workspace authority lease_token does not match expected lease",
			record,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.ExpectedHolderAuthorityNodeID,
				ExpectedTerm:            input.ExpectedTerm,
				LeaseToken:              input.ExpectedLeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	if record.Term != input.ExpectedTerm {
		return WorkspaceAuthorityRecord{}, authorityRejectError(
			AuthorityRejectTermMismatch,
			fmt.Sprintf("workspace authority term %d does not match expected term %d", record.Term, input.ExpectedTerm),
			record,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.ExpectedHolderAuthorityNodeID,
				ExpectedTerm:            input.ExpectedTerm,
				LeaseToken:              input.ExpectedLeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	live, err := workspaceAuthorityIsLiveAt(record, referenceAt)
	if err != nil {
		return WorkspaceAuthorityRecord{}, fmt.Errorf("evaluate workspace authority freshness: %w", err)
	}
	if !live {
		return WorkspaceAuthorityRecord{}, authorityRejectError(
			AuthorityRejectLeaseExpired,
			"workspace authority lease is expired or stale at reference_at",
			record,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.ExpectedHolderAuthorityNodeID,
				ExpectedTerm:            input.ExpectedTerm,
				LeaseToken:              input.ExpectedLeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	return record, nil
}

func (s *Store) CheckWorkspaceAuthorityFence(ctx context.Context, input WorkspaceAuthorityFenceInput) (WorkspaceAuthorityRecord, error) {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceAuthorityRecord{}, fmt.Errorf("begin workspace authority fence tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, err := s.checkWorkspaceAuthorityFenceTx(ctx, tx, input)
	if err != nil {
		_ = tx.Rollback()
		return WorkspaceAuthorityRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, input)
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceAuthorityRecord{}, fmt.Errorf("commit workspace authority fence tx: %w", err)
	}
	return record, nil
}

func (s *Store) WithFencedWorkspaceAuthorityTx(ctx context.Context, tx *sql.Tx, input WorkspaceAuthorityFenceInput, mutate WorkspaceFencedWriteTxFunc) (WorkspaceAuthorityRecord, error) {
	if tx == nil {
		return WorkspaceAuthorityRecord{}, errors.New("fenced workspace authority tx is required")
	}
	record, err := s.checkWorkspaceAuthorityFenceTx(ctx, tx, input)
	if err != nil {
		return WorkspaceAuthorityRecord{}, err
	}
	if mutate != nil {
		if err := mutate(tx, record); err != nil {
			return WorkspaceAuthorityRecord{}, err
		}
	}
	return record, nil
}

func (s *Store) WithFencedWorkspaceAuthority(ctx context.Context, input WorkspaceAuthorityFenceInput, mutate WorkspaceFencedWriteTxFunc) (WorkspaceAuthorityRecord, error) {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceAuthorityRecord{}, fmt.Errorf("begin fenced workspace authority tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, input, mutate)
	if err != nil {
		_ = tx.Rollback()
		return WorkspaceAuthorityRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, input)
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceAuthorityRecord{}, fmt.Errorf("commit fenced workspace authority tx: %w", err)
	}
	return record, nil
}

func (s *Store) ClaimWorkspaceAuthority(ctx context.Context, input WorkspaceAuthorityClaimInput) (WorkspaceAuthorityRecord, RuntimeEventRecord, error) {
	input, leaseExpiresAt, _, err := normalizeWorkspaceAuthorityClaimInput(input)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	serverReferenceAt := time.Now().UTC()
	if err := validateWorkspaceAuthorityNewLeaseLive(leaseExpiresAt, serverReferenceAt); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin workspace authority claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, input.HolderAuthorityNodeID); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
			AuthorityRejectMissing,
			err.Error(),
			WorkspaceAuthorityRecord{},
			authorityRejectContext{
				WorkspaceID:             input.WorkspaceID,
				Scope:                   input.Scope,
				ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
				ExpectedTerm:            input.Term,
				LeaseToken:              input.LeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}

	current, err := s.getWorkspaceAuthorityTx(ctx, tx, input.WorkspaceID, input.Scope)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("read current workspace authority: %w", err)
	default:
		if current.Status == WorkspaceAuthorityStatusTransferring {
			return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
				AuthorityRejectStale,
				"workspace authority is transferring and cannot be claimed",
				current,
				authorityRejectContext{
					ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
					ExpectedTerm:            input.Term,
					LeaseToken:              input.LeaseToken,
					ReferenceAt:             input.ReferenceAt,
				},
			)
		}
		live, liveErr := workspaceAuthorityIsLiveAt(current, serverReferenceAt)
		if liveErr != nil {
			return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("evaluate current workspace authority: %w", liveErr)
		}
		if live {
			return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
				AuthorityRejectStale,
				fmt.Sprintf("workspace authority already active for holder %q", current.HolderAuthorityNodeID),
				current,
				authorityRejectContext{
					ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
					ExpectedTerm:            input.Term,
					LeaseToken:              input.LeaseToken,
					ReferenceAt:             input.ReferenceAt,
				},
			)
		}
		if input.Term <= current.Term {
			return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
				AuthorityRejectTermMismatch,
				fmt.Sprintf("authority claim term %d must advance beyond current term %d", input.Term, current.Term),
				current,
				authorityRejectContext{
					ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
					ExpectedTerm:            input.Term,
					LeaseToken:              input.LeaseToken,
					ReferenceAt:             input.ReferenceAt,
				},
			)
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	record := WorkspaceAuthorityRecord{
		WorkspaceID:           input.WorkspaceID,
		Scope:                 input.Scope,
		HolderAuthorityNodeID: input.HolderAuthorityNodeID,
		LeaseToken:            input.LeaseToken,
		Term:                  input.Term,
		LeaseExpiresAt:        leaseExpiresAt.Format(time.RFC3339Nano),
		CommitWatermark:       input.CommitWatermark,
		AppliedWatermark:      input.AppliedWatermark,
		Status:                WorkspaceAuthorityStatusActive,
		UpdatedAt:             now,
	}
	if current.WorkspaceID != "" {
		if err := authorityWatermarksMonotonic(current, record); err != nil {
			return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
		}
	}
	if err := s.upsertWorkspaceAuthorityTx(ctx, tx, record); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	runtimeEvent, err := s.recordAuthorityEventTx(ctx, tx, AuthorityEventInput{
		WorkspaceID:                   input.WorkspaceID,
		Scope:                         input.Scope,
		EventType:                     AuthorityEventGranted,
		HolderAuthorityNodeID:         input.HolderAuthorityNodeID,
		PreviousHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		LeaseToken:                    input.LeaseToken,
		Term:                          input.Term,
		LeaseExpiresAt:                record.LeaseExpiresAt,
		CommitWatermark:               input.CommitWatermark,
		AppliedWatermark:              input.AppliedWatermark,
		Status:                        WorkspaceAuthorityStatusActive,
		ActorType:                     input.ActorType,
		ActorID:                       input.ActorID,
		ReferenceAt:                   serverReferenceAt.Format(time.RFC3339Nano),
	}, now)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit workspace authority claim tx: %w", err)
	}
	return record, runtimeEvent, nil
}

func (s *Store) RenewWorkspaceAuthority(ctx context.Context, input WorkspaceAuthorityRenewInput) (WorkspaceAuthorityRecord, RuntimeEventRecord, error) {
	input, leaseExpiresAt, _, err := normalizeWorkspaceAuthorityRenewInput(input)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	serverReferenceAt := time.Now().UTC()
	if err := validateWorkspaceAuthorityNewLeaseLive(leaseExpiresAt, serverReferenceAt); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin workspace authority renew tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, input.HolderAuthorityNodeID); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
			AuthorityRejectMissing,
			err.Error(),
			WorkspaceAuthorityRecord{},
			authorityRejectContext{
				WorkspaceID:             input.WorkspaceID,
				Scope:                   input.Scope,
				ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
				ExpectedTerm:            input.Term,
				LeaseToken:              input.LeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	fenceInput := WorkspaceAuthorityFenceInput{
		WorkspaceID:                   input.WorkspaceID,
		Scope:                         input.Scope,
		ExpectedHolderAuthorityNodeID: input.HolderAuthorityNodeID,
		ExpectedLeaseToken:            input.LeaseToken,
		ExpectedTerm:                  input.Term,
		ReferenceAt:                   serverReferenceAt.Format(time.RFC3339Nano),
	}
	current, err := s.checkWorkspaceAuthorityFenceTx(ctx, tx, fenceInput)
	if err != nil {
		_ = tx.Rollback()
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	record := current
	record.LeaseExpiresAt = leaseExpiresAt.Format(time.RFC3339Nano)
	record.CommitWatermark = input.CommitWatermark
	record.AppliedWatermark = input.AppliedWatermark
	record.UpdatedAt = nextWorkspaceAuthorityTransitionTimestamp(current.UpdatedAt, serverReferenceAt)
	if err := authorityWatermarksMonotonic(current, record); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if err := s.upsertWorkspaceAuthorityTx(ctx, tx, record); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	runtimeEvent, err := s.recordAuthorityEventTx(ctx, tx, AuthorityEventInput{
		WorkspaceID:           input.WorkspaceID,
		Scope:                 input.Scope,
		EventType:             AuthorityEventRenewed,
		HolderAuthorityNodeID: input.HolderAuthorityNodeID,
		LeaseToken:            input.LeaseToken,
		Term:                  input.Term,
		LeaseExpiresAt:        record.LeaseExpiresAt,
		CommitWatermark:       input.CommitWatermark,
		AppliedWatermark:      input.AppliedWatermark,
		Status:                WorkspaceAuthorityStatusActive,
		ActorType:             input.ActorType,
		ActorID:               input.ActorID,
		ReferenceAt:           serverReferenceAt.Format(time.RFC3339Nano),
	}, record.UpdatedAt)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit workspace authority renew tx: %w", err)
	}
	return record, runtimeEvent, nil
}

func (s *Store) ExpireWorkspaceAuthority(ctx context.Context, input WorkspaceAuthorityExpireInput) (WorkspaceAuthorityRecord, RuntimeEventRecord, error) {
	input, referenceAt, err := normalizeWorkspaceAuthorityExpireInput(input)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin workspace authority expire tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	current, err := s.getWorkspaceAuthorityTx(ctx, tx, input.WorkspaceID, input.Scope)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
			AuthorityRejectMissing,
			"workspace authority is missing",
			WorkspaceAuthorityRecord{},
			authorityRejectContext{
				WorkspaceID:             input.WorkspaceID,
				Scope:                   input.Scope,
				ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
				ExpectedTerm:            input.Term,
				LeaseToken:              input.LeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("read current workspace authority: %w", err)
	}
	if current.HolderAuthorityNodeID != input.HolderAuthorityNodeID {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
			AuthorityRejectStale,
			fmt.Sprintf("workspace authority holder %q does not match expected holder %q", current.HolderAuthorityNodeID, input.HolderAuthorityNodeID),
			current,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
				ExpectedTerm:            input.Term,
				LeaseToken:              input.LeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	if current.LeaseToken != input.LeaseToken {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
			AuthorityRejectRejoinRejected,
			"workspace authority lease_token does not match expected lease",
			current,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
				ExpectedTerm:            input.Term,
				LeaseToken:              input.LeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	if current.Term != input.Term {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
			AuthorityRejectTermMismatch,
			fmt.Sprintf("workspace authority term %d does not match expected term %d", current.Term, input.Term),
			current,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
				ExpectedTerm:            input.Term,
				LeaseToken:              input.LeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	updatedAt, err := parseAuthorityTimestamp("workspace_authority.updated_at", current.UpdatedAt, true)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if updatedAt.After(referenceAt) {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
			AuthorityRejectStale,
			"workspace authority has newer state than reference_at",
			current,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
				ExpectedTerm:            input.Term,
				LeaseToken:              input.LeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	if current.Status == WorkspaceAuthorityStatusExpired {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
			AuthorityRejectLeaseExpired,
			"workspace authority is already expired",
			current,
			authorityRejectContext{
				ExpectedAuthorityNodeID: input.HolderAuthorityNodeID,
				ExpectedTerm:            input.Term,
				LeaseToken:              input.LeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	// CA-11 (assessed/deferred): a lease_expires_at liveness re-check was tried here
	// but ExpireWorkspaceAuthority is also the FORCED-expiry path (operators/tests
	// expire a still-live lease to fence it), so refusing future-expiry leases breaks
	// legitimate forced expiry. The 2/3 refuter dissent holds: the in-repo lease
	// manager captures referenceAt before any renew, so a concurrent renew yields
	// updated_at >= referenceAt and the staleness gate above already rejects the
	// expire. The residual exploit (an Expire caller supplying referenceAt >= the
	// renewed updated_at via truncated timestamps or a second operator-driven pass)
	// requires a CAS on the caller's observed lease updated_at/token, which is an API
	// change tracked in the coordination audit tracker rather than a safe local fix.
	record := current
	record.Status = WorkspaceAuthorityStatusExpired
	record.CommitWatermark = input.CommitWatermark
	record.AppliedWatermark = input.AppliedWatermark
	record.UpdatedAt = nextWorkspaceAuthorityTransitionTimestamp(current.UpdatedAt, referenceAt)
	if err := authorityWatermarksMonotonic(current, record); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if err := s.upsertWorkspaceAuthorityTx(ctx, tx, record); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	runtimeEvent, err := s.recordAuthorityEventTx(ctx, tx, AuthorityEventInput{
		WorkspaceID:           input.WorkspaceID,
		Scope:                 input.Scope,
		EventType:             AuthorityEventExpired,
		HolderAuthorityNodeID: input.HolderAuthorityNodeID,
		LeaseToken:            input.LeaseToken,
		Term:                  input.Term,
		LeaseExpiresAt:        current.LeaseExpiresAt,
		CommitWatermark:       input.CommitWatermark,
		AppliedWatermark:      input.AppliedWatermark,
		Status:                WorkspaceAuthorityStatusExpired,
		ActorType:             input.ActorType,
		ActorID:               input.ActorID,
		ReferenceAt:           strings.TrimSpace(input.ReferenceAt),
	}, record.UpdatedAt)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit workspace authority expire tx: %w", err)
	}
	return record, runtimeEvent, nil
}

func (s *Store) TransferWorkspaceAuthority(ctx context.Context, input WorkspaceAuthorityTransferInput) (WorkspaceAuthorityRecord, RuntimeEventRecord, error) {
	input, leaseExpiresAt, _, err := normalizeWorkspaceAuthorityTransferInput(input)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	serverReferenceAt := time.Now().UTC()
	if err := validateWorkspaceAuthorityNewLeaseLive(leaseExpiresAt, serverReferenceAt); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin workspace authority transfer tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, input.NewHolderAuthorityNodeID); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
			AuthorityRejectMissing,
			err.Error(),
			WorkspaceAuthorityRecord{},
			authorityRejectContext{
				WorkspaceID:             input.WorkspaceID,
				Scope:                   input.Scope,
				ExpectedAuthorityNodeID: input.NewHolderAuthorityNodeID,
				ExpectedTerm:            input.NewTerm,
				LeaseToken:              input.NewLeaseToken,
				ReferenceAt:             input.ReferenceAt,
			},
		)
	}
	fenceInput := WorkspaceAuthorityFenceInput{
		WorkspaceID:                   input.WorkspaceID,
		Scope:                         input.Scope,
		ExpectedHolderAuthorityNodeID: input.CurrentHolderAuthorityNodeID,
		ExpectedLeaseToken:            input.CurrentLeaseToken,
		ExpectedTerm:                  input.CurrentTerm,
		ReferenceAt:                   serverReferenceAt.Format(time.RFC3339Nano),
	}
	current, err := s.checkWorkspaceAuthorityFenceTx(ctx, tx, fenceInput)
	if err != nil {
		_ = tx.Rollback()
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	journalHead, err := s.workspaceRuntimeJournalHeadTx(ctx, tx, input.WorkspaceID)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if input.CommitWatermark < journalHead {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, authorityRejectError(
			AuthorityRejectStale,
			fmt.Sprintf("authority transfer commit_watermark %d does not cover runtime journal head %d", input.CommitWatermark, journalHead),
			current,
			authorityRejectContext{
				WorkspaceID:             input.WorkspaceID,
				Scope:                   input.Scope,
				ExpectedAuthorityNodeID: input.CurrentHolderAuthorityNodeID,
				ExpectedTerm:            input.CurrentTerm,
				LeaseToken:              input.CurrentLeaseToken,
				ReferenceAt:             input.ReferenceAt,
				CommitWatermark:         input.CommitWatermark,
				AppliedWatermark:        input.AppliedWatermark,
			},
		)
	}
	record := WorkspaceAuthorityRecord{
		WorkspaceID:           input.WorkspaceID,
		Scope:                 input.Scope,
		HolderAuthorityNodeID: input.NewHolderAuthorityNodeID,
		LeaseToken:            input.NewLeaseToken,
		Term:                  input.NewTerm,
		LeaseExpiresAt:        leaseExpiresAt.Format(time.RFC3339Nano),
		CommitWatermark:       input.CommitWatermark,
		AppliedWatermark:      input.AppliedWatermark,
		Status:                WorkspaceAuthorityStatusActive,
		UpdatedAt:             nextWorkspaceAuthorityTransitionTimestamp(current.UpdatedAt, serverReferenceAt),
	}
	if err := authorityWatermarksMonotonic(current, record); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if err := s.upsertWorkspaceAuthorityTx(ctx, tx, record); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	runtimeEvent, err := s.recordAuthorityEventTx(ctx, tx, AuthorityEventInput{
		WorkspaceID:                   input.WorkspaceID,
		Scope:                         input.Scope,
		EventType:                     AuthorityEventTransferred,
		HolderAuthorityNodeID:         input.NewHolderAuthorityNodeID,
		PreviousHolderAuthorityNodeID: input.CurrentHolderAuthorityNodeID,
		LeaseToken:                    input.NewLeaseToken,
		Term:                          input.NewTerm,
		LeaseExpiresAt:                record.LeaseExpiresAt,
		CommitWatermark:               input.CommitWatermark,
		AppliedWatermark:              input.AppliedWatermark,
		Status:                        WorkspaceAuthorityStatusActive,
		ActorType:                     input.ActorType,
		ActorID:                       input.ActorID,
		ExpectedAuthorityNodeID:       input.CurrentHolderAuthorityNodeID,
		ExpectedTerm:                  input.CurrentTerm,
		ReferenceAt:                   serverReferenceAt.Format(time.RFC3339Nano),
	}, record.UpdatedAt)
	if err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceAuthorityRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit workspace authority transfer tx: %w", err)
	}
	return record, runtimeEvent, nil
}

func (s *Store) EnsureLocalWorkspaceAuthority(ctx context.Context, input EnsureLocalWorkspaceAuthorityInput) (EnsureLocalWorkspaceAuthorityResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	if input.WorkspaceID == "" {
		return EnsureLocalWorkspaceAuthorityResult{}, errors.New("workspace_id is required")
	}

	actorType, actorID, err := normalizeLocalAuthorityActor(input.ActorType, input.ActorID)
	if err != nil {
		return EnsureLocalWorkspaceAuthorityResult{}, err
	}
	referenceAt := time.Now().UTC()
	status, err := s.GetLocalWorkspaceAuthorityStatus(ctx, input.WorkspaceID, input.Scope)
	if err != nil {
		return EnsureLocalWorkspaceAuthorityResult{}, err
	}

	leaseExpiresAt := referenceAt.Add(localAuthorityLeaseTTL).Format(time.RFC3339Nano)
	journalHead := status.JournalHead
	current := status.Authority
	if current != nil {
		journalHead = maxInt64(journalHead, current.CommitWatermark)
	}
	localNodeID := strings.TrimSpace(status.LocalNode.AuthorityNodeID)

	switch status.LeaseState {
	case "missing":
		record, runtimeEvent, err := s.ClaimWorkspaceAuthority(ctx, WorkspaceAuthorityClaimInput{
			WorkspaceID:           input.WorkspaceID,
			Scope:                 input.Scope,
			HolderAuthorityNodeID: localNodeID,
			LeaseToken:            nextID("lease"),
			Term:                  1,
			LeaseExpiresAt:        leaseExpiresAt,
			CommitWatermark:       journalHead,
			AppliedWatermark:      journalHead,
			ActorType:             actorType,
			ActorID:               actorID,
			ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
		})
		if err != nil {
			return EnsureLocalWorkspaceAuthorityResult{}, err
		}
		nextStatus, statusErr := s.GetLocalWorkspaceAuthorityStatus(ctx, input.WorkspaceID, input.Scope)
		if statusErr != nil {
			return EnsureLocalWorkspaceAuthorityResult{}, statusErr
		}
		nextStatus.Authority = &record
		return EnsureLocalWorkspaceAuthorityResult{
			Action:       "CLAIMED",
			Status:       nextStatus,
			RuntimeEvent: &runtimeEvent,
		}, nil
	case "healthy":
		return EnsureLocalWorkspaceAuthorityResult{
			Action: "UNCHANGED",
			Status: status,
		}, nil
	case "renew_due":
		record, runtimeEvent, err := s.RenewWorkspaceAuthority(ctx, WorkspaceAuthorityRenewInput{
			WorkspaceID:           input.WorkspaceID,
			Scope:                 input.Scope,
			HolderAuthorityNodeID: localNodeID,
			LeaseToken:            current.LeaseToken,
			Term:                  current.Term,
			LeaseExpiresAt:        leaseExpiresAt,
			CommitWatermark:       journalHead,
			AppliedWatermark:      maxInt64(status.JournalHead, current.AppliedWatermark),
			ActorType:             actorType,
			ActorID:               actorID,
			ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
		})
		if err != nil {
			return EnsureLocalWorkspaceAuthorityResult{}, err
		}
		nextStatus, statusErr := s.GetLocalWorkspaceAuthorityStatus(ctx, input.WorkspaceID, input.Scope)
		if statusErr != nil {
			return EnsureLocalWorkspaceAuthorityResult{}, statusErr
		}
		nextStatus.Authority = &record
		return EnsureLocalWorkspaceAuthorityResult{
			Action:       "RENEWED",
			Status:       nextStatus,
			RuntimeEvent: &runtimeEvent,
		}, nil
	case "stale", "orphaned_holder":
		// CA-25: "orphaned_holder" is an ACTIVE row whose holder node row was deleted
		// (NULL holder). Like "stale", the local node adopts it via a fresh term-bump
		// claim -- now permitted because workspaceAuthorityIsLiveAt treats a holderless
		// lease as not-live -- instead of being permanently wedged behind a force-break
		// that ForceBreak refuses to perform on an incomplete row.
		action := "RECLAIMED"
		if status.LeaseState == "orphaned_holder" {
			action = "ADOPTED"
		}
		nextTerm := int64(1)
		if current != nil && current.Term >= nextTerm {
			nextTerm = current.Term + 1
		}
		record, runtimeEvent, err := s.ClaimWorkspaceAuthority(ctx, WorkspaceAuthorityClaimInput{
			WorkspaceID:           input.WorkspaceID,
			Scope:                 input.Scope,
			HolderAuthorityNodeID: localNodeID,
			LeaseToken:            nextID("lease"),
			Term:                  nextTerm,
			LeaseExpiresAt:        leaseExpiresAt,
			CommitWatermark:       journalHead,
			AppliedWatermark:      journalHead,
			ActorType:             actorType,
			ActorID:               actorID,
			ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
		})
		if err != nil {
			return EnsureLocalWorkspaceAuthorityResult{}, err
		}
		nextStatus, statusErr := s.GetLocalWorkspaceAuthorityStatus(ctx, input.WorkspaceID, input.Scope)
		if statusErr != nil {
			return EnsureLocalWorkspaceAuthorityResult{}, statusErr
		}
		nextStatus.Authority = &record
		return EnsureLocalWorkspaceAuthorityResult{
			Action:       action,
			Status:       nextStatus,
			RuntimeEvent: &runtimeEvent,
		}, nil
	case "foreign_live":
		return EnsureLocalWorkspaceAuthorityResult{}, authorityRejectError(
			AuthorityRejectStale,
			"workspace authority is held by another live node; use force-break before reclaiming locally",
			*current,
			authorityRejectContext{
				WorkspaceID:             input.WorkspaceID,
				Scope:                   input.Scope,
				ExpectedAuthorityNodeID: localNodeID,
				ExpectedTerm:            current.Term,
				ReferenceAt:             referenceAt.Format(time.RFC3339Nano),
				Retryable:               true,
			},
		)
	case "incomplete", "invalid", "transferring":
		if current == nil {
			return EnsureLocalWorkspaceAuthorityResult{}, errors.New("workspace authority state is unavailable")
		}
		return EnsureLocalWorkspaceAuthorityResult{}, authorityRejectError(
			AuthorityRejectStale,
			"workspace authority requires force-break before local reclaim",
			*current,
			authorityRejectContext{
				WorkspaceID:             input.WorkspaceID,
				Scope:                   input.Scope,
				ExpectedAuthorityNodeID: localNodeID,
				ExpectedTerm:            current.Term,
				ReferenceAt:             referenceAt.Format(time.RFC3339Nano),
				Retryable:               true,
			},
		)
	default:
		return EnsureLocalWorkspaceAuthorityResult{}, fmt.Errorf("unsupported workspace authority lease_state %q", status.LeaseState)
	}
}

func (s *Store) ForceBreakWorkspaceAuthority(ctx context.Context, input ForceBreakWorkspaceAuthorityInput) (ForceBreakWorkspaceAuthorityResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	if input.WorkspaceID == "" {
		return ForceBreakWorkspaceAuthorityResult{}, errors.New("workspace_id is required")
	}

	actorType, actorID, err := normalizeLocalAuthorityActor(input.ActorType, input.ActorID)
	if err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, err
	}
	if _, err := s.EnsureLocalAuthorityNode(ctx); err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, fmt.Errorf("ensure local authority node: %w", err)
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, fmt.Errorf("begin workspace authority force-break tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, err
	}

	current, err := s.getWorkspaceAuthorityTx(ctx, tx, input.WorkspaceID, input.Scope)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return ForceBreakWorkspaceAuthorityResult{}, fmt.Errorf("commit workspace authority force-break missing tx: %w", err)
		}
		status, statusErr := s.GetLocalWorkspaceAuthorityStatus(ctx, input.WorkspaceID, input.Scope)
		if statusErr != nil {
			return ForceBreakWorkspaceAuthorityResult{}, statusErr
		}
		return ForceBreakWorkspaceAuthorityResult{
			Action: "MISSING",
			Status: status,
		}, nil
	}
	if err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, fmt.Errorf("read current workspace authority for force-break: %w", err)
	}
	if strings.TrimSpace(current.HolderAuthorityNodeID) == "" || strings.TrimSpace(current.LeaseToken) == "" || current.Term <= 0 {
		return ForceBreakWorkspaceAuthorityResult{}, authorityRejectError(
			AuthorityRejectStale,
			"workspace authority row is incomplete for force-break",
			current,
			authorityRejectContext{
				WorkspaceID:             input.WorkspaceID,
				Scope:                   input.Scope,
				ExpectedAuthorityNodeID: current.HolderAuthorityNodeID,
				ExpectedTerm:            current.Term,
				ReferenceAt:             time.Now().UTC().Format(time.RFC3339Nano),
				Retryable:               true,
			},
		)
	}
	if current.Status == WorkspaceAuthorityStatusExpired {
		if err := tx.Commit(); err != nil {
			return ForceBreakWorkspaceAuthorityResult{}, fmt.Errorf("commit workspace authority already-expired tx: %w", err)
		}
		status, statusErr := s.GetLocalWorkspaceAuthorityStatus(ctx, input.WorkspaceID, input.Scope)
		if statusErr != nil {
			return ForceBreakWorkspaceAuthorityResult{}, statusErr
		}
		return ForceBreakWorkspaceAuthorityResult{
			Action: "ALREADY_EXPIRED",
			Status: status,
		}, nil
	}

	journalHead, err := s.workspaceRuntimeJournalHeadTx(ctx, tx, input.WorkspaceID)
	if err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, err
	}
	record := current
	record.Status = WorkspaceAuthorityStatusExpired
	record.CommitWatermark = maxInt64(current.CommitWatermark, journalHead)
	record.AppliedWatermark = maxInt64(current.AppliedWatermark, journalHead)
	record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := authorityWatermarksMonotonic(current, record); err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, err
	}
	if err := s.upsertWorkspaceAuthorityTx(ctx, tx, record); err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, err
	}
	runtimeEvent, err := s.recordAuthorityEventTx(ctx, tx, AuthorityEventInput{
		WorkspaceID:           input.WorkspaceID,
		Scope:                 input.Scope,
		EventType:             AuthorityEventExpired,
		HolderAuthorityNodeID: current.HolderAuthorityNodeID,
		LeaseToken:            current.LeaseToken,
		Term:                  current.Term,
		LeaseExpiresAt:        current.LeaseExpiresAt,
		CommitWatermark:       record.CommitWatermark,
		AppliedWatermark:      record.AppliedWatermark,
		Status:                WorkspaceAuthorityStatusExpired,
		ActorType:             actorType,
		ActorID:               actorID,
		ReferenceAt:           record.UpdatedAt,
	}, record.UpdatedAt)
	if err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, fmt.Errorf("commit workspace authority force-break tx: %w", err)
	}

	status, err := s.GetLocalWorkspaceAuthorityStatus(ctx, input.WorkspaceID, input.Scope)
	if err != nil {
		return ForceBreakWorkspaceAuthorityResult{}, err
	}
	status.Authority = &record
	return ForceBreakWorkspaceAuthorityResult{
		Action:       "EXPIRED",
		Status:       status,
		RuntimeEvent: &runtimeEvent,
	}, nil
}

func (s *Store) EnsureLocalAuthorityNode(ctx context.Context) (RuntimeNodeRecord, error) {
	if s == nil {
		return RuntimeNodeRecord{}, errors.New("store is nil")
	}
	if strings.TrimSpace(s.authorityIdentityPath) == "" {
		err := errors.New("authority identity path is not configured")
		s.setAuthorityDiagnostics(AuthorityNodeDiagnostics{State: "error", Message: err.Error()})
		return RuntimeNodeRecord{}, err
	}

	authorityNodeID, err := readOrCreateAuthorityNodeID(s.authorityIdentityPath)
	if err != nil {
		s.setAuthorityDiagnostics(AuthorityNodeDiagnostics{State: "error", Message: err.Error()})
		return RuntimeNodeRecord{}, err
	}

	hostLabel, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostLabel) == "" {
		hostLabel = "unknown"
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	bootInstanceID := strings.TrimSpace(s.authorityBootInstanceID)
	if bootInstanceID == "" {
		bootInstanceID = nextID("authboot")
		s.authorityBootInstanceID = bootInstanceID
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		s.setAuthorityDiagnostics(AuthorityNodeDiagnostics{State: "error", Message: err.Error()})
		return RuntimeNodeRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
INSERT INTO runtime_nodes (
	authority_node_id,
	node_kind,
	host_label,
	boot_instance_id,
	registered_at,
	last_seen_at,
	status
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(authority_node_id) DO UPDATE SET
	node_kind = excluded.node_kind,
	host_label = excluded.host_label,
	boot_instance_id = excluded.boot_instance_id,
	last_seen_at = excluded.last_seen_at,
	status = excluded.status
`,
		authorityNodeID,
		localAuthorityNodeKind,
		hostLabel,
		bootInstanceID,
		now,
		now,
		string(RuntimeNodeStatusOnline),
	); err != nil {
		s.setAuthorityDiagnostics(AuthorityNodeDiagnostics{State: "error", Message: err.Error()})
		return RuntimeNodeRecord{}, fmt.Errorf("upsert runtime node %s: %w", authorityNodeID, err)
	}

	record := RuntimeNodeRecord{}
	if err := tx.QueryRowContext(ctx, `
SELECT authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status
FROM runtime_nodes
WHERE authority_node_id = ?`,
		authorityNodeID,
	).Scan(
		&record.AuthorityNodeID,
		&record.NodeKind,
		&record.HostLabel,
		&record.BootInstanceID,
		&record.RegisteredAt,
		&record.LastSeenAt,
		&record.Status,
	); err != nil {
		s.setAuthorityDiagnostics(AuthorityNodeDiagnostics{State: "error", Message: err.Error()})
		return RuntimeNodeRecord{}, fmt.Errorf("read runtime node %s: %w", authorityNodeID, err)
	}

	if err := tx.Commit(); err != nil {
		s.setAuthorityDiagnostics(AuthorityNodeDiagnostics{State: "error", Message: err.Error()})
		return RuntimeNodeRecord{}, fmt.Errorf("commit runtime node %s: %w", authorityNodeID, err)
	}

	s.setAuthorityDiagnostics(AuthorityNodeDiagnostics{
		State:           "ok",
		AuthorityNodeID: record.AuthorityNodeID,
		NodeKind:        record.NodeKind,
		HostLabel:       record.HostLabel,
		BootInstanceID:  record.BootInstanceID,
		Status:          string(record.Status),
	})
	return record, nil
}

func (s *Store) LocalAuthorityNodeDiagnostics() AuthorityNodeDiagnostics {
	if s == nil {
		return AuthorityNodeDiagnostics{
			State:   "missing",
			Message: "sqlite store unavailable",
		}
	}
	if value := s.authorityDiagnostics.Load(); value != nil {
		return value.(AuthorityNodeDiagnostics)
	}
	return AuthorityNodeDiagnostics{
		State:   "missing",
		Message: "authority node identity not initialized",
	}
}

func (s *Store) CurrentAuthorityNodeDiagnostics(ctx context.Context) AuthorityNodeDiagnostics {
	if s == nil {
		return AuthorityNodeDiagnostics{
			State:   "missing",
			Message: "sqlite store unavailable",
		}
	}
	if strings.TrimSpace(s.authorityIdentityPath) == "" {
		diag := AuthorityNodeDiagnostics{
			State:   "error",
			Message: "authority identity path is not configured",
		}
		s.setAuthorityDiagnostics(diag)
		return diag
	}

	authorityNodeID, err := readAuthorityNodeID(s.authorityIdentityPath)
	if err != nil {
		diag := AuthorityNodeDiagnostics{
			State:   "error",
			Message: err.Error(),
		}
		if errors.Is(err, os.ErrNotExist) {
			diag.State = "missing"
		}
		s.setAuthorityDiagnostics(diag)
		return diag
	}

	cached := s.LocalAuthorityNodeDiagnostics()
	if cached.AuthorityNodeID != "" && strings.TrimSpace(cached.AuthorityNodeID) != authorityNodeID {
		diag := AuthorityNodeDiagnostics{
			State:           "error",
			Message:         "authority identity drift detected",
			AuthorityNodeID: authorityNodeID,
		}
		s.setAuthorityDiagnostics(diag)
		return diag
	}

	record, err := getRuntimeNodeRecord(ctx, s.db, authorityNodeID)
	if err != nil {
		diag := AuthorityNodeDiagnostics{
			State:           "error",
			Message:         err.Error(),
			AuthorityNodeID: authorityNodeID,
		}
		if errors.Is(err, sql.ErrNoRows) {
			diag.State = "missing"
			diag.Message = "authority node row is missing"
		}
		s.setAuthorityDiagnostics(diag)
		return diag
	}

	diag := AuthorityNodeDiagnostics{
		State:           "ok",
		AuthorityNodeID: record.AuthorityNodeID,
		NodeKind:        record.NodeKind,
		HostLabel:       record.HostLabel,
		BootInstanceID:  record.BootInstanceID,
		Status:          string(record.Status),
	}
	if record.Status != RuntimeNodeStatusOnline {
		diag.State = "degraded"
		diag.Message = fmt.Sprintf("authority runtime node status is %s", record.Status)
	}
	s.setAuthorityDiagnostics(diag)
	return diag
}

func (s *Store) setAuthorityDiagnostics(diag AuthorityNodeDiagnostics) {
	if s == nil {
		return
	}
	s.authorityDiagnostics.Store(diag)
}

func readOrCreateAuthorityNodeID(path string) (string, error) {
	if id, err := readAuthorityNodeID(path); err == nil {
		return id, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := fssecure.EnsurePrivateParentDir(dir); err != nil {
			return "", fmt.Errorf("create authority identity directory: %w", err)
		}
	}

	candidate := nextID("authnode")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return readAuthorityNodeID(path)
		}
		return "", fmt.Errorf("create authority identity file: %w", err)
	}
	defer func() { _ = file.Close() }()

	if _, err := file.WriteString(candidate + "\n"); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("write authority identity file: %w", err)
	}
	return candidate, nil
}

func readAuthorityNodeID(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(raw))
	if id == "" {
		return "", fmt.Errorf("authority identity file %s is empty", path)
	}
	if err := validateAuthorityNodeID(id); err != nil {
		return "", fmt.Errorf("authority identity file %s is invalid: %w", path, err)
	}
	return id, nil
}

func validateAuthorityNodeID(id string) error {
	parts := strings.Split(strings.TrimSpace(id), "-")
	if len(parts) != 3 || parts[0] != "authnode" {
		return errors.New("expected authnode-<epoch>-<counter> format")
	}
	for _, part := range parts[1:] {
		if _, err := strconv.ParseUint(part, 10, 64); err != nil {
			return fmt.Errorf("expected numeric authority node suffix, got %q", part)
		}
	}
	return nil
}

func getRuntimeNodeRecord(ctx context.Context, db *sql.DB, authorityNodeID string) (RuntimeNodeRecord, error) {
	record := RuntimeNodeRecord{}
	if err := db.QueryRowContext(ctx, `
SELECT authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status
FROM runtime_nodes
WHERE authority_node_id = ?`,
		strings.TrimSpace(authorityNodeID),
	).Scan(
		&record.AuthorityNodeID,
		&record.NodeKind,
		&record.HostLabel,
		&record.BootInstanceID,
		&record.RegisteredAt,
		&record.LastSeenAt,
		&record.Status,
	); err != nil {
		return RuntimeNodeRecord{}, err
	}
	return record, nil
}
