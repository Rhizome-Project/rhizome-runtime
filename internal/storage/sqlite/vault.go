package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/secret"
)

// VaultEntry represents a stored credential set.
type VaultEntry struct {
	EntryID     string `json:"entry_id"`
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	FieldsJSON  string `json:"fields_json"`
	CreatedBy   string `json:"created_by"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type VaultEntryMutationInput struct {
	Entry     VaultEntry
	ActorID   string
	ActorType string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type VaultEntryDeleteInput struct {
	WorkspaceID string
	EntryID     string
	ActorID     string
	ActorType   string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type VaultAccessEventInput struct {
	WorkspaceID string
	EntryID     string
	EntryTitle  string
	Action      string
	ActorID     string
	ActorType   string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

// CreateVaultEntry inserts a new vault entry.
func (s *Store) CreateVaultEntry(ctx context.Context, entry VaultEntry) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin vault create tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.createVaultEntryTx(ctx, tx, entry, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit vault create: %w", err)
	}
	return nil
}

func (s *Store) CreateVaultEntryWithEvent(ctx context.Context, input VaultEntryMutationInput) (RuntimeEventRecord, error) {
	entry, actorID, actorType, err := normalizeVaultEntryMutationInput(input, true)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, entry.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin vault create tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.createVaultEntryTx(ctx, tx, entry, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, entry.WorkspaceID, now, "vault entry create"); err != nil {
			return err
		}
		if err := s.logVaultAccessTx(ctx, tx, entry.WorkspaceID, entry.EntryID, entry.Title, "create", actorID, now); err != nil {
			return err
		}
		payload, err := attachVaultPromptContextEnvelope(map[string]any{
			"workspace_id":        entry.WorkspaceID,
			"entry_id":            entry.EntryID,
			"actor_id":            actorID,
			"title":               entry.Title,
			"description_present": strings.TrimSpace(entry.Description) != "",
			"fields_sha256":       "sha256:" + contentSHA256(normalizeVaultFieldsJSON(entry.FieldsJSON)),
			"entity_type":         "vault_entry",
			"entity_id":           entry.EntryID,
			"summary":             "Vault entry created: " + entry.Title,
			"mutation_operation":  "create",
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "vault.create"), map[string]string{
			"workspace_id":   entry.WorkspaceID,
			"entry_id":       entry.EntryID,
			"actor_id":       actorID,
			"principal_type": actorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: entry.WorkspaceID,
			EventType:   "vault.entry.created",
			EntityType:  "vault_entry",
			EntityID:    entry.EntryID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit vault create: %w", err)
	}
	return event, nil
}

func (s *Store) createVaultEntryTx(ctx context.Context, tx *sql.Tx, entry VaultEntry, now string) error {
	entry.EntryID = strings.TrimSpace(entry.EntryID)
	entry.WorkspaceID = strings.TrimSpace(entry.WorkspaceID)
	entry.Title = strings.TrimSpace(entry.Title)
	entry.CreatedBy = strings.TrimSpace(entry.CreatedBy)
	if entry.EntryID == "" {
		return errors.New("entry_id is required")
	}
	if entry.WorkspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if entry.Title == "" {
		return errors.New("title is required")
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, entry.WorkspaceID); err != nil {
		return err
	}
	if entry.FieldsJSON == "" {
		entry.FieldsJSON = "{}"
	}

	encryptedFields, err := secret.EncryptVaultData(entry.FieldsJSON)
	if err != nil {
		return fmt.Errorf("encrypt vault fields: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO vault_entries (entry_id, workspace_id, title, description, fields_json, created_by, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.EntryID,
		entry.WorkspaceID,
		entry.Title,
		entry.Description,
		encryptedFields,
		entry.CreatedBy,
		now, now,
	)
	if err != nil {
		return fmt.Errorf("create vault entry: %w", err)
	}
	return nil
}

// UpdateVaultEntry updates an existing vault entry.
func (s *Store) UpdateVaultEntry(ctx context.Context, entry VaultEntry) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin vault update tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.updateVaultEntryTx(ctx, tx, entry, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit vault update: %w", err)
	}
	return nil
}

func (s *Store) UpdateVaultEntryWithEvent(ctx context.Context, input VaultEntryMutationInput) (RuntimeEventRecord, error) {
	entry, actorID, actorType, err := normalizeVaultEntryMutationInput(input, false)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, entry.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin vault update tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.updateVaultEntryTx(ctx, tx, entry, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, entry.WorkspaceID, now, "vault entry update"); err != nil {
			return err
		}
		if err := s.logVaultAccessTx(ctx, tx, entry.WorkspaceID, entry.EntryID, entry.Title, "update", actorID, now); err != nil {
			return err
		}
		payload, err := attachVaultPromptContextEnvelope(map[string]any{
			"workspace_id":        entry.WorkspaceID,
			"entry_id":            entry.EntryID,
			"actor_id":            actorID,
			"title":               entry.Title,
			"description_present": strings.TrimSpace(entry.Description) != "",
			"fields_sha256":       "sha256:" + contentSHA256(normalizeVaultFieldsJSON(entry.FieldsJSON)),
			"entity_type":         "vault_entry",
			"entity_id":           entry.EntryID,
			"summary":             "Vault entry updated: " + entry.EntryID,
			"mutation_operation":  "update",
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "vault.update"), map[string]string{
			"workspace_id":   entry.WorkspaceID,
			"entry_id":       entry.EntryID,
			"actor_id":       actorID,
			"principal_type": actorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: entry.WorkspaceID,
			EventType:   "vault.entry.updated",
			EntityType:  "vault_entry",
			EntityID:    entry.EntryID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit vault update: %w", err)
	}
	return event, nil
}

func (s *Store) updateVaultEntryTx(ctx context.Context, tx *sql.Tx, entry VaultEntry, now string) error {
	entry.EntryID = strings.TrimSpace(entry.EntryID)
	entry.WorkspaceID = strings.TrimSpace(entry.WorkspaceID)
	entry.Title = strings.TrimSpace(entry.Title)
	if entry.EntryID == "" {
		return errors.New("entry_id is required")
	}
	if entry.WorkspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if entry.Title == "" {
		return errors.New("title is required")
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, entry.WorkspaceID); err != nil {
		return err
	}
	if entry.FieldsJSON == "" {
		entry.FieldsJSON = "{}"
	}
	encryptedFields, err := secret.EncryptVaultData(entry.FieldsJSON)
	if err != nil {
		return fmt.Errorf("encrypt vault fields: %w", err)
	}

	res, err := tx.ExecContext(ctx,
		`UPDATE vault_entries SET title = ?, description = ?, fields_json = ?, updated_at = ?
		 WHERE entry_id = ? AND workspace_id = ?`,
		entry.Title,
		entry.Description,
		encryptedFields,
		now,
		entry.EntryID,
		entry.WorkspaceID,
	)
	if err != nil {
		return fmt.Errorf("update vault entry: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("vault entry not found: %s", entry.EntryID)
	}
	return nil
}

// DeleteVaultEntry deletes a vault entry by ID.
func (s *Store) DeleteVaultEntry(ctx context.Context, workspaceID, entryID string) error {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin vault delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := s.deleteVaultEntryTx(ctx, tx, workspaceID, entryID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit vault delete: %w", err)
	}
	return nil
}

func (s *Store) DeleteVaultEntryWithEvent(ctx context.Context, input VaultEntryDeleteInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	entryID := strings.TrimSpace(input.EntryID)
	actorID := strings.TrimSpace(input.ActorID)
	actorType := firstNonEmpty(strings.TrimSpace(input.ActorType), "human")
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if entryID == "" {
		return RuntimeEventRecord{}, errors.New("entry_id is required")
	}
	if actorID == "" {
		return RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin vault delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		title, err := s.deleteVaultEntryTx(ctx, tx, workspaceID, entryID)
		if err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "vault entry delete"); err != nil {
			return err
		}
		if err := s.logVaultAccessTx(ctx, tx, workspaceID, entryID, title, "delete", actorID, now); err != nil {
			return err
		}
		payload, err := attachVaultPromptContextEnvelope(map[string]any{
			"workspace_id":       workspaceID,
			"entry_id":           entryID,
			"actor_id":           actorID,
			"title":              title,
			"entity_type":        "vault_entry",
			"entity_id":          entryID,
			"summary":            "Vault entry deleted: " + entryID,
			"mutation_operation": "delete",
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "vault.delete"), map[string]string{
			"workspace_id":   workspaceID,
			"entry_id":       entryID,
			"actor_id":       actorID,
			"principal_type": actorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "vault.entry.deleted",
			EntityType:  "vault_entry",
			EntityID:    entryID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit vault delete: %w", err)
	}
	return event, nil
}

func (s *Store) deleteVaultEntryTx(ctx context.Context, tx *sql.Tx, workspaceID, entryID string) (string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	entryID = strings.TrimSpace(entryID)
	if workspaceID == "" {
		return "", errors.New("workspace_id is required")
	}
	if entryID == "" {
		return "", errors.New("entry_id is required")
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return "", err
	}
	title := entryID
	if err := tx.QueryRowContext(ctx,
		`SELECT title FROM vault_entries WHERE entry_id = ? AND workspace_id = ?`,
		entryID, workspaceID,
	).Scan(&title); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("vault entry not found: %s", entryID)
		}
		return "", fmt.Errorf("get vault entry before delete: %w", err)
	}
	res, err := tx.ExecContext(ctx,
		`DELETE FROM vault_entries WHERE entry_id = ? AND workspace_id = ?`,
		entryID,
		workspaceID,
	)
	if err != nil {
		return "", fmt.Errorf("delete vault entry: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return "", fmt.Errorf("vault entry not found: %s", entryID)
	}
	return title, nil
}

// GetVaultEntry retrieves a single vault entry.
func (s *Store) GetVaultEntry(ctx context.Context, workspaceID, entryID string) (*VaultEntry, error) {
	var e VaultEntry
	var fieldsJSON string
	err := s.db.QueryRowContext(ctx,
		`SELECT entry_id, workspace_id, title, description, fields_json, created_by, created_at, updated_at
		 FROM vault_entries WHERE entry_id = ? AND workspace_id = ?`,
		strings.TrimSpace(entryID),
		strings.TrimSpace(workspaceID),
	).Scan(&e.EntryID, &e.WorkspaceID, &e.Title, &e.Description, &fieldsJSON, &e.CreatedBy, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("get vault entry: %w", err)
	}
	decrypted, err := secret.DecryptVaultData(fieldsJSON)
	if err != nil {
		return nil, fmt.Errorf("decrypt vault entry: %w", err)
	}
	e.FieldsJSON = decrypted
	return &e, nil
}

// ListVaultEntries lists all vault entries for a workspace.
func (s *Store) ListVaultEntries(ctx context.Context, workspaceID string) ([]VaultEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT entry_id, workspace_id, title, description, fields_json, created_by, created_at, updated_at
		 FROM vault_entries WHERE workspace_id = ? ORDER BY title`,
		strings.TrimSpace(workspaceID),
	)
	if err != nil {
		return nil, fmt.Errorf("list vault entries: %w", err)
	}
	defer rows.Close()

	var entries []VaultEntry
	for rows.Next() {
		var e VaultEntry
		var fieldsJSON string
		if err := rows.Scan(&e.EntryID, &e.WorkspaceID, &e.Title, &e.Description, &fieldsJSON, &e.CreatedBy, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan vault entry: %w", err)
		}
		decrypted, err := secret.DecryptVaultData(fieldsJSON)
		if err != nil {
			return nil, fmt.Errorf("decrypt vault entry %s: %w", e.EntryID, err)
		}
		e.FieldsJSON = decrypted
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// VaultAuditEntry represents a single audit log record.
type VaultAuditEntry struct {
	ID          int64  `json:"id"`
	WorkspaceID string `json:"workspace_id"`
	EntryID     string `json:"entry_id"`
	EntryTitle  string `json:"entry_title"`
	Action      string `json:"action"`
	Actor       string `json:"actor"`
	CreatedAt   string `json:"created_at"`
}

// LogVaultAccess records a vault access event.
func (s *Store) LogVaultAccess(ctx context.Context, workspaceID, entryID, entryTitle, action, actor string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.logVaultAccessTx(ctx, nil, workspaceID, entryID, entryTitle, action, actor, now)
}

func (s *Store) LogVaultAccessWithEvent(ctx context.Context, input VaultAccessEventInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	entryID := strings.TrimSpace(input.EntryID)
	entryTitle := strings.TrimSpace(input.EntryTitle)
	action := strings.TrimSpace(input.Action)
	actorID := strings.TrimSpace(input.ActorID)
	actorType := firstNonEmpty(strings.TrimSpace(input.ActorType), "human")
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if entryID == "" {
		return RuntimeEventRecord{}, errors.New("entry_id is required")
	}
	if entryTitle == "" {
		entryTitle = entryID
	}
	if action == "" {
		return RuntimeEventRecord{}, errors.New("action is required")
	}
	if actorID == "" {
		return RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin vault access tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.logVaultAccessTx(ctx, tx, workspaceID, entryID, entryTitle, action, actorID, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "vault access audit"); err != nil {
			return err
		}
		expectedSurface := vaultAccessPromptSurface(action)
		if expectedSurface == "" {
			return fmt.Errorf("unsupported vault access action: %s", action)
		}
		surface := firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), expectedSurface)
		if surface != expectedSurface {
			return fmt.Errorf("vault access surface %q does not match action %q", surface, action)
		}
		payload, err := attachVaultPromptContextEnvelope(map[string]any{
			"workspace_id":       workspaceID,
			"entry_id":           entryID,
			"actor_id":           actorID,
			"entry_title":        entryTitle,
			"action":             action,
			"entity_type":        "vault_entry",
			"entity_id":          entryID,
			"summary":            "Vault access logged: " + action + " " + entryID,
			"mutation_operation": "access_audit",
		}, input.PromptContextEnvelope, surface, map[string]string{
			"workspace_id":   workspaceID,
			"entry_id":       entryID,
			"actor_id":       actorID,
			"principal_type": actorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   vaultAccessEventType(action),
			EntityType:  "vault_entry",
			EntityID:    entryID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit vault access: %w", err)
	}
	return event, nil
}

func (s *Store) logVaultAccessTx(ctx context.Context, tx *sql.Tx, workspaceID, entryID, entryTitle, action, actor, now string) error {
	exec := s.writeDB.ExecContext
	if tx != nil {
		exec = tx.ExecContext
	}
	_, err := exec(ctx,
		`INSERT INTO vault_audit_log (workspace_id, entry_id, entry_title, action, actor, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		workspaceID, entryID, entryTitle, action, actor, now,
	)
	if err != nil {
		return fmt.Errorf("log vault access: %w", err)
	}
	return nil
}

func vaultAccessPromptSurface(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "read":
		return "vault.get"
	case "list":
		return "vault.list"
	default:
		return ""
	}
}

func vaultAccessEventType(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "read":
		return "vault.entry.read"
	case "list":
		return "vault.entries.listed"
	default:
		return "vault.access.logged"
	}
}

func normalizeVaultEntryMutationInput(input VaultEntryMutationInput, requireCreatedBy bool) (VaultEntry, string, string, error) {
	entry := input.Entry
	entry.EntryID = strings.TrimSpace(entry.EntryID)
	entry.WorkspaceID = strings.TrimSpace(entry.WorkspaceID)
	entry.Title = strings.TrimSpace(entry.Title)
	entry.CreatedBy = strings.TrimSpace(entry.CreatedBy)
	actorID := strings.TrimSpace(input.ActorID)
	actorType := firstNonEmpty(strings.TrimSpace(input.ActorType), "human")
	if entry.EntryID == "" {
		return entry, "", "", errors.New("entry_id is required")
	}
	if entry.WorkspaceID == "" {
		return entry, "", "", errors.New("workspace_id is required")
	}
	if entry.Title == "" {
		return entry, "", "", errors.New("title is required")
	}
	if actorID == "" {
		return entry, "", "", errors.New("actor_id is required")
	}
	if requireCreatedBy && entry.CreatedBy == "" {
		entry.CreatedBy = actorID
	}
	if requireCreatedBy && entry.CreatedBy != actorID {
		return entry, "", "", errors.New("created_by must match actor_id")
	}
	if entry.FieldsJSON == "" {
		entry.FieldsJSON = "{}"
	}
	return entry, actorID, actorType, nil
}

func normalizeVaultFieldsJSON(fieldsJSON string) string {
	fieldsJSON = strings.TrimSpace(fieldsJSON)
	if fieldsJSON == "" {
		return "{}"
	}
	return fieldsJSON
}

func attachVaultPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachVaultPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
		"entry_id":     strings.TrimSpace(fields["entry_id"]),
		"actor_id":     strings.TrimSpace(fields["actor_id"]),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("vault_entry.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

// ListVaultAuditLog returns the last N audit log entries for a workspace.
func (s *Store) ListVaultAuditLog(ctx context.Context, workspaceID string, limit int) ([]VaultAuditEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, workspace_id, entry_id, entry_title, action, actor, created_at
		 FROM vault_audit_log WHERE workspace_id = ? ORDER BY created_at DESC LIMIT ?`,
		strings.TrimSpace(workspaceID), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list vault audit: %w", err)
	}
	defer rows.Close()

	var entries []VaultAuditEntry
	for rows.Next() {
		var e VaultAuditEntry
		if err := rows.Scan(&e.ID, &e.WorkspaceID, &e.EntryID, &e.EntryTitle, &e.Action, &e.Actor, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan vault audit: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}
