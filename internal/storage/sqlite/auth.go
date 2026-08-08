package sqlite

import (
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const passwordHashIterations = 310000
const passwordHashKeyLen = 32
const systemAgentMessageSenderID = "system"
const systemAgentTokenMessageChannel = "security"
const systemAgentTokenMessageContentType = "application/vnd.rhizome.auth-token"

type AgentSelfRegisterInput struct {
	WorkspaceID       string
	WorkspacePassword string
	AgentID           string
	DisplayName       string
	Role              string
	ProtocolVersion   string
	Capabilities      []string
	Summary           string
	OwnerUserID       string
	IPAddress         string
	UserAgent         string
}

type AgentRegistrationResult struct {
	WorkspaceID string      `json:"workspace_id"`
	AgentID     string      `json:"agent_id"`
	DisplayName string      `json:"display_name"`
	Token       string      `json:"token"`
	Agent       AgentRecord `json:"agent,omitempty"`
}

type HumanRegisterInput struct {
	WorkspaceID       string
	WorkspacePassword string
	Username          string
	DisplayName       string
	Password          string
	IPAddress         string
	UserAgent         string
}

type HumanLoginInput struct {
	WorkspaceID string
	Username    string
	DisplayName string
	Password    string
	IPAddress   string
	UserAgent   string
}

type HumanAuthResult struct {
	WorkspaceID string `json:"workspace_id"`
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Token       string `json:"token"`
}

type HumanProfileUpdateInput struct {
	WorkspaceID    string
	UserID         string
	DisplayName    string
	Password       string
	IPAddress      string
	UserAgent      string
	TelegramUserID *int64
}

type HumanProfileRecord struct {
	WorkspaceID    string        `json:"workspace_id"`
	UserID         string        `json:"user_id"`
	Username       string        `json:"username"`
	DisplayName    string        `json:"display_name"`
	Status         string        `json:"status"`
	CreatedAt      string        `json:"created_at"`
	UpdatedAt      string        `json:"updated_at"`
	LastLoginAt    *string       `json:"last_login_at,omitempty"`
	TelegramUserID *int64        `json:"telegram_user_id,omitempty"`
	Agents         []AgentRecord `json:"agents"`
	AgentCount     int           `json:"agent_count"`
}

type AuthPrincipalRecord struct {
	WorkspaceID  string `json:"workspace_id"`
	SubjectType  string `json:"subject_type"`
	SubjectID    string `json:"subject_id"`
	TokenID      string `json:"token_id"`
	TokenPrefix  string `json:"token_prefix"`
	DisplayName  string `json:"display_name"`
	MetadataJSON string `json:"metadata_json,omitempty"`
}

type AuthTokenRecord struct {
	TokenID       string  `json:"token_id"`
	WorkspaceID   string  `json:"workspace_id"`
	SubjectType   string  `json:"subject_type"`
	SubjectID     string  `json:"subject_id"`
	SubjectLabel  string  `json:"subject_label"`
	TokenPrefix   string  `json:"token_prefix"`
	IssuedBy      string  `json:"issued_by"`
	IssuedAt      string  `json:"issued_at"`
	LastUsedAt    *string `json:"last_used_at,omitempty"`
	RevokedAt     *string `json:"revoked_at,omitempty"`
	RevokedReason string  `json:"revoked_reason,omitempty"`
	MetadataJSON  string  `json:"metadata_json,omitempty"`
}

type HumanSessionRecord struct {
	TokenID      string  `json:"token_id"`
	TokenPrefix  string  `json:"token_prefix"`
	SubjectID    string  `json:"subject_id"`
	SubjectLabel string  `json:"subject_label"`
	IssuedBy     string  `json:"issued_by"`
	IssuedAt     string  `json:"issued_at"`
	LastUsedAt   *string `json:"last_used_at,omitempty"`
	IsCurrent    bool    `json:"is_current"`
}

type HumanSessionRevokeInput struct {
	WorkspaceID    string
	UserID         string
	TokenID        string
	CurrentTokenID string
	Scope          string
	ActorType      string
	ActorID        string
	IPAddress      string
	UserAgent      string
}

type AgentTokenRotateInput struct {
	WorkspaceID string
	AgentID     string
	ActorType   string
	ActorID     string
	IPAddress   string
	UserAgent   string
}

type AgentTokenRotateResult struct {
	WorkspaceID  string             `json:"workspace_id"`
	AgentID      string             `json:"agent_id"`
	DisplayName  string             `json:"display_name"`
	Token        string             `json:"token"`
	MessageID    string             `json:"message_id,omitempty"`
	MessageEvent RuntimeEventRecord `json:"-"`
}

type WorkspaceSecuritySettingsInput struct {
	WorkspaceID       string
	Title             string
	Description       string
	WorkspacePassword string
	UpdatedByType     string
	UpdatedByID       string
	IPAddress         string
	UserAgent         string
}

type WorkspaceSecuritySettingsRecord struct {
	WorkspaceID       string  `json:"workspace_id"`
	Title             string  `json:"title"`
	Description       string  `json:"description"`
	PasswordUpdatedBy string  `json:"password_updated_by"`
	PasswordUpdatedAt string  `json:"password_updated_at"`
	HumanCount        int     `json:"human_count"`
	AgentCount        int     `json:"agent_count"`
	CreatedAt         string  `json:"created_at"`
	UpdatedAt         string  `json:"updated_at"`
	LastSecurityEvent *string `json:"last_security_event,omitempty"`
}

type SecurityEventRecord struct {
	EventID     string `json:"event_id"`
	WorkspaceID string `json:"workspace_id"`
	EventType   string `json:"event_type"`
	ActorType   string `json:"actor_type"`
	ActorID     string `json:"actor_id"`
	SubjectType string `json:"subject_type"`
	SubjectID   string `json:"subject_id"`
	IPAddress   string `json:"ip_address,omitempty"`
	UserAgent   string `json:"user_agent,omitempty"`
	DetailJSON  string `json:"detail_json,omitempty"`
	CreatedAt   string `json:"created_at"`
}

var ErrHumanNotFound = errors.New("human not found")
var ErrHumanUsernameConflict = errors.New("human username already exists in workspace")
var ErrHumanDisplayNameConflict = errors.New("human display name already exists in workspace")
var ErrHumanTelegramUserIDConflict = errors.New("human telegram user id already exists in workspace")
var ErrAuthTokenNotFound = errors.New("auth token not found")

func (s *Store) BootstrapWorkspaceSecuritySettings(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT workspace_id FROM workspaces ORDER BY workspace_id`)
	if err != nil {
		return fmt.Errorf("query workspaces for security bootstrap: %w", err)
	}
	defer rows.Close()

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin workspace security bootstrap tx: %w", err)
	}
	for rows.Next() {
		var workspaceID string
		if err := rows.Scan(&workspaceID); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("scan workspace for security bootstrap: %w", err)
		}
		if err := s.ensureWorkspaceAuthSettingsTx(ctx, tx, workspaceID); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("iterate workspaces for security bootstrap: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit workspace security bootstrap tx: %w", err)
	}
	return nil
}

// DefaultWorkspacePassword is an ephemeral, process-local bootstrap value.
// Public entry points must provide an explicit password.
var DefaultWorkspacePassword = newEphemeralWorkspacePassword()

const (
	minimumPasswordCharacters = 12
	maximumPasswordBytes      = 256
)

func newEphemeralWorkspacePassword() string {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		panic(fmt.Sprintf("generate ephemeral workspace password: %v", err))
	}
	return hex.EncodeToString(raw)
}

func (s *Store) ensureWorkspaceAuthSettingsTx(ctx context.Context, tx *sql.Tx, workspaceID string, bootstrapPassword ...string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}

	var count int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(1) FROM workspace_security_settings WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&count); err != nil {
		return fmt.Errorf("check workspace auth settings: %w", err)
	}
	if count > 0 {
		return nil
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	password := DefaultWorkspacePassword
	if len(bootstrapPassword) > 0 && strings.TrimSpace(bootstrapPassword[0]) != "" {
		password = strings.TrimSpace(bootstrapPassword[0])
	}
	if err := validateNewPassword("workspace password", password); err != nil {
		return err
	}
	passwordHash, err := hashPassword(password)
	if err != nil {
		return fmt.Errorf("hash default workspace password: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO workspace_security_settings(
			workspace_id, password_hash, password_updated_by, password_updated_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		workspaceID, passwordHash, "system", now, now, now,
	); err != nil {
		return fmt.Errorf("insert default workspace auth settings: %w", err)
	}
	return nil
}

func (s *Store) GetWorkspaceSecuritySettings(ctx context.Context, workspaceID string) (WorkspaceSecuritySettingsRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return WorkspaceSecuritySettingsRecord{}, errors.New("workspace_id is required")
	}

	workspace, err := s.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return WorkspaceSecuritySettingsRecord{}, err
	}
	if err := s.ensureWorkspaceSecuritySettings(ctx, workspaceID); err != nil {
		return WorkspaceSecuritySettingsRecord{}, err
	}

	var record WorkspaceSecuritySettingsRecord
	if err := s.db.QueryRowContext(ctx,
		`SELECT password_updated_by, password_updated_at, created_at, updated_at
		   FROM workspace_security_settings
		  WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&record.PasswordUpdatedBy, &record.PasswordUpdatedAt, &record.CreatedAt, &record.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceSecuritySettingsRecord{}, ErrWorkspaceNotFound
		}
		return WorkspaceSecuritySettingsRecord{}, fmt.Errorf("query workspace security settings: %w", err)
	}
	record.WorkspaceID = workspace.WorkspaceID
	record.Title = workspace.Title
	record.Description = workspace.Description
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM workspace_humans WHERE workspace_id = ?`, workspaceID).Scan(&record.HumanCount); err != nil {
		return WorkspaceSecuritySettingsRecord{}, fmt.Errorf("count human accounts: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM agents WHERE workspace_id = ?`, workspaceID).Scan(&record.AgentCount); err != nil {
		return WorkspaceSecuritySettingsRecord{}, fmt.Errorf("count agents: %w", err)
	}
	var lastEvent sql.NullString
	if err := s.db.QueryRowContext(ctx,
		`SELECT created_at FROM workspace_security_events WHERE workspace_id = ? ORDER BY created_at DESC, event_id DESC LIMIT 1`,
		workspaceID,
	).Scan(&lastEvent); err == nil && lastEvent.Valid {
		record.LastSecurityEvent = &lastEvent.String
	}
	return record, nil
}

func (s *Store) UpdateWorkspaceSecuritySettings(ctx context.Context, input WorkspaceSecuritySettingsInput) (WorkspaceSecuritySettingsRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return WorkspaceSecuritySettingsRecord{}, errors.New("workspace_id is required")
	}
	workspace, err := s.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return WorkspaceSecuritySettingsRecord{}, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceSecuritySettingsRecord{}, fmt.Errorf("begin update workspace security settings tx: %w", err)
	}
	if err := s.ensureWorkspaceAuthSettingsTx(ctx, tx, workspaceID); err != nil {
		_ = tx.Rollback()
		return WorkspaceSecuritySettingsRecord{}, err
	}

	title := strings.TrimSpace(input.Title)
	if title == "" {
		title = workspace.Title
	}
	description := strings.TrimSpace(input.Description)
	if description == "" {
		description = workspace.Description
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	passwordChanged := strings.TrimSpace(input.WorkspacePassword) != ""
	settingsChanged := title != workspace.Title || description != workspace.Description || passwordChanged

	if _, err := tx.ExecContext(ctx,
		`UPDATE workspaces SET title = ?, description = ?, updated_at = ? WHERE workspace_id = ?`,
		title, description, now, workspaceID,
	); err != nil {
		_ = tx.Rollback()
		return WorkspaceSecuritySettingsRecord{}, fmt.Errorf("update workspace details: %w", err)
	}

	if passwordChanged {
		if err := validateNewPassword("workspace password", input.WorkspacePassword); err != nil {
			_ = tx.Rollback()
			return WorkspaceSecuritySettingsRecord{}, err
		}
		passwordHash, err := hashPassword(input.WorkspacePassword)
		if err != nil {
			_ = tx.Rollback()
			return WorkspaceSecuritySettingsRecord{}, err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE workspace_security_settings
			    SET password_hash = ?, password_updated_by = ?, password_updated_at = ?, updated_at = ?
			  WHERE workspace_id = ?`,
			passwordHash,
			firstNonEmpty(strings.TrimSpace(input.UpdatedByID), strings.TrimSpace(input.UpdatedByType), "dashboard"),
			now,
			now,
			workspaceID,
		); err != nil {
			_ = tx.Rollback()
			return WorkspaceSecuritySettingsRecord{}, fmt.Errorf("update workspace password: %w", err)
		}
	}

	if settingsChanged {
		if err := s.addSecurityEventTx(ctx, tx, SecurityEventRecord{
			WorkspaceID: workspaceID,
			EventType:   "workspace_settings_updated",
			ActorType:   firstNonEmpty(strings.TrimSpace(input.UpdatedByType), "human"),
			ActorID:     firstNonEmpty(strings.TrimSpace(input.UpdatedByID), "dashboard"),
			SubjectType: "workspace",
			SubjectID:   workspaceID,
			IPAddress:   strings.TrimSpace(input.IPAddress),
			UserAgent:   strings.TrimSpace(input.UserAgent),
			DetailJSON: mustJSON(map[string]any{
				"title":              title,
				"description":        description,
				"workspace_password": passwordChanged,
			}),
		}); err != nil {
			_ = tx.Rollback()
			return WorkspaceSecuritySettingsRecord{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return WorkspaceSecuritySettingsRecord{}, fmt.Errorf("commit update workspace security settings tx: %w", err)
	}
	return s.GetWorkspaceSecuritySettings(ctx, workspaceID)
}

func (s *Store) RegisterAgentWithWorkspacePassword(ctx context.Context, input AgentSelfRegisterInput) (AgentRegistrationResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return AgentRegistrationResult{}, errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return AgentRegistrationResult{}, errors.New("agent_id is required")
	}
	if ok, err := s.verifyWorkspacePassword(ctx, workspaceID, input.WorkspacePassword); err != nil {
		return AgentRegistrationResult{}, err
	} else if !ok {
		return AgentRegistrationResult{}, errors.New("invalid workspace password")
	}

	existingAgent, err := s.GetAgent(ctx, workspaceID, agentID)
	hasExistingAgent := err == nil
	if err != nil && !errors.Is(err, ErrAgentNotFound) {
		return AgentRegistrationResult{}, err
	}

	// Resolve human-readable owner_user_id (username/display_name) into internal user UUID.
	resolvedOwnerUserID := strings.TrimSpace(input.OwnerUserID)
	if resolvedOwnerUserID == "" {
		if hasExistingAgent {
			resolvedOwnerUserID = existingAgent.OwnerUserID
		} else {
			resolvedOwnerUserID = "agent-self-register"
		}
	}
	if resolvedOwnerUserID != "" && resolvedOwnerUserID != "agent-self-register" {
		if profile, err := s.GetHumanProfileByUsername(ctx, workspaceID, resolvedOwnerUserID); err == nil {
			resolvedOwnerUserID = profile.UserID
		}
		// If resolution fails, keep the original value — it might already be a valid user_id.
	}

	displayName := strings.TrimSpace(input.DisplayName)
	if displayName == "" {
		if hasExistingAgent {
			displayName = existingAgent.DisplayName
		} else {
			displayName = agentID
		}
	}
	role := strings.TrimSpace(input.Role)
	if role == "" && hasExistingAgent {
		role = existingAgent.Role
	}
	protocolVersion := strings.TrimSpace(input.ProtocolVersion)
	if protocolVersion == "" && hasExistingAgent {
		protocolVersion = existingAgent.ProtocolVersion
	}
	capabilities := input.Capabilities
	if capabilities == nil && hasExistingAgent {
		capabilities = existingAgent.Capabilities
	}
	summary := strings.TrimSpace(input.Summary)
	if summary == "" && hasExistingAgent {
		summary = existingAgent.Summary
	}

	if err := s.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		OwnerUserID:     resolvedOwnerUserID,
		DisplayName:     displayName,
		Role:            role,
		ProtocolVersion: protocolVersion,
		Capabilities:    capabilities,
		Summary:         summary,
	}); err != nil {
		return AgentRegistrationResult{}, err
	}

	token, err := s.issueAccessToken(ctx, accessTokenSubject{
		WorkspaceID:    workspaceID,
		SubjectType:    "agent",
		SubjectID:      agentID,
		SubjectLabel:   displayName,
		CreatedBy:      resolvedOwnerUserID,
		IPAddress:      strings.TrimSpace(input.IPAddress),
		UserAgent:      strings.TrimSpace(input.UserAgent),
		RotateExisting: true,
	})
	if err != nil {
		return AgentRegistrationResult{}, err
	}
	if err := s.addSecurityEvent(ctx, SecurityEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "agent_registered",
		ActorType:   "agent",
		ActorID:     agentID,
		SubjectType: "agent",
		SubjectID:   agentID,
		IPAddress:   strings.TrimSpace(input.IPAddress),
		UserAgent:   strings.TrimSpace(input.UserAgent),
		DetailJSON: mustJSON(map[string]any{
			"display_name": displayName,
		}),
	}); err != nil {
		return AgentRegistrationResult{}, err
	}
	agent, err := s.GetAgent(ctx, workspaceID, agentID)
	if err != nil {
		return AgentRegistrationResult{}, err
	}
	return AgentRegistrationResult{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		DisplayName: agent.DisplayName,
		Token:       token,
		Agent:       agent,
	}, nil
}

func (s *Store) RegisterHuman(ctx context.Context, input HumanRegisterInput) (HumanAuthResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return HumanAuthResult{}, errors.New("workspace_id is required")
	}
	if ok, err := s.verifyWorkspacePassword(ctx, workspaceID, input.WorkspacePassword); err != nil {
		return HumanAuthResult{}, err
	} else if !ok {
		return HumanAuthResult{}, errors.New("invalid workspace password")
	}

	userID := nextID("user")
	username := firstNonEmpty(strings.TrimSpace(input.Username), strings.TrimSpace(input.DisplayName))
	displayName := strings.TrimSpace(input.DisplayName)
	if username == "" {
		username = displayName
	}
	if displayName == "" {
		displayName = username
	}
	if username == "" {
		return HumanAuthResult{}, errors.New("username is required")
	}
	if displayName == "" {
		return HumanAuthResult{}, errors.New("display_name is required")
	}
	if err := validateNewPassword("human password", input.Password); err != nil {
		return HumanAuthResult{}, err
	}
	passwordHash, err := hashPassword(strings.TrimSpace(input.Password))
	if err != nil {
		return HumanAuthResult{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	usernameKey := normalizeNameKey(username)
	displayNameKey := normalizeNameKey(displayName)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return HumanAuthResult{}, fmt.Errorf("begin human register tx: %w", err)
	}
	if err := s.ensureWorkspaceAuthSettingsTx(ctx, tx, workspaceID); err != nil {
		_ = tx.Rollback()
		return HumanAuthResult{}, err
	}
	if err := ensureHumanUsernameAvailableTx(ctx, tx, workspaceID, "", username); err != nil {
		_ = tx.Rollback()
		return HumanAuthResult{}, err
	}
	if err := ensureHumanDisplayNameAvailableTx(ctx, tx, workspaceID, "", displayName); err != nil {
		_ = tx.Rollback()
		return HumanAuthResult{}, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO workspace_humans(
			workspace_id, human_id, username, username_norm, display_name, display_name_norm, password_hash, status, created_at, updated_at, last_login_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?)`,
		workspaceID, userID, username, usernameKey, displayName, displayNameKey, passwordHash, now, now, now,
	); err != nil {
		_ = tx.Rollback()
		if isHumanUsernameConflictError(err) {
			return HumanAuthResult{}, ErrHumanUsernameConflict
		}
		if isHumanDisplayNameConflictError(err) {
			return HumanAuthResult{}, ErrHumanDisplayNameConflict
		}
		return HumanAuthResult{}, fmt.Errorf("insert human account: %w", err)
	}
	token, err := s.issueAccessTokenTx(ctx, tx, accessTokenSubject{
		WorkspaceID:    workspaceID,
		SubjectType:    "human",
		SubjectID:      userID,
		SubjectLabel:   displayName,
		CreatedBy:      userID,
		IPAddress:      strings.TrimSpace(input.IPAddress),
		UserAgent:      strings.TrimSpace(input.UserAgent),
		RotateExisting: false,
	})
	if err != nil {
		_ = tx.Rollback()
		return HumanAuthResult{}, err
	}
	if err := s.addSecurityEventTx(ctx, tx, SecurityEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "human_registered",
		ActorType:   "human",
		ActorID:     userID,
		SubjectType: "human",
		SubjectID:   userID,
		IPAddress:   strings.TrimSpace(input.IPAddress),
		UserAgent:   strings.TrimSpace(input.UserAgent),
		DetailJSON: mustJSON(map[string]any{
			"username":     username,
			"display_name": displayName,
		}),
	}); err != nil {
		_ = tx.Rollback()
		return HumanAuthResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return HumanAuthResult{}, fmt.Errorf("commit human register tx: %w", err)
	}
	return HumanAuthResult{
		WorkspaceID: workspaceID,
		UserID:      userID,
		Username:    username,
		DisplayName: displayName,
		Token:       token,
	}, nil
}

func (s *Store) LoginHuman(ctx context.Context, input HumanLoginInput) (HumanAuthResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return HumanAuthResult{}, errors.New("workspace_id is required")
	}
	username := strings.TrimSpace(input.Username)
	if username == "" {
		return HumanAuthResult{}, errors.New("username is required")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return HumanAuthResult{}, fmt.Errorf("begin human login tx: %w", err)
	}
	if err := s.ensureWorkspaceAuthSettingsTx(ctx, tx, workspaceID); err != nil {
		_ = tx.Rollback()
		return HumanAuthResult{}, err
	}

	var userID, storedUsername, storedName, passwordHash string
	if err := tx.QueryRowContext(ctx,
		`SELECT human_id, username, display_name, password_hash
		   FROM workspace_humans
		  WHERE workspace_id = ? AND username_norm = ?`,
		workspaceID, normalizeNameKey(username),
	).Scan(&userID, &storedUsername, &storedName, &passwordHash); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			// Dummy hash cycle to prevent user enumeration timing attacks
			_, _ = hashPassword(strings.TrimSpace(input.Password))
			return HumanAuthResult{}, errors.New("invalid human credentials")
		}
		return HumanAuthResult{}, fmt.Errorf("query human account: %w", err)
	}
	if !verifyPassword(strings.TrimSpace(input.Password), "", passwordHash) {
		_ = tx.Rollback()
		return HumanAuthResult{}, errors.New("invalid human credentials")
	}

	token, err := s.issueAccessTokenTx(ctx, tx, accessTokenSubject{
		WorkspaceID:    workspaceID,
		SubjectType:    "human",
		SubjectID:      userID,
		SubjectLabel:   storedName,
		CreatedBy:      userID,
		IPAddress:      strings.TrimSpace(input.IPAddress),
		UserAgent:      strings.TrimSpace(input.UserAgent),
		RotateExisting: false,
	})
	if err != nil {
		_ = tx.Rollback()
		return HumanAuthResult{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx,
		`UPDATE workspace_humans SET last_login_at = ?, updated_at = ? WHERE workspace_id = ? AND human_id = ?`,
		now, now, workspaceID, userID,
	); err != nil {
		_ = tx.Rollback()
		return HumanAuthResult{}, fmt.Errorf("update human last_login_at: %w", err)
	}
	if err := s.addSecurityEventTx(ctx, tx, SecurityEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "human_login",
		ActorType:   "human",
		ActorID:     userID,
		SubjectType: "human",
		SubjectID:   userID,
		IPAddress:   strings.TrimSpace(input.IPAddress),
		UserAgent:   strings.TrimSpace(input.UserAgent),
		DetailJSON: mustJSON(map[string]any{
			"username":     storedUsername,
			"display_name": storedName,
		}),
	}); err != nil {
		_ = tx.Rollback()
		return HumanAuthResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return HumanAuthResult{}, fmt.Errorf("commit human login tx: %w", err)
	}
	return HumanAuthResult{
		WorkspaceID: workspaceID,
		UserID:      userID,
		Username:    storedUsername,
		DisplayName: storedName,
		Token:       token,
	}, nil
}

func (s *Store) GetHumanProfile(ctx context.Context, workspaceID, userID string) (HumanProfileRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return HumanProfileRecord{}, errors.New("workspace_id is required")
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return HumanProfileRecord{}, errors.New("user_id is required")
	}

	var record HumanProfileRecord
	var lastLogin sql.NullString
	var tgUserID sql.NullInt64
	if err := s.db.QueryRowContext(ctx,
		`SELECT workspace_id, human_id, username, display_name, status, created_at, updated_at, last_login_at, telegram_user_id
		   FROM workspace_humans
		  WHERE workspace_id = ? AND human_id = ?`,
		workspaceID, userID,
	).Scan(
		&record.WorkspaceID,
		&record.UserID,
		&record.Username,
		&record.DisplayName,
		&record.Status,
		&record.CreatedAt,
		&record.UpdatedAt,
		&lastLogin,
		&tgUserID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return HumanProfileRecord{}, ErrHumanNotFound
		}
		return HumanProfileRecord{}, fmt.Errorf("query human profile: %w", err)
	}
	record.LastLoginAt = nullStringPtr(lastLogin)
	if tgUserID.Valid {
		v := tgUserID.Int64
		record.TelegramUserID = &v
	}

	agents, err := s.ListHumanOwnedAgents(ctx, workspaceID, userID)
	if err != nil {
		return HumanProfileRecord{}, err
	}
	record.Agents = agents
	record.AgentCount = len(agents)
	if record.Agents == nil {
		record.Agents = []AgentRecord{}
	}
	return record, nil
}

func (s *Store) GetHumanProfileByTelegramID(ctx context.Context, workspaceID string, telegramUserID int64) (HumanProfileRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return HumanProfileRecord{}, errors.New("workspace_id is required")
	}
	if telegramUserID == 0 {
		return HumanProfileRecord{}, errors.New("telegram_user_id is required")
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT human_id
		   FROM workspace_humans
		  WHERE workspace_id = ? AND telegram_user_id = ?
		  ORDER BY updated_at DESC, human_id DESC
		  LIMIT 2`,
		workspaceID, telegramUserID,
	)
	if err != nil {
		return HumanProfileRecord{}, fmt.Errorf("query human by telegram id: %w", err)
	}
	defer rows.Close()

	userIDs := make([]string, 0, 2)
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return HumanProfileRecord{}, fmt.Errorf("scan human by telegram id: %w", err)
		}
		userIDs = append(userIDs, userID)
	}
	if err := rows.Err(); err != nil {
		return HumanProfileRecord{}, fmt.Errorf("iterate humans by telegram id: %w", err)
	}
	switch len(userIDs) {
	case 0:
		return HumanProfileRecord{}, ErrHumanNotFound
	case 1:
		return s.GetHumanProfile(ctx, workspaceID, userIDs[0])
	default:
		return HumanProfileRecord{}, ErrHumanTelegramUserIDConflict
	}
}

func (s *Store) GetHumanProfileByUsername(ctx context.Context, workspaceID string, username string) (HumanProfileRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return HumanProfileRecord{}, errors.New("workspace_id is required")
	}
	username = strings.TrimSpace(username)
	if username == "" {
		return HumanProfileRecord{}, errors.New("username is required")
	}

	normKey := normalizeNameKey(username)
	var userID string
	if err := s.db.QueryRowContext(ctx,
		`SELECT human_id FROM workspace_humans WHERE workspace_id = ? AND (username_norm = ? OR display_name_norm = ?) LIMIT 1`,
		workspaceID, normKey, normKey,
	).Scan(&userID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return HumanProfileRecord{}, ErrHumanNotFound
		}
		return HumanProfileRecord{}, fmt.Errorf("query human by username: %w", err)
	}
	return s.GetHumanProfile(ctx, workspaceID, userID)
}

func (s *Store) ListHumanProfiles(ctx context.Context, workspaceID string) ([]HumanProfileRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT human_id, username, display_name FROM workspace_humans WHERE workspace_id = ? AND status = 'ACTIVE' ORDER BY username ASC`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("list human profiles: %w", err)
	}
	defer rows.Close()

	var out []HumanProfileRecord
	for rows.Next() {
		var rec HumanProfileRecord
		var dName sql.NullString
		if err := rows.Scan(&rec.UserID, &rec.Username, &dName); err != nil {
			return nil, err
		}
		rec.DisplayName = dName.String
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) UpdateHumanProfile(ctx context.Context, input HumanProfileUpdateInput) (HumanProfileRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return HumanProfileRecord{}, errors.New("workspace_id is required")
	}
	userID := strings.TrimSpace(input.UserID)
	if userID == "" {
		return HumanProfileRecord{}, errors.New("user_id is required")
	}

	current, err := s.GetHumanProfile(ctx, workspaceID, userID)
	if err != nil {
		return HumanProfileRecord{}, err
	}

	displayName := strings.TrimSpace(input.DisplayName)
	if displayName == "" {
		displayName = current.DisplayName
	}
	password := strings.TrimSpace(input.Password)
	displayNameChanged := displayName != current.DisplayName
	passwordChanged := password != ""

	var tgUserIDChanged bool
	var telegramUserIDValue int64
	if input.TelegramUserID != nil {
		telegramUserIDValue = *input.TelegramUserID
		if current.TelegramUserID == nil || *current.TelegramUserID != telegramUserIDValue {
			tgUserIDChanged = true
		}
	}

	if !displayNameChanged && !passwordChanged && !tgUserIDChanged {
		return s.GetHumanProfile(ctx, workspaceID, userID)
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return HumanProfileRecord{}, fmt.Errorf("begin human profile update tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := s.ensureWorkspaceAuthSettingsTx(ctx, tx, workspaceID); err != nil {
		return HumanProfileRecord{}, err
	}
	if displayNameChanged {
		var conflict string
		if err := tx.QueryRowContext(ctx,
			`SELECT human_id
			   FROM workspace_humans
			  WHERE workspace_id = ? AND display_name_norm = ? AND human_id <> ?
			  LIMIT 1`,
			workspaceID, normalizeNameKey(displayName), userID,
		).Scan(&conflict); err == nil {
			return HumanProfileRecord{}, ErrHumanDisplayNameConflict
		} else if !errors.Is(err, sql.ErrNoRows) {
			return HumanProfileRecord{}, fmt.Errorf("check human display name uniqueness: %w", err)
		}
	}
	if tgUserIDChanged && telegramUserIDValue != 0 {
		if err := ensureHumanTelegramUserIDAvailableTx(ctx, tx, workspaceID, userID, telegramUserIDValue); err != nil {
			return HumanProfileRecord{}, err
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	setParts := []string{"display_name = ?", "display_name_norm = ?", "updated_at = ?"}
	args := []any{displayName, normalizeNameKey(displayName), now}
	if passwordChanged {
		if err := validateNewPassword("human password", password); err != nil {
			return HumanProfileRecord{}, err
		}
		passwordHash, err := hashPassword(password)
		if err != nil {
			return HumanProfileRecord{}, err
		}
		setParts = append(setParts, "password_hash = ?")
		args = append(args, passwordHash)
	}
	if tgUserIDChanged {
		setParts = append(setParts, "telegram_user_id = ?")
		if *input.TelegramUserID == 0 {
			args = append(args, nil)
		} else {
			args = append(args, *input.TelegramUserID)
		}
	}
	args = append(args, workspaceID, userID)

	_, err = tx.ExecContext(ctx,
		`UPDATE workspace_humans
		    SET `+strings.Join(setParts, ", ")+`
		  WHERE workspace_id = ? AND human_id = ?`,
		args...,
	)
	if err != nil {
		if isHumanDisplayNameConflictError(err) {
			return HumanProfileRecord{}, ErrHumanDisplayNameConflict
		}
		if isHumanTelegramUserIDConflictError(err) {
			return HumanProfileRecord{}, ErrHumanTelegramUserIDConflict
		}
		return HumanProfileRecord{}, fmt.Errorf("update human profile: %w", err)
	}

	if displayNameChanged {
		if _, err := tx.ExecContext(ctx,
			`UPDATE workspace_auth_tokens
			    SET subject_label = ?
			  WHERE workspace_id = ? AND subject_type = 'human' AND subject_id = ? AND revoked_at IS NULL`,
			displayName, workspaceID, userID,
		); err != nil {
			return HumanProfileRecord{}, fmt.Errorf("sync human token labels: %w", err)
		}
	}

	if err := s.addSecurityEventTx(ctx, tx, SecurityEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "human_profile_updated",
		ActorType:   "human",
		ActorID:     userID,
		SubjectType: "human",
		SubjectID:   userID,
		IPAddress:   strings.TrimSpace(input.IPAddress),
		UserAgent:   strings.TrimSpace(input.UserAgent),
		DetailJSON: mustJSON(map[string]any{
			"username":         current.Username,
			"display_name":     displayName,
			"display_changed":  displayNameChanged,
			"password_changed": passwordChanged,
			"tg_changed":       tgUserIDChanged,
		}),
	}); err != nil {
		return HumanProfileRecord{}, err
	}

	if err := tx.Commit(); err != nil {
		return HumanProfileRecord{}, fmt.Errorf("commit human profile update tx: %w", err)
	}
	return s.GetHumanProfile(ctx, workspaceID, userID)
}

func (s *Store) ListHumanOwnedAgents(ctx context.Context, workspaceID, userID string) ([]AgentRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil, errors.New("user_id is required")
	}

	agents, err := s.listWorkspaceAgents(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	out := make([]AgentRecord, 0, len(agents))
	for _, agent := range agents {
		if agent.OwnerUserID == userID {
			out = append(out, agent)
		}
	}
	if out == nil {
		out = []AgentRecord{}
	}
	return out, nil
}

func (s *Store) ListHumanSessions(ctx context.Context, workspaceID, userID, currentTokenID string) ([]HumanSessionRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil, errors.New("user_id is required")
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT token_id, token_prefix, subject_id, subject_label, issued_by, issued_at, last_used_at
		   FROM workspace_auth_tokens
		  WHERE workspace_id = ? AND subject_type = 'human' AND subject_id = ? AND revoked_at IS NULL
		  ORDER BY issued_at DESC, token_id DESC`,
		workspaceID, userID,
	)
	if err != nil {
		return nil, fmt.Errorf("query human sessions: %w", err)
	}
	defer rows.Close()

	out := make([]HumanSessionRecord, 0, 8)
	for rows.Next() {
		var row HumanSessionRecord
		var lastUsed sql.NullString
		if err := rows.Scan(
			&row.TokenID,
			&row.TokenPrefix,
			&row.SubjectID,
			&row.SubjectLabel,
			&row.IssuedBy,
			&row.IssuedAt,
			&lastUsed,
		); err != nil {
			return nil, fmt.Errorf("scan human session: %w", err)
		}
		row.LastUsedAt = nullStringPtr(lastUsed)
		row.IsCurrent = strings.TrimSpace(currentTokenID) != "" && row.TokenID == strings.TrimSpace(currentTokenID)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate human sessions: %w", err)
	}
	if out == nil {
		out = []HumanSessionRecord{}
	}
	return out, nil
}

func (s *Store) RevokeHumanSessions(ctx context.Context, input HumanSessionRevokeInput) (int, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return 0, errors.New("workspace_id is required")
	}
	userID := strings.TrimSpace(input.UserID)
	if userID == "" {
		return 0, errors.New("user_id is required")
	}
	scope := firstNonEmpty(strings.TrimSpace(input.Scope), "token")

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin revoke human sessions tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	args := []any{now, firstNonEmpty(scope, "revoked"), workspaceID, userID}
	query := `UPDATE workspace_auth_tokens
	             SET revoked_at = ?, revoked_reason = ?
	           WHERE workspace_id = ? AND subject_type = 'human' AND subject_id = ? AND revoked_at IS NULL`
	switch scope {
	case "current":
		tokenID := strings.TrimSpace(input.CurrentTokenID)
		if tokenID == "" {
			return 0, errors.New("current token is required")
		}
		query += ` AND token_id = ?`
		args = append(args, tokenID)
	case "others":
		tokenID := strings.TrimSpace(input.CurrentTokenID)
		if tokenID == "" {
			return 0, errors.New("current token is required")
		}
		query += ` AND token_id <> ?`
		args = append(args, tokenID)
	case "token":
		tokenID := strings.TrimSpace(input.TokenID)
		if tokenID == "" {
			return 0, errors.New("token_id is required")
		}
		query += ` AND token_id = ?`
		args = append(args, tokenID)
	case "all":
		// workspace-scoped full logout for this user
	default:
		return 0, errors.New("scope must be one of: token, current, others, all")
	}

	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("revoke human sessions: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count revoked human sessions: %w", err)
	}
	if scope == "token" && affected == 0 {
		return 0, ErrAuthTokenNotFound
	}
	if affected > 0 {
		if err := s.addSecurityEventTx(ctx, tx, SecurityEventRecord{
			WorkspaceID: workspaceID,
			EventType:   "human_sessions_revoked",
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     firstNonEmpty(strings.TrimSpace(input.ActorID), userID),
			SubjectType: "human",
			SubjectID:   userID,
			IPAddress:   strings.TrimSpace(input.IPAddress),
			UserAgent:   strings.TrimSpace(input.UserAgent),
			DetailJSON: mustJSON(map[string]any{
				"scope":            scope,
				"token_id":         strings.TrimSpace(input.TokenID),
				"current_token_id": strings.TrimSpace(input.CurrentTokenID),
				"revoked_count":    affected,
			}),
		}); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit revoke human sessions tx: %w", err)
	}
	return int(affected), nil
}

func (s *Store) AuthenticateAccessToken(ctx context.Context, token string) (AuthPrincipalRecord, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return AuthPrincipalRecord{}, errors.New("token is required")
	}
	tokenHash := hashToken(token)
	var record AuthPrincipalRecord
	if err := s.db.QueryRowContext(ctx,
		`SELECT token_id, workspace_id, subject_type, subject_id, subject_label, token_prefix, COALESCE(metadata_json, '{}')
		   FROM workspace_auth_tokens
		  WHERE token_hash = ? AND revoked_at IS NULL`,
		tokenHash,
	).Scan(&record.TokenID, &record.WorkspaceID, &record.SubjectType, &record.SubjectID, &record.DisplayName, &record.TokenPrefix, &record.MetadataJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AuthPrincipalRecord{}, errors.New("invalid access token")
		}
		return AuthPrincipalRecord{}, fmt.Errorf("query access token: %w", err)
	}

	if record.DisplayName == "" {
		record.DisplayName = record.SubjectID
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, _ = s.writeDB.ExecContext(ctx,
		`UPDATE workspace_auth_tokens SET last_used_at = ? WHERE token_id = ? AND revoked_at IS NULL`,
		now,
		record.TokenID,
	)
	if record.SubjectType == "agent" && record.TokenID != "" {
		var latestTokenID string
		if err := s.db.QueryRowContext(ctx,
			`SELECT token_id
			   FROM workspace_auth_tokens
			  WHERE workspace_id = ? AND subject_type = 'agent' AND subject_id = ? AND revoked_at IS NULL
			  ORDER BY issued_at DESC, token_id DESC
			  LIMIT 1`,
			record.WorkspaceID,
			record.SubjectID,
		).Scan(&latestTokenID); err == nil && latestTokenID == record.TokenID {
			_, _ = s.writeDB.ExecContext(ctx,
				`UPDATE workspace_auth_tokens
				    SET revoked_at = ?, revoked_reason = ?
				  WHERE workspace_id = ? AND subject_type = 'agent' AND subject_id = ? AND token_id <> ? AND revoked_at IS NULL`,
				now, "superseded_by_new_agent_token", record.WorkspaceID, record.SubjectID, record.TokenID,
			)
		}
	}
	return record, nil
}

func (s *Store) ListSecurityEvents(ctx context.Context, workspaceID string, limit int) ([]SecurityEventRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT event_id, workspace_id, event_type, actor_type, actor_id, subject_type, subject_id, remote_ip, user_agent, detail_json, created_at
		   FROM workspace_security_events
		  WHERE workspace_id = ?
		  ORDER BY created_at DESC, event_id DESC
		  LIMIT ?`,
		workspaceID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query security events: %w", err)
	}
	defer rows.Close()

	out := make([]SecurityEventRecord, 0, limit)
	for rows.Next() {
		var row SecurityEventRecord
		var ipAddress, userAgent, detailJSON sql.NullString
		if err := rows.Scan(
			&row.EventID,
			&row.WorkspaceID,
			&row.EventType,
			&row.ActorType,
			&row.ActorID,
			&row.SubjectType,
			&row.SubjectID,
			&ipAddress,
			&userAgent,
			&detailJSON,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan security event: %w", err)
		}
		if ipAddress.Valid {
			row.IPAddress = ipAddress.String
		}
		if userAgent.Valid {
			row.UserAgent = userAgent.String
		}
		if detailJSON.Valid {
			row.DetailJSON = detailJSON.String
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate security events: %w", err)
	}
	return out, nil
}

func (s *Store) ensureWorkspaceSecuritySettings(ctx context.Context, workspaceID string) error {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin workspace security settings tx: %w", err)
	}
	if err := s.ensureWorkspaceAuthSettingsTx(ctx, tx, workspaceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit workspace security settings tx: %w", err)
	}
	return nil
}

func (s *Store) verifyWorkspacePassword(ctx context.Context, workspaceID, password string) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return false, errors.New("workspace_id is required")
	}
	if password == "" {
		return false, nil
	}
	if err := s.ensureWorkspaceSecuritySettings(ctx, workspaceID); err != nil {
		return false, err
	}
	var passwordHash string
	if err := s.db.QueryRowContext(ctx,
		`SELECT password_hash FROM workspace_security_settings WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&passwordHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("query workspace password hash: %w", err)
	}
	return verifyPassword(password, "", passwordHash), nil
}

type accessTokenSubject struct {
	WorkspaceID    string
	SubjectType    string
	SubjectID      string
	SubjectLabel   string
	CreatedBy      string
	IPAddress      string
	UserAgent      string
	RotateExisting bool
	Metadata       map[string]any
}

func (s *Store) issueAccessToken(ctx context.Context, input accessTokenSubject) (string, error) {
	return s.issueAccessTokenTx(ctx, nil, input)
}

func (s *Store) issueAccessTokenTx(ctx context.Context, tx *sql.Tx, input accessTokenSubject) (string, error) {
	token, err := newPlainToken()
	if err != nil {
		return "", fmt.Errorf("generate access token: %w", err)
	}
	tokenHash := hashToken(token)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	exec := func(query string, args ...any) error {
		if tx != nil {
			_, err = tx.ExecContext(ctx, query, args...)
		} else {
			_, err = s.writeDB.ExecContext(ctx, query, args...)
		}
		return err
	}
	if input.RotateExisting {
		if err := exec(
			`UPDATE workspace_auth_tokens SET revoked_at = ?, revoked_reason = ? WHERE workspace_id = ? AND subject_type = ? AND subject_id = ? AND revoked_at IS NULL`,
			now, "rotated", input.WorkspaceID, input.SubjectType, input.SubjectID,
		); err != nil {
			return "", fmt.Errorf("revoke prior access tokens: %w", err)
		}
	}
	metadata := map[string]any{
		"issued_ip":  strings.TrimSpace(input.IPAddress),
		"user_agent": strings.TrimSpace(input.UserAgent),
	}
	for key, value := range input.Metadata {
		metadata[key] = value
	}
	if err := exec(
		`INSERT INTO workspace_auth_tokens(
			token_id, workspace_id, subject_type, subject_id, subject_label, token_hash, token_prefix, issued_by, issued_at, last_used_at, revoked_at, revoked_reason, metadata_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, '', ?)`,
		nextID("token"),
		input.WorkspaceID,
		input.SubjectType,
		input.SubjectID,
		firstNonEmpty(strings.TrimSpace(input.SubjectLabel), input.SubjectID),
		tokenHash,
		token[:8],
		firstNonEmpty(strings.TrimSpace(input.CreatedBy), input.SubjectID),
		now,
		now,
		mustJSON(metadata),
	); err != nil {
		return "", fmt.Errorf("insert access token: %w", err)
	}
	return token, nil
}

func (s *Store) ListAccessTokens(ctx context.Context, workspaceID, subjectType, subjectID string, includeRevoked bool, limit int) ([]AuthTokenRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	subjectType = strings.TrimSpace(subjectType)
	if subjectType == "" {
		return nil, errors.New("subject_type is required")
	}
	subjectID = strings.TrimSpace(subjectID)
	if subjectID == "" {
		return nil, errors.New("subject_id is required")
	}
	if limit <= 0 {
		limit = 50
	}

	query := `SELECT token_id, workspace_id, subject_type, subject_id, subject_label, token_prefix, issued_by, issued_at, last_used_at, revoked_at, revoked_reason, metadata_json
	            FROM workspace_auth_tokens
	           WHERE workspace_id = ? AND subject_type = ? AND subject_id = ?`
	args := []any{workspaceID, subjectType, subjectID}
	if !includeRevoked {
		query += ` AND revoked_at IS NULL`
	}
	query += ` ORDER BY issued_at DESC, token_id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list access tokens: %w", err)
	}
	defer rows.Close()

	out := make([]AuthTokenRecord, 0, limit)
	for rows.Next() {
		var record AuthTokenRecord
		var lastUsedAt sql.NullString
		var revokedAt sql.NullString
		if err := rows.Scan(
			&record.TokenID,
			&record.WorkspaceID,
			&record.SubjectType,
			&record.SubjectID,
			&record.SubjectLabel,
			&record.TokenPrefix,
			&record.IssuedBy,
			&record.IssuedAt,
			&lastUsedAt,
			&revokedAt,
			&record.RevokedReason,
			&record.MetadataJSON,
		); err != nil {
			return nil, fmt.Errorf("scan access token: %w", err)
		}
		record.LastUsedAt = nullStringPtr(lastUsedAt)
		record.RevokedAt = nullStringPtr(revokedAt)
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate access tokens: %w", err)
	}
	if out == nil {
		out = []AuthTokenRecord{}
	}
	return out, nil
}

func (s *Store) GetLatestAccessToken(ctx context.Context, workspaceID, subjectType, subjectID string) (AuthTokenRecord, error) {
	tokens, err := s.ListAccessTokens(ctx, workspaceID, subjectType, subjectID, false, 1)
	if err != nil {
		return AuthTokenRecord{}, err
	}
	if len(tokens) == 0 {
		return AuthTokenRecord{}, ErrAuthTokenNotFound
	}
	return tokens[0], nil
}

func (s *Store) RevokeAccessToken(ctx context.Context, workspaceID, subjectType, subjectID, tokenID, reason, actorType, actorID, ipAddress, userAgent string) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return false, errors.New("workspace_id is required")
	}
	subjectType = strings.TrimSpace(subjectType)
	if subjectType == "" {
		return false, errors.New("subject_type is required")
	}
	subjectID = strings.TrimSpace(subjectID)
	if subjectID == "" {
		return false, errors.New("subject_id is required")
	}
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return false, errors.New("token_id is required")
	}
	reason = firstNonEmpty(strings.TrimSpace(reason), "revoked")
	now := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return false, fmt.Errorf("begin revoke access token tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(ctx,
		`UPDATE workspace_auth_tokens
		    SET revoked_at = ?, revoked_reason = ?
		  WHERE workspace_id = ? AND subject_type = ? AND subject_id = ? AND token_id = ? AND revoked_at IS NULL`,
		now, reason, workspaceID, subjectType, subjectID, tokenID,
	)
	if err != nil {
		return false, fmt.Errorf("revoke access token: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect revoked access token rows: %w", err)
	}
	if rowsAffected == 0 {
		return false, nil
	}
	if err := s.addSecurityEventTx(ctx, tx, SecurityEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "auth_token_revoked",
		ActorType:   firstNonEmpty(strings.TrimSpace(actorType), subjectType),
		ActorID:     firstNonEmpty(strings.TrimSpace(actorID), subjectID),
		SubjectType: subjectType,
		SubjectID:   subjectID,
		IPAddress:   strings.TrimSpace(ipAddress),
		UserAgent:   strings.TrimSpace(userAgent),
		DetailJSON: mustJSON(map[string]any{
			"token_id": tokenID,
			"reason":   reason,
		}),
	}); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit revoke access token tx: %w", err)
	}
	return true, nil
}

func (s *Store) RevokeOtherAccessTokens(ctx context.Context, workspaceID, subjectType, subjectID, exceptTokenID, reason, actorType, actorID, ipAddress, userAgent string) (int, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return 0, errors.New("workspace_id is required")
	}
	subjectType = strings.TrimSpace(subjectType)
	if subjectType == "" {
		return 0, errors.New("subject_type is required")
	}
	subjectID = strings.TrimSpace(subjectID)
	if subjectID == "" {
		return 0, errors.New("subject_id is required")
	}
	reason = firstNonEmpty(strings.TrimSpace(reason), "revoked_other_sessions")
	now := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin revoke other access tokens tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(ctx,
		`UPDATE workspace_auth_tokens
		    SET revoked_at = ?, revoked_reason = ?
		  WHERE workspace_id = ? AND subject_type = ? AND subject_id = ? AND revoked_at IS NULL AND token_id <> ?`,
		now, reason, workspaceID, subjectType, subjectID, strings.TrimSpace(exceptTokenID),
	)
	if err != nil {
		return 0, fmt.Errorf("revoke other access tokens: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect revoked other access tokens rows: %w", err)
	}
	if rowsAffected == 0 {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("commit revoke other access tokens tx: %w", err)
		}
		return 0, nil
	}
	if err := s.addSecurityEventTx(ctx, tx, SecurityEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "auth_tokens_revoked",
		ActorType:   firstNonEmpty(strings.TrimSpace(actorType), subjectType),
		ActorID:     firstNonEmpty(strings.TrimSpace(actorID), subjectID),
		SubjectType: subjectType,
		SubjectID:   subjectID,
		IPAddress:   strings.TrimSpace(ipAddress),
		UserAgent:   strings.TrimSpace(userAgent),
		DetailJSON: mustJSON(map[string]any{
			"except_token_id": strings.TrimSpace(exceptTokenID),
			"reason":          reason,
			"revoked_count":   rowsAffected,
		}),
	}); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit revoke other access tokens tx: %w", err)
	}
	return int(rowsAffected), nil
}

func agentTokenRotationMessage(workspaceID, agentID, token string) string {
	return strings.TrimSpace(fmt.Sprintf(
		"Agent token was rotated for %s in workspace %s.\n\nNew token:\n%s\n\nThis notice is private to the agent inbox. The previous token remains usable until this new token is used successfully once, after which older agent tokens are revoked automatically.",
		strings.TrimSpace(agentID),
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(token),
	))
}

func (s *Store) RotateAgentAccessToken(ctx context.Context, input AgentTokenRotateInput) (AgentTokenRotateResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return AgentTokenRotateResult{}, errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return AgentTokenRotateResult{}, errors.New("agent_id is required")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return AgentTokenRotateResult{}, fmt.Errorf("begin rotate agent access token tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var displayName string
	if err := tx.QueryRowContext(ctx,
		`SELECT display_name FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID, agentID,
	).Scan(&displayName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AgentTokenRotateResult{}, ErrAgentNotFound
		}
		return AgentTokenRotateResult{}, fmt.Errorf("query agent for token rotate: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInputTx(ctx, tx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		_ = tx.Rollback()
		return AgentTokenRotateResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	token := ""
	messageID := ""
	messageEvent := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		token, innerErr = s.issueAccessTokenTx(ctx, tx, accessTokenSubject{
			WorkspaceID:  workspaceID,
			SubjectType:  "agent",
			SubjectID:    agentID,
			SubjectLabel: displayName,
			CreatedBy:    firstNonEmpty(strings.TrimSpace(input.ActorID), agentID),
			IPAddress:    strings.TrimSpace(input.IPAddress),
			UserAgent:    strings.TrimSpace(input.UserAgent),
			Metadata: map[string]any{
				"rotation_mode": "grace_until_first_use",
			},
		})
		if innerErr != nil {
			return innerErr
		}

		messageID = nextID("msg")
		if _, innerErr = tx.ExecContext(ctx,
			`INSERT INTO agent_messages (message_id, workspace_id, from_agent_id, to_agent_id, channel, content_type, content, metadata_json, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			messageID,
			workspaceID,
			systemAgentMessageSenderID,
			agentID,
			systemAgentTokenMessageChannel,
			systemAgentTokenMessageContentType,
			agentTokenRotationMessage(workspaceID, agentID, token),
			mustJSON(map[string]any{
				"kind":         "agent_token_rotated",
				"agent_id":     agentID,
				"token_prefix": token[:8],
			}),
			now,
		); innerErr != nil {
			return fmt.Errorf("insert agent rotation message: %w", innerErr)
		}
		runtimeContext, innerErr := s.inferMessagingRuntimeContextTx(ctx, tx, workspaceID, systemAgentMessageSenderID, agentID)
		if innerErr != nil {
			return innerErr
		}
		messageEvent, innerErr = s.appendMessagingRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: workspaceID,
			EventType:   "agent_message.sent",
			EntityType:  "agent_message",
			EntityID:    messageID,
			ActorType:   "agent",
			ActorID:     systemAgentMessageSenderID,
			AgentID:     systemAgentMessageSenderID,
			SessionID:   runtimeContext.SessionID,
			TaskID:      runtimeContext.TaskID,
			PayloadJSON: mustJSON(messagingRuntimeEventPayload(workspaceID, messageID, systemAgentMessageSenderID, agentID, systemAgentTokenMessageChannel, systemAgentTokenMessageContentType, "SENT", runtimeContext)),
			CreatedAt:   now,
		})
		if innerErr != nil {
			return innerErr
		}

		return s.addSecurityEventTx(ctx, tx, SecurityEventRecord{
			WorkspaceID: workspaceID,
			EventType:   "agent_token_rotated",
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     firstNonEmpty(strings.TrimSpace(input.ActorID), "dashboard"),
			SubjectType: "agent",
			SubjectID:   agentID,
			IPAddress:   strings.TrimSpace(input.IPAddress),
			UserAgent:   strings.TrimSpace(input.UserAgent),
			DetailJSON: mustJSON(map[string]any{
				"display_name": displayName,
				"message_id":   messageID,
				"token_prefix": token[:8],
			}),
		})
	}); err != nil {
		_ = tx.Rollback()
		return AgentTokenRotateResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return AgentTokenRotateResult{}, fmt.Errorf("commit rotate agent access token tx: %w", err)
	}
	return AgentTokenRotateResult{
		WorkspaceID:  workspaceID,
		AgentID:      agentID,
		DisplayName:  displayName,
		Token:        token,
		MessageID:    messageID,
		MessageEvent: messageEvent,
	}, nil
}

func (s *Store) addSecurityEvent(ctx context.Context, input SecurityEventRecord) error {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin security event tx: %w", err)
	}
	if err := s.addSecurityEventTx(ctx, tx, input); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit security event tx: %w", err)
	}
	return nil
}

func (s *Store) addSecurityEventTx(ctx context.Context, tx *sql.Tx, input SecurityEventRecord) error {
	if strings.TrimSpace(input.WorkspaceID) == "" {
		return errors.New("workspace_id is required")
	}
	if strings.TrimSpace(input.EventType) == "" {
		return errors.New("event_type is required")
	}
	if strings.TrimSpace(input.ActorType) == "" {
		return errors.New("actor_type is required")
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return errors.New("actor_id is required")
	}
	if strings.TrimSpace(input.SubjectType) == "" {
		return errors.New("subject_type is required")
	}
	if strings.TrimSpace(input.SubjectID) == "" {
		return errors.New("subject_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := tx.ExecContext(ctx,
		`INSERT INTO workspace_security_events(
			event_id, workspace_id, event_type, actor_type, actor_id, actor_label, subject_type, subject_id, subject_label, remote_ip, user_agent, detail_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		firstNonEmpty(strings.TrimSpace(input.EventID), nextID("security")),
		strings.TrimSpace(input.WorkspaceID),
		strings.TrimSpace(input.EventType),
		strings.TrimSpace(input.ActorType),
		strings.TrimSpace(input.ActorID),
		"",
		strings.TrimSpace(input.SubjectType),
		strings.TrimSpace(input.SubjectID),
		"",
		strings.TrimSpace(input.IPAddress),
		strings.TrimSpace(input.UserAgent),
		firstNonEmpty(strings.TrimSpace(input.DetailJSON), "{}"),
		now,
	)
	if err != nil {
		return fmt.Errorf("insert security event: %w", err)
	}
	return nil
}

func hashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	derived, err := pbkdf2.Key(sha256.New, strings.TrimSpace(password), salt, passwordHashIterations, passwordHashKeyLen)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pbkdf2-sha256:%d:%s:%s", passwordHashIterations, hex.EncodeToString(salt), hex.EncodeToString(derived)), nil
}

func validateNewPassword(label, password string) error {
	password = strings.TrimSpace(password)
	if password == "" {
		return fmt.Errorf("%s is required", label)
	}
	if utf8.RuneCountInString(password) < minimumPasswordCharacters {
		return fmt.Errorf("invalid %s: must be at least %d characters", label, minimumPasswordCharacters)
	}
	if len(password) > maximumPasswordBytes {
		return fmt.Errorf("invalid %s: must be at most %d bytes", label, maximumPasswordBytes)
	}
	return nil
}

func verifyPassword(secret, _ string, stored string) bool {
	parts := strings.Split(strings.TrimSpace(stored), ":")
	switch {
	case len(parts) == 4 && parts[0] == "pbkdf2-sha256":
		iterations, err := strconv.Atoi(parts[1])
		if err != nil || iterations <= 0 {
			return false
		}
		salt, err := hex.DecodeString(parts[2])
		if err != nil {
			return false
		}
		expected, err := hex.DecodeString(parts[3])
		if err != nil {
			return false
		}
		derived, err := pbkdf2.Key(sha256.New, strings.TrimSpace(secret), salt, iterations, len(expected))
		if err != nil {
			return false
		}
		return subtle.ConstantTimeCompare(derived, expected) == 1
	case len(parts) == 3 && parts[0] == "sha256":
		salt, err := hex.DecodeString(parts[1])
		if err != nil {
			return false
		}
		expected, err := hex.DecodeString(parts[2])
		if err != nil {
			return false
		}
		sum := sha256.Sum256(append(salt, []byte(strings.TrimSpace(secret))...))
		return subtle.ConstantTimeCompare(sum[:], expected) == 1
	default:
		return false
	}
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return hex.EncodeToString(sum[:])
}

func newPlainToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "rhz_" + base64.RawURLEncoding.EncodeToString(buf), nil
}

func normalizeNameKey(name string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(name))), " ")
}

func ensureHumanUsernameAvailableTx(ctx context.Context, tx *sql.Tx, workspaceID, humanID, username string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	username = strings.TrimSpace(username)
	if username == "" {
		return errors.New("username is required")
	}

	query := `SELECT human_id
	            FROM workspace_humans
	           WHERE workspace_id = ? AND username_norm = ?`
	args := []any{workspaceID, normalizeNameKey(username)}
	if strings.TrimSpace(humanID) != "" {
		query += ` AND human_id <> ?`
		args = append(args, strings.TrimSpace(humanID))
	}
	query += ` LIMIT 1`

	var conflict string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&conflict); err == nil {
		return ErrHumanUsernameConflict
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check human username availability: %w", err)
	}
	return nil
}

func ensureHumanDisplayNameAvailableTx(ctx context.Context, tx *sql.Tx, workspaceID, humanID, displayName string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	displayName = strings.TrimSpace(displayName)
	if displayName == "" {
		return errors.New("display_name is required")
	}

	query := `SELECT human_id
	            FROM workspace_humans
	           WHERE workspace_id = ? AND display_name_norm = ?`
	args := []any{workspaceID, normalizeNameKey(displayName)}
	if strings.TrimSpace(humanID) != "" {
		query += ` AND human_id <> ?`
		args = append(args, strings.TrimSpace(humanID))
	}
	query += ` LIMIT 1`

	var conflict string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&conflict); err == nil {
		return ErrHumanDisplayNameConflict
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check human display name availability: %w", err)
	}
	return nil
}

func ensureHumanTelegramUserIDAvailableTx(ctx context.Context, tx *sql.Tx, workspaceID, humanID string, telegramUserID int64) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if telegramUserID == 0 {
		return nil
	}

	query := `SELECT human_id
	            FROM workspace_humans
	           WHERE workspace_id = ? AND telegram_user_id = ?`
	args := []any{workspaceID, telegramUserID}
	if strings.TrimSpace(humanID) != "" {
		query += ` AND human_id <> ?`
		args = append(args, strings.TrimSpace(humanID))
	}
	query += ` LIMIT 1`

	var conflict string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&conflict); err == nil {
		return ErrHumanTelegramUserIDConflict
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check human telegram user id uniqueness: %w", err)
	}
	return nil
}

func isHumanDisplayNameConflictError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique constraint failed") &&
		strings.Contains(message, "workspace_humans") &&
		strings.Contains(message, "display_name_norm")
}

func isHumanUsernameConflictError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique constraint failed") &&
		strings.Contains(message, "workspace_humans") &&
		strings.Contains(message, "username_norm")
}

func isHumanTelegramUserIDConflictError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique constraint failed") &&
		strings.Contains(message, "workspace_humans") &&
		strings.Contains(message, "telegram_user_id")
}
