package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/bridgepolicy"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

var (
	ErrWorkspaceNotFound         = errors.New("workspace not found")
	ErrWorkspaceRefAmbiguous     = errors.New("workspace reference is ambiguous")
	ErrAgentNotFound             = errors.New("agent not found")
	ErrToolNotFound              = errors.New("tool not found")
	ErrTaskClaimConflict         = errors.New("task claim conflict")
	ErrTaskClaimStaleTransition  = errors.New("task claim transition is stale or duplicate")
	ErrTaskClaimNotFound         = errors.New("task claim not found")
	ErrTaskClaimAdmissionInvalid = errors.New("task claim project admission invalid")
	ErrTaskCompletionContract    = errors.New("task completion contract unsatisfied")
	ErrWorkspaceTaskAbsent       = errors.New("task is not attached to workspace")
	ErrTaskWorkspaceAmbiguous    = errors.New("task is attached to multiple workspaces")
	ErrTaskProjectNotFound       = errors.New("task project not found")
	ErrDocConflict               = errors.New("document conflict: content has been modified")
)

const (
	CoordinationModeStrict     = "strict"
	CoordinationModeTrustFirst = "trust_first"
)

const RemovedLegacyProviderToolID = "antigravity-provider"

func IsRemovedWorkspaceToolID(toolID string) bool {
	return strings.EqualFold(strings.TrimSpace(toolID), RemovedLegacyProviderToolID)
}

func normalizeCoordinationMode(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = strings.ReplaceAll(normalized, " ", "_")
	switch normalized {
	case "", CoordinationModeStrict:
		return CoordinationModeStrict
	case CoordinationModeTrustFirst, "trustfirst", "autonomy", "autonomous":
		return CoordinationModeTrustFirst
	default:
		return normalized
	}
}

func coordinationModeTrustFirst(value string) bool {
	return normalizeCoordinationMode(value) == CoordinationModeTrustFirst
}

type WorkspaceCreateInput struct {
	WorkspaceID       string
	Title             string
	Description       string
	CreatedBy         string
	Status            string
	WorkspacePassword string
}

type WorkspaceRecord struct {
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	CreatedBy   string `json:"created_by"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type WorkspaceDocInput struct {
	WorkspaceID           string
	DocKey                string
	Title                 string
	Content               string
	UpdatedBy             string
	ExpectedSHA           string // optional: if set, reject write if current content SHA doesn't match
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type WorkspaceDocRecord struct {
	DocKey     string  `json:"doc_key"`
	Title      string  `json:"title"`
	Content    string  `json:"content"`
	UpdatedBy  string  `json:"updated_by"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  string  `json:"updated_at"`
	ArchivedAt *string `json:"archived_at,omitempty"`
	ArchivedBy *string `json:"archived_by,omitempty"`
	SHA        string  `json:"sha"`
}

type WorkspaceDocRevisionRecord struct {
	RevisionID  string `json:"revision_id"`
	WorkspaceID string `json:"workspace_id"`
	DocKey      string `json:"doc_key"`
	Title       string `json:"title"`
	Content     string `json:"content"`
	UpdatedBy   string `json:"updated_by"`
	CreatedAt   string `json:"created_at"`
}

type AgentRegisterInput struct {
	WorkspaceID     string
	AgentID         string
	OwnerUserID     string
	DisplayName     string
	Role            string
	Status          string
	ProtocolVersion string
	Capabilities    []string
	Summary         string
}

type AgentRegisterPatchInput struct {
	WorkspaceID     string
	AgentID         string
	OwnerUserID     *string
	DisplayName     *string
	Role            *string
	Status          *string
	ProtocolVersion *string
	Capabilities    *[]string
	Summary         *string
}

type AgentDeleteInput struct {
	WorkspaceID string
	AgentID     string
	ActorID     string
	ActorType   string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type AgentMetadataPatchInput struct {
	WorkspaceID     string
	AgentID         string
	DisplayName     *string
	Role            *string
	ProtocolVersion *string
	Capabilities    *[]string
	UpdatedBy       string
}

type AgentHeartbeatInput struct {
	WorkspaceID string
	AgentID     string
	Status      string
	Summary     string
}

type AgentCurrentTask struct {
	TaskID      string `json:"task_id"`
	ClaimStatus string `json:"claim_status"`
	Summary     string `json:"summary"`
}

type AgentRecord struct {
	AgentID         string                   `json:"agent_id"`
	WorkspaceID     string                   `json:"workspace_id"`
	OwnerUserID     string                   `json:"owner_user_id"`
	DisplayName     string                   `json:"display_name"`
	Role            string                   `json:"role"`
	Status          string                   `json:"status"`
	ProtocolVersion string                   `json:"protocol_version"`
	Capabilities    []string                 `json:"capabilities"`
	Summary         string                   `json:"summary"`
	CreatedAt       string                   `json:"created_at"`
	UpdatedAt       string                   `json:"updated_at"`
	LastSeenAt      *string                  `json:"last_seen_at,omitempty"`
	IsOnline        bool                     `json:"is_online"`
	ActiveTasks     []AgentCurrentTask       `json:"active_tasks"`
	CurrentSession  *AgentSessionStateRecord `json:"current_session,omitempty"`
}

type WorkspaceToolInput struct {
	WorkspaceID                        string
	ToolID                             string
	DisplayName                        string
	Description                        string
	OwnerUserID                        string
	OwnerAgentID                       string
	Kind                               string
	Status                             string
	Version                            string
	AccessLevel                        string
	Endpoint                           string
	Capabilities                       []string
	ManifestJSON                       string
	PromptContextEnvelope              map[string]any
	PromptContextSurface               string
	PromptContextPrincipalType         string
	PromptContextPrincipalID           string
	PromptContextProjectionSource      string
	PromptContextProjectionOperationID string
}

type WorkspaceToolRemoveInput struct {
	WorkspaceID                        string
	ToolID                             string
	RemovedBy                          string
	PromptContextEnvelope              map[string]any
	PromptContextSurface               string
	PromptContextPrincipalType         string
	PromptContextPrincipalID           string
	PromptContextProjectionSource      string
	PromptContextProjectionOperationID string
}

type WorkspaceToolFilter struct {
	WorkspaceID string
	Status      string
}

type WorkspaceToolRecord struct {
	ToolID         string                       `json:"tool_id"`
	WorkspaceID    string                       `json:"workspace_id"`
	DisplayName    string                       `json:"display_name"`
	Description    string                       `json:"description"`
	OwnerUserID    string                       `json:"owner_user_id"`
	OwnerAgentID   string                       `json:"owner_agent_id,omitempty"`
	Kind           string                       `json:"kind"`
	Status         string                       `json:"status"`
	Version        string                       `json:"version"`
	AccessLevel    string                       `json:"access_level"`
	Endpoint       string                       `json:"endpoint,omitempty"`
	Capabilities   []string                     `json:"capabilities,omitempty"`
	ManifestJSON   string                       `json:"manifest_json,omitempty"`
	PolicyEnvelope *bridgepolicy.PolicyEnvelope `json:"policy_envelope,omitempty"`
	CreatedAt      string                       `json:"created_at"`
	UpdatedAt      string                       `json:"updated_at"`
}

type TaskAttachmentInput struct {
	WorkspaceID string
	TaskID      string
	LinkedBy    string
}

type WorkspaceTaskLinkInput struct {
	WorkspaceID string
	FromTaskID  string
	ToTaskID    string
	LinkType    string
	CreatedBy   string
}

type WorkspaceTaskDependencyLinksInput struct {
	WorkspaceID       string
	TaskID            string
	DependencyTaskIDs []string
	CreatedBy         string
}

type WorkspaceTaskRelatedLinksInput struct {
	WorkspaceID    string
	TaskID         string
	RelatedTaskIDs []string
	CreatedBy      string
}

type WorkspaceTaskLinkFilter struct {
	WorkspaceID string
	TaskID      string
	LinkType    string
	Limit       int
}

type WorkspaceTaskLinkRecord struct {
	WorkspaceID string `json:"workspace_id"`
	FromTaskID  string `json:"from_task_id"`
	ToTaskID    string `json:"to_task_id"`
	LinkType    string `json:"link_type"`
	CreatedBy   string `json:"created_by"`
	CreatedAt   string `json:"created_at"`
}

type TaskClaimInput struct {
	WorkspaceID           string
	TaskID                string
	AgentID               string
	ProjectRoleID         string
	RepoID                string
	CheckoutID            string
	BranchID              string
	WriteScopeJSON        string
	CoordinationMode      string
	Summary               string
	SelectedFromFrontier  bool
	FrontierGenerationID  string
	SelfFitSummary        string
	PromptContextEnvelope map[string]any
}

type TaskReleaseInput struct {
	WorkspaceID           string
	TaskID                string
	AgentID               string
	Reason                string
	SessionTransitionKind string
	PromptContextEnvelope map[string]any
}

type TaskCompleteInput struct {
	WorkspaceID           string
	TaskID                string
	AgentID               string
	Summary               string
	PromptContextEnvelope map[string]any
}

type TaskBlockInput struct {
	WorkspaceID           string
	TaskID                string
	AgentID               string
	Reason                string
	PromptContextEnvelope map[string]any
}

type taskClaimTransitionSnapshot struct {
	AgentID        string
	ClaimStatus    string
	Summary        string
	ClaimedAt      string
	ReleasedAt     sql.NullString
	ProjectRoleID  string
	RepoID         string
	CheckoutID     string
	BranchID       string
	WriteScopeJSON string
	UpdatedAt      string
}

type taskTransitionSnapshot struct {
	Status    string
	ProjectID string
	UpdatedAt string
}

type taskSessionTransitionSnapshot struct {
	SessionID string
	AgentID   string
	Status    string
	StartedAt string
}

type NodeClaimInput struct {
	WorkspaceID           string
	TaskID                string
	NodeID                string
	AgentID               string
	Summary               string
	PromptContextEnvelope map[string]any
}

type NodeReleaseInput struct {
	WorkspaceID           string
	TaskID                string
	NodeID                string
	AgentID               string
	Reason                string
	PromptContextEnvelope map[string]any
}

type NodeCompleteInput struct {
	WorkspaceID           string
	TaskID                string
	NodeID                string
	AgentID               string
	Summary               string
	PromptContextEnvelope map[string]any
}

type WorkspaceTaskRecord struct {
	TaskID               string   `json:"task_id"`
	Title                string   `json:"title,omitempty"`
	Description          string   `json:"description,omitempty"`
	OwnerUserID          string   `json:"owner_user_id"`
	Priority             string   `json:"priority"`
	Status               string   `json:"status"`
	TaskKind             string   `json:"task_kind"`
	TaskTemplate         string   `json:"task_template"`
	TaskClass            string   `json:"task_class,omitempty"`
	TaskClassSource      string   `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt   string   `json:"task_class_updated_at,omitempty"`
	Tags                 []string `json:"tags,omitempty"`
	ProjectID            string   `json:"project_id,omitempty"`
	ProjectLane          string   `json:"project_lane,omitempty"`
	RequiresProjectGate  bool     `json:"requires_project_gate,omitempty"`
	TaskRequirementsJSON string   `json:"task_requirements_json,omitempty"`
	WriteScopeHints      []string `json:"write_scope_hints,omitempty"`
	CloseReason          string   `json:"close_reason,omitempty"`
	LinkedBy             string   `json:"linked_by"`
	LinkedAt             string   `json:"linked_at"`
	UpdatedAt            string   `json:"updated_at,omitempty"`
	ClaimAgentID         *string  `json:"claim_agent_id,omitempty"`
	ClaimStatus          *string  `json:"claim_status,omitempty"`
	ClaimSummary         *string  `json:"claim_summary,omitempty"`
	ClaimUpdatedAt       *string  `json:"claim_updated_at,omitempty"`
	ClaimProjectRoleID   *string  `json:"claim_project_role_id,omitempty"`
	ClaimRepoID          *string  `json:"claim_repo_id,omitempty"`
	ClaimCheckoutID      *string  `json:"claim_checkout_id,omitempty"`
	ClaimBranchID        *string  `json:"claim_branch_id,omitempty"`
	ClaimWriteScopeJSON  *string  `json:"claim_write_scope_json,omitempty"`
}

type AgentUpdateInput struct {
	UpdateID              string
	WorkspaceID           string
	AgentID               string
	UpdateType            string
	Summary               string
	PayloadJSON           string
	RequiresHuman         bool
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type AgentUpdateRecord struct {
	UpdateID      string `json:"update_id"`
	AgentID       string `json:"agent_id"`
	AgentName     string `json:"agent_name"`
	UpdateType    string `json:"update_type"`
	Summary       string `json:"summary"`
	PayloadJSON   string `json:"payload_json,omitempty"`
	RequiresHuman bool   `json:"requires_human"`
	CreatedAt     string `json:"created_at"`
}

type WorkspaceArtifactInput struct {
	ArtifactID            string
	WorkspaceID           string
	TaskID                string
	UpdateID              string
	Title                 string
	ArtifactRef           string
	Kind                  string
	ContentType           string
	CreatedBy             string
	MetadataJSON          string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type WorkspaceArtifactFilter struct {
	WorkspaceID string
	TaskID      string
	UpdateID    string
	Limit       int
}

type WorkspaceArtifactRecord struct {
	ArtifactID   string  `json:"artifact_id"`
	WorkspaceID  string  `json:"workspace_id"`
	TaskID       *string `json:"task_id,omitempty"`
	UpdateID     *string `json:"update_id,omitempty"`
	Title        string  `json:"title"`
	ArtifactRef  string  `json:"artifact_ref"`
	Kind         string  `json:"kind"`
	ContentType  string  `json:"content_type"`
	CreatedBy    string  `json:"created_by"`
	MetadataJSON string  `json:"metadata_json,omitempty"`
	CreatedAt    string  `json:"created_at"`
}

type WorkspaceSearchFilter struct {
	WorkspaceID string
	Query       string
	EntityType  string
	Limit       int
}

type WorkspaceSearchResult struct {
	EntityType string `json:"entity_type"`
	EntityID   string `json:"entity_id"`
	Title      string `json:"title"`
	Snippet    string `json:"snippet,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

type WorkspaceSnapshot struct {
	Workspace       WorkspaceRecord           `json:"workspace"`
	Docs            []WorkspaceDocRecord      `json:"docs"`
	Agents          []AgentRecord             `json:"agents"`
	Sessions        []AgentSessionStateRecord `json:"sessions"`
	Tools           []WorkspaceToolRecord     `json:"tools"`
	Tasks           []WorkspaceTaskRecord     `json:"tasks"`
	TaskLinks       []WorkspaceTaskLinkRecord `json:"task_links"`
	RecentMemory    []WorkspaceMemoryRecord   `json:"recent_memory"`
	RecentArtifacts []WorkspaceArtifactRecord `json:"recent_artifacts"`
	RecentUpdates   []AgentUpdateRecord       `json:"recent_updates"`
	RecentMessages  []MessageRecord           `json:"recent_messages"`
	Projects        []ProjectRecord           `json:"projects"`
}

func (s *Store) CreateWorkspace(ctx context.Context, input WorkspaceCreateInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return errors.New("title is required")
	}
	createdBy := strings.TrimSpace(input.CreatedBy)
	if createdBy == "" {
		return errors.New("created_by is required")
	}

	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = model.WorkspaceStatusActive
	}
	if !model.ValidWorkspaceStatus(status) {
		return fmt.Errorf("invalid workspace status: %s", status)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin workspace tx: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO workspaces(workspace_id, title, description, created_by, status, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		workspaceID,
		title,
		strings.TrimSpace(input.Description),
		createdBy,
		status,
		now,
		now,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert workspace: %w", err)
	}
	if err := s.ensureWorkspaceAuthSettingsTx(ctx, tx, workspaceID, input.WorkspacePassword); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("initialize workspace security settings: %w", err)
	}
	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "workspace_created",
		EntityType: "workspace",
		EntityID:   workspaceID,
		ActorID:    createdBy,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id": workspaceID,
			"title":        title,
			"description":  strings.TrimSpace(input.Description),
			"status":       status,
		}),
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit workspace tx: %w", err)
	}
	return nil
}

func (s *Store) GetWorkspace(ctx context.Context, workspaceID string) (WorkspaceRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return WorkspaceRecord{}, errors.New("workspace_id is required")
	}

	var record WorkspaceRecord
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT workspace_id, title, description, created_by, status, created_at, updated_at
		 FROM workspaces
		 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(
		&record.WorkspaceID,
		&record.Title,
		&record.Description,
		&record.CreatedBy,
		&record.Status,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceRecord{}, ErrWorkspaceNotFound
		}
		return WorkspaceRecord{}, fmt.Errorf("query workspace: %w", err)
	}
	return record, nil
}

func (s *Store) GetWorkspaceByTitle(ctx context.Context, title string) (WorkspaceRecord, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return WorkspaceRecord{}, errors.New("workspace_title is required")
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT workspace_id, title, description, created_by, status, created_at, updated_at
		 FROM workspaces
		 WHERE title = ? COLLATE NOCASE
		 ORDER BY workspace_id
		 LIMIT 2`,
		title,
	)
	if err != nil {
		return WorkspaceRecord{}, fmt.Errorf("query workspace by title: %w", err)
	}
	defer rows.Close()

	matches := make([]WorkspaceRecord, 0, 2)
	for rows.Next() {
		var record WorkspaceRecord
		if err := rows.Scan(
			&record.WorkspaceID,
			&record.Title,
			&record.Description,
			&record.CreatedBy,
			&record.Status,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return WorkspaceRecord{}, fmt.Errorf("scan workspace by title: %w", err)
		}
		matches = append(matches, record)
	}
	if err := rows.Err(); err != nil {
		return WorkspaceRecord{}, fmt.Errorf("iterate workspace by title: %w", err)
	}
	switch len(matches) {
	case 0:
		return WorkspaceRecord{}, ErrWorkspaceNotFound
	case 1:
		return matches[0], nil
	default:
		// Prefer a single active workspace when archived duplicates still exist.
		activeMatches := make([]WorkspaceRecord, 0, len(matches))
		for _, match := range matches {
			if match.Status == model.WorkspaceStatusActive {
				activeMatches = append(activeMatches, match)
			}
		}
		if len(activeMatches) == 1 {
			return activeMatches[0], nil
		}
		return WorkspaceRecord{}, ErrWorkspaceRefAmbiguous
	}
}

func (s *Store) ListWorkspaces(ctx context.Context, limit int) ([]WorkspaceRecord, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT workspace_id, title, description, created_by, status, created_at, updated_at
		 FROM workspaces
		 ORDER BY
		   CASE status
		     WHEN 'ACTIVE' THEN 0
		     WHEN 'PAUSED' THEN 1
		     WHEN 'ARCHIVED' THEN 2
		     ELSE 3
		   END,
		   updated_at DESC,
		   workspace_id
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query workspaces: %w", err)
	}
	defer rows.Close()

	out := []WorkspaceRecord{}
	for rows.Next() {
		var row WorkspaceRecord
		if err := rows.Scan(
			&row.WorkspaceID,
			&row.Title,
			&row.Description,
			&row.CreatedBy,
			&row.Status,
			&row.CreatedAt,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workspace row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspaces: %w", err)
	}
	return out, nil
}

func (s *Store) UpsertWorkspaceDoc(ctx context.Context, input WorkspaceDocInput) error {
	_, err := s.UpsertWorkspaceDocWithEvent(ctx, input)
	return err
}

func (s *Store) UpsertWorkspaceDocWithEvent(ctx context.Context, input WorkspaceDocInput) (RuntimeEventRecord, error) {
	event, _, err := s.UpsertWorkspaceDocWithEffects(ctx, input)
	return event, err
}

func (s *Store) UpsertWorkspaceDocWithEffects(ctx context.Context, input WorkspaceDocInput) (RuntimeEventRecord, []RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	docKey := strings.TrimSpace(input.DocKey)
	if docKey == "" {
		return RuntimeEventRecord{}, nil, errors.New("doc_key is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return RuntimeEventRecord{}, nil, errors.New("title is required")
	}
	updatedBy := strings.TrimSpace(input.UpdatedBy)
	if updatedBy == "" {
		return RuntimeEventRecord{}, nil, errors.New("updated_by is required")
	}
	promptContextSurface := strings.TrimSpace(input.PromptContextSurface)
	if promptContextSurface == "" {
		promptContextSurface = "workspace.doc.put"
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, nil, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, nil, fmt.Errorf("begin workspace doc tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var runtimeEvent RuntimeEventRecord
	var invalidationEvents []RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}

		// Optimistic locking: if ExpectedSHA is set, verify current content matches.
		if input.ExpectedSHA != "" {
			var currentContent sql.NullString
			err := tx.QueryRowContext(ctx,
				`SELECT content FROM workspace_docs WHERE workspace_id = ? AND doc_key = ?`,
				workspaceID, docKey,
			).Scan(&currentContent)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("check doc for conflict: %w", err)
			}
			if currentContent.Valid {
				currentSHA := contentSHA256(currentContent.String)
				if currentSHA != input.ExpectedSHA {
					return ErrDocConflict
				}
			}
			// If doc doesn't exist yet (ErrNoRows), no conflict possible — proceed.
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO workspace_docs(workspace_id, doc_key, title, content, updated_by, created_at, updated_at, archived_at, archived_by)
		 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
		 ON CONFLICT(workspace_id, doc_key) DO UPDATE SET
		   title = excluded.title,
		   content = excluded.content,
		   updated_by = excluded.updated_by,
		   updated_at = excluded.updated_at,
		   archived_at = NULL,
		   archived_by = NULL`,
			workspaceID,
			docKey,
			title,
			input.Content,
			updatedBy,
			now,
			now,
		); err != nil {
			return fmt.Errorf("upsert workspace doc: %w", err)
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO workspace_doc_revisions(revision_id, workspace_id, doc_key, title, content, updated_by, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			nextID("doc_revision"),
			workspaceID,
			docKey,
			title,
			input.Content,
			updatedBy,
			now,
		); err != nil {
			return fmt.Errorf("insert workspace doc revision: %w", err)
		}

		if _, err := tx.ExecContext(
			ctx,
			`UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`,
			now,
			workspaceID,
		); err != nil {
			return fmt.Errorf("touch workspace after doc update: %w", err)
		}

		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "workspace_doc_upserted",
			EntityType: "workspace_doc",
			EntityID:   workspaceID + "/" + docKey,
			ActorID:    updatedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id": workspaceID,
				"doc_key":      docKey,
				"title":        title,
			}),
		}); err != nil {
			return err
		}
		runtimePayload, err := attachWorkspaceDocPromptContextEnvelope(map[string]any{
			"workspace_id": workspaceID,
			"doc_key":      docKey,
			"title":        title,
			"updated_by":   updatedBy,
		}, input.PromptContextEnvelope, promptContextSurface, map[string]string{
			"workspace_id": workspaceID,
			"doc_key":      docKey,
			"title":        title,
			"updated_by":   updatedBy,
		})
		if err != nil {
			return err
		}
		runtimeEvent, err = s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "workspace_doc.upserted",
			EntityType:  "workspace_doc",
			EntityID:    docKey,
			ActorID:     updatedBy,
			PayloadJSON: mustJSON(runtimePayload),
			CreatedAt:   now,
		})
		if err != nil {
			return err
		}
		invalidationEvents = make([]RuntimeEventRecord, 0, 2)
		if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, workspaceID, memoryInvalidationRefChange{
			RefKind:             "workspace_doc",
			RefID:               docKey,
			CurrentVersionToken: contentSHA256(input.Content),
			Cause:               "workspace_doc.upserted",
		}); err != nil {
			return err
		} else {
			invalidationEvents = append(invalidationEvents, events...)
		}
		if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, workspaceID, memoryInvalidationRefChange{
			RefKind:             "segment_ref",
			RefID:               buildWorkspaceDocSegmentRef(workspaceID, docKey, "root"),
			CurrentVersionToken: memoryGraphSegmentVersionToken(buildWorkspaceDocSegmentRef(workspaceID, docKey, "root"), contentSHA256(input.Content)),
			Cause:               "workspace_doc.upserted",
		}); err != nil {
			return err
		} else {
			invalidationEvents = append(invalidationEvents, events...)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, nil, fmt.Errorf("commit workspace doc tx: %w", err)
	}
	return runtimeEvent, invalidationEvents, nil
}

func (s *Store) GetWorkspaceDoc(ctx context.Context, workspaceID, docKey string) (WorkspaceDocRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	docKey = strings.TrimSpace(docKey)
	if workspaceID == "" {
		return WorkspaceDocRecord{}, errors.New("workspace_id is required")
	}
	if docKey == "" {
		return WorkspaceDocRecord{}, errors.New("doc_key is required")
	}

	var record WorkspaceDocRecord
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT doc_key, title, content, updated_by, created_at, updated_at, archived_at, archived_by
		 FROM workspace_docs
		 WHERE workspace_id = ? AND doc_key = ?`,
		workspaceID,
		docKey,
	).Scan(
		&record.DocKey,
		&record.Title,
		&record.Content,
		&record.UpdatedBy,
		&record.CreatedAt,
		&record.UpdatedAt,
		&record.ArchivedAt,
		&record.ArchivedBy,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceDocRecord{}, fmt.Errorf("workspace doc not found: %s/%s", workspaceID, docKey)
		}
		return WorkspaceDocRecord{}, fmt.Errorf("query workspace doc: %w", err)
	}
	record.SHA = contentSHA256(record.Content)
	return record, nil
}

func (s *Store) ListWorkspaceDocRevisions(ctx context.Context, workspaceID, docKey string, limit int) ([]WorkspaceDocRevisionRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	docKey = strings.TrimSpace(docKey)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if docKey == "" {
		return nil, errors.New("doc_key is required")
	}
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT revision_id, workspace_id, doc_key, title, content, updated_by, created_at
		 FROM workspace_doc_revisions
		 WHERE workspace_id = ? AND doc_key = ?
		 ORDER BY created_at DESC, revision_id DESC
		 LIMIT ?`,
		workspaceID,
		docKey,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query workspace doc revisions: %w", err)
	}
	defer rows.Close()

	out := []WorkspaceDocRevisionRecord{}
	for rows.Next() {
		var row WorkspaceDocRevisionRecord
		if err := rows.Scan(
			&row.RevisionID,
			&row.WorkspaceID,
			&row.DocKey,
			&row.Title,
			&row.Content,
			&row.UpdatedBy,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workspace doc revision: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace doc revisions: %w", err)
	}
	return out, nil
}

func (s *Store) RegisterAgent(ctx context.Context, input AgentRegisterInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return errors.New("agent_id is required")
	}
	ownerUserID := strings.TrimSpace(input.OwnerUserID)
	if ownerUserID == "" {
		return errors.New("owner_user_id is required")
	}
	displayName := strings.TrimSpace(input.DisplayName)
	if displayName == "" {
		return errors.New("display_name is required")
	}
	role := strings.TrimSpace(input.Role)
	if role == "" {
		role = "generalist"
	}
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = model.AgentStatusRegistered
	}
	status = model.NormalizeStatus(status)
	if !model.ValidAgentStatus(status) {
		return fmt.Errorf("invalid agent status: %s. Valid: REGISTERED, ACTIVE, PAUSED, BLOCKED, OFFLINE", status)
	}
	protocolVersion := strings.TrimSpace(input.ProtocolVersion)
	if protocolVersion == "" {
		protocolVersion = "workspace-bootstrap/v1"
	}
	capabilitiesJSON, err := json.Marshal(normalizeCapabilities(input.Capabilities))
	if err != nil {
		return fmt.Errorf("encode capabilities: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin agent register tx: %w", err)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		_ = tx.Rollback()
		return err
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO agents(
		  workspace_id, agent_id, owner_user_id, display_name, role, status, protocol_version,
		  capabilities_json, summary, created_at, updated_at, last_seen_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(workspace_id, agent_id) DO UPDATE SET
		  owner_user_id = excluded.owner_user_id,
		  display_name = excluded.display_name,
		  role = excluded.role,
		  status = excluded.status,
		  protocol_version = excluded.protocol_version,
		  capabilities_json = excluded.capabilities_json,
		  summary = excluded.summary,
		  updated_at = excluded.updated_at`,
		workspaceID,
		agentID,
		ownerUserID,
		displayName,
		role,
		status,
		protocolVersion,
		string(capabilitiesJSON),
		strings.TrimSpace(input.Summary),
		now,
		now,
		nil,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("upsert agent: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("touch workspace after agent register: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "agent_registered",
		EntityType: "agent",
		EntityID:   agentID,
		ActorID:    ownerUserID,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id":     workspaceID,
			"agent_id":         agentID,
			"display_name":     displayName,
			"role":             role,
			"status":           status,
			"protocol_version": protocolVersion,
			"capabilities":     normalizeCapabilities(input.Capabilities),
		}),
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit agent register tx: %w", err)
	}
	return nil
}

func (s *Store) EnsureAgentRegistered(ctx context.Context, input AgentRegisterInput) (AgentRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return AgentRecord{}, errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return AgentRecord{}, errors.New("agent_id is required")
	}

	record, err := s.GetAgent(ctx, workspaceID, agentID)
	if err == nil {
		return record, nil
	}
	if err != nil && !errors.Is(err, ErrAgentNotFound) {
		return AgentRecord{}, err
	}

	if err := s.RegisterAgent(ctx, input); err != nil {
		return AgentRecord{}, err
	}
	return s.GetAgent(ctx, workspaceID, agentID)
}

func (s *Store) RegisterAgentPreservingOmitted(ctx context.Context, input AgentRegisterPatchInput) (AgentRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return AgentRecord{}, errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return AgentRecord{}, errors.New("agent_id is required")
	}

	existing, err := s.GetAgent(ctx, workspaceID, agentID)
	hasExisting := err == nil
	if err != nil && !errors.Is(err, ErrAgentNotFound) {
		return AgentRecord{}, err
	}

	ownerUserID := ""
	if input.OwnerUserID != nil {
		ownerUserID = strings.TrimSpace(*input.OwnerUserID)
		if ownerUserID == "" && hasExisting {
			ownerUserID = existing.OwnerUserID
		}
	} else if hasExisting {
		ownerUserID = existing.OwnerUserID
	}

	displayName := ""
	if input.DisplayName != nil {
		displayName = strings.TrimSpace(*input.DisplayName)
		if displayName == "" && hasExisting {
			displayName = existing.DisplayName
		}
	} else if hasExisting {
		displayName = existing.DisplayName
	}

	role := ""
	if input.Role != nil {
		role = strings.TrimSpace(*input.Role)
		if role == "" && hasExisting {
			role = existing.Role
		}
	} else if hasExisting {
		role = existing.Role
	}

	status := ""
	if input.Status != nil {
		status = strings.TrimSpace(*input.Status)
		if status == "" && hasExisting {
			status = existing.Status
		}
	} else if hasExisting {
		status = existing.Status
	}

	protocolVersion := ""
	if input.ProtocolVersion != nil {
		protocolVersion = strings.TrimSpace(*input.ProtocolVersion)
		if protocolVersion == "" && hasExisting {
			protocolVersion = existing.ProtocolVersion
		}
	} else if hasExisting {
		protocolVersion = existing.ProtocolVersion
	}

	var capabilities []string
	if input.Capabilities != nil {
		if *input.Capabilities == nil && hasExisting {
			capabilities = append([]string(nil), existing.Capabilities...)
		} else {
			capabilities = append([]string(nil), (*input.Capabilities)...)
		}
	} else if hasExisting {
		capabilities = append([]string(nil), existing.Capabilities...)
	}

	summary := ""
	if input.Summary != nil {
		summary = strings.TrimSpace(*input.Summary)
		if summary == "" && hasExisting {
			summary = existing.Summary
		}
	} else if hasExisting {
		summary = existing.Summary
	}

	if err := s.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		OwnerUserID:     ownerUserID,
		DisplayName:     displayName,
		Role:            role,
		Status:          status,
		ProtocolVersion: protocolVersion,
		Capabilities:    capabilities,
		Summary:         summary,
	}); err != nil {
		return AgentRecord{}, err
	}

	return s.GetAgent(ctx, workspaceID, agentID)
}

func (s *Store) UpdateAgentMetadataPreservingOmitted(ctx context.Context, input AgentMetadataPatchInput) (AgentRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return AgentRecord{}, errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return AgentRecord{}, errors.New("agent_id is required")
	}
	updatedBy := strings.TrimSpace(input.UpdatedBy)
	if updatedBy == "" {
		return AgentRecord{}, errors.New("updated_by is required")
	}

	existing, err := s.GetAgent(ctx, workspaceID, agentID)
	if err != nil {
		return AgentRecord{}, err
	}

	displayName := existing.DisplayName
	if input.DisplayName != nil {
		displayName = strings.TrimSpace(*input.DisplayName)
		if displayName == "" {
			displayName = existing.DisplayName
		}
	}

	role := existing.Role
	if input.Role != nil {
		role = strings.TrimSpace(*input.Role)
		if role == "" {
			role = existing.Role
		}
	}

	protocolVersion := existing.ProtocolVersion
	if input.ProtocolVersion != nil {
		protocolVersion = strings.TrimSpace(*input.ProtocolVersion)
		if protocolVersion == "" {
			protocolVersion = existing.ProtocolVersion
		}
	}

	capabilities := append([]string(nil), existing.Capabilities...)
	if input.Capabilities != nil {
		if *input.Capabilities == nil {
			capabilities = append([]string(nil), existing.Capabilities...)
		} else {
			capabilities = append([]string(nil), (*input.Capabilities)...)
		}
	}
	capabilitiesJSON, err := json.Marshal(normalizeCapabilities(capabilities))
	if err != nil {
		return AgentRecord{}, fmt.Errorf("encode capabilities: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return AgentRecord{}, fmt.Errorf("begin agent metadata update tx: %w", err)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		_ = tx.Rollback()
		return AgentRecord{}, err
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE agents
		 SET display_name = ?, role = ?, protocol_version = ?, capabilities_json = ?, updated_at = ?
		 WHERE workspace_id = ? AND agent_id = ?`,
		displayName,
		role,
		protocolVersion,
		string(capabilitiesJSON),
		now,
		workspaceID,
		agentID,
	); err != nil {
		_ = tx.Rollback()
		return AgentRecord{}, fmt.Errorf("update agent metadata: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		_ = tx.Rollback()
		return AgentRecord{}, fmt.Errorf("touch workspace after agent metadata update: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "agent_metadata_updated",
		EntityType: "agent",
		EntityID:   agentID,
		ActorID:    updatedBy,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id":     workspaceID,
			"agent_id":         agentID,
			"display_name":     displayName,
			"role":             role,
			"protocol_version": protocolVersion,
			"capabilities":     normalizeCapabilities(capabilities),
		}),
	}); err != nil {
		_ = tx.Rollback()
		return AgentRecord{}, err
	}

	if err := tx.Commit(); err != nil {
		return AgentRecord{}, fmt.Errorf("commit agent metadata update tx: %w", err)
	}
	return s.GetAgent(ctx, workspaceID, agentID)
}

func (s *Store) GetAgent(ctx context.Context, workspaceID, agentID string) (AgentRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" {
		return AgentRecord{}, errors.New("workspace_id is required")
	}
	if agentID == "" {
		return AgentRecord{}, errors.New("agent_id is required")
	}

	var record AgentRecord
	var capabilitiesJSON string
	var lastSeen sql.NullString
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT agent_id, workspace_id, owner_user_id, display_name, role, status, protocol_version,
		        capabilities_json, summary, created_at, updated_at, last_seen_at
		 FROM agents
		 WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID,
		agentID,
	).Scan(
		&record.AgentID,
		&record.WorkspaceID,
		&record.OwnerUserID,
		&record.DisplayName,
		&record.Role,
		&record.Status,
		&record.ProtocolVersion,
		&capabilitiesJSON,
		&record.Summary,
		&record.CreatedAt,
		&record.UpdatedAt,
		&lastSeen,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AgentRecord{}, ErrAgentNotFound
		}
		return AgentRecord{}, fmt.Errorf("query agent: %w", err)
	}

	record.LastSeenAt = nullStringPtr(lastSeen)
	record.Capabilities = decodeCapabilities(capabilitiesJSON)
	record.IsOnline = computeIsOnline(record.LastSeenAt)
	return record, nil
}

// DeleteAgent removes a registered agent from a workspace and logs the action.
func (s *Store) DeleteAgent(ctx context.Context, workspaceID, agentID, actor string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return errors.New("agent_id is required")
	}
	if actor == "" {
		actor = "dashboard"
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin agent delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := s.deleteAgentTx(ctx, tx, workspaceID, agentID, actor, now); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit agent delete tx: %w", err)
	}
	return nil
}

func (s *Store) DeleteAgentWithEvent(ctx context.Context, input AgentDeleteInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return RuntimeEventRecord{}, errors.New("agent_id is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
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
		return RuntimeEventRecord{}, fmt.Errorf("begin agent delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		displayName, err := s.deleteAgentTx(ctx, tx, workspaceID, agentID, actorID, now)
		if err != nil {
			return err
		}
		payload, err := attachAgentLifecyclePromptContextEnvelope(map[string]any{
			"workspace_id":       workspaceID,
			"agent_id":           agentID,
			"actor_id":           actorID,
			"display_name":       displayName,
			"entity_type":        "agent",
			"entity_id":          agentID,
			"summary":            "Agent deleted: " + agentID,
			"mutation_operation": "delete",
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "agent.delete"), map[string]string{
			"workspace_id":   workspaceID,
			"agent_id":       agentID,
			"actor_id":       actorID,
			"principal_type": firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "agent.deleted",
			EntityType:  "agent",
			EntityID:    agentID,
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit agent delete: %w", err)
	}
	return event, nil
}

func (s *Store) deleteAgentTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID, actor, now string) (string, error) {
	var displayName string
	if err := tx.QueryRowContext(ctx,
		`SELECT display_name FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID, agentID,
	).Scan(&displayName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrAgentNotFound
		}
		return "", fmt.Errorf("query agent for delete: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID, agentID,
	); err != nil {
		return "", fmt.Errorf("delete agent: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`,
		now, workspaceID,
	); err != nil {
		return "", fmt.Errorf("touch workspace after agent delete: %w", err)
	}
	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "agent_deleted",
		EntityType: "agent",
		EntityID:   agentID,
		ActorID:    actor,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id": workspaceID,
			"agent_id":     agentID,
			"display_name": displayName,
		}),
	}); err != nil {
		return "", err
	}
	return displayName, nil
}

func (s *Store) RegisterWorkspaceTool(ctx context.Context, input WorkspaceToolInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin tool register tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		return s.registerWorkspaceToolTx(ctx, tx, authority, input, now)
	}); err != nil {
		_ = tx.Rollback()
		return s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tool register tx: %w", err)
	}
	return nil
}

func (s *Store) GetWorkspaceTool(ctx context.Context, workspaceID, toolID string) (WorkspaceToolRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	toolID = strings.TrimSpace(toolID)
	if workspaceID == "" {
		return WorkspaceToolRecord{}, errors.New("workspace_id is required")
	}
	if toolID == "" {
		return WorkspaceToolRecord{}, errors.New("tool_id is required")
	}

	var record WorkspaceToolRecord
	var ownerAgentID sql.NullString
	var capabilitiesJSON string
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT workspace_id, tool_id, display_name, description, owner_user_id, owner_agent_id, kind, status,
		        version, access_level, endpoint, capabilities_json, manifest_json, created_at, updated_at
		 FROM workspace_tools
		 WHERE workspace_id = ? AND tool_id = ?`,
		workspaceID,
		toolID,
	).Scan(
		&record.WorkspaceID,
		&record.ToolID,
		&record.DisplayName,
		&record.Description,
		&record.OwnerUserID,
		&ownerAgentID,
		&record.Kind,
		&record.Status,
		&record.Version,
		&record.AccessLevel,
		&record.Endpoint,
		&capabilitiesJSON,
		&record.ManifestJSON,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceToolRecord{}, ErrToolNotFound
		}
		return WorkspaceToolRecord{}, fmt.Errorf("query workspace tool: %w", err)
	}
	if ownerAgentID.Valid {
		record.OwnerAgentID = ownerAgentID.String
	}
	record.Capabilities = decodeCapabilities(capabilitiesJSON)
	record.PolicyEnvelope = bridgepolicy.ParsePolicyEnvelopeFromManifest(record.ManifestJSON)
	return record, nil
}

func (s *Store) ListWorkspaceTools(ctx context.Context, filter WorkspaceToolFilter) ([]WorkspaceToolRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}

	args := []any{workspaceID}
	query := `
SELECT workspace_id, tool_id, display_name, description, owner_user_id, owner_agent_id, kind, status,
       version, access_level, endpoint, capabilities_json, manifest_json, created_at, updated_at
FROM workspace_tools
WHERE workspace_id = ?`
	if status := strings.TrimSpace(filter.Status); status != "" {
		query += " AND status = ?"
		args = append(args, status)
	}
	query += " ORDER BY display_name, tool_id"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query workspace tools: %w", err)
	}
	defer rows.Close()

	out := []WorkspaceToolRecord{}
	for rows.Next() {
		var row WorkspaceToolRecord
		var ownerAgentID sql.NullString
		var capabilitiesJSON string
		if err := rows.Scan(
			&row.WorkspaceID,
			&row.ToolID,
			&row.DisplayName,
			&row.Description,
			&row.OwnerUserID,
			&ownerAgentID,
			&row.Kind,
			&row.Status,
			&row.Version,
			&row.AccessLevel,
			&row.Endpoint,
			&capabilitiesJSON,
			&row.ManifestJSON,
			&row.CreatedAt,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workspace tool: %w", err)
		}
		if ownerAgentID.Valid {
			row.OwnerAgentID = ownerAgentID.String
		}
		row.Capabilities = decodeCapabilities(capabilitiesJSON)
		row.PolicyEnvelope = bridgepolicy.ParsePolicyEnvelopeFromManifest(row.ManifestJSON)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace tools: %w", err)
	}
	return out, nil
}

func (s *Store) RemoveWorkspaceTool(ctx context.Context, input WorkspaceToolRemoveInput) (bool, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return false, errors.New("workspace_id is required")
	}
	toolID := strings.TrimSpace(input.ToolID)
	if toolID == "" {
		return false, errors.New("tool_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return false, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tool remove tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	existed := false
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var removeErr error
		existed, removeErr = s.removeWorkspaceToolTx(ctx, tx, authority, input, now)
		return removeErr
	}); err != nil {
		_ = tx.Rollback()
		return false, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit tool remove tx: %w", err)
	}
	return existed, nil
}

type workspaceToolRouteManifest struct {
	Route struct {
		Kind     string `json:"kind,omitempty"`
		ServerID string `json:"server_id,omitempty"`
		ToolName string `json:"tool_name,omitempty"`
	} `json:"route,omitempty"`
}

var allowedMCPWorkspaceToolProjectionSourceSurfaces = map[string]struct{}{
	"mcp.tool.discover":   {},
	"mcp.server.register": {},
	"mcp.server.remove":   {},
}

func parseWorkspaceToolRouteManifest(raw string) workspaceToolRouteManifest {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return workspaceToolRouteManifest{}
	}
	var manifest workspaceToolRouteManifest
	_ = json.Unmarshal([]byte(raw), &manifest)
	return manifest
}

func (s *Store) ReconcileMCPWorkspaceToolsTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, serverID, actorID string, desired []WorkspaceToolInput, projectionSourceSurface, projectionOperationID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	serverID = strings.TrimSpace(serverID)
	if serverID == "" {
		return errors.New("server_id is required")
	}
	projectionSourceSurface = strings.TrimSpace(projectionSourceSurface)
	if projectionSourceSurface == "" {
		return errors.New("mcp workspace tool projection_source_surface is required")
	}
	if _, ok := allowedMCPWorkspaceToolProjectionSourceSurfaces[projectionSourceSurface]; !ok {
		return fmt.Errorf("mcp workspace tool projection_source_surface %q is not allowed", projectionSourceSurface)
	}
	projectionOperationID = strings.TrimSpace(projectionOperationID)
	if projectionOperationID == "" {
		return errors.New("mcp workspace tool projection_operation_id is required")
	}
	if tx == nil {
		return errors.New("reconcile mcp workspace tools tx is required")
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return err
	}
	if err := s.ensureMCPWorkspaceToolProjectionOperationTx(ctx, tx, workspaceID, projectionSourceSurface, projectionOperationID); err != nil {
		return err
	}

	desiredByToolID := make(map[string]WorkspaceToolInput, len(desired))
	for _, input := range desired {
		if !strings.EqualFold(strings.TrimSpace(input.WorkspaceID), workspaceID) {
			return fmt.Errorf("workspace tool %q does not belong to workspace %s", strings.TrimSpace(input.ToolID), workspaceID)
		}
		toolID := strings.TrimSpace(input.ToolID)
		if toolID == "" {
			return errors.New("tool_id is required")
		}
		if IsRemovedWorkspaceToolID(toolID) {
			return fmt.Errorf("tool_id %q has been removed from Rhizome", toolID)
		}
		manifest := parseWorkspaceToolRouteManifest(input.ManifestJSON)
		if !strings.EqualFold(strings.TrimSpace(manifest.Route.Kind), "mcp") ||
			!strings.EqualFold(strings.TrimSpace(manifest.Route.ServerID), serverID) ||
			strings.TrimSpace(manifest.Route.ToolName) == "" {
			return fmt.Errorf("workspace tool %s is missing an mcp route for server %s", toolID, serverID)
		}
		if _, exists := desiredByToolID[toolID]; exists {
			return fmt.Errorf("workspace tool alias collision for %s", toolID)
		}
		desiredByToolID[toolID] = input
	}

	type existingToolRecord struct {
		toolID       string
		manifestJSON string
	}
	rows, err := tx.QueryContext(ctx, `SELECT tool_id, manifest_json FROM workspace_tools WHERE workspace_id = ?`, workspaceID)
	if err != nil {
		return fmt.Errorf("query workspace tools for mcp reconcile: %w", err)
	}
	defer rows.Close()

	var existing []existingToolRecord
	for rows.Next() {
		var record existingToolRecord
		if err := rows.Scan(&record.toolID, &record.manifestJSON); err != nil {
			return fmt.Errorf("scan workspace tool for mcp reconcile: %w", err)
		}
		existing = append(existing, record)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate workspace tools for mcp reconcile: %w", err)
	}

	existingServerToolIDs := make(map[string]struct{})
	for _, record := range existing {
		manifest := parseWorkspaceToolRouteManifest(record.manifestJSON)
		if strings.EqualFold(strings.TrimSpace(manifest.Route.Kind), "mcp") &&
			strings.EqualFold(strings.TrimSpace(manifest.Route.ServerID), serverID) {
			existingServerToolIDs[record.toolID] = struct{}{}
		}
	}

	for toolID, input := range desiredByToolID {
		for _, record := range existing {
			if !strings.EqualFold(strings.TrimSpace(record.toolID), toolID) {
				continue
			}
			existingManifest := parseWorkspaceToolRouteManifest(record.manifestJSON)
			desiredManifest := parseWorkspaceToolRouteManifest(input.ManifestJSON)
			if !strings.EqualFold(strings.TrimSpace(existingManifest.Route.Kind), "mcp") ||
				!strings.EqualFold(strings.TrimSpace(existingManifest.Route.ServerID), strings.TrimSpace(desiredManifest.Route.ServerID)) ||
				!strings.EqualFold(strings.TrimSpace(existingManifest.Route.ToolName), strings.TrimSpace(desiredManifest.Route.ToolName)) {
				return fmt.Errorf("workspace tool alias collision for %s", toolID)
			}
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for toolID := range existingServerToolIDs {
		if _, keep := desiredByToolID[toolID]; keep {
			continue
		}
		removeInput := WorkspaceToolRemoveInput{
			WorkspaceID: workspaceID,
			ToolID:      toolID,
			RemovedBy:   strings.TrimSpace(actorID),
		}
		attachMCPWorkspaceToolProjectionPromptContext(&removeInput, workspaceID, actorID, projectionSourceSurface, projectionOperationID)
		if _, err := s.removeWorkspaceToolTx(ctx, tx, authority, removeInput, now); err != nil {
			return err
		}
	}
	for _, input := range desired {
		attachMCPWorkspaceToolProjectionPromptContext(&input, workspaceID, actorID, projectionSourceSurface, projectionOperationID)
		if err := s.registerWorkspaceToolTx(ctx, tx, authority, input, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensureMCPWorkspaceToolProjectionOperationTx(ctx context.Context, tx *sql.Tx, workspaceID, projectionSourceSurface, projectionOperationID string) error {
	expectedOperationKind, ok := mcpWorkspaceToolProjectionOperationKind(projectionSourceSurface)
	if !ok {
		return fmt.Errorf("mcp workspace tool projection_source_surface %q is not allowed", projectionSourceSurface)
	}
	run, err := s.getExecutionRunTx(ctx, tx, workspaceID, projectionOperationID)
	if err != nil {
		return fmt.Errorf("mcp workspace tool projection_operation_id %q is not a workspace execution run: %w", projectionOperationID, err)
	}
	envelope, ok := run.VerificationJSON[executionPromptContextEnvelopeKey].(map[string]any)
	if !ok {
		return fmt.Errorf("mcp workspace tool projection operation %q is missing prompt_context_envelope", projectionOperationID)
	}
	if got := toolRegistryPromptContextString(envelope["context_kind"]); got != executionPromptContextKind {
		return fmt.Errorf("mcp workspace tool projection operation %q has context_kind %q, want %q", projectionOperationID, got, executionPromptContextKind)
	}
	if got := toolRegistryPromptContextString(envelope["origin"]); got != "server_operation_ledger" {
		return fmt.Errorf("mcp workspace tool projection operation %q has origin %q, want server_operation_ledger", projectionOperationID, got)
	}
	if got := toolRegistryPromptContextString(envelope["surface"]); got != projectionSourceSurface {
		return fmt.Errorf("mcp workspace tool projection operation %q has surface %q, want %q", projectionOperationID, got, projectionSourceSurface)
	}
	ledger, ok := run.VerificationJSON["operation_ledger"].(map[string]any)
	if !ok {
		return fmt.Errorf("mcp workspace tool projection operation %q is missing operation_ledger", projectionOperationID)
	}
	if got := toolRegistryPromptContextString(ledger["schema"]); got != "operation_ledger.v1" {
		return fmt.Errorf("mcp workspace tool projection operation %q has operation_ledger schema %q, want operation_ledger.v1", projectionOperationID, got)
	}
	if got := toolRegistryPromptContextString(ledger["operation_id"]); got != projectionOperationID {
		return fmt.Errorf("mcp workspace tool projection operation %q has operation_ledger operation_id %q", projectionOperationID, got)
	}
	if got := toolRegistryPromptContextString(ledger["operation_kind"]); got != expectedOperationKind {
		return fmt.Errorf("mcp workspace tool projection operation %q has operation_ledger operation_kind %q, want %q", projectionOperationID, got, expectedOperationKind)
	}
	capabilitySnapshot, ok := ledger["capability_snapshot"].(map[string]any)
	if !ok {
		return fmt.Errorf("mcp workspace tool projection operation %q is missing capability_snapshot", projectionOperationID)
	}
	if got := toolRegistryPromptContextString(capabilitySnapshot["requested_capability"]); got != projectionSourceSurface {
		return fmt.Errorf("mcp workspace tool projection operation %q has capability requested_capability %q, want %q", projectionOperationID, got, projectionSourceSurface)
	}
	return nil
}

func mcpWorkspaceToolProjectionOperationKind(surface string) (string, bool) {
	switch strings.TrimSpace(surface) {
	case "mcp.tool.discover":
		return "mcp_discover", true
	case "mcp.server.register":
		return "mcp_server_register", true
	case "mcp.server.remove":
		return "mcp_server_remove", true
	default:
		return "", false
	}
}

func attachMCPWorkspaceToolProjectionPromptContext(input any, workspaceID, actorID, projectionSourceSurface, projectionOperationID string) {
	principalType, principalID := workspaceToolProjectionPrincipal(actorID)
	envelope := BuildToolRegistryPromptContextEnvelope("mcp.workspace_tool.project", "server_mcp_projection", workspaceID, principalType, principalID)
	switch typed := input.(type) {
	case *WorkspaceToolInput:
		if typed == nil {
			return
		}
		typed.PromptContextEnvelope = envelope
		typed.PromptContextSurface = "mcp.workspace_tool.project"
		typed.PromptContextPrincipalType = principalType
		typed.PromptContextPrincipalID = principalID
		typed.PromptContextProjectionSource = strings.TrimSpace(projectionSourceSurface)
		typed.PromptContextProjectionOperationID = strings.TrimSpace(projectionOperationID)
	case *WorkspaceToolRemoveInput:
		if typed == nil {
			return
		}
		typed.PromptContextEnvelope = envelope
		typed.PromptContextSurface = "mcp.workspace_tool.project"
		typed.PromptContextPrincipalType = principalType
		typed.PromptContextPrincipalID = principalID
		typed.PromptContextProjectionSource = strings.TrimSpace(projectionSourceSurface)
		typed.PromptContextProjectionOperationID = strings.TrimSpace(projectionOperationID)
	}
}

func workspaceToolProjectionPrincipal(actorID string) (string, string) {
	actorID = strings.TrimSpace(actorID)
	if actorID == "" {
		return "system", "mcp_projection"
	}
	parts := strings.SplitN(actorID, ":", 2)
	if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != "" {
		return strings.ToLower(strings.TrimSpace(parts[0])), strings.TrimSpace(parts[1])
	}
	if strings.EqualFold(actorID, "system") {
		return "system", "system"
	}
	return "human", actorID
}

func toolRegistryPromptContextFields(payload map[string]any, surface, manifestJSON, action, projectionSourceSurface, projectionOperationID string) {
	if payload == nil {
		return
	}
	if strings.TrimSpace(surface) != "mcp.workspace_tool.project" {
		return
	}
	payload["projection_action"] = strings.TrimSpace(action)
	payload["projection_source"] = "mcp_workspace_tool_reconcile"
	if projectionSourceSurface = strings.TrimSpace(projectionSourceSurface); projectionSourceSurface != "" {
		payload["projection_source_surface"] = projectionSourceSurface
	}
	if projectionOperationID = strings.TrimSpace(projectionOperationID); projectionOperationID != "" {
		payload["projection_operation_id"] = projectionOperationID
	}
	manifest := parseWorkspaceToolRouteManifest(manifestJSON)
	if strings.EqualFold(strings.TrimSpace(manifest.Route.Kind), "mcp") {
		payload["mcp_server_id"] = strings.TrimSpace(manifest.Route.ServerID)
		payload["mcp_tool_name"] = strings.TrimSpace(manifest.Route.ToolName)
	}
}

func toolRegistryPromptContextRequiredFields(payload map[string]any) map[string]string {
	fields := make(map[string]string)
	for _, key := range []string{
		"workspace_id",
		"tool_id",
		"display_name",
		"owner_user_id",
		"owner_agent_id",
		"kind",
		"status",
		"version",
		"access_level",
		"endpoint",
		"capabilities_sha256",
		"manifest_sha256",
		"removed_by",
		"event_type",
		"entity_type",
		"entity_id",
		"actor_type",
		"actor_id",
		"projection_action",
		"projection_source",
		"projection_source_surface",
		"projection_operation_id",
		"mcp_server_id",
		"mcp_tool_name",
	} {
		if value, ok := payload[key]; ok {
			fields[key] = toolRegistryPromptContextString(value)
		}
	}
	return fields
}

func toolRegistryPromptContextString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	case nil:
		return ""
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func requiredToolRegistryPromptContextPrincipal(principalType, principalID string) (string, string, error) {
	principalType = strings.TrimSpace(principalType)
	principalID = strings.TrimSpace(principalID)
	if principalType == "" || principalID == "" {
		return "", "", errors.New("tool registry prompt context requires explicit expected principal binding")
	}
	return principalType, principalID, nil
}

func (s *Store) registerWorkspaceToolTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input WorkspaceToolInput, now string) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	toolID := strings.TrimSpace(input.ToolID)
	if toolID == "" {
		return errors.New("tool_id is required")
	}
	if IsRemovedWorkspaceToolID(toolID) {
		return fmt.Errorf("tool_id %q has been removed from Rhizome", toolID)
	}
	displayName := strings.TrimSpace(input.DisplayName)
	if displayName == "" {
		return errors.New("display_name is required")
	}
	ownerUserID := strings.TrimSpace(input.OwnerUserID)
	if ownerUserID == "" {
		return errors.New("owner_user_id is required")
	}
	kind := strings.TrimSpace(input.Kind)
	if kind == "" {
		kind = model.ToolKindOther
	}
	if !model.ValidToolKind(kind) {
		return fmt.Errorf("invalid tool kind: %s", kind)
	}
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = model.ToolStatusActive
	}
	if !model.ValidToolStatus(status) {
		return fmt.Errorf("invalid tool status: %s", status)
	}
	accessLevel := strings.TrimSpace(input.AccessLevel)
	if accessLevel == "" {
		accessLevel = model.ToolAccessWorkspace
	}
	if !model.ValidToolAccessLevel(accessLevel) {
		return fmt.Errorf("invalid tool access level: %s", accessLevel)
	}

	capabilitiesJSON, err := json.Marshal(normalizeCapabilities(input.Capabilities))
	if err != nil {
		return fmt.Errorf("encode tool capabilities: %w", err)
	}
	manifestJSON := strings.TrimSpace(input.ManifestJSON)
	if manifestJSON == "" {
		manifestJSON = "{}"
	}

	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return err
	}
	if ownerAgentID := strings.TrimSpace(input.OwnerAgentID); ownerAgentID != "" {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, ownerAgentID); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO workspace_tools(
		  workspace_id, tool_id, display_name, description, owner_user_id, owner_agent_id, kind, status,
		  version, access_level, endpoint, capabilities_json, manifest_json, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(workspace_id, tool_id) DO UPDATE SET
		  display_name = excluded.display_name,
		  description = excluded.description,
		  owner_user_id = excluded.owner_user_id,
		  owner_agent_id = excluded.owner_agent_id,
		  kind = excluded.kind,
		  status = excluded.status,
		  version = excluded.version,
		  access_level = excluded.access_level,
		  endpoint = excluded.endpoint,
		  capabilities_json = excluded.capabilities_json,
		  manifest_json = excluded.manifest_json,
		  updated_at = excluded.updated_at`,
		workspaceID,
		toolID,
		displayName,
		strings.TrimSpace(input.Description),
		ownerUserID,
		strings.TrimSpace(input.OwnerAgentID),
		kind,
		status,
		strings.TrimSpace(input.Version),
		accessLevel,
		strings.TrimSpace(input.Endpoint),
		string(capabilitiesJSON),
		manifestJSON,
		now,
		now,
	); err != nil {
		return fmt.Errorf("upsert workspace tool: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		return fmt.Errorf("touch workspace after tool register: %w", err)
	}

	payload := map[string]any{
		"workspace_id":        workspaceID,
		"tool_id":             toolID,
		"display_name":        displayName,
		"owner_user_id":       ownerUserID,
		"owner_agent_id":      strings.TrimSpace(input.OwnerAgentID),
		"kind":                kind,
		"status":              status,
		"version":             strings.TrimSpace(input.Version),
		"access_level":        accessLevel,
		"endpoint":            strings.TrimSpace(input.Endpoint),
		"capabilities":        normalizeCapabilities(input.Capabilities),
		"capabilities_sha256": contentSHA256(string(capabilitiesJSON)),
		"manifest_sha256":     contentSHA256(manifestJSON),
		"event_type":          "workspace_tool.registered",
		"entity_type":         "workspace_tool",
		"entity_id":           toolID,
		"actor_type":          "operator",
		"actor_id":            ownerUserID,
	}
	toolRegistryPromptContextFields(payload, strings.TrimSpace(input.PromptContextSurface), manifestJSON, "register", input.PromptContextProjectionSource, input.PromptContextProjectionOperationID)
	if input.PromptContextEnvelope != nil {
		required := toolRegistryPromptContextRequiredFields(payload)
		principalType, principalID, err := requiredToolRegistryPromptContextPrincipal(input.PromptContextPrincipalType, input.PromptContextPrincipalID)
		if err != nil {
			return err
		}
		required["principal_type"] = principalType
		required["principal_id"] = principalID
		enrichedPayload, err := AttachToolRegistryPromptContextEnvelope(payload, input.PromptContextEnvelope, input.PromptContextSurface, required)
		if err != nil {
			return err
		}
		payload = enrichedPayload
	}
	payloadJSON := mustJSON(payload)
	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:     nextID("audit"),
		EventType:   "workspace_tool_registered",
		EntityType:  "workspace_tool",
		EntityID:    workspaceID + "/" + toolID,
		ActorID:     ownerUserID,
		PayloadJSON: payloadJSON,
	}); err != nil {
		return err
	}
	_, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		ActorType:   "operator",
		ActorID:     ownerUserID,
		PayloadJSON: payloadJSON,
		CreatedAt:   now,
	})
	return err
}

func (s *Store) removeWorkspaceToolTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input WorkspaceToolRemoveInput, now string) (bool, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return false, errors.New("workspace_id is required")
	}
	toolID := strings.TrimSpace(input.ToolID)
	if toolID == "" {
		return false, errors.New("tool_id is required")
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return false, err
	}

	var record WorkspaceToolRecord
	var ownerAgentID sql.NullString
	var capabilitiesJSON string
	err := tx.QueryRowContext(
		ctx,
		`SELECT workspace_id, tool_id, display_name, description, owner_user_id, owner_agent_id, kind, status,
		        version, access_level, endpoint, capabilities_json, manifest_json, created_at, updated_at
		 FROM workspace_tools
		 WHERE workspace_id = ? AND tool_id = ?`,
		workspaceID,
		toolID,
	).Scan(
		&record.WorkspaceID,
		&record.ToolID,
		&record.DisplayName,
		&record.Description,
		&record.OwnerUserID,
		&ownerAgentID,
		&record.Kind,
		&record.Status,
		&record.Version,
		&record.AccessLevel,
		&record.Endpoint,
		&capabilitiesJSON,
		&record.ManifestJSON,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("query workspace tool for remove: %w", err)
	}
	if ownerAgentID.Valid {
		record.OwnerAgentID = ownerAgentID.String
	}
	record.Capabilities = decodeCapabilities(capabilitiesJSON)
	removedCapabilitiesJSON, err := json.Marshal(normalizeCapabilities(record.Capabilities))
	if err != nil {
		return false, fmt.Errorf("encode removed tool capabilities: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM workspace_tools WHERE workspace_id = ? AND tool_id = ?`,
		workspaceID,
		toolID,
	); err != nil {
		return false, fmt.Errorf("delete workspace tool: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		return false, fmt.Errorf("touch workspace after tool remove: %w", err)
	}

	payload := map[string]any{
		"workspace_id":        workspaceID,
		"tool_id":             toolID,
		"removed_by":          strings.TrimSpace(input.RemovedBy),
		"display_name":        record.DisplayName,
		"owner_user_id":       record.OwnerUserID,
		"owner_agent_id":      record.OwnerAgentID,
		"kind":                record.Kind,
		"status":              record.Status,
		"version":             record.Version,
		"access_level":        record.AccessLevel,
		"endpoint":            record.Endpoint,
		"capabilities_sha256": contentSHA256(string(removedCapabilitiesJSON)),
		"manifest_sha256":     contentSHA256(record.ManifestJSON),
		"tool":                record,
		"event_type":          "workspace_tool.removed",
		"entity_type":         "workspace_tool",
		"entity_id":           toolID,
		"actor_type":          "operator",
		"actor_id":            strings.TrimSpace(input.RemovedBy),
	}
	toolRegistryPromptContextFields(payload, strings.TrimSpace(input.PromptContextSurface), record.ManifestJSON, "remove", input.PromptContextProjectionSource, input.PromptContextProjectionOperationID)
	if input.PromptContextEnvelope != nil {
		required := toolRegistryPromptContextRequiredFields(payload)
		principalType, principalID, err := requiredToolRegistryPromptContextPrincipal(input.PromptContextPrincipalType, input.PromptContextPrincipalID)
		if err != nil {
			return false, err
		}
		required["principal_type"] = principalType
		required["principal_id"] = principalID
		enrichedPayload, err := AttachToolRegistryPromptContextEnvelope(payload, input.PromptContextEnvelope, input.PromptContextSurface, required)
		if err != nil {
			return false, err
		}
		payload = enrichedPayload
	}
	payloadJSON := mustJSON(payload)
	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:     nextID("audit"),
		EventType:   "workspace_tool_removed",
		EntityType:  "workspace_tool",
		EntityID:    workspaceID + "/" + toolID,
		ActorID:     strings.TrimSpace(input.RemovedBy),
		PayloadJSON: payloadJSON,
	}); err != nil {
		return false, err
	}
	_, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.removed",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		ActorType:   "operator",
		ActorID:     strings.TrimSpace(input.RemovedBy),
		PayloadJSON: payloadJSON,
		CreatedAt:   now,
	})
	return true, err
}

func (s *Store) CreateWorkspaceArtifact(ctx context.Context, input WorkspaceArtifactInput) error {
	_, err := s.RecordWorkspaceArtifact(ctx, input)
	return err
}

func (s *Store) RecordWorkspaceArtifactWithEvent(ctx context.Context, input WorkspaceArtifactInput) (WorkspaceArtifactRecord, RuntimeEventRecord, error) {
	record, event, _, err := s.RecordWorkspaceArtifactWithEffects(ctx, input)
	if err != nil {
		return WorkspaceArtifactRecord{}, RuntimeEventRecord{}, err
	}
	return record, event, nil
}

func (s *Store) RecordWorkspaceArtifactWithEffects(ctx context.Context, input WorkspaceArtifactInput) (WorkspaceArtifactRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.recordWorkspaceArtifactWithEffects(ctx, input)
}

func (s *Store) RecordWorkspaceArtifact(ctx context.Context, input WorkspaceArtifactInput) (WorkspaceArtifactRecord, error) {
	record, _, _, err := s.recordWorkspaceArtifactWithEffects(ctx, input)
	return record, err
}

func (s *Store) recordWorkspaceArtifactWithEffects(ctx context.Context, input WorkspaceArtifactInput) (WorkspaceArtifactRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return WorkspaceArtifactRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return WorkspaceArtifactRecord{}, RuntimeEventRecord{}, nil, errors.New("title is required")
	}
	artifactRef := strings.TrimSpace(input.ArtifactRef)
	if artifactRef == "" {
		return WorkspaceArtifactRecord{}, RuntimeEventRecord{}, nil, errors.New("artifact_ref is required")
	}
	createdBy := strings.TrimSpace(input.CreatedBy)
	if createdBy == "" {
		return WorkspaceArtifactRecord{}, RuntimeEventRecord{}, nil, errors.New("created_by is required")
	}
	artifactID := strings.TrimSpace(input.ArtifactID)
	if artifactID == "" {
		artifactID = nextID("artifact")
	}
	kind := strings.TrimSpace(input.Kind)
	if kind == "" {
		kind = "reference"
	}
	contentType := strings.TrimSpace(input.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	metadataJSON := strings.TrimSpace(input.MetadataJSON)
	if metadataJSON == "" {
		metadataJSON = "{}"
	}
	if err := validateWorkspaceArtifactMetadataPromptContext(metadataJSON, input.PromptContextEnvelope != nil); err != nil {
		return WorkspaceArtifactRecord{}, RuntimeEventRecord{}, nil, err
	}
	promptContextSurface := strings.TrimSpace(input.PromptContextSurface)
	if promptContextSurface == "" {
		promptContextSurface = "workspace.artifact.write"
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return WorkspaceArtifactRecord{}, RuntimeEventRecord{}, nil, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceArtifactRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("begin artifact tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	record := WorkspaceArtifactRecord{
		ArtifactID:   artifactID,
		WorkspaceID:  workspaceID,
		TaskID:       blankStringPtr(strings.TrimSpace(input.TaskID)),
		UpdateID:     blankStringPtr(strings.TrimSpace(input.UpdateID)),
		Title:        title,
		ArtifactRef:  artifactRef,
		Kind:         kind,
		ContentType:  contentType,
		CreatedBy:    createdBy,
		MetadataJSON: metadataJSON,
		CreatedAt:    now,
	}
	var runtimeEvent RuntimeEventRecord
	var invalidationEvents []RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		if taskID := strings.TrimSpace(input.TaskID); taskID != "" {
			if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
				return err
			}
		}
		if updateID := strings.TrimSpace(input.UpdateID); updateID != "" {
			if err := s.ensureAgentUpdateInWorkspaceTx(ctx, tx, workspaceID, updateID); err != nil {
				return err
			}
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO workspace_artifacts(
		  artifact_id, workspace_id, task_id, update_id, title, artifact_ref, kind, content_type,
		  created_by, metadata_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			artifactID,
			workspaceID,
			blankStringOrNil(input.TaskID),
			blankStringOrNil(input.UpdateID),
			title,
			artifactRef,
			kind,
			contentType,
			createdBy,
			metadataJSON,
			now,
		); err != nil {
			return fmt.Errorf("insert workspace artifact: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after artifact create: %w", err)
		}

		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "workspace_artifact_created",
			EntityType: "workspace_artifact",
			EntityID:   workspaceID + "/" + artifactID,
			ActorID:    createdBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id": workspaceID,
				"artifact_id":  artifactID,
				"task_id":      strings.TrimSpace(input.TaskID),
				"update_id":    strings.TrimSpace(input.UpdateID),
				"title":        title,
				"artifact_ref": artifactRef,
				"kind":         kind,
				"content_type": contentType,
			}),
		}); err != nil {
			return err
		}
		runtimePayload := map[string]any{
			"workspace_id": workspaceID,
			"artifact_id":  artifactID,
			"task_id":      strings.TrimSpace(input.TaskID),
			"update_id":    strings.TrimSpace(input.UpdateID),
			"title":        title,
			"artifact_ref": artifactRef,
			"kind":         kind,
			"content_type": contentType,
			"created_by":   createdBy,
			"event_type":   "workspace_artifact.created",
			"entity_type":  "workspace_artifact",
			"entity_id":    artifactID,
			"actor_id":     createdBy,
		}
		runtimePayload, err = attachWorkspaceArtifactPromptContextEnvelope(runtimePayload, input.PromptContextEnvelope, promptContextSurface, map[string]string{
			"workspace_id":    workspaceID,
			"artifact_id":     artifactID,
			"task_id":         strings.TrimSpace(input.TaskID),
			"update_id":       strings.TrimSpace(input.UpdateID),
			"title":           title,
			"artifact_ref":    artifactRef,
			"kind":            kind,
			"content_type":    contentType,
			"created_by":      createdBy,
			"event_type":      "workspace_artifact.created",
			"entity_type":     "workspace_artifact",
			"entity_id":       artifactID,
			"actor_id":        createdBy,
			"metadata_sha256": contentSHA256(metadataJSON),
		})
		if err != nil {
			return err
		}
		runtimeEvent, err = s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "workspace_artifact.created",
			EntityType:  "workspace_artifact",
			EntityID:    artifactID,
			ActorID:     createdBy,
			TaskID:      strings.TrimSpace(input.TaskID),
			PayloadJSON: mustJSON(runtimePayload),
			CreatedAt:   now,
		})
		if err != nil {
			return err
		}
		artifactToken := workspaceArtifactVersionToken(WorkspaceArtifactRecord{ArtifactID: artifactID, CreatedAt: now})
		invalidationEvents = make([]RuntimeEventRecord, 0, 2)
		if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, workspaceID, memoryInvalidationRefChange{
			RefKind:             "artifact_ref",
			RefID:               artifactRef,
			CurrentVersionToken: artifactToken,
			Cause:               "workspace_artifact.created",
		}); err != nil {
			return err
		} else {
			invalidationEvents = append(invalidationEvents, events...)
		}
		if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, workspaceID, memoryInvalidationRefChange{
			RefKind:             "segment_ref",
			RefID:               buildWorkspaceArtifactSegmentRef(workspaceID, artifactRef, "root"),
			CurrentVersionToken: memoryGraphSegmentVersionToken(buildWorkspaceArtifactSegmentRef(workspaceID, artifactRef, "root"), artifactToken),
			Cause:               "workspace_artifact.created",
		}); err != nil {
			return err
		} else {
			invalidationEvents = append(invalidationEvents, events...)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return WorkspaceArtifactRecord{}, RuntimeEventRecord{}, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return WorkspaceArtifactRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("commit artifact tx: %w", err)
	}
	return record, runtimeEvent, invalidationEvents, nil
}

func (s *Store) ListWorkspaceArtifacts(ctx context.Context, filter WorkspaceArtifactFilter) ([]WorkspaceArtifactRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}

	query := `
SELECT artifact_id, workspace_id, task_id, update_id, title, artifact_ref, kind, content_type,
       created_by, metadata_json, created_at
FROM workspace_artifacts
WHERE workspace_id = ?`
	args := []any{workspaceID}
	if taskID := strings.TrimSpace(filter.TaskID); taskID != "" {
		query += " AND task_id = ?"
		args = append(args, taskID)
	}
	if updateID := strings.TrimSpace(filter.UpdateID); updateID != "" {
		query += " AND update_id = ?"
		args = append(args, updateID)
	}
	query += " ORDER BY created_at DESC, artifact_id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query workspace artifacts: %w", err)
	}
	defer rows.Close()

	out := []WorkspaceArtifactRecord{}
	for rows.Next() {
		var row WorkspaceArtifactRecord
		var taskID, updateID sql.NullString
		if err := rows.Scan(
			&row.ArtifactID,
			&row.WorkspaceID,
			&taskID,
			&updateID,
			&row.Title,
			&row.ArtifactRef,
			&row.Kind,
			&row.ContentType,
			&row.CreatedBy,
			&row.MetadataJSON,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workspace artifact: %w", err)
		}
		row.TaskID = nullStringPtr(taskID)
		row.UpdateID = nullStringPtr(updateID)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace artifacts: %w", err)
	}
	return out, nil
}

func (s *Store) TouchAgentActivity(ctx context.Context, workspaceID, agentID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if agentID == "" {
		return errors.New("agent_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.writeDB.ExecContext(ctx,
		`UPDATE agents
		 SET last_seen_at = ?, updated_at = ?, status = 'active'
		 WHERE workspace_id = ? AND agent_id = ?`,
		now, now, workspaceID, agentID,
	)
	if err != nil {
		return fmt.Errorf("touch agent activity: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for agent activity: %w", err)
	}
	if affected == 0 {
		return ErrAgentNotFound
	}
	_, err = s.writeDB.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID)
	if err != nil {
		return fmt.Errorf("touch workspace after agent activity: %w", err)
	}
	return nil
}

func (s *Store) RecordAgentHeartbeat(ctx context.Context, input AgentHeartbeatInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return errors.New("agent_id is required")
	}
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = model.AgentStatusActive
	}
	status = model.NormalizeStatus(status)
	if !model.ValidAgentStatus(status) {
		return fmt.Errorf("invalid agent status: %s. Valid: REGISTERED, ACTIVE, PAUSED, BLOCKED, OFFLINE", status)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.writeDB.ExecContext(
		ctx,
		`UPDATE agents
		 SET status = ?, summary = ?, last_seen_at = ?, updated_at = ?
		 WHERE workspace_id = ? AND agent_id = ?`,
		status,
		strings.TrimSpace(input.Summary),
		now,
		now,
		workspaceID,
		agentID,
	)
	if err != nil {
		return fmt.Errorf("update agent heartbeat: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for agent heartbeat: %w", err)
	}
	if affected == 0 {
		return ErrAgentNotFound
	}

	_, err = s.writeDB.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID)
	if err != nil {
		return fmt.Errorf("touch workspace after heartbeat: %w", err)
	}
	return nil
}

func (s *Store) RecordAgentUpdate(ctx context.Context, input AgentUpdateInput) error {
	_, err := s.RecordAgentUpdateWithEvent(ctx, input)
	return err
}

func (s *Store) RecordAgentUpdateWithEvent(ctx context.Context, input AgentUpdateInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return RuntimeEventRecord{}, errors.New("agent_id is required")
	}
	updateType := strings.TrimSpace(input.UpdateType)
	if updateType == "" {
		return RuntimeEventRecord{}, errors.New("update_type is required")
	}
	summary := strings.TrimSpace(input.Summary)
	if summary == "" {
		return RuntimeEventRecord{}, errors.New("summary is required")
	}
	updateID := strings.TrimSpace(input.UpdateID)
	if updateID == "" {
		updateID = nextID("agent_update")
	}
	promptContextSurface := strings.TrimSpace(input.PromptContextSurface)
	if promptContextSurface == "" {
		promptContextSurface = "agent.update.post"
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin agent update tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var runtimeEvent RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, agentID); err != nil {
			return err
		}
		promptContextFields := map[string]string{
			"workspace_id":   workspaceID,
			"update_id":      updateID,
			"agent_id":       agentID,
			"actor_agent_id": agentID,
			"update_type":    updateType,
			"summary":        summary,
			"requires_human": fmt.Sprintf("%t", input.RequiresHuman),
		}
		payloadJSON, err := mergeAgentUpdatePromptContextPayloadJSON(input.PayloadJSON, input.PromptContextEnvelope, promptContextSurface, promptContextFields)
		if err != nil {
			return err
		}
		if _, err := model.ParseAgentUpdateSideEffectsV1(payloadJSON); err != nil {
			return err
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			updateID,
			workspaceID,
			agentID,
			updateType,
			summary,
			payloadJSON,
			boolToInt(input.RequiresHuman),
			now,
		); err != nil {
			return fmt.Errorf("insert agent update: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after agent update: %w", err)
		}

		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "agent_update_posted",
			EntityType: "agent_update",
			EntityID:   updateID,
			ActorID:    agentID,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":   workspaceID,
				"agent_id":       agentID,
				"update_type":    updateType,
				"summary":        summary,
				"requires_human": input.RequiresHuman,
			}),
		}); err != nil {
			return err
		}
		runtimePayload, err := attachAgentUpdatePromptContextEnvelope(map[string]any{
			"workspace_id":   workspaceID,
			"update_id":      updateID,
			"agent_id":       agentID,
			"update_type":    updateType,
			"summary":        summary,
			"requires_human": input.RequiresHuman,
		}, input.PromptContextEnvelope, promptContextSurface, promptContextFields)
		if err != nil {
			return err
		}
		runtimeEvent, err = s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "agent_update.posted",
			EntityType:  "agent_update",
			EntityID:    updateID,
			ActorType:   "agent",
			ActorID:     agentID,
			AgentID:     agentID,
			PayloadJSON: mustJSON(runtimePayload),
			CreatedAt:   now,
		})
		return err
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit agent update tx: %w", err)
	}
	return runtimeEvent, nil
}

func (s *Store) AttachTaskToWorkspace(ctx context.Context, input TaskAttachmentInput) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin workspace task tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.attachTaskToWorkspaceTx(ctx, tx, input, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit workspace task tx: %w", err)
	}
	return nil
}

func (s *Store) attachTaskToWorkspaceTx(ctx context.Context, tx *sql.Tx, input TaskAttachmentInput, now string) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return errors.New("task_id is required")
	}
	linkedBy := strings.TrimSpace(input.LinkedBy)
	if linkedBy == "" {
		return errors.New("linked_by is required")
	}

	now = strings.TrimSpace(now)
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return err
	}
	if err := s.ensureTaskExistsTx(ctx, tx, taskID); err != nil {
		return err
	}
	if err := s.ensureProjectLinkedTaskCanAttachToWorkspaceTx(ctx, tx, workspaceID, taskID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO workspace_tasks(workspace_id, task_id, linked_by, created_at)
		 VALUES (?, ?, ?, ?)`,
		workspaceID,
		taskID,
		linkedBy,
		now,
	); err != nil {
		return fmt.Errorf("attach task to workspace: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		return fmt.Errorf("touch workspace after task attach: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "workspace_task_attached",
		EntityType: "workspace_task",
		EntityID:   workspaceID + "/" + taskID,
		ActorID:    linkedBy,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id": workspaceID,
			"task_id":      taskID,
		}),
	}); err != nil {
		return err
	}
	return nil
}

func (s *Store) AddWorkspaceTaskLink(ctx context.Context, input WorkspaceTaskLinkInput) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin workspace task link tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.addWorkspaceTaskLinkTx(ctx, tx, input, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit workspace task link tx: %w", err)
	}
	return nil
}

func (s *Store) addWorkspaceTaskDependencyLinksTx(ctx context.Context, tx *sql.Tx, input WorkspaceTaskDependencyLinksInput, now string) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return errors.New("task_id is required")
	}
	createdBy := strings.TrimSpace(input.CreatedBy)
	if createdBy == "" {
		return errors.New("created_by is required")
	}
	seen := map[string]struct{}{}
	for _, dependencyTaskID := range input.DependencyTaskIDs {
		dependencyTaskID = strings.TrimSpace(dependencyTaskID)
		if dependencyTaskID == "" || dependencyTaskID == taskID {
			continue
		}
		if _, ok := seen[dependencyTaskID]; ok {
			continue
		}
		seen[dependencyTaskID] = struct{}{}
		if err := s.addWorkspaceTaskLinkTx(ctx, tx, WorkspaceTaskLinkInput{
			WorkspaceID: workspaceID,
			FromTaskID:  dependencyTaskID,
			ToTaskID:    taskID,
			LinkType:    model.TaskLinkBlocks,
			CreatedBy:   createdBy,
		}, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) addWorkspaceTaskRelatedLinksTx(ctx context.Context, tx *sql.Tx, input WorkspaceTaskRelatedLinksInput, now string) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return errors.New("task_id is required")
	}
	createdBy := strings.TrimSpace(input.CreatedBy)
	if createdBy == "" {
		return errors.New("created_by is required")
	}
	seen := map[string]struct{}{}
	for _, relatedTaskID := range input.RelatedTaskIDs {
		relatedTaskID = strings.TrimSpace(relatedTaskID)
		if relatedTaskID == "" || relatedTaskID == taskID {
			continue
		}
		if _, ok := seen[relatedTaskID]; ok {
			continue
		}
		seen[relatedTaskID] = struct{}{}
		if err := s.addWorkspaceTaskLinkTx(ctx, tx, WorkspaceTaskLinkInput{
			WorkspaceID: workspaceID,
			FromTaskID:  relatedTaskID,
			ToTaskID:    taskID,
			LinkType:    model.TaskLinkRelatesTo,
			CreatedBy:   createdBy,
		}, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) unresolvedWorkspaceTaskDependencyMap(ctx context.Context, workspaceID string) (map[string][]string, error) {
	return unresolvedWorkspaceTaskDependencyMapWithQuerier(ctx, s.db, workspaceID)
}

func unresolvedWorkspaceTaskDependencyMapWithQuerier(ctx context.Context, q sqlReadQuerier, workspaceID string) (map[string][]string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	rows, err := q.QueryContext(ctx, `
SELECT l.to_task_id, l.from_task_id
  FROM workspace_task_links l
  LEFT JOIN tasks dep ON dep.task_id = l.from_task_id
  LEFT JOIN tasks target ON target.task_id = l.to_task_id
 WHERE l.workspace_id = ?
   AND l.link_type = ?
   AND UPPER(TRIM(COALESCE(dep.status, ''))) <> ?
   AND NOT (
       UPPER(TRIM(COALESCE(dep.status, ''))) = ?
       AND UPPER(TRIM(COALESCE(dep.task_kind, ''))) = ?
       AND (
           LOWER(TRIM(COALESCE(dep.project_lane, ''))) IN ('strategy', 'coordination', 'planning')
           OR LOWER(TRIM(COALESCE(dep.task_id, ''))) LIKE 'root-%'
       )
       AND TRIM(COALESCE(dep.project_id, '')) <> ''
       AND TRIM(COALESCE(dep.project_id, '')) = TRIM(COALESCE(target.project_id, ''))
   )
 ORDER BY l.created_at ASC, l.from_task_id ASC, l.to_task_id ASC`,
		workspaceID,
		model.TaskLinkBlocks,
		model.TaskStatusResolved,
		model.TaskStatusRunning,
		model.TaskKindCoordination,
	)
	if err != nil {
		return nil, fmt.Errorf("query unresolved workspace task dependencies: %w", err)
	}
	defer rows.Close()

	out := map[string][]string{}
	for rows.Next() {
		var taskID, dependencyTaskID string
		if err := rows.Scan(&taskID, &dependencyTaskID); err != nil {
			return nil, fmt.Errorf("scan unresolved workspace task dependency: %w", err)
		}
		taskID = strings.TrimSpace(taskID)
		dependencyTaskID = strings.TrimSpace(dependencyTaskID)
		if taskID == "" || dependencyTaskID == "" {
			continue
		}
		out[taskID] = append(out[taskID], dependencyTaskID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unresolved workspace task dependencies: %w", err)
	}
	return out, nil
}

func unresolvedWorkspaceTaskDependencyIDsWithQuerier(ctx context.Context, q sqlReadQuerier, workspaceID, taskID string) ([]string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if taskID == "" {
		return nil, errors.New("task_id is required")
	}
	rows, err := q.QueryContext(ctx, `
SELECT l.from_task_id
  FROM workspace_task_links l
  LEFT JOIN tasks dep ON dep.task_id = l.from_task_id
  LEFT JOIN tasks target ON target.task_id = l.to_task_id
 WHERE l.workspace_id = ?
   AND l.to_task_id = ?
   AND l.link_type = ?
   AND UPPER(TRIM(COALESCE(dep.status, ''))) <> ?
   AND NOT (
       UPPER(TRIM(COALESCE(dep.status, ''))) = ?
       AND UPPER(TRIM(COALESCE(dep.task_kind, ''))) = ?
       AND (
           LOWER(TRIM(COALESCE(dep.project_lane, ''))) IN ('strategy', 'coordination', 'planning')
           OR LOWER(TRIM(COALESCE(dep.task_id, ''))) LIKE 'root-%'
       )
       AND TRIM(COALESCE(dep.project_id, '')) <> ''
       AND TRIM(COALESCE(dep.project_id, '')) = TRIM(COALESCE(target.project_id, ''))
   )
 ORDER BY l.created_at ASC, l.from_task_id ASC`,
		workspaceID,
		taskID,
		model.TaskLinkBlocks,
		model.TaskStatusResolved,
		model.TaskStatusRunning,
		model.TaskKindCoordination,
	)
	if err != nil {
		return nil, fmt.Errorf("query unresolved workspace task dependency ids: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var dependencyTaskID string
		if err := rows.Scan(&dependencyTaskID); err != nil {
			return nil, fmt.Errorf("scan unresolved workspace task dependency id: %w", err)
		}
		if dependencyTaskID = strings.TrimSpace(dependencyTaskID); dependencyTaskID != "" {
			out = append(out, dependencyTaskID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unresolved workspace task dependency ids: %w", err)
	}
	return out, nil
}

func (s *Store) addWorkspaceTaskLinkTx(ctx context.Context, tx *sql.Tx, input WorkspaceTaskLinkInput, now string) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	fromTaskID := strings.TrimSpace(input.FromTaskID)
	if fromTaskID == "" {
		return errors.New("from_task_id is required")
	}
	toTaskID := strings.TrimSpace(input.ToTaskID)
	if toTaskID == "" {
		return errors.New("to_task_id is required")
	}
	if fromTaskID == toTaskID {
		return errors.New("task link endpoints must differ")
	}
	linkType := strings.ToUpper(strings.TrimSpace(input.LinkType))
	if linkType == "" {
		linkType = model.TaskLinkRelatesTo
	}
	if !model.ValidTaskLinkType(linkType) {
		return fmt.Errorf("invalid task link type: %s", linkType)
	}
	createdBy := strings.TrimSpace(input.CreatedBy)
	if createdBy == "" {
		return errors.New("created_by is required")
	}

	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return err
	}
	if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, fromTaskID); err != nil {
		return err
	}
	if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, toTaskID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO workspace_task_links(workspace_id, from_task_id, to_task_id, link_type, created_by, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		workspaceID,
		fromTaskID,
		toTaskID,
		linkType,
		createdBy,
		now,
	); err != nil {
		return fmt.Errorf("insert workspace task link: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		return fmt.Errorf("touch workspace after task link: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "workspace_task_linked",
		EntityType: "workspace_task_link",
		EntityID:   workspaceID + "/" + fromTaskID + "->" + toTaskID + ":" + linkType,
		ActorID:    createdBy,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id": workspaceID,
			"from_task_id": fromTaskID,
			"to_task_id":   toTaskID,
			"link_type":    linkType,
		}),
	}); err != nil {
		return err
	}

	return nil
}

func (s *Store) ensureProjectLinkedTaskCanAttachToWorkspaceTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) error {
	var projectID string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(project_id, '') FROM tasks WHERE task_id = ?`, strings.TrimSpace(taskID)).Scan(&projectID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTaskNotFound
		}
		return fmt.Errorf("query task project before workspace attach: %w", err)
	}
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil
	}
	if err := s.ensureTaskProjectInWorkspaceTx(ctx, tx, workspaceID, projectID); err != nil {
		return err
	}
	var otherWorkspaceCount int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1)
		   FROM workspace_tasks
		  WHERE task_id = ?
		    AND workspace_id <> ?`,
		strings.TrimSpace(taskID),
		strings.TrimSpace(workspaceID),
	).Scan(&otherWorkspaceCount); err != nil {
		return fmt.Errorf("query project task existing workspace attachments: %w", err)
	}
	if otherWorkspaceCount > 0 {
		return fmt.Errorf("%w: project-linked task %s cannot attach to multiple workspaces", ErrTaskWorkspaceAmbiguous, strings.TrimSpace(taskID))
	}
	return nil
}

func (s *Store) ListWorkspaceTaskLinks(ctx context.Context, filter WorkspaceTaskLinkFilter) ([]WorkspaceTaskLinkRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}

	clauses := []string{"workspace_id = ?"}
	args := []any{workspaceID}
	if taskID := strings.TrimSpace(filter.TaskID); taskID != "" {
		clauses = append(clauses, "(from_task_id = ? OR to_task_id = ?)")
		args = append(args, taskID, taskID)
	}
	if linkType := strings.ToUpper(strings.TrimSpace(filter.LinkType)); linkType != "" {
		clauses = append(clauses, "link_type = ?")
		args = append(args, linkType)
	}

	query := `
SELECT workspace_id, from_task_id, to_task_id, link_type, created_by, created_at
FROM workspace_task_links
WHERE ` + strings.Join(clauses, " AND ") + `
ORDER BY created_at DESC, from_task_id, to_task_id
LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query workspace task links: %w", err)
	}
	defer rows.Close()

	out := []WorkspaceTaskLinkRecord{}
	for rows.Next() {
		var row WorkspaceTaskLinkRecord
		if err := rows.Scan(
			&row.WorkspaceID,
			&row.FromTaskID,
			&row.ToTaskID,
			&row.LinkType,
			&row.CreatedBy,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workspace task link: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace task links: %w", err)
	}
	return out, nil
}

func loadTaskClaimTransitionSnapshotTx(ctx context.Context, tx *sql.Tx, taskID, workspaceID string) (taskClaimTransitionSnapshot, bool, error) {
	var snapshot taskClaimTransitionSnapshot
	err := tx.QueryRowContext(
		ctx,
		`SELECT agent_id, claim_status, summary, claimed_at, released_at,
		        COALESCE(project_role_id, ''), COALESCE(repo_id, ''), COALESCE(checkout_id, ''),
		        COALESCE(branch_id, ''), COALESCE(write_scope_json, ''), updated_at
		   FROM task_claims
		  WHERE task_id = ? AND workspace_id = ?`,
		taskID,
		workspaceID,
	).Scan(
		&snapshot.AgentID,
		&snapshot.ClaimStatus,
		&snapshot.Summary,
		&snapshot.ClaimedAt,
		&snapshot.ReleasedAt,
		&snapshot.ProjectRoleID,
		&snapshot.RepoID,
		&snapshot.CheckoutID,
		&snapshot.BranchID,
		&snapshot.WriteScopeJSON,
		&snapshot.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return taskClaimTransitionSnapshot{}, false, nil
		}
		return taskClaimTransitionSnapshot{}, false, fmt.Errorf("query task claim transition snapshot: %w", err)
	}
	snapshot.AgentID = strings.TrimSpace(snapshot.AgentID)
	snapshot.ClaimStatus = strings.TrimSpace(snapshot.ClaimStatus)
	snapshot.Summary = strings.TrimSpace(snapshot.Summary)
	snapshot.ClaimedAt = strings.TrimSpace(snapshot.ClaimedAt)
	snapshot.ProjectRoleID = strings.TrimSpace(snapshot.ProjectRoleID)
	snapshot.RepoID = strings.TrimSpace(snapshot.RepoID)
	snapshot.CheckoutID = strings.TrimSpace(snapshot.CheckoutID)
	snapshot.BranchID = strings.TrimSpace(snapshot.BranchID)
	snapshot.WriteScopeJSON = strings.TrimSpace(snapshot.WriteScopeJSON)
	snapshot.UpdatedAt = strings.TrimSpace(snapshot.UpdatedAt)
	if snapshot.ReleasedAt.Valid {
		snapshot.ReleasedAt.String = strings.TrimSpace(snapshot.ReleasedAt.String)
	}
	return snapshot, true, nil
}

func loadTaskTransitionSnapshotTx(ctx context.Context, tx *sql.Tx, taskID string) (taskTransitionSnapshot, bool, error) {
	var snapshot taskTransitionSnapshot
	err := tx.QueryRowContext(
		ctx,
		`SELECT status, COALESCE(project_id, ''), updated_at
		   FROM tasks
		  WHERE task_id = ?`,
		taskID,
	).Scan(&snapshot.Status, &snapshot.ProjectID, &snapshot.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return taskTransitionSnapshot{}, false, nil
		}
		return taskTransitionSnapshot{}, false, fmt.Errorf("query task transition snapshot: %w", err)
	}
	snapshot.Status = strings.TrimSpace(snapshot.Status)
	snapshot.ProjectID = strings.TrimSpace(snapshot.ProjectID)
	snapshot.UpdatedAt = strings.TrimSpace(snapshot.UpdatedAt)
	return snapshot, true, nil
}

func loadTaskSessionTransitionSnapshotTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string) (taskSessionTransitionSnapshot, bool, error) {
	var snapshot taskSessionTransitionSnapshot
	err := tx.QueryRowContext(
		ctx,
		`SELECT session_id, agent_id, status, started_at
		   FROM agent_sessions
		  WHERE workspace_id = ? AND task_id = ?
		    AND agent_id = ?
		  ORDER BY started_at DESC, session_id DESC
		  LIMIT 1`,
		workspaceID,
		taskID,
		strings.TrimSpace(agentID),
	).Scan(
		&snapshot.SessionID,
		&snapshot.AgentID,
		&snapshot.Status,
		&snapshot.StartedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return taskSessionTransitionSnapshot{}, false, nil
		}
		return taskSessionTransitionSnapshot{}, false, fmt.Errorf("query task session transition snapshot: %w", err)
	}
	snapshot.SessionID = strings.TrimSpace(snapshot.SessionID)
	snapshot.AgentID = strings.TrimSpace(snapshot.AgentID)
	snapshot.Status = strings.TrimSpace(snapshot.Status)
	snapshot.StartedAt = strings.TrimSpace(snapshot.StartedAt)
	return snapshot, true, nil
}

func ensureTaskSessionAuthorityForTransitionTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID, transitionKind string) error {
	snapshot, ok, err := loadTaskSessionTransitionSnapshotTx(ctx, tx, workspaceID, taskID, agentID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	agentID = strings.TrimSpace(agentID)
	transitionKind = strings.TrimSpace(transitionKind)
	if model.IsSessionStatusActive(snapshot.Status) {
		if snapshot.AgentID != agentID {
			return ErrTaskClaimStaleTransition
		}
		return nil
	}
	if snapshot.AgentID != agentID {
		return ErrTaskClaimStaleTransition
	}
	switch transitionKind {
	case "reclaim_release", "block":
		return nil
	}
	return ErrTaskClaimStaleTransition
}

func isTerminalTaskClaimStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case model.TaskClaimStatusCompleted, model.TaskClaimStatusFailed, model.TaskClaimStatusCancelled:
		return true
	default:
		return false
	}
}

func (s *Store) ensureProjectClaimRepairTaskClaimableTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID, now string) error {
	var projectID, projectLane, title, description, tagsJSON string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(t.project_id, ''), COALESCE(t.project_lane, ''), COALESCE(t.title, ''), COALESCE(t.description, ''), COALESCE(t.tags_json, '[]')
  FROM workspace_tasks wt
  JOIN tasks t ON t.task_id = wt.task_id
 WHERE wt.workspace_id = ? AND wt.task_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(taskID),
	).Scan(&projectID, &projectLane, &title, &description, &tagsJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrWorkspaceTaskAbsent
		}
		return fmt.Errorf("load project claim repair task guard: %w", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:      strings.TrimSpace(taskID),
		ProjectID:   strings.TrimSpace(projectID),
		ProjectLane: strings.TrimSpace(projectLane),
		Title:       strings.TrimSpace(title),
		Description: strings.TrimSpace(description),
		Tags:        parseTaskTagsJSON(tagsJSON),
	}
	if !projectStrategicLeadCoordinationTask(task) {
		return nil
	}
	taskLabel := "project claim repair task"
	if projectRoleScopeTask(task) {
		taskLabel = "project role/scope coordination task"
	} else if projectRepositoryRepairTask(task) {
		taskLabel = "project repository repair task"
	}
	if task.ProjectID == "" {
		return fmt.Errorf("%w: %s %s requires project_id", ErrTaskClaimConflict, taskLabel, task.TaskID)
	}
	ok, err := s.agentMayClaimProjectClaimRepairTaskTx(ctx, tx, strings.TrimSpace(workspaceID), task.ProjectID, strings.TrimSpace(agentID), strings.TrimSpace(now))
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %s %s may only be claimed by active strategic lead or a strategy/synthesis backstop after lead staleness for project %s", ErrTaskClaimConflict, taskLabel, task.TaskID, task.ProjectID)
	}
	return nil
}

func (s *Store) ensureProjectStrategyTaskClaimableTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID, now string) error {
	var projectID, projectLane, title, description, taskKind, taskTemplate, taskClass, tagsJSON string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(t.project_id, ''), COALESCE(t.project_lane, ''), COALESCE(t.title, ''), COALESCE(t.description, ''),
       COALESCE(t.task_kind, ''), COALESCE(t.task_template, ''), COALESCE(t.task_class, ''), COALESCE(t.tags_json, '[]')
  FROM workspace_tasks wt
  JOIN tasks t ON t.task_id = wt.task_id
 WHERE wt.workspace_id = ? AND wt.task_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(taskID),
	).Scan(&projectID, &projectLane, &title, &description, &taskKind, &taskTemplate, &taskClass, &tagsJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrWorkspaceTaskAbsent
		}
		return fmt.Errorf("load project strategy task guard: %w", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:       strings.TrimSpace(taskID),
		ProjectID:    strings.TrimSpace(projectID),
		ProjectLane:  strings.TrimSpace(projectLane),
		Title:        strings.TrimSpace(title),
		Description:  strings.TrimSpace(description),
		TaskKind:     strings.TrimSpace(taskKind),
		TaskTemplate: strings.TrimSpace(taskTemplate),
		TaskClass:    strings.TrimSpace(taskClass),
		Tags:         parseTaskTagsJSON(tagsJSON),
	}
	if !agentWorkTaskRequiresStrategyProfile(task) {
		return nil
	}
	agentID = strings.TrimSpace(agentID)
	agent, err := getAgentIdentityForProjectClaimRepairTx(ctx, tx, workspaceID, agentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return fmt.Errorf("%w: strategy/root task %s requires a registered strategy-capable agent", ErrTaskClaimConflict, task.TaskID)
		}
		return err
	}
	profile, err := s.getAgentProfileTx(ctx, tx, workspaceID, agentID)
	if err != nil {
		return err
	}
	profile = agentWorkProfileWithAgentFallback(profile, agent)
	if !agentProfileAllowsStrategyTaskSelection(profile) {
		return fmt.Errorf("%w: strategy/root task %s is reserved for a strategy-capable agent; fresh-selection mode %s is not eligible", ErrTaskClaimConflict, task.TaskID, agentProfileFreshSelectionMode(profile))
	}
	if task.ProjectID == "" {
		return nil
	}
	lead, ok, err := s.getActiveProjectStrategicLeadTx(ctx, tx, strings.TrimSpace(workspaceID), task.ProjectID, now)
	if err != nil {
		return err
	}
	if !ok || strings.TrimSpace(lead.AgentID) == agentID {
		return nil
	}
	fresh, err := projectClaimRepairAgentFreshTx(ctx, tx, strings.TrimSpace(workspaceID), lead.AgentID, now)
	if err != nil {
		return err
	}
	if fresh {
		return fmt.Errorf("%w: strategy/root task %s is sticky to active strategic lead %s for project %s", ErrTaskClaimConflict, task.TaskID, strings.TrimSpace(lead.AgentID), task.ProjectID)
	}
	canBackstop, err := s.agentCanBackstopProjectClaimRepairTx(ctx, tx, strings.TrimSpace(workspaceID), agentID)
	if err != nil {
		return err
	}
	if !canBackstop {
		return fmt.Errorf("%w: strategy/root task %s may only be claimed by active strategic lead or a strategy/synthesis backstop after lead staleness for project %s", ErrTaskClaimConflict, task.TaskID, task.ProjectID)
	}
	return nil
}

func (s *Store) ensureProjectPatchQueueReviewTaskClaimableTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID, now string) error {
	var projectID, projectLane, title, description, taskKind, taskTemplate, taskClass, tagsJSON string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(t.project_id, ''), COALESCE(t.project_lane, ''), COALESCE(t.title, ''), COALESCE(t.description, ''), COALESCE(t.task_kind, ''), COALESCE(t.task_template, ''), COALESCE(t.task_class, ''), COALESCE(t.tags_json, '[]')
  FROM workspace_tasks wt
  JOIN tasks t ON t.task_id = wt.task_id
 WHERE wt.workspace_id = ? AND wt.task_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(taskID),
	).Scan(&projectID, &projectLane, &title, &description, &taskKind, &taskTemplate, &taskClass, &tagsJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrWorkspaceTaskAbsent
		}
		return fmt.Errorf("load project patch queue review task guard: %w", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:       strings.TrimSpace(taskID),
		ProjectID:    strings.TrimSpace(projectID),
		ProjectLane:  strings.TrimSpace(projectLane),
		Title:        strings.TrimSpace(title),
		Description:  strings.TrimSpace(description),
		TaskKind:     strings.TrimSpace(taskKind),
		TaskTemplate: strings.TrimSpace(taskTemplate),
		TaskClass:    strings.TrimSpace(taskClass),
		Tags:         parseTaskTagsJSON(tagsJSON),
	}
	if !agentWorkPatchQueueReviewTask(task) {
		return nil
	}
	return s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, strings.TrimSpace(workspaceID), task.ProjectID, strings.TrimSpace(agentID), "agent", strings.TrimSpace(now))
}

func (s *Store) agentMayClaimProjectClaimRepairTaskTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, agentID, now string) (bool, error) {
	lead, ok, err := s.getActiveProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now)
	if err != nil {
		return false, err
	}
	if ok && strings.TrimSpace(lead.AgentID) == strings.TrimSpace(agentID) {
		return true, nil
	}
	if ok {
		fresh, err := projectClaimRepairAgentFreshTx(ctx, tx, workspaceID, lead.AgentID, now)
		if err != nil {
			return false, err
		}
		if fresh {
			return false, nil
		}
	}
	if canRecover, err := agentHasRecoverableProjectStrategicLeadRoleTx(ctx, tx, workspaceID, agentID, projectID); err != nil || canRecover {
		return canRecover, err
	}
	return s.agentCanBackstopProjectClaimRepairTx(ctx, tx, workspaceID, agentID)
}

func agentHasRecoverableProjectStrategicLeadRoleTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID, projectID string) (bool, error) {
	agentID = strings.TrimSpace(agentID)
	projectID = strings.TrimSpace(projectID)
	if agentID == "" || projectID == "" {
		return false, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT role_id, workspace_id, project_id, agent_id, role_type, status, write_scope_json, lease_token,
       lease_expires_at, summary, claimed_at, released_at, updated_by, created_at, updated_at
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND role_type = ?
 ORDER BY updated_at DESC`,
		strings.TrimSpace(workspaceID), projectID, agentID, ProjectRoleStrategicLead)
	if err != nil {
		return false, fmt.Errorf("list recoverable strategic lead roles for claim repair: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		role, err := scanProjectRole(rows)
		if err != nil {
			return false, err
		}
		if projectRoleIsRecoverableStrategicLead(role) {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (s *Store) agentCanBackstopProjectClaimRepairTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID string) (bool, error) {
	agent, err := getAgentIdentityForProjectClaimRepairTx(ctx, tx, workspaceID, agentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return false, nil
		}
		return false, err
	}
	profile, err := s.getAgentProfileTx(ctx, tx, workspaceID, agentID)
	if err != nil {
		return false, err
	}
	profile = agentWorkProfileWithAgentFallback(profile, agent)
	if !agentProfileAllowsAutonomousExecution(profile) {
		return false, nil
	}
	switch agentProfileFreshSelectionMode(profile) {
	case "strategy", "synthesis":
		return true, nil
	default:
		return false, nil
	}
}

func getAgentIdentityForProjectClaimRepairTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID string) (AgentRecord, error) {
	var record AgentRecord
	var lastSeen sql.NullString
	err := tx.QueryRowContext(ctx, `
SELECT agent_id, workspace_id, role, status, last_seen_at
  FROM agents
 WHERE workspace_id = ? AND agent_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(agentID),
	).Scan(&record.AgentID, &record.WorkspaceID, &record.Role, &record.Status, &lastSeen)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AgentRecord{}, ErrAgentNotFound
		}
		return AgentRecord{}, err
	}
	if lastSeen.Valid {
		value := lastSeen.String
		record.LastSeenAt = &value
	}
	record.IsOnline = computeIsOnline(record.LastSeenAt)
	return record, nil
}

func projectClaimRepairAgentFreshTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID, now string) (bool, error) {
	var lastSeen sql.NullString
	if err := tx.QueryRowContext(ctx, `
SELECT last_seen_at
  FROM agents
 WHERE workspace_id = ? AND agent_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(agentID),
	).Scan(&lastSeen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if !lastSeen.Valid || strings.TrimSpace(lastSeen.String) == "" {
		return false, nil
	}
	seenAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(lastSeen.String))
	if err != nil {
		return false, nil
	}
	referenceAt := time.Now().UTC()
	if parsedNow, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(now)); err == nil {
		referenceAt = parsedNow
	}
	return referenceAt.Sub(seenAt) < projectClaimRepairLeadStaleAfter, nil
}

func (s *Store) ClaimTaskWithEvent(ctx context.Context, input TaskClaimInput) (RuntimeEventRecord, error) {
	return s.claimTaskWithEvent(ctx, input)
}

func (s *Store) ClaimTask(ctx context.Context, input TaskClaimInput) error {
	_, err := s.claimTaskWithEvent(ctx, input)
	return err
}

func (s *Store) claimTaskWithEvent(ctx context.Context, input TaskClaimInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return RuntimeEventRecord{}, errors.New("task_id is required")
	}
	// SA-3: canonicalize a case/alias-variant task_id to the stored id (best-effort; keep original
	// on miss so attach-on-claim and the real not-found error still work).
	if resolved, rerr := s.resolveTaskIDWithQuerier(ctx, s.db, workspaceID, taskID); rerr == nil {
		taskID = resolved
		input.TaskID = resolved
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return RuntimeEventRecord{}, errors.New("agent_id is required")
	}
	frontierGenerationID := strings.TrimSpace(input.FrontierGenerationID)
	selfFitSummary := strings.TrimSpace(input.SelfFitSummary)
	if input.SelectedFromFrontier {
		if frontierGenerationID == "" {
			return RuntimeEventRecord{}, fmt.Errorf("%w: frontier_generation_id is required when selected_from_frontier is true", ErrTaskClaimAdmissionInvalid)
		}
		if selfFitSummary == "" {
			return RuntimeEventRecord{}, fmt.Errorf("%w: self_fit_summary is required when selected_from_frontier is true", ErrTaskClaimAdmissionInvalid)
		}
	}
	task, err := s.getWorkspaceTaskRecord(ctx, workspaceID, taskID)
	if err != nil {
		if !errors.Is(err, ErrWorkspaceTaskAbsent) {
			return RuntimeEventRecord{}, err
		}
	} else {
		superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task)
		if err != nil {
			return RuntimeEventRecord{}, fmt.Errorf("check task superseded before claim: %w", err)
		}
		if superseded {
			return RuntimeEventRecord{}, fmt.Errorf("%w: task %s is superseded by newer project evidence", ErrTaskClaimAdmissionInvalid, taskID)
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin task claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var runtimeEvent RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, agentID); err != nil {
			return err
		}
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
			return err
		}
		taskSnapshot, ok, err := loadTaskTransitionSnapshotTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrWorkspaceTaskAbsent
		}
		if err := s.rejectForeignAgentOwnedTaskClaimTx(ctx, tx, workspaceID, taskID, agentID); err != nil {
			return err
		}
		switch strings.TrimSpace(taskSnapshot.Status) {
		case model.TaskStatusPending, model.TaskStatusRunning:
		default:
			return ErrTaskClaimStaleTransition
		}
		// CA-08: the supersede gate above runs on the read pool BEFORE this fenced tx
		// acquired the write lock, so a supersede committed during the admission window
		// would otherwise be missed and the claim would commit onto a now-superseded
		// task. Re-validate here: BeginTxImmediate already holds the write lock, so any
		// supersede-driving mutation has either committed (and is visible to this fresh
		// read) or is serialized after this commit. Skipped only when the pre-tx task
		// record was absent (just-created race), preserving prior behavior.
		if strings.TrimSpace(task.TaskID) != "" {
			if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task); err != nil {
				return fmt.Errorf("re-check task superseded in claim tx: %w", err)
			} else if superseded {
				return fmt.Errorf("%w: task %s is superseded by newer project evidence", ErrTaskClaimAdmissionInvalid, taskID)
			}
		}
		// CA-01 (targeted): if this agent already holds a live, un-consumed 'selected'
		// frontier generation for this task, a claim must reference it rather than
		// bypassing the frontier-evidence gate with selected_from_frontier=false. This
		// closes the select-then-bypass hole without touching lanes that never enter
		// the frontier flow (direct/role/repair claims have no 'selected' generation, so
		// this guard never fires for them). Full class-based frontier mandating for
		// tasks that were never selected is a broader, higher-risk change tracked in the
		// coordination audit tracker.
		if !input.SelectedFromFrontier {
			var pendingSelected int
			if err := tx.QueryRowContext(ctx,
				`SELECT COUNT(1) FROM agent_task_frontiers WHERE workspace_id = ? AND agent_id = ? AND selected_task_id = ? AND decision_state = ?`,
				workspaceID, agentID, taskID, taskFrontierDecisionSelected,
			).Scan(&pendingSelected); err != nil {
				return fmt.Errorf("check pending frontier selection: %w", err)
			}
			if pendingSelected > 0 {
				return fmt.Errorf("%w: task %s has a live frontier selection for agent %s; claim must reference its generation via selected_from_frontier", ErrTaskClaimAdmissionInvalid, taskID, agentID)
			}
		}
		if input.SelectedFromFrontier {
			if err := s.ensureTaskFrontierClaimEvidenceTx(ctx, tx, workspaceID, agentID, taskID, frontierGenerationID); err != nil {
				return err
			}
		}
		unresolvedDependencies, err := unresolvedWorkspaceTaskDependencyIDsWithQuerier(ctx, tx, workspaceID, taskID)
		if err != nil {
			return err
		}
		if len(unresolvedDependencies) > 0 {
			dependencyTasks, err := s.workspaceTaskRecordsByIDsWithQuerier(ctx, tx, workspaceID, unresolvedDependencies)
			if err != nil {
				return err
			}
			dependencyTasks[taskID] = task
			taskDependencyBlocks, err := s.filterSupersededAgentWorkDependencyBlocks(ctx, workspaceID, dependencyTasks, map[string][]string{
				taskID: unresolvedDependencies,
			})
			if err != nil {
				return err
			}
			unresolvedDependencies = unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, taskID)
		}
		if len(unresolvedDependencies) > 0 {
			return fmt.Errorf("%w: task %s is blocked by unresolved dependency task(s): %s", ErrTaskClaimAdmissionInvalid, taskID, strings.Join(unresolvedDependencies, ", "))
		}
		if err := s.ensureProjectStrategyTaskClaimableTx(ctx, tx, workspaceID, taskID, agentID, now); err != nil {
			return err
		}
		if err := s.ensureProjectClaimRepairTaskClaimableTx(ctx, tx, workspaceID, taskID, agentID, now); err != nil {
			return err
		}
		if err := s.ensureProjectPatchQueueReviewTaskClaimableTx(ctx, tx, workspaceID, taskID, agentID, now); err != nil {
			return err
		}
		admission, err := s.validateTaskClaimProjectAdmissionTx(ctx, tx, workspaceID, taskID, agentID, input)
		if err != nil {
			return err
		}

		snapshot, ok, err := loadTaskClaimTransitionSnapshotTx(ctx, tx, taskID, workspaceID)
		if err != nil {
			return err
		}
		if strings.TrimSpace(taskSnapshot.Status) == model.TaskStatusRunning {
			if !ok || snapshot.ClaimStatus == model.TaskClaimStatusReleased {
				return ErrTaskClaimStaleTransition
			}
		}
		if ok {
			if isTerminalTaskClaimStatus(snapshot.ClaimStatus) {
				return ErrTaskClaimStaleTransition
			}
			if snapshot.ClaimStatus == model.TaskClaimStatusClaimed {
				if snapshot.AgentID == agentID {
					return ErrTaskClaimStaleTransition
				}
				return ErrTaskClaimConflict
			}
			if snapshot.ClaimStatus == model.TaskClaimStatusBlocked && snapshot.AgentID != agentID {
				return ErrTaskClaimConflict
			}
			res, err := tx.ExecContext(
				ctx,
				`UPDATE task_claims
				 SET workspace_id = ?, agent_id = ?, claim_status = ?, summary = ?, released_at = NULL,
				     project_role_id = ?, repo_id = ?, checkout_id = ?, branch_id = ?, write_scope_json = ?,
				     updated_at = ?, claimed_at = ?
				 WHERE task_id = ? AND workspace_id = ? AND updated_at = ?`,
				workspaceID,
				agentID,
				model.TaskClaimStatusClaimed,
				strings.TrimSpace(input.Summary),
				admission.ProjectRoleID,
				admission.RepoID,
				admission.CheckoutID,
				admission.BranchID,
				admission.WriteScopeJSON,
				now,
				now,
				taskID,
				workspaceID,
				snapshot.UpdatedAt,
			)
			if err != nil {
				return fmt.Errorf("update task claim: %w", err)
			}
			if affected, _ := res.RowsAffected(); affected == 0 {
				return ErrTaskClaimStaleTransition
			}
			// CA-20: a same-owner BLOCKED->CLAIMED self-reclaim upgrades the claim via
			// the generic CAS above but previously left the task_claim_blockers snapshot
			// in place, orphaning the blocker/operator-queue projection against a now
			// live CLAIMED+RUNNING task. Clear it on resume from a self-owned BLOCKED
			// claim (mirrors the reaper's clearTaskClaimBlockerSnapshotTx path).
			if snapshot.ClaimStatus == model.TaskClaimStatusBlocked && snapshot.AgentID == agentID {
				if err := clearTaskClaimBlockerSnapshotTx(ctx, tx, taskID, workspaceID); err != nil {
					return err
				}
			}
		} else {
			if _, err := tx.ExecContext(
				ctx,
				`INSERT INTO task_claims(
				     task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at,
				     project_role_id, repo_id, checkout_id, branch_id, write_scope_json, updated_at
				   )
				 VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)`,
				taskID,
				workspaceID,
				agentID,
				model.TaskClaimStatusClaimed,
				strings.TrimSpace(input.Summary),
				now,
				admission.ProjectRoleID,
				admission.RepoID,
				admission.CheckoutID,
				admission.BranchID,
				admission.WriteScopeJSON,
				now,
			); err != nil {
				return fmt.Errorf("insert task claim: %w", err)
			}
		}
		if err := s.bindTaskClaimProjectAdmissionTx(ctx, tx, workspaceID, taskID, agentID, admission, now); err != nil {
			return err
		}

		taskStatusResult, err := tx.ExecContext(
			ctx,
			`UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ? AND status = ?`,
			model.TaskStatusRunning,
			now,
			taskID,
			model.TaskStatusPending,
		)
		if err != nil {
			return fmt.Errorf("set task running after claim: %w", err)
		}
		if strings.TrimSpace(taskSnapshot.Status) == model.TaskStatusPending {
			if affected, _ := taskStatusResult.RowsAffected(); affected != 1 {
				return ErrTaskClaimStaleTransition
			}
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after claim: %w", err)
		}

		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "task_claimed",
			EntityType: "task_claim",
			EntityID:   taskID,
			ActorID:    agentID,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":           workspaceID,
				"task_id":                taskID,
				"agent_id":               agentID,
				"summary":                strings.TrimSpace(input.Summary),
				"selected_from_frontier": input.SelectedFromFrontier,
				"frontier_generation_id": frontierGenerationID,
				"self_fit_summary":       selfFitSummary,
			}),
		}); err != nil {
			return err
		}
		runtimePayload, err := attachAgentTaskPromptContextEnvelope(map[string]any{
			"workspace_id":           workspaceID,
			"task_id":                taskID,
			"agent_id":               agentID,
			"claim_status":           model.TaskClaimStatusClaimed,
			"summary":                strings.TrimSpace(input.Summary),
			"selected_from_frontier": input.SelectedFromFrontier,
			"frontier_generation_id": frontierGenerationID,
			"self_fit_summary":       selfFitSummary,
		}, input.PromptContextEnvelope, "agent.task.claim", workspaceID, taskID, agentID)
		if err != nil {
			return err
		}
		addTaskClaimAdmissionPayload(runtimePayload, admission)
		var runtimeEventErr error
		runtimeEvent, runtimeEventErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "task.claimed",
			EntityType:  "task",
			EntityID:    taskID,
			ActorType:   "agent",
			ActorID:     agentID,
			AgentID:     agentID,
			TaskID:      taskID,
			PayloadJSON: mustJSON(runtimePayload),
			CreatedAt:   now,
		})
		if runtimeEventErr != nil {
			return runtimeEventErr
		}
		if input.SelectedFromFrontier {
			if err := s.consumeTaskFrontierClaimEvidenceTx(ctx, tx, workspaceID, agentID, taskID, frontierGenerationID, runtimeEvent.EventID, selfFitSummary, now); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit task claim tx: %w", err)
	}
	return runtimeEvent, nil
}

func (s *Store) rejectForeignAgentOwnedTaskClaimTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || taskID == "" || agentID == "" {
		return nil
	}
	var ownerID, projectID, requirementsJSON string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(owner_user_id, ''), COALESCE(project_id, ''), COALESCE(task_requirements_json, '{}') FROM tasks WHERE task_id = ?`, taskID).Scan(&ownerID, &projectID, &requirementsJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load task owner for claim admission: %w", err)
	}
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" || ownerID == "system" || ownerID == agentID {
		return nil
	}
	if !taskOwnerUserIDIsHardClaimRoute(projectID, requirementsJSON) {
		return nil
	}
	if allowed, err := s.taskOwnerBoundRequiredAgentMayClaimDespiteOwnerUserIDTx(ctx, tx, workspaceID, taskID, agentID, projectID); err != nil {
		return err
	} else if allowed {
		return nil
	}
	var registered int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM agents WHERE workspace_id = ? AND agent_id = ?`, workspaceID, ownerID).Scan(&registered); err != nil {
		return fmt.Errorf("check task owner agent binding for claim admission: %w", err)
	}
	if registered > 0 {
		return fmt.Errorf("%w: task %s is assigned to agent %s; agent %s cannot claim it", ErrTaskClaimAdmissionInvalid, taskID, ownerID, agentID)
	}
	return nil
}

func (s *Store) taskOwnerBoundRequiredAgentMayClaimDespiteOwnerUserIDTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID, projectID string) (bool, error) {
	req, ok, err := s.taskClaimOwnerBoundRequirementTx(ctx, tx, workspaceID, taskID, taskClaimProjectContext{ProjectID: strings.TrimSpace(projectID)})
	if err != nil || !ok {
		return false, err
	}
	if req.RepairNeeded || strings.TrimSpace(req.RequiredAgentID) == "" {
		return false, nil
	}
	return strings.TrimSpace(req.RequiredAgentID) == strings.TrimSpace(agentID), nil
}

func taskOwnerUserIDIsHardClaimRoute(projectID, requirementsJSON string) bool {
	task := WorkspaceTaskRecord{
		ProjectID:            strings.TrimSpace(projectID),
		TaskRequirementsJSON: normalizeTaskRequirementsJSON(requirementsJSON),
	}
	if !agentWorkPatchQueueDecisionContinuationTask(task) {
		return false
	}
	if taskOwnerUserIDRouteIsRoleRoutedPatchQueueIntegration(task) {
		return false
	}
	return true
}

func taskOwnerUserIDRouteIsRoleRoutedPatchQueueIntegration(task WorkspaceTaskRecord) bool {
	if !strings.EqualFold(agentWorkTaskRequirementString(task, "patch_queue_task_kind"), "integration") {
		return false
	}
	return strings.EqualFold(agentWorkTaskRequirementString(task, "required_project_role"), ProjectRoleIntegrator) &&
		strings.EqualFold(agentWorkTaskRequirementString(task, "required_tool"), "project_patch_queue_integrate")
}

func (s *Store) ReleaseTaskClaimWithEvent(ctx context.Context, input TaskReleaseInput) (RuntimeEventRecord, error) {
	return s.releaseTaskClaimWithEvent(ctx, input)
}

func (s *Store) ReleaseTaskClaim(ctx context.Context, input TaskReleaseInput) error {
	_, err := s.releaseTaskClaimWithEvent(ctx, input)
	return err
}

func (s *Store) releaseTaskClaimWithAuthorityActorTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input TaskReleaseInput, runtimeActorType, runtimeActorID, now, sessionTransitionKind string) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	taskID := strings.TrimSpace(input.TaskID)
	agentID := strings.TrimSpace(input.AgentID)
	sessionTransitionKind = strings.TrimSpace(sessionTransitionKind)
	if sessionTransitionKind == "" {
		sessionTransitionKind = "release"
	}
	actorType := strings.TrimSpace(runtimeActorType)
	if actorType == "" {
		actorType = "agent"
	}
	actorID := strings.TrimSpace(runtimeActorID)
	if actorID == "" {
		actorID = agentID
	}

	snapshot, ok, err := loadTaskClaimTransitionSnapshotTx(ctx, tx, taskID, workspaceID)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if !ok {
		return RuntimeEventRecord{}, ErrTaskClaimNotFound
	}
	if isTerminalTaskClaimStatus(snapshot.ClaimStatus) {
		return RuntimeEventRecord{}, ErrTaskClaimStaleTransition
	}
	if snapshot.AgentID != agentID {
		return RuntimeEventRecord{}, ErrTaskClaimConflict
	}
	if snapshot.ClaimStatus == model.TaskClaimStatusReleased {
		return RuntimeEventRecord{}, ErrTaskClaimStaleTransition
	}
	if err := ensureTaskSessionAuthorityForTransitionTx(ctx, tx, workspaceID, taskID, agentID, sessionTransitionKind); err != nil {
		return RuntimeEventRecord{}, err
	}
	res, err := tx.ExecContext(
		ctx,
		`UPDATE task_claims
			 SET claim_status = ?, summary = ?, released_at = ?, updated_at = ?
			 WHERE task_id = ? AND workspace_id = ? AND updated_at = ?`,
		model.TaskClaimStatusReleased,
		strings.TrimSpace(input.Reason),
		now,
		now,
		taskID,
		workspaceID,
		snapshot.UpdatedAt,
	)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("release task claim: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return RuntimeEventRecord{}, ErrTaskClaimStaleTransition
	}
	// CA-21 (accepted/intentional): the RowsAffected result is deliberately not
	// gated here. This UPDATE only returns a RUNNING task to PENDING on release; a
	// task that has already drifted to a terminal status (RESOLVED/CANCELLED) or is
	// still PENDING must be left untouched. Forcing it to PENDING would resurrect
	// terminal work. A RELEASED claim over a terminal task is a benign read-model
	// artifact: re-claimability is independently protected because claimTaskWithEvent
	// rejects any non-PENDING/RUNNING task.
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ? AND status = ?`,
		model.TaskStatusPending,
		now,
		taskID,
		model.TaskStatusRunning,
	); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("set task pending after release: %w", err)
	}
	// CA-13/CA-14: releasing/resetting a claim (this is the shared path for both the
	// explicit release and the reaper) makes any still-'selected', un-consumed
	// frontier generations for this task stale. Invalidate them so a later re-claim
	// cannot silently replay a pre-reset generation, and so a generation left dangling
	// by a lost claim CAS does not remain claim-eligible.
	if err := s.invalidateSelectedTaskFrontierGenerationsTx(ctx, tx, workspaceID, taskID, now); err != nil {
		return RuntimeEventRecord{}, err
	}
	clearOptions := taskClaimProjectAdmissionClearOptions{}
	if sessionTransitionKind == "reclaim_release" {
		clearOptions.PreserveBranchStatus = true
	}
	projectAdmissionTransition, err := clearTaskClaimProjectAdmissionTx(ctx, tx, workspaceID, taskID, agentID, model.TaskClaimStatusReleased, now, snapshot, clearOptions)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	executionRunsCancelled, err := releaseTaskExecutionRunsTx(ctx, tx, workspaceID, taskID, agentID, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	patchQueueClaimRelease, err := s.releaseProjectPatchQueueClaimsForReleasedTaskTx(ctx, tx, authority, workspaceID, taskID, agentID, actorType, actorID, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("touch workspace after release: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "task_released",
		EntityType: "task_claim",
		EntityID:   taskID,
		ActorID:    actorID,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id": workspaceID,
			"task_id":      taskID,
			"agent_id":     agentID,
			"actor_type":   actorType,
			"actor_id":     actorID,
			"reason":       strings.TrimSpace(input.Reason),
		}),
	}); err != nil {
		return RuntimeEventRecord{}, err
	}
	runtimePayload, err := attachAgentTaskPromptContextEnvelope(map[string]any{
		"workspace_id": workspaceID,
		"task_id":      taskID,
		"agent_id":     agentID,
		"claim_status": model.TaskClaimStatusReleased,
		"reason":       strings.TrimSpace(input.Reason),
	}, input.PromptContextEnvelope, "agent.task.release", workspaceID, taskID, agentID)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if projectAdmissionTransition != nil {
		runtimePayload["project_admission_transition"] = projectAdmissionTransition
	}
	if executionRunsCancelled {
		runtimePayload["execution_runs_cancelled"] = true
	}
	if !patchQueueClaimRelease.empty() {
		runtimePayload["patch_queue_claim_release"] = patchQueueClaimRelease
	}
	runtimeEvent, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "task.released",
		EntityType:  "task",
		EntityID:    taskID,
		ActorType:   actorType,
		ActorID:     actorID,
		AgentID:     agentID,
		TaskID:      taskID,
		PayloadJSON: mustJSON(runtimePayload),
		CreatedAt:   now,
	})
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return runtimeEvent, nil
}

func (s *Store) releaseTaskClaimWithEvent(ctx context.Context, input TaskReleaseInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return RuntimeEventRecord{}, errors.New("task_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return RuntimeEventRecord{}, errors.New("agent_id is required")
	}
	sessionTransitionKind := strings.TrimSpace(input.SessionTransitionKind)
	switch sessionTransitionKind {
	case "":
		sessionTransitionKind = "release"
	case "reclaim_release":
	default:
		return RuntimeEventRecord{}, fmt.Errorf("unsupported task release session_transition_kind %q", sessionTransitionKind)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin task release tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var runtimeEvent RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		runtimeEvent, innerErr = s.releaseTaskClaimWithAuthorityActorTx(ctx, tx, authority, input, "agent", agentID, now, sessionTransitionKind)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit task release tx: %w", err)
	}
	return runtimeEvent, nil
}

func (s *Store) CompleteTaskWithEvent(ctx context.Context, input TaskCompleteInput) (RuntimeEventRecord, error) {
	return s.transitionTaskClaimWithEvent(ctx, input.WorkspaceID, input.TaskID, input.AgentID,
		model.TaskClaimStatusCompleted, input.Summary, "task_completed", "agent.task.complete", input.PromptContextEnvelope)
}

func (s *Store) CompleteTask(ctx context.Context, input TaskCompleteInput) error {
	_, err := s.CompleteTaskWithEvent(ctx, input)
	return err
}

func (s *Store) enforceEmptyProductFrontierCompletionTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	if workspaceID == "" || taskID == "" {
		return nil
	}
	var task WorkspaceTaskRecord
	var tagsJSON, requirementsJSON string
	if err := tx.QueryRowContext(ctx, `
SELECT task_id, title, description, status, task_kind, task_template, COALESCE(task_class, ''),
       COALESCE(project_id, ''), COALESCE(project_lane, ''), COALESCE(tags_json, '[]'),
       COALESCE(task_requirements_json, '{}')
  FROM tasks
 WHERE task_id = ?`,
		taskID,
	).Scan(
		&task.TaskID,
		&task.Title,
		&task.Description,
		&task.Status,
		&task.TaskKind,
		&task.TaskTemplate,
		&task.TaskClass,
		&task.ProjectID,
		&task.ProjectLane,
		&tagsJSON,
		&requirementsJSON,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTaskNotFound
		}
		return fmt.Errorf("load task completion contract: %w", err)
	}
	task.Tags = parseTaskTagsJSON(tagsJSON)
	task.TaskRequirementsJSON = normalizeTaskRequirementsJSON(requirementsJSON)
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" || !agentWorkTaskIsProactiveMetacognition(task) {
		return nil
	}

	var phase string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(current_phase, '')
  FROM project_profiles
 WHERE workspace_id = ? AND project_id = ?`,
		workspaceID, projectID,
	).Scan(&phase); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load project phase for task completion contract: %w", err)
	}
	phase = strings.ToUpper(strings.TrimSpace(phase))
	if phase == ProjectPhaseDone {
		return nil
	}
	requiresEmptyOutcome := strings.EqualFold(agentWorkTaskRequirementString(task, "required_transition"), "empty_product_frontier_outcome")
	if !requiresEmptyOutcome && !projectPhaseAllowsImplementationWork(phase) {
		return nil
	}

	tasks, err := s.listWorkspaceTasksFiltered(ctx, tx, workspaceID, projectID)
	if err != nil {
		return err
	}
	for _, candidate := range tasks {
		if strings.TrimSpace(candidate.TaskID) == taskID || isTerminalTaskStatus(candidate.Status) {
			continue
		}
		if agentWorkTaskIsProactiveMetacognition(candidate) {
			continue
		}
		return nil
	}
	return fmt.Errorf("%w: empty product frontier reflection task %s cannot resolve while project %s remains %s; open a bounded non-reflection project task or transition the project to DONE",
		ErrTaskCompletionContract, taskID, projectID, firstNonEmpty(phase, "UNKNOWN"))
}

func (s *Store) BlockTaskWithEvent(ctx context.Context, input TaskBlockInput) (RuntimeEventRecord, error) {
	return s.transitionTaskClaimWithEvent(ctx, input.WorkspaceID, input.TaskID, input.AgentID,
		model.TaskClaimStatusBlocked, input.Reason, "task_blocked", "agent.task.block", input.PromptContextEnvelope)
}

func (s *Store) BlockTask(ctx context.Context, input TaskBlockInput) error {
	_, err := s.BlockTaskWithEvent(ctx, input)
	return err
}

// transitionTaskClaim is a generic helper to move a claim from CLAIMED → target status.
func (s *Store) transitionTaskClaim(ctx context.Context, workspaceID, taskID, agentID, targetStatus, summary, auditEvent string) error {
	_, err := s.transitionTaskClaimWithEvent(ctx, workspaceID, taskID, agentID, targetStatus, summary, auditEvent, "", nil)
	return err
}

func (s *Store) transitionTaskClaimWithEvent(ctx context.Context, workspaceID, taskID, agentID, targetStatus, summary, auditEvent, promptContextSurface string, promptContextEnvelope map[string]any) (RuntimeEventRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || taskID == "" || agentID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id, task_id, and agent_id are required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var runtimeEvent RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		snapshot, ok, err := loadTaskClaimTransitionSnapshotTx(ctx, tx, taskID, workspaceID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrTaskClaimNotFound
		}
		if isTerminalTaskClaimStatus(snapshot.ClaimStatus) {
			return ErrTaskClaimStaleTransition
		}
		if snapshot.AgentID != agentID {
			return ErrTaskClaimConflict
		}
		if snapshot.ClaimStatus == targetStatus {
			return ErrTaskClaimStaleTransition
		}
		if snapshot.ClaimStatus != model.TaskClaimStatusClaimed && snapshot.ClaimStatus != model.TaskClaimStatusBlocked {
			return fmt.Errorf("cannot transition from %s to %s", snapshot.ClaimStatus, targetStatus)
		}

		taskStatus := ""
		switch targetStatus {
		case model.TaskClaimStatusCompleted:
			taskStatus = model.TaskStatusResolved
		case model.TaskClaimStatusFailed:
			taskStatus = model.TaskStatusFailed
		case model.TaskClaimStatusCancelled:
			taskStatus = model.TaskStatusCancelled
		}
		taskSnapshot := taskTransitionSnapshot{}
		if taskStatus != "" {
			var taskFound bool
			taskSnapshot, taskFound, err = loadTaskTransitionSnapshotTx(ctx, tx, taskID)
			if err != nil {
				return err
			}
			if !taskFound {
				return ErrTaskNotFound
			}
			if taskSnapshot.Status != model.TaskStatusRunning {
				return ErrTaskClaimStaleTransition
			}
			if taskStatus == model.TaskStatusResolved {
				adm, err := s.evaluateTerminalAdmissionTx(ctx, tx, workspaceID, WorkspaceTaskRecord{TaskID: taskID}, TerminalWriteIntent{
					Side:       SideAdmission,
					Kind:       GenuineCompletion,
					Resolution: taskStatus,
					Origin:     OriginP02,
					ActorID:    agentID,
				})
				if err != nil {
					return err
				}
				if adm.Decision == TerminalReject {
					return adm.Err
				}
			}
		}
		sessionTransitionKind := "transition"
		if targetStatus == model.TaskClaimStatusBlocked {
			sessionTransitionKind = "block"
		}
		if err := ensureTaskSessionAuthorityForTransitionTx(ctx, tx, workspaceID, taskID, agentID, sessionTransitionKind); err != nil {
			return err
		}

		res, err := tx.ExecContext(ctx,
			`UPDATE task_claims SET claim_status = ?, summary = ?, updated_at = ? WHERE task_id = ? AND workspace_id = ? AND updated_at = ?`,
			targetStatus, strings.TrimSpace(summary), now, taskID, workspaceID, snapshot.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("update claim: %w", err)
		}
		if affected, _ := res.RowsAffected(); affected == 0 {
			return ErrTaskClaimStaleTransition
		}
		if taskStatus != "" {
			res, err := tx.ExecContext(
				ctx,
				`UPDATE tasks
				    SET status = ?, close_reason = ?, updated_at = ?
				  WHERE task_id = ? AND status = ? AND updated_at = ?`,
				taskStatus,
				strings.TrimSpace(summary),
				now,
				taskID,
				taskSnapshot.Status,
				taskSnapshot.UpdatedAt,
			)
			if err != nil {
				return fmt.Errorf("update task terminal status from claim: %w", err)
			}
			if affected, _ := res.RowsAffected(); affected == 0 {
				return ErrTaskClaimStaleTransition
			}
		}
		projectAdmissionTransition, err := clearTaskClaimProjectAdmissionTx(ctx, tx, workspaceID, taskID, agentID, targetStatus, now, snapshot, taskClaimProjectAdmissionClearOptions{})
		if err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace: %w", err)
		}

		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  auditEvent,
			EntityType: "task_claim",
			EntityID:   taskID,
			ActorID:    agentID,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id": workspaceID,
				"task_id":      taskID,
				"agent_id":     agentID,
				"summary":      strings.TrimSpace(summary),
			}),
		}); err != nil {
			return err
		}
		runtimeEventType := "task." + strings.ToLower(strings.TrimSpace(targetStatus))
		runtimePayload := map[string]any{
			"workspace_id": workspaceID,
			"task_id":      taskID,
			"agent_id":     agentID,
			"claim_status": targetStatus,
		}
		if trimmed := strings.TrimSpace(summary); trimmed != "" {
			if targetStatus == model.TaskClaimStatusBlocked {
				runtimePayload["reason"] = trimmed
			} else {
				runtimePayload["summary"] = trimmed
			}
		}
		if projectAdmissionTransition != nil {
			runtimePayload["project_admission_transition"] = projectAdmissionTransition
		}
		if taskStatus != "" {
			executionRunsClosed, err := closeTaskExecutionRunsTx(ctx, tx, workspaceID, taskID, taskStatus, now)
			if err != nil {
				return err
			}
			if executionRunsClosed {
				runtimePayload["execution_runs_closed"] = true
			}
			if err := s.resolveOpenOperatorQueuesForClosedTaskTx(ctx, tx, workspaceID, taskID, taskStatus, agentID, now); err != nil {
				return err
			}
			if strings.TrimSpace(taskSnapshot.ProjectID) != "" {
				if err := s.releaseProjectRolesIfNoOpenTasksTx(ctx, tx, workspaceID, taskSnapshot.ProjectID, agentID, summary, now); err != nil {
					return err
				}
			}
		}
		if taskStatus == "" && targetStatus == model.TaskClaimStatusBlocked {
			executionRunsClosed, err := closeTaskExecutionRunsTx(ctx, tx, workspaceID, taskID, targetStatus, now)
			if err != nil {
				return err
			}
			if executionRunsClosed {
				runtimePayload["execution_runs_closed"] = true
			}
		}
		switch targetStatus {
		case model.TaskClaimStatusCompleted:
			runtimeEventType = "task.completed"
			promptContextSurface = firstNonEmpty(strings.TrimSpace(promptContextSurface), "agent.task.complete")
		case model.TaskClaimStatusBlocked:
			runtimeEventType = "task.blocked"
			promptContextSurface = firstNonEmpty(strings.TrimSpace(promptContextSurface), "agent.task.block")
		}
		runtimePayload, err = attachAgentTaskPromptContextEnvelope(runtimePayload, promptContextEnvelope, promptContextSurface, workspaceID, taskID, agentID)
		if err != nil {
			return err
		}
		var appendErr error
		runtimeEvent, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   runtimeEventType,
			EntityType:  "task",
			EntityID:    taskID,
			ActorType:   "agent",
			ActorID:     agentID,
			AgentID:     agentID,
			TaskID:      taskID,
			PayloadJSON: mustJSON(runtimePayload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, err
	}
	return runtimeEvent, nil
}

func (s *Store) GetWorkspaceSnapshot(ctx context.Context, workspaceID string, updatesLimit int) (WorkspaceSnapshot, error) {
	workspace, err := s.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	docs, err := s.listWorkspaceDocs(ctx, workspaceID)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	agents, err := s.listWorkspaceAgents(ctx, workspaceID)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	sessions, err := s.ListWorkspaceSessionStates(ctx, workspaceID, true, 50)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	tools, err := s.listWorkspaceTools(ctx, workspaceID)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	tasks, err := s.listWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	taskLinks, err := s.listWorkspaceTaskLinks(ctx, workspaceID)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	recentMemory, err := s.ListWorkspaceMemory(ctx, WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		Limit:       updatesLimit,
	})
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	artifacts, err := s.listWorkspaceArtifacts(ctx, workspaceID, updatesLimit)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	updates, err := s.listAgentUpdates(ctx, workspaceID, updatesLimit)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	messages, err := s.ListWorkspaceMessages(ctx, workspaceID, "", 20)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	projects, err := s.listWorkspaceProjects(ctx, workspaceID)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}

	return WorkspaceSnapshot{
		Workspace:       workspace,
		Docs:            docs,
		Agents:          agents,
		Sessions:        sessions,
		Tools:           tools,
		Tasks:           tasks,
		TaskLinks:       taskLinks,
		RecentMemory:    recentMemory,
		RecentArtifacts: artifacts,
		RecentUpdates:   updates,
		RecentMessages:  messages,
		Projects:        projects,
	}, nil
}

func (s *Store) listWorkspaceDocs(ctx context.Context, workspaceID string) ([]WorkspaceDocRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT doc_key, title, content, updated_by, created_at, updated_at
		 FROM workspace_docs
		 WHERE workspace_id = ?
		 ORDER BY doc_key`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("query workspace docs: %w", err)
	}
	defer rows.Close()

	out := []WorkspaceDocRecord{}
	for rows.Next() {
		var row WorkspaceDocRecord
		if err := rows.Scan(&row.DocKey, &row.Title, &row.Content, &row.UpdatedBy, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan workspace doc: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace docs: %w", err)
	}
	return out, nil
}

// ListWorkspaceAgents returns all agents in a workspace.
func (s *Store) ListWorkspaceAgents(ctx context.Context, workspaceID string) ([]AgentRecord, error) {
	return s.listWorkspaceAgents(ctx, workspaceID)
}

func (s *Store) listWorkspaceAgents(ctx context.Context, workspaceID string) ([]AgentRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT agent_id, workspace_id, owner_user_id, display_name, role, status, protocol_version,
		        capabilities_json, summary, created_at, updated_at, last_seen_at
		 FROM agents
		 WHERE workspace_id = ?
		 ORDER BY display_name, agent_id`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("query workspace agents: %w", err)
	}
	defer rows.Close()

	out := []AgentRecord{}
	for rows.Next() {
		var row AgentRecord
		var capabilitiesJSON string
		var lastSeen sql.NullString
		if err := rows.Scan(
			&row.AgentID,
			&row.WorkspaceID,
			&row.OwnerUserID,
			&row.DisplayName,
			&row.Role,
			&row.Status,
			&row.ProtocolVersion,
			&capabilitiesJSON,
			&row.Summary,
			&row.CreatedAt,
			&row.UpdatedAt,
			&lastSeen,
		); err != nil {
			return nil, fmt.Errorf("scan workspace agent: %w", err)
		}
		row.Capabilities = decodeCapabilities(capabilitiesJSON)
		row.LastSeenAt = nullStringPtr(lastSeen)
		row.IsOnline = computeIsOnline(row.LastSeenAt)
		row.ActiveTasks = []AgentCurrentTask{} // default to empty, not nil
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace agents: %w", err)
	}

	// Load active claims (CLAIMED/BLOCKED) and map to agents.
	claimRows, err := s.db.QueryContext(ctx,
		`SELECT agent_id, task_id, claim_status, summary
		 FROM task_claims
		 WHERE workspace_id = ? AND claim_status IN ('CLAIMED', 'BLOCKED')
		 ORDER BY claimed_at`,
		workspaceID,
	)
	if err != nil {
		return out, nil // non-fatal: return agents without tasks
	}
	defer claimRows.Close()

	agentIdx := map[string]int{}
	for i, a := range out {
		agentIdx[a.AgentID] = i
	}
	for claimRows.Next() {
		var agentID, taskID, claimStatus, summary string
		if err := claimRows.Scan(&agentID, &taskID, &claimStatus, &summary); err != nil {
			continue
		}
		if idx, ok := agentIdx[agentID]; ok {
			out[idx].ActiveTasks = append(out[idx].ActiveTasks, AgentCurrentTask{
				TaskID:      taskID,
				ClaimStatus: claimStatus,
				Summary:     summary,
			})
		}
	}

	sessionStates, err := s.ListWorkspaceSessionStates(ctx, workspaceID, true, len(out)*4)
	if err == nil {
		for _, state := range sessionStates {
			if state.AgentID == "" {
				continue
			}
			if idx, ok := agentIdx[state.AgentID]; ok && out[idx].CurrentSession == nil {
				session := state
				out[idx].CurrentSession = &session
			}
		}
	}

	return out, nil
}

// WorkspaceDocSummary is a lightweight doc record without content.
type WorkspaceDocSummary struct {
	DocKey     string  `json:"doc_key"`
	Title      string  `json:"title"`
	UpdatedBy  string  `json:"updated_by"`
	UpdatedAt  string  `json:"updated_at"`
	ArchivedAt *string `json:"archived_at,omitempty"`
	ArchivedBy *string `json:"archived_by,omitempty"`
}

// ListWorkspaceDocs returns all docs in a workspace (without content).
func (s *Store) ListWorkspaceDocs(ctx context.Context, workspaceID string, includeArchived bool) ([]WorkspaceDocSummary, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}

	query := `SELECT doc_key, title, updated_by, updated_at, archived_at, archived_by
		 FROM workspace_docs
		 WHERE workspace_id = ?`
	args := []any{workspaceID}
	if !includeArchived {
		query += ` AND archived_at IS NULL`
	}
	query += ` ORDER BY updated_at DESC, doc_key`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query workspace docs: %w", err)
	}
	defer rows.Close()

	out := []WorkspaceDocSummary{}
	for rows.Next() {
		var row WorkspaceDocSummary
		if err := rows.Scan(&row.DocKey, &row.Title, &row.UpdatedBy, &row.UpdatedAt, &row.ArchivedAt, &row.ArchivedBy); err != nil {
			return nil, fmt.Errorf("scan workspace doc: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace docs: %w", err)
	}
	return out, nil
}

// contentSHA256 computes SHA256 hex hash of a string.
func (s *Store) ArchiveWorkspaceDocWithEvent(ctx context.Context, workspaceID, docKey, archivedBy string) (RuntimeEventRecord, error) {
	event, _, err := s.ArchiveWorkspaceDocWithEffects(ctx, workspaceID, docKey, archivedBy)
	return event, err
}

func (s *Store) ArchiveWorkspaceDocWithEffects(ctx context.Context, workspaceID, docKey, archivedBy string) (RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.archiveWorkspaceDocWithEffects(ctx, workspaceID, docKey, archivedBy, nil, "")
}

func (s *Store) ArchiveWorkspaceDocWithEffectsAndPromptContext(ctx context.Context, workspaceID, docKey, archivedBy string, promptContextEnvelope map[string]any) (RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.archiveWorkspaceDocWithEffects(ctx, workspaceID, docKey, archivedBy, promptContextEnvelope, "")
}

func (s *Store) ArchiveWorkspaceDocWithEffectsAndPromptContextSurface(ctx context.Context, workspaceID, docKey, archivedBy string, promptContextEnvelope map[string]any, promptContextSurface string) (RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.archiveWorkspaceDocWithEffects(ctx, workspaceID, docKey, archivedBy, promptContextEnvelope, promptContextSurface)
}

func (s *Store) ArchiveWorkspaceDoc(ctx context.Context, workspaceID, docKey, archivedBy string) error {
	_, _, err := s.archiveWorkspaceDocWithEffects(ctx, workspaceID, docKey, archivedBy, nil, "")
	return err
}

func (s *Store) archiveWorkspaceDocWithEffects(ctx context.Context, workspaceID, docKey, archivedBy string, promptContextEnvelope map[string]any, promptContextSurface string) (RuntimeEventRecord, []RuntimeEventRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	docKey = strings.TrimSpace(docKey)
	archivedBy = strings.TrimSpace(archivedBy)
	if workspaceID == "" {
		return RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	if docKey == "" {
		return RuntimeEventRecord{}, nil, errors.New("doc_key is required")
	}
	if archivedBy == "" {
		return RuntimeEventRecord{}, nil, errors.New("archived_by is required")
	}
	promptContextSurface = strings.TrimSpace(promptContextSurface)
	if promptContextSurface == "" {
		promptContextSurface = "workspace.doc.archive"
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, nil, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, nil, fmt.Errorf("begin archive workspace doc tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var runtimeEvent RuntimeEventRecord
	var invalidationEvents []RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE workspace_docs
		 SET archived_at = ?, archived_by = ?, updated_at = ?
		 WHERE workspace_id = ? AND doc_key = ? AND archived_at IS NULL`,
			now, archivedBy, now, workspaceID, docKey,
		)
		if err != nil {
			return fmt.Errorf("archive workspace doc: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("workspace doc not found or already archived: %s/%s", workspaceID, docKey)
		}
		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "workspace_doc_archived",
			EntityType: "workspace_doc",
			EntityID:   workspaceID + "/" + docKey,
			ActorID:    archivedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id": workspaceID,
				"doc_key":      docKey,
			}),
		}); err != nil {
			return err
		}
		runtimePayload, err := attachWorkspaceDocPromptContextEnvelope(map[string]any{
			"workspace_id": workspaceID,
			"doc_key":      docKey,
			"archived_by":  archivedBy,
		}, promptContextEnvelope, promptContextSurface, map[string]string{
			"workspace_id": workspaceID,
			"doc_key":      docKey,
			"archived_by":  archivedBy,
		})
		if err != nil {
			return err
		}
		runtimeEvent, err = s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "workspace_doc.archived",
			EntityType:  "workspace_doc",
			EntityID:    docKey,
			ActorID:     archivedBy,
			PayloadJSON: mustJSON(runtimePayload),
			CreatedAt:   now,
		})
		if err != nil {
			return err
		}
		invalidationEvents = make([]RuntimeEventRecord, 0, 2)
		if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, workspaceID, memoryInvalidationRefChange{
			RefKind: "workspace_doc",
			RefID:   docKey,
			Cause:   "workspace_doc.archived",
		}); err != nil {
			return err
		} else {
			invalidationEvents = append(invalidationEvents, events...)
		}
		if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, workspaceID, memoryInvalidationRefChange{
			RefKind: "segment_ref",
			RefID:   buildWorkspaceDocSegmentRef(workspaceID, docKey, "root"),
			Cause:   "workspace_doc.archived",
		}); err != nil {
			return err
		} else {
			invalidationEvents = append(invalidationEvents, events...)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, nil, err
	}
	return runtimeEvent, invalidationEvents, nil
}

func (s *Store) DeleteWorkspaceDocWithEvent(ctx context.Context, workspaceID, docKey, deletedBy string) (RuntimeEventRecord, error) {
	event, _, err := s.DeleteWorkspaceDocWithEffects(ctx, workspaceID, docKey, deletedBy)
	return event, err
}

func (s *Store) DeleteWorkspaceDocWithEffects(ctx context.Context, workspaceID, docKey, deletedBy string) (RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.deleteWorkspaceDocWithEffects(ctx, workspaceID, docKey, deletedBy, nil, "")
}

func (s *Store) DeleteWorkspaceDocWithEffectsAndPromptContext(ctx context.Context, workspaceID, docKey, deletedBy string, promptContextEnvelope map[string]any) (RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.deleteWorkspaceDocWithEffects(ctx, workspaceID, docKey, deletedBy, promptContextEnvelope, "")
}

func (s *Store) DeleteWorkspaceDocWithEffectsAndPromptContextSurface(ctx context.Context, workspaceID, docKey, deletedBy string, promptContextEnvelope map[string]any, promptContextSurface string) (RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.deleteWorkspaceDocWithEffects(ctx, workspaceID, docKey, deletedBy, promptContextEnvelope, promptContextSurface)
}

func (s *Store) DeleteWorkspaceDoc(ctx context.Context, workspaceID, docKey, deletedBy string) error {
	_, _, err := s.deleteWorkspaceDocWithEffects(ctx, workspaceID, docKey, deletedBy, nil, "")
	return err
}

func (s *Store) deleteWorkspaceDocWithEffects(ctx context.Context, workspaceID, docKey, deletedBy string, promptContextEnvelope map[string]any, promptContextSurface string) (RuntimeEventRecord, []RuntimeEventRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	docKey = strings.TrimSpace(docKey)
	deletedBy = strings.TrimSpace(deletedBy)
	if workspaceID == "" {
		return RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	if docKey == "" {
		return RuntimeEventRecord{}, nil, errors.New("doc_key is required")
	}
	if deletedBy == "" {
		return RuntimeEventRecord{}, nil, errors.New("deleted_by is required")
	}
	promptContextSurface = strings.TrimSpace(promptContextSurface)
	if promptContextSurface == "" {
		promptContextSurface = "workspace.doc.delete"
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, nil, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, nil, fmt.Errorf("begin delete workspace doc tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var runtimeEvent RuntimeEventRecord
	var invalidationEvents []RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		res, err := tx.ExecContext(ctx,
			`DELETE FROM workspace_docs WHERE workspace_id = ? AND doc_key = ?`,
			workspaceID, docKey,
		)
		if err != nil {
			return fmt.Errorf("delete workspace doc: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("workspace doc not found: %s/%s", workspaceID, docKey)
		}
		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "workspace_doc_deleted",
			EntityType: "workspace_doc",
			EntityID:   workspaceID + "/" + docKey,
			ActorID:    deletedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id": workspaceID,
				"doc_key":      docKey,
			}),
		}); err != nil {
			return err
		}
		runtimePayload, err := attachWorkspaceDocPromptContextEnvelope(map[string]any{
			"workspace_id": workspaceID,
			"doc_key":      docKey,
			"deleted_by":   deletedBy,
		}, promptContextEnvelope, promptContextSurface, map[string]string{
			"workspace_id": workspaceID,
			"doc_key":      docKey,
			"deleted_by":   deletedBy,
		})
		if err != nil {
			return err
		}
		runtimeEvent, err = s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "workspace_doc.deleted",
			EntityType:  "workspace_doc",
			EntityID:    docKey,
			ActorID:     deletedBy,
			PayloadJSON: mustJSON(runtimePayload),
			CreatedAt:   now,
		})
		if err != nil {
			return err
		}
		invalidationEvents = make([]RuntimeEventRecord, 0, 2)
		if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, workspaceID, memoryInvalidationRefChange{
			RefKind: "workspace_doc",
			RefID:   docKey,
			Cause:   "workspace_doc.deleted",
		}); err != nil {
			return err
		} else {
			invalidationEvents = append(invalidationEvents, events...)
		}
		if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, workspaceID, memoryInvalidationRefChange{
			RefKind: "segment_ref",
			RefID:   buildWorkspaceDocSegmentRef(workspaceID, docKey, "root"),
			Cause:   "workspace_doc.deleted",
		}); err != nil {
			return err
		} else {
			invalidationEvents = append(invalidationEvents, events...)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, nil, err
	}
	return runtimeEvent, invalidationEvents, nil
}

func contentSHA256(content string) string {
	h := sha256.Sum256([]byte(content))
	return hex.EncodeToString(h[:])
}

func validateWorkspaceArtifactMetadataPromptContext(metadataJSON string, requireValid bool) error {
	metadataJSON = strings.TrimSpace(metadataJSON)
	if metadataJSON == "" {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(metadataJSON), &decoded); err != nil {
		if requireValid {
			return fmt.Errorf("metadata_json must be valid JSON when prompt context envelope is present: %w", err)
		}
		return nil
	}
	return rejectCallerSuppliedPromptContextMarkers("workspace_artifact.metadata_json", decoded)
}

// computeIsOnline returns true if last_seen_at is within 15 minutes.
func computeIsOnline(lastSeenAt *string) bool {
	if lastSeenAt == nil || *lastSeenAt == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339Nano, *lastSeenAt)
	if err != nil {
		return false
	}
	return time.Since(t) < 15*time.Minute
}

func (s *Store) listWorkspaceTasks(ctx context.Context, workspaceID string) ([]WorkspaceTaskRecord, error) {
	return s.listWorkspaceTasksFiltered(ctx, s.db, workspaceID, "")
}

func (s *Store) listWorkspaceTasksForProject(ctx context.Context, workspaceID, projectID string) ([]WorkspaceTaskRecord, error) {
	return s.listWorkspaceTasksForProjectWithQuerier(ctx, s.db, workspaceID, projectID)
}

func (s *Store) listWorkspaceTasksForProjectTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) ([]WorkspaceTaskRecord, error) {
	return s.listWorkspaceTasksForProjectWithQuerier(ctx, tx, workspaceID, projectID)
}

func (s *Store) listWorkspaceTasksForProjectWithQuerier(ctx context.Context, q sqlReadQuerier, workspaceID, projectID string) ([]WorkspaceTaskRecord, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil, errors.New("project_id is required")
	}
	return s.listWorkspaceTasksFiltered(ctx, q, workspaceID, projectID)
}

func (s *Store) listWorkspaceTasksFiltered(ctx context.Context, q sqlReadQuerier, workspaceID, projectID string) ([]WorkspaceTaskRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if q == nil {
		return nil, errors.New("queryer is required")
	}
	query := `SELECT wt.task_id, t.title, t.description, t.owner_user_id, t.priority, t.status, t.task_kind, t.task_template, COALESCE(t.task_class, ''), COALESCE(t.task_class_source, ''), COALESCE(t.task_class_updated_at, ''), COALESCE(t.project_id, ''), COALESCE(t.project_lane, ''), COALESCE(t.requires_project_gate, 0), COALESCE(t.task_requirements_json, '{}'), COALESCE(t.write_scope_hints_json, '[]'), t.close_reason, wt.linked_by, wt.created_at,
		        COALESCE(t.tags_json, '[]'), t.updated_at,
		        tc.agent_id, tc.claim_status, tc.summary, tc.updated_at,
		        tc.project_role_id, tc.repo_id, tc.checkout_id, tc.branch_id, tc.write_scope_json
		 FROM workspace_tasks wt
		 JOIN tasks t ON t.task_id = wt.task_id
		 LEFT JOIN task_claims tc ON tc.task_id = wt.task_id AND tc.workspace_id = wt.workspace_id
		 WHERE wt.workspace_id = ?`
	args := []any{workspaceID}
	if projectID != "" {
		query += ` AND t.project_id = ?`
		args = append(args, projectID)
	}
	query += `
		 ORDER BY
		   CASE LOWER(TRIM(t.priority))
		     WHEN 'critical' THEN 0
		     WHEN 'high' THEN 1
		     WHEN 'normal' THEN 2
		     WHEN 'low' THEN 3
		     ELSE 4
		   END,
		   t.updated_at DESC,
		   wt.task_id`
	rows, err := q.QueryContext(
		ctx,
		query,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("query workspace tasks: %w", err)
	}
	defer rows.Close()

	out := []WorkspaceTaskRecord{}
	for rows.Next() {
		var row WorkspaceTaskRecord
		var claimAgentID, claimStatus, claimSummary, claimUpdatedAt sql.NullString
		var claimProjectRoleID, claimRepoID, claimCheckoutID, claimBranchID, claimWriteScopeJSON sql.NullString
		var requiresProjectGate int
		var tagsJSON, taskRequirementsJSON, writeScopeHintsJSON string
		if err := rows.Scan(
			&row.TaskID,
			&row.Title,
			&row.Description,
			&row.OwnerUserID,
			&row.Priority,
			&row.Status,
			&row.TaskKind,
			&row.TaskTemplate,
			&row.TaskClass,
			&row.TaskClassSource,
			&row.TaskClassUpdatedAt,
			&row.ProjectID,
			&row.ProjectLane,
			&requiresProjectGate,
			&taskRequirementsJSON,
			&writeScopeHintsJSON,
			&row.CloseReason,
			&row.LinkedBy,
			&row.LinkedAt,
			&tagsJSON,
			&row.UpdatedAt,
			&claimAgentID,
			&claimStatus,
			&claimSummary,
			&claimUpdatedAt,
			&claimProjectRoleID,
			&claimRepoID,
			&claimCheckoutID,
			&claimBranchID,
			&claimWriteScopeJSON,
		); err != nil {
			return nil, fmt.Errorf("scan workspace task: %w", err)
		}
		row.ClaimAgentID = nullStringPtr(claimAgentID)
		row.ClaimStatus = nullStringPtr(claimStatus)
		row.ClaimSummary = nullStringPtr(claimSummary)
		row.ClaimUpdatedAt = nullStringPtr(claimUpdatedAt)
		row.ClaimProjectRoleID = nullStringPtr(claimProjectRoleID)
		row.ClaimRepoID = nullStringPtr(claimRepoID)
		row.ClaimCheckoutID = nullStringPtr(claimCheckoutID)
		row.ClaimBranchID = nullStringPtr(claimBranchID)
		row.ClaimWriteScopeJSON = nullStringPtr(claimWriteScopeJSON)
		row.RequiresProjectGate = sqliteIntToBool(requiresProjectGate)
		row.TaskRequirementsJSON = normalizeTaskRequirementsJSON(taskRequirementsJSON)
		row.WriteScopeHints = parseTaskWriteScopeHintsJSON(writeScopeHintsJSON)
		row.Tags = parseTaskTagsJSON(tagsJSON)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace tasks: %w", err)
	}
	return out, nil
}

func (s *Store) workspaceTaskRecordsByIDsWithQuerier(ctx context.Context, q sqlReadQuerier, workspaceID string, taskIDs []string) (map[string]WorkspaceTaskRecord, error) {
	wanted := map[string]struct{}{}
	for _, taskID := range taskIDs {
		taskID = strings.TrimSpace(taskID)
		if taskID != "" {
			wanted[taskID] = struct{}{}
		}
	}
	if len(wanted) == 0 {
		return map[string]WorkspaceTaskRecord{}, nil
	}
	tasks, err := s.listWorkspaceTasksFiltered(ctx, q, workspaceID, "")
	if err != nil {
		return nil, err
	}
	out := make(map[string]WorkspaceTaskRecord, len(wanted))
	for _, task := range tasks {
		taskID := strings.TrimSpace(task.TaskID)
		if _, ok := wanted[taskID]; ok {
			out[taskID] = task
		}
	}
	return out, nil
}

func parseTaskTagsJSON(raw string) []string {
	var tags []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &tags); err != nil {
		return []string{}
	}
	return normalizeStringSlice(tags)
}

func normalizeTaskRequirementsJSON(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "{}"
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return "{}"
	}
	if _, ok := decoded.(map[string]any); !ok {
		return "{}"
	}
	return raw
}

func encodeTaskWriteScopeHintsJSON(hints []string) string {
	hints = normalizeStringSlice(hints)
	if len(hints) == 0 {
		return "[]"
	}
	raw, err := json.Marshal(hints)
	if err != nil {
		return "[]"
	}
	return string(raw)
}

func parseTaskWriteScopeHintsJSON(raw string) []string {
	var hints []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &hints); err != nil {
		return []string{}
	}
	return normalizeStringSlice(hints)
}

// ListWorkspaceTasks returns all tasks linked to a workspace with their claim status.
func (s *Store) ListWorkspaceTasks(ctx context.Context, workspaceID string) ([]WorkspaceTaskRecord, error) {
	return s.listWorkspaceTasks(ctx, workspaceID)
}

func (s *Store) listWorkspaceTools(ctx context.Context, workspaceID string) ([]WorkspaceToolRecord, error) {
	return s.ListWorkspaceTools(ctx, WorkspaceToolFilter{WorkspaceID: workspaceID})
}

func (s *Store) listWorkspaceTaskLinks(ctx context.Context, workspaceID string) ([]WorkspaceTaskLinkRecord, error) {
	return s.ListWorkspaceTaskLinks(ctx, WorkspaceTaskLinkFilter{WorkspaceID: workspaceID, Limit: 100})
}

func (s *Store) listWorkspaceArtifacts(ctx context.Context, workspaceID string, limit int) ([]WorkspaceArtifactRecord, error) {
	return s.ListWorkspaceArtifacts(ctx, WorkspaceArtifactFilter{WorkspaceID: workspaceID, Limit: limit})
}

func (s *Store) listAgentUpdates(ctx context.Context, workspaceID string, limit int) ([]AgentUpdateRecord, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT u.update_id, u.agent_id, a.display_name, u.update_type, u.summary, u.payload_json, u.requires_human, u.created_at
		 FROM agent_updates u
		 JOIN agents a ON a.agent_id = u.agent_id AND a.workspace_id = u.workspace_id
		 WHERE u.workspace_id = ?
		 ORDER BY u.created_at DESC, u.update_id DESC
		 LIMIT ?`,
		workspaceID,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query agent updates: %w", err)
	}
	defer rows.Close()

	out := []AgentUpdateRecord{}
	for rows.Next() {
		var row AgentUpdateRecord
		var requiresHuman int
		if err := rows.Scan(
			&row.UpdateID,
			&row.AgentID,
			&row.AgentName,
			&row.UpdateType,
			&row.Summary,
			&row.PayloadJSON,
			&requiresHuman,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent update: %w", err)
		}
		row.RequiresHuman = requiresHuman != 0
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agent updates: %w", err)
	}
	return out, nil
}

func (s *Store) SearchWorkspace(ctx context.Context, filter WorkspaceSearchFilter) ([]WorkspaceSearchResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	queryText := strings.TrimSpace(filter.Query)
	if queryText == "" {
		return nil, errors.New("query is required")
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}

	entityType := strings.ToLower(strings.TrimSpace(filter.EntityType))
	needle := "%" + queryText + "%"
	results := make([]WorkspaceSearchResult, 0, limit)
	appendRows := func(query string, args []any, scan func(*sql.Rows) (WorkspaceSearchResult, error)) error {
		if len(results) >= limit {
			return nil
		}
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			row, err := scan(rows)
			if err != nil {
				return err
			}
			results = append(results, row)
			if len(results) >= limit {
				break
			}
		}
		return rows.Err()
	}

	if entityType == "" || entityType == "doc" || entityType == "docs" {
		err := appendRows(
			`SELECT doc_key, title, content, updated_at
			 FROM workspace_docs
			 WHERE workspace_id = ? AND (title LIKE ? OR content LIKE ? OR doc_key LIKE ?)
			 ORDER BY updated_at DESC, doc_key
			 LIMIT ?`,
			[]any{workspaceID, needle, needle, needle, limit},
			func(rows *sql.Rows) (WorkspaceSearchResult, error) {
				var docKey, title, content, updatedAt string
				if err := rows.Scan(&docKey, &title, &content, &updatedAt); err != nil {
					return WorkspaceSearchResult{}, fmt.Errorf("scan workspace doc search row: %w", err)
				}
				return WorkspaceSearchResult{
					EntityType: "doc",
					EntityID:   docKey,
					Title:      title,
					Snippet:    clipSearchSnippet(content, queryText),
					UpdatedAt:  updatedAt,
				}, nil
			},
		)
		if err != nil {
			return nil, fmt.Errorf("search workspace docs: %w", err)
		}
	}

	if entityType == "" || entityType == "task" || entityType == "tasks" {
		err := appendRows(
			`SELECT t.task_id, t.title, t.description, t.updated_at
			 FROM workspace_tasks wt
			 JOIN tasks t ON t.task_id = wt.task_id
			 WHERE wt.workspace_id = ? AND (t.task_id LIKE ? OR t.title LIKE ? OR t.description LIKE ? OR t.task_template LIKE ?)
			 ORDER BY t.updated_at DESC, t.task_id
			 LIMIT ?`,
			[]any{workspaceID, needle, needle, needle, needle, limit},
			func(rows *sql.Rows) (WorkspaceSearchResult, error) {
				var taskID, title, description, updatedAt string
				if err := rows.Scan(&taskID, &title, &description, &updatedAt); err != nil {
					return WorkspaceSearchResult{}, fmt.Errorf("scan workspace task search row: %w", err)
				}
				return WorkspaceSearchResult{
					EntityType: "task",
					EntityID:   taskID,
					Title:      firstNonEmpty(title, taskID),
					Snippet:    clipSearchSnippet(description, queryText),
					UpdatedAt:  updatedAt,
				}, nil
			},
		)
		if err != nil {
			return nil, fmt.Errorf("search workspace tasks: %w", err)
		}
	}

	if entityType == "" || entityType == "update" || entityType == "updates" {
		err := appendRows(
			`SELECT update_id, summary, payload_json, created_at
			 FROM agent_updates
			 WHERE workspace_id = ? AND (update_type LIKE ? OR summary LIKE ? OR payload_json LIKE ?)
			 ORDER BY created_at DESC, update_id DESC
			 LIMIT ?`,
			[]any{workspaceID, needle, needle, needle, limit},
			func(rows *sql.Rows) (WorkspaceSearchResult, error) {
				var updateID, summary, payloadJSON, createdAt string
				if err := rows.Scan(&updateID, &summary, &payloadJSON, &createdAt); err != nil {
					return WorkspaceSearchResult{}, fmt.Errorf("scan workspace update search row: %w", err)
				}
				return WorkspaceSearchResult{
					EntityType: "update",
					EntityID:   updateID,
					Title:      summary,
					Snippet:    clipSearchSnippet(payloadJSON, queryText),
					UpdatedAt:  createdAt,
				}, nil
			},
		)
		if err != nil {
			return nil, fmt.Errorf("search workspace updates: %w", err)
		}
	}

	if entityType == "" || entityType == "tool" || entityType == "tools" {
		err := appendRows(
			`SELECT tool_id, display_name, description, updated_at
			 FROM workspace_tools
			 WHERE workspace_id = ? AND (tool_id LIKE ? OR display_name LIKE ? OR description LIKE ? OR capabilities_json LIKE ?)
			 ORDER BY updated_at DESC, tool_id
			 LIMIT ?`,
			[]any{workspaceID, needle, needle, needle, needle, limit},
			func(rows *sql.Rows) (WorkspaceSearchResult, error) {
				var toolID, displayName, description, updatedAt string
				if err := rows.Scan(&toolID, &displayName, &description, &updatedAt); err != nil {
					return WorkspaceSearchResult{}, fmt.Errorf("scan workspace tool search row: %w", err)
				}
				return WorkspaceSearchResult{
					EntityType: "tool",
					EntityID:   toolID,
					Title:      firstNonEmpty(displayName, toolID),
					Snippet:    clipSearchSnippet(description, queryText),
					UpdatedAt:  updatedAt,
				}, nil
			},
		)
		if err != nil {
			return nil, fmt.Errorf("search workspace tools: %w", err)
		}
	}

	if entityType == "" || entityType == "memory" || entityType == "memories" {
		remaining := limit - len(results)
		if remaining > 0 {
			memories, err := s.SearchWorkspaceMemory(ctx, WorkspaceMemoryFilter{
				WorkspaceID: workspaceID,
				Query:       queryText,
				Limit:       remaining,
			})
			if err != nil {
				return nil, fmt.Errorf("search workspace memory: %w", err)
			}
			for _, memory := range memories {
				results = append(results, WorkspaceSearchResult{
					EntityType: "memory",
					EntityID:   memory.MemoryID,
					Title:      firstNonEmpty(memory.Title, memory.MemoryType, memory.MemoryID),
					Snippet:    clipSearchSnippet(firstNonEmpty(memory.Summary, memory.Body), queryText),
					UpdatedAt:  memory.UpdatedAt,
				})
				if len(results) >= limit {
					break
				}
			}
		}
	}

	if entityType == "" || entityType == "claim" || entityType == "claims" {
		remaining := limit - len(results)
		if remaining > 0 {
			claims, err := s.SearchKnowledgeClaims(ctx, KnowledgeClaimFilter{
				WorkspaceID: workspaceID,
				Query:       queryText,
				Limit:       remaining,
			})
			if err != nil {
				return nil, fmt.Errorf("search knowledge claims: %w", err)
			}
			for _, claim := range claims {
				results = append(results, WorkspaceSearchResult{
					EntityType: "claim",
					EntityID:   claim.ClaimID,
					Title:      firstNonEmpty(claim.Subject, claim.ClaimType, claim.ClaimID),
					Snippet:    clipSearchSnippet(firstNonEmpty(claim.Summary, claim.Body), queryText),
					UpdatedAt:  claim.UpdatedAt,
				})
				if len(results) >= limit {
					break
				}
			}
		}
	}

	if entityType == "" || entityType == "artifact" || entityType == "artifacts" {
		err := appendRows(
			`SELECT artifact_id, title, artifact_ref, created_at
			 FROM workspace_artifacts
			 WHERE workspace_id = ? AND (artifact_id LIKE ? OR title LIKE ? OR artifact_ref LIKE ? OR metadata_json LIKE ?)
			 ORDER BY created_at DESC, artifact_id DESC
			 LIMIT ?`,
			[]any{workspaceID, needle, needle, needle, needle, limit},
			func(rows *sql.Rows) (WorkspaceSearchResult, error) {
				var artifactID, title, artifactRef, createdAt string
				if err := rows.Scan(&artifactID, &title, &artifactRef, &createdAt); err != nil {
					return WorkspaceSearchResult{}, fmt.Errorf("scan workspace artifact search row: %w", err)
				}
				return WorkspaceSearchResult{
					EntityType: "artifact",
					EntityID:   artifactID,
					Title:      title,
					Snippet:    clipSearchSnippet(artifactRef, queryText),
					UpdatedAt:  createdAt,
				}, nil
			},
		)
		if err != nil {
			return nil, fmt.Errorf("search workspace artifacts: %w", err)
		}
	}

	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func (s *Store) ensureWorkspaceExistsTx(ctx context.Context, tx *sql.Tx, workspaceID string) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&count); err != nil {
		return fmt.Errorf("check workspace existence: %w", err)
	}
	if count == 0 {
		return ErrWorkspaceNotFound
	}
	return nil
}

func (s *Store) ensureTaskExistsTx(ctx context.Context, tx *sql.Tx, taskID string) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM tasks WHERE task_id = ?`, taskID).Scan(&count); err != nil {
		return fmt.Errorf("check task existence: %w", err)
	}
	if count == 0 {
		return ErrTaskNotFound
	}
	return nil
}

func (s *Store) ensureAgentInWorkspaceTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID string) error {
	var count int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1) FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID,
		agentID,
	).Scan(&count); err != nil {
		return fmt.Errorf("check agent existence: %w", err)
	}
	if count == 0 {
		return ErrAgentNotFound
	}
	return nil
}

func (s *Store) ensureAgentUpdateInWorkspaceTx(ctx context.Context, tx *sql.Tx, workspaceID, updateID string) error {
	var count int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1) FROM agent_updates WHERE workspace_id = ? AND update_id = ?`,
		workspaceID,
		updateID,
	).Scan(&count); err != nil {
		return fmt.Errorf("check agent update existence: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("agent update not found: %s", updateID)
	}
	return nil
}

func (s *Store) ensureWorkspaceTaskAttachedTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) error {
	var count int
	query := `SELECT COUNT(1) FROM workspace_tasks WHERE workspace_id = ? AND task_id = ?`
	var err error
	if tx != nil {
		err = tx.QueryRowContext(ctx, query, workspaceID, taskID).Scan(&count)
	} else {
		err = s.db.QueryRowContext(ctx, query, workspaceID, taskID).Scan(&count)
	}
	if err != nil {
		return fmt.Errorf("check workspace task existence: %w", err)
	}
	if count == 0 {
		return ErrWorkspaceTaskAbsent
	}
	return nil
}

func normalizeCapabilities(raw []string) []string {
	out := make([]string, 0, len(raw))
	seen := map[string]struct{}{}
	for _, item := range raw {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func clipSearchSnippet(body string, query string) string {
	text := strings.TrimSpace(body)
	if text == "" {
		return ""
	}
	if len(text) <= 180 {
		return text
	}
	lowerBody := strings.ToLower(text)
	lowerQuery := strings.ToLower(strings.TrimSpace(query))
	if lowerQuery == "" {
		return text[:180]
	}
	index := strings.Index(lowerBody, lowerQuery)
	if index < 0 {
		return text[:180]
	}
	start := index - 60
	if start < 0 {
		start = 0
	}
	end := start + 180
	if end > len(text) {
		end = len(text)
	}
	return strings.TrimSpace(text[start:end])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func decodeCapabilities(raw string) []string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	var items []string
	if err := json.Unmarshal([]byte(trimmed), &items); err != nil {
		return nil
	}
	return normalizeCapabilities(items)
}

func blankStringOrNil(v string) any {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func blankInt64OrNil(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func blankStringPtr(v string) *string {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

// ── Agent Profiles ──────────────────────────────────────────────────

type AgentProfileInput struct {
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	ActorID        string
	ActorType      string
	Bio            string         `json:"bio"`
	Specialization string         `json:"specialization"`
	OwnerName      string         `json:"owner_name"`
	OwnerContact   string         `json:"owner_contact"`
	AvatarURL      string         `json:"avatar_url"`
	Links          []string       `json:"links"`
	Tags           []string       `json:"tags"`
	ToolsAccess    []string       `json:"tools_access"`
	Metadata       map[string]any `json:"metadata"`

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type AgentProfileRecord struct {
	WorkspaceID    string         `json:"workspace_id"`
	AgentID        string         `json:"agent_id"`
	Bio            string         `json:"bio"`
	Specialization string         `json:"specialization"`
	OwnerName      string         `json:"owner_name"`
	OwnerContact   string         `json:"owner_contact"`
	AvatarURL      string         `json:"avatar_url"`
	Links          []string       `json:"links"`
	Tags           []string       `json:"tags"`
	ToolsAccess    []string       `json:"tools_access"`
	Metadata       map[string]any `json:"metadata"`
	UpdatedAt      string         `json:"updated_at"`
}

func (s *Store) UpsertAgentProfile(ctx context.Context, input AgentProfileInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return errors.New("agent_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	return upsertAgentProfileExec(ctx, s.db, input, now)
}

func (s *Store) UpsertAgentProfileWithEvent(ctx context.Context, input AgentProfileInput) (AgentProfileRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return AgentProfileRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return AgentProfileRecord{}, RuntimeEventRecord{}, errors.New("agent_id is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		return AgentProfileRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return AgentProfileRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	actorType := strings.ToLower(firstNonEmpty(strings.TrimSpace(input.ActorType), "human"))
	if actorType == "agent" && actorID != agentID {
		return AgentProfileRecord{}, RuntimeEventRecord{}, errors.New("actor mismatch: agent actor_id must match agent_id")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return AgentProfileRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return AgentProfileRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin agent profile update tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	promptContextSurface := firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "agent.profile.update")
	var profile AgentProfileRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, agentID); err != nil {
			return err
		}
		if err := upsertAgentProfileExec(ctx, tx, input, now); err != nil {
			return err
		}
		var err error
		profile, err = s.getAgentProfileTx(ctx, tx, workspaceID, agentID)
		if err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "agent profile update"); err != nil {
			return err
		}

		metadataJSON := agentProfileMetadataJSON(profile.Metadata)
		gateBasisJSON := agentProfileGateBasisJSON(profile)
		payload, err := attachAgentProfilePromptContextEnvelope(map[string]any{
			"workspace_id":                           workspaceID,
			"agent_id":                               agentID,
			"actor_id":                               actorID,
			"actor_type":                             actorType,
			"entity_type":                            "agent_profile",
			"entity_id":                              agentID,
			"summary":                                "Agent profile updated: " + agentID,
			"mutation_operation":                     "update",
			"bio_present":                            strings.TrimSpace(profile.Bio) != "",
			"specialization":                         strings.TrimSpace(profile.Specialization),
			"tags":                                   profile.Tags,
			"tags_count":                             len(profile.Tags),
			"links_count":                            len(profile.Links),
			"tools_access_count":                     len(profile.ToolsAccess),
			"metadata_keys":                          agentProfileMetadataKeys(profile.Metadata),
			"metadata_sha256":                        contentSHA256(metadataJSON),
			"autonomous_execution_allowed_after":     agentProfileAllowsAutonomousExecution(profile),
			"profile_gate_basis_sha256":              contentSHA256(gateBasisJSON),
			"profile_gate_basis_fields":              []string{"bio", "specialization", "tags", "metadata.default_work_mode"},
			"affects_autonomous_execution_selection": true,
		}, input.PromptContextEnvelope, promptContextSurface, map[string]string{
			"workspace_id":   workspaceID,
			"agent_id":       agentID,
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
			EventType:   "agent.profile.updated",
			EntityType:  "agent_profile",
			EntityID:    agentID,
			ActorType:   actorType,
			ActorID:     actorID,
			AgentID:     agentID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		_ = tx.Rollback()
		return AgentProfileRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return AgentProfileRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit agent profile update: %w", err)
	}
	return profile, event, nil
}

type agentProfileExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func upsertAgentProfileExec(ctx context.Context, execer agentProfileExecer, input AgentProfileInput, now string) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return errors.New("agent_id is required")
	}
	now = strings.TrimSpace(now)
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}

	linksJSON, _ := json.Marshal(normalizeStringSlice(input.Links))
	tagsJSON, _ := json.Marshal(normalizeStringSlice(input.Tags))
	toolsJSON, _ := json.Marshal(normalizeStringSlice(input.ToolsAccess))
	metaJSON := agentProfileMetadataJSON(input.Metadata)

	_, err := execer.ExecContext(ctx,
		`INSERT INTO agent_profiles(workspace_id, agent_id, bio, specialization, owner_name, owner_contact, avatar_url, links_json, tags_json, tools_access_json, metadata_json, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(workspace_id, agent_id) DO UPDATE SET
		   bio = excluded.bio,
		   specialization = excluded.specialization,
		   owner_name = excluded.owner_name,
		   owner_contact = excluded.owner_contact,
		   avatar_url = excluded.avatar_url,
		   links_json = excluded.links_json,
		   tags_json = excluded.tags_json,
		   tools_access_json = excluded.tools_access_json,
		   metadata_json = excluded.metadata_json,
		   updated_at = excluded.updated_at`,
		workspaceID, agentID, input.Bio, input.Specialization,
		input.OwnerName, input.OwnerContact, input.AvatarURL,
		string(linksJSON), string(tagsJSON), string(toolsJSON), metaJSON, now,
	)
	if err != nil {
		return fmt.Errorf("upsert agent profile: %w", err)
	}
	return nil
}

func agentProfileMetadataJSON(metadata map[string]any) string {
	if metadata == nil {
		return "{}"
	}
	b, err := json.Marshal(metadata)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func agentProfileGateBasisJSON(profile AgentProfileRecord) string {
	payload := map[string]any{
		"bio":                        strings.TrimSpace(profile.Bio),
		"specialization":             strings.TrimSpace(profile.Specialization),
		"tags":                       normalizeStringSlice(profile.Tags),
		"metadata.default_work_mode": strings.TrimSpace(agentProfileMetadataString(profile.Metadata, "default_work_mode")),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func agentProfileMetadataKeys(metadata map[string]any) []string {
	if len(metadata) == 0 {
		return []string{}
	}
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		if trimmed := strings.TrimSpace(key); trimmed != "" {
			keys = append(keys, trimmed)
		}
	}
	sort.Strings(keys)
	return keys
}

func (s *Store) GetAgentProfile(ctx context.Context, workspaceID, agentID string) (AgentProfileRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return AgentProfileRecord{}, errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return AgentProfileRecord{}, errors.New("agent_id is required")
	}

	var rec AgentProfileRecord
	var linksJSON, tagsJSON, toolsJSON, metaJSON string
	err := s.db.QueryRowContext(ctx,
		`SELECT workspace_id, agent_id, bio, specialization, owner_name, owner_contact, avatar_url, links_json, tags_json, tools_access_json, metadata_json, updated_at
		 FROM agent_profiles WHERE workspace_id = ? AND agent_id = ?`, workspaceID, agentID,
	).Scan(&rec.WorkspaceID, &rec.AgentID, &rec.Bio, &rec.Specialization,
		&rec.OwnerName, &rec.OwnerContact, &rec.AvatarURL,
		&linksJSON, &tagsJSON, &toolsJSON, &metaJSON, &rec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AgentProfileRecord{
				WorkspaceID: workspaceID,
				AgentID:     agentID,
				Links:       []string{},
				Tags:        []string{},
				ToolsAccess: []string{},
				Metadata:    map[string]any{},
			}, nil
		}
		return AgentProfileRecord{}, fmt.Errorf("get agent profile: %w", err)
	}

	_ = json.Unmarshal([]byte(linksJSON), &rec.Links)
	_ = json.Unmarshal([]byte(tagsJSON), &rec.Tags)
	_ = json.Unmarshal([]byte(toolsJSON), &rec.ToolsAccess)
	_ = json.Unmarshal([]byte(metaJSON), &rec.Metadata)
	if rec.Links == nil {
		rec.Links = []string{}
	}
	if rec.Tags == nil {
		rec.Tags = []string{}
	}
	if rec.ToolsAccess == nil {
		rec.ToolsAccess = []string{}
	}
	if rec.Metadata == nil {
		rec.Metadata = map[string]any{}
	}
	return rec, nil
}

func (s *Store) getAgentProfileTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID string) (AgentProfileRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return AgentProfileRecord{}, errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return AgentProfileRecord{}, errors.New("agent_id is required")
	}

	var rec AgentProfileRecord
	var linksJSON, tagsJSON, toolsJSON, metaJSON string
	err := tx.QueryRowContext(ctx,
		`SELECT workspace_id, agent_id, bio, specialization, owner_name, owner_contact, avatar_url, links_json, tags_json, tools_access_json, metadata_json, updated_at
		 FROM agent_profiles WHERE workspace_id = ? AND agent_id = ?`, workspaceID, agentID,
	).Scan(&rec.WorkspaceID, &rec.AgentID, &rec.Bio, &rec.Specialization,
		&rec.OwnerName, &rec.OwnerContact, &rec.AvatarURL,
		&linksJSON, &tagsJSON, &toolsJSON, &metaJSON, &rec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AgentProfileRecord{
				WorkspaceID: workspaceID,
				AgentID:     agentID,
				Links:       []string{},
				Tags:        []string{},
				ToolsAccess: []string{},
				Metadata:    map[string]any{},
			}, nil
		}
		return AgentProfileRecord{}, fmt.Errorf("get agent profile: %w", err)
	}

	_ = json.Unmarshal([]byte(linksJSON), &rec.Links)
	_ = json.Unmarshal([]byte(tagsJSON), &rec.Tags)
	_ = json.Unmarshal([]byte(toolsJSON), &rec.ToolsAccess)
	_ = json.Unmarshal([]byte(metaJSON), &rec.Metadata)
	if rec.Links == nil {
		rec.Links = []string{}
	}
	if rec.Tags == nil {
		rec.Tags = []string{}
	}
	if rec.ToolsAccess == nil {
		rec.ToolsAccess = []string{}
	}
	if rec.Metadata == nil {
		rec.Metadata = map[string]any{}
	}
	return rec, nil
}

// AgentSearchResult combines agent info with matching tags.
type AgentSearchResult struct {
	AgentID     string   `json:"agent_id"`
	DisplayName string   `json:"display_name"`
	Tags        []string `json:"tags"`
	MatchedTags []string `json:"matched_tags"`
	IsOnline    bool     `json:"is_online"`
}

// SearchAgentsByTags finds agents whose profile tags overlap with searchTags.
func (s *Store) SearchAgentsByTags(ctx context.Context, workspaceID string, searchTags []string) ([]AgentSearchResult, error) {
	if workspaceID == "" || len(searchTags) == 0 {
		return []AgentSearchResult{}, nil
	}
	// Build a set of search tags (lowercased).
	searchSet := map[string]bool{}
	for _, t := range searchTags {
		searchSet[strings.ToLower(strings.TrimSpace(t))] = true
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT a.agent_id, a.display_name, a.last_seen_at, COALESCE(p.tags_json, '[]')
		 FROM agents a
		 LEFT JOIN agent_profiles p ON p.workspace_id = a.workspace_id AND p.agent_id = a.agent_id
		 WHERE a.workspace_id = ?`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("search agents by tags: %w", err)
	}
	defer rows.Close()

	var results []AgentSearchResult
	for rows.Next() {
		var agentID, displayName, tagsJSON string
		var lastSeen sql.NullString
		if err := rows.Scan(&agentID, &displayName, &lastSeen, &tagsJSON); err != nil {
			continue
		}
		var tags []string
		_ = json.Unmarshal([]byte(tagsJSON), &tags)

		var matched []string
		for _, t := range tags {
			if searchSet[strings.ToLower(t)] {
				matched = append(matched, t)
			}
		}
		if len(matched) > 0 {
			results = append(results, AgentSearchResult{
				AgentID:     agentID,
				DisplayName: displayName,
				Tags:        tags,
				MatchedTags: matched,
				IsOnline:    computeIsOnline(nullStringPtr(lastSeen)),
			})
		}
	}
	if results == nil {
		results = []AgentSearchResult{}
	}
	return results, nil
}

func normalizeStringSlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	out := make([]string, 0, len(s))
	for _, v := range s {
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

// ── Webhooks ────────────────────────────────────────────────────────

type WebhookInput struct {
	WorkspaceID string   `json:"workspace_id"`
	URL         string   `json:"url"`
	EventTypes  []string `json:"event_types"` // e.g. ["message.send", "doc.put", "*"]
	Secret      string   `json:"secret"`
	CreatedBy   string   `json:"created_by"`
}

type WebhookRecord struct {
	WebhookID   string   `json:"webhook_id"`
	WorkspaceID string   `json:"workspace_id"`
	URL         string   `json:"url"`
	EventTypes  []string `json:"event_types"`
	Secret      string   `json:"secret,omitempty"`
	Active      bool     `json:"active"`
	CreatedBy   string   `json:"created_by"`
	CreatedAt   string   `json:"created_at"`
}

func (s *Store) RegisterWebhook(ctx context.Context, input WebhookInput) (string, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return "", errors.New("workspace_id is required")
	}
	url := strings.TrimSpace(input.URL)
	if url == "" {
		return "", errors.New("url is required")
	}
	eventTypes := normalizeStringSlice(input.EventTypes)
	if len(eventTypes) == 0 {
		eventTypes = []string{"*"}
	}
	evJSON, _ := json.Marshal(eventTypes)
	id := nextID("webhook")
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO webhook_subscriptions(webhook_id, workspace_id, url, event_types_json, secret, active, created_by, created_at)
		 VALUES (?, ?, ?, ?, ?, 1, ?, ?)`,
		id, workspaceID, url, string(evJSON), input.Secret, input.CreatedBy, now,
	)
	if err != nil {
		return "", fmt.Errorf("register webhook: %w", err)
	}
	return id, nil
}

func (s *Store) ListWebhooks(ctx context.Context, workspaceID string) ([]WebhookRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT webhook_id, workspace_id, url, event_types_json, secret, active, created_by, created_at
		 FROM webhook_subscriptions WHERE workspace_id = ? AND active = 1
		 ORDER BY created_at`, workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("list webhooks: %w", err)
	}
	defer rows.Close()

	out := []WebhookRecord{}
	for rows.Next() {
		var r WebhookRecord
		var evJSON string
		var active int
		if err := rows.Scan(&r.WebhookID, &r.WorkspaceID, &r.URL, &evJSON, &r.Secret, &active, &r.CreatedBy, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan webhook: %w", err)
		}
		_ = json.Unmarshal([]byte(evJSON), &r.EventTypes)
		r.Active = active == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) RemoveWebhook(ctx context.Context, webhookID string) error {
	webhookID = strings.TrimSpace(webhookID)
	if webhookID == "" {
		return errors.New("webhook_id is required")
	}
	_, err := s.writeDB.ExecContext(ctx,
		`UPDATE webhook_subscriptions SET active = 0 WHERE webhook_id = ?`, webhookID,
	)
	if err != nil {
		return fmt.Errorf("remove webhook: %w", err)
	}
	return nil
}

// GetActiveWebhooks returns webhooks matching an event type for a workspace.
func (s *Store) GetActiveWebhooks(ctx context.Context, workspaceID, eventType string) ([]WebhookRecord, error) {
	all, err := s.ListWebhooks(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	var matched []WebhookRecord
	for _, w := range all {
		for _, et := range w.EventTypes {
			if et == "*" || et == eventType {
				matched = append(matched, w)
				break
			}
		}
	}
	return matched, nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
