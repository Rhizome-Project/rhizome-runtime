package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ── News types ───────────────────────────────────────────────────────

type NewsRecord struct {
	NewsID      string `json:"news_id"`
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Content     string `json:"content"`
	AuthorID    string `json:"author_id"`
	AuthorType  string `json:"author_type"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type NewsCreateInput struct {
	WorkspaceID string
	Title       string
	Content     string
	AuthorID    string
	AuthorType  string // "agent" or "human"

	ActorID               string
	ActorType             string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type NewsDeleteInput struct {
	WorkspaceID string
	NewsID      string
	ActorID     string
	ActorType   string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

// ── News CRUD ────────────────────────────────────────────────────────

func (s *Store) CreateNews(ctx context.Context, input NewsCreateInput) (*NewsRecord, error) {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin news create tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, err := s.createNewsTx(ctx, tx, input)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit news create: %w", err)
	}
	return record, nil
}

func (s *Store) CreateNewsWithEvent(ctx context.Context, input NewsCreateInput) (*NewsRecord, RuntimeEventRecord, error) {
	workspaceID, title, content, authorID, authorType, actorID, actorType, err := normalizeNewsCreateInput(input)
	if err != nil {
		return nil, RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return nil, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return nil, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, RuntimeEventRecord{}, fmt.Errorf("begin news publish tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var record *NewsRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, innerErr = s.createNewsTx(ctx, tx, NewsCreateInput{
			WorkspaceID: workspaceID,
			Title:       title,
			Content:     content,
			AuthorID:    authorID,
			AuthorType:  authorType,
		})
		if innerErr != nil {
			return innerErr
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "news publish"); err != nil {
			return err
		}
		payload, err := attachNewsPromptContextEnvelope(map[string]any{
			"workspace_id":       workspaceID,
			"news_id":            record.NewsID,
			"actor_id":           actorID,
			"author_id":          authorID,
			"author_type":        authorType,
			"title":              title,
			"content_sha256":     "sha256:" + contentSHA256(content),
			"content_length":     len(content),
			"entity_type":        "news",
			"entity_id":          record.NewsID,
			"summary":            "News published: " + title,
			"mutation_operation": "publish",
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "news.publish"), map[string]string{
			"workspace_id":   workspaceID,
			"news_id":        record.NewsID,
			"actor_id":       actorID,
			"author_id":      authorID,
			"principal_type": actorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "news.published",
			EntityType:  "news",
			EntityID:    record.NewsID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return nil, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return nil, RuntimeEventRecord{}, fmt.Errorf("commit news publish: %w", err)
	}
	return record, event, nil
}

func (s *Store) createNewsTx(ctx context.Context, tx *sql.Tx, input NewsCreateInput) (*NewsRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return nil, errors.New("title is required")
	}
	authorID := strings.TrimSpace(input.AuthorID)
	if authorID == "" {
		return nil, errors.New("author_id is required")
	}
	authorType := strings.TrimSpace(input.AuthorType)
	if authorType == "" {
		authorType = "agent"
	}
	content := strings.TrimSpace(input.Content)
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return nil, err
	}

	newsID := nextID("news")
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := tx.ExecContext(ctx,
		`INSERT INTO news (news_id, workspace_id, title, content, author_id, author_type, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		newsID, workspaceID, title, content, authorID, authorType, now, now,
	)
	if err != nil {
		return nil, fmt.Errorf("create news: %w", err)
	}
	return &NewsRecord{
		NewsID:      newsID,
		WorkspaceID: workspaceID,
		Title:       title,
		Content:     content,
		AuthorID:    authorID,
		AuthorType:  authorType,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

func (s *Store) ListNews(ctx context.Context, workspaceID string, limit int) ([]NewsRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT news_id, workspace_id, title, content, author_id, author_type, created_at, updated_at
		 FROM news WHERE workspace_id = ?
		 ORDER BY created_at DESC LIMIT ?`,
		workspaceID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list news: %w", err)
	}
	defer rows.Close()

	var items []NewsRecord
	for rows.Next() {
		var n NewsRecord
		if err := rows.Scan(&n.NewsID, &n.WorkspaceID, &n.Title, &n.Content, &n.AuthorID, &n.AuthorType, &n.CreatedAt, &n.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan news: %w", err)
		}
		items = append(items, n)
	}
	return items, rows.Err()
}

func (s *Store) ListNewsAfter(ctx context.Context, workspaceID, afterCreatedAt, afterNewsID string, limit, lookbackHours int) ([]NewsRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	afterCreatedAt = strings.TrimSpace(afterCreatedAt)
	afterNewsID = strings.TrimSpace(afterNewsID)
	if limit <= 0 {
		limit = 20
	}
	if lookbackHours <= 0 {
		lookbackHours = 24
	}

	query := `SELECT news_id, workspace_id, title, content, author_id, author_type, created_at, updated_at
		FROM news
		WHERE workspace_id = ?`
	args := []any{workspaceID}

	switch {
	case afterCreatedAt != "" && afterNewsID != "":
		query += `
		 AND (
			created_at > ?
			OR (
				created_at = ?
				AND rowid > COALESCE((SELECT rowid FROM news WHERE news_id = ?), -1)
			)
		 )`
		args = append(args, afterCreatedAt, afterCreatedAt, afterNewsID)
	case afterCreatedAt != "":
		query += ` AND created_at > ?`
		args = append(args, afterCreatedAt)
	default:
		cutoff := time.Now().UTC().Add(-time.Duration(lookbackHours) * time.Hour).Format(time.RFC3339Nano)
		query += ` AND created_at >= ?`
		args = append(args, cutoff)
	}

	query += ` ORDER BY created_at ASC, rowid ASC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list news after: %w", err)
	}
	defer rows.Close()

	var items []NewsRecord
	for rows.Next() {
		var n NewsRecord
		if err := rows.Scan(&n.NewsID, &n.WorkspaceID, &n.Title, &n.Content, &n.AuthorID, &n.AuthorType, &n.CreatedAt, &n.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan news after: %w", err)
		}
		items = append(items, n)
	}
	return items, rows.Err()
}

func (s *Store) GetNews(ctx context.Context, newsID string) (*NewsRecord, error) {
	newsID = strings.TrimSpace(newsID)
	if newsID == "" {
		return nil, errors.New("news_id is required")
	}
	var n NewsRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT news_id, workspace_id, title, content, author_id, author_type, created_at, updated_at
		 FROM news WHERE news_id = ?`, newsID,
	).Scan(&n.NewsID, &n.WorkspaceID, &n.Title, &n.Content, &n.AuthorID, &n.AuthorType, &n.CreatedAt, &n.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("news not found: %s", newsID)
		}
		return nil, fmt.Errorf("get news: %w", err)
	}
	return &n, nil
}

func (s *Store) DeleteNews(ctx context.Context, newsID string) error {
	newsID = strings.TrimSpace(newsID)
	if newsID == "" {
		return errors.New("news_id is required")
	}
	res, err := s.writeDB.ExecContext(ctx, `DELETE FROM news WHERE news_id = ?`, newsID)
	if err != nil {
		return fmt.Errorf("delete news: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("news not found: %s", newsID)
	}
	return nil
}

func (s *Store) DeleteNewsWithEvent(ctx context.Context, input NewsDeleteInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	newsID := strings.TrimSpace(input.NewsID)
	actorID := strings.TrimSpace(input.ActorID)
	actorType := firstNonEmpty(strings.TrimSpace(input.ActorType), "human")
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if newsID == "" {
		return RuntimeEventRecord{}, errors.New("news_id is required")
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
		return RuntimeEventRecord{}, fmt.Errorf("begin news delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		news, err := s.getNewsTx(ctx, tx, newsID)
		if err != nil {
			return err
		}
		if news.WorkspaceID != workspaceID {
			return fmt.Errorf("news workspace mismatch: %s belongs to %s", newsID, news.WorkspaceID)
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM news WHERE news_id = ? AND workspace_id = ?`, newsID, workspaceID)
		if err != nil {
			return fmt.Errorf("delete news: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("news not found: %s", newsID)
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "news delete"); err != nil {
			return err
		}
		payload, err := attachNewsPromptContextEnvelope(map[string]any{
			"workspace_id":       workspaceID,
			"news_id":            newsID,
			"actor_id":           actorID,
			"author_id":          news.AuthorID,
			"author_type":        news.AuthorType,
			"title":              news.Title,
			"content_sha256":     "sha256:" + contentSHA256(news.Content),
			"entity_type":        "news",
			"entity_id":          newsID,
			"summary":            "News deleted: " + newsID,
			"mutation_operation": "delete",
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "news.delete"), map[string]string{
			"workspace_id":   workspaceID,
			"news_id":        newsID,
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
			EventType:   "news.deleted",
			EntityType:  "news",
			EntityID:    newsID,
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
		return RuntimeEventRecord{}, fmt.Errorf("commit news delete: %w", err)
	}
	return event, nil
}

func (s *Store) getNewsTx(ctx context.Context, tx *sql.Tx, newsID string) (*NewsRecord, error) {
	newsID = strings.TrimSpace(newsID)
	if newsID == "" {
		return nil, errors.New("news_id is required")
	}
	var n NewsRecord
	err := tx.QueryRowContext(ctx,
		`SELECT news_id, workspace_id, title, content, author_id, author_type, created_at, updated_at
		 FROM news WHERE news_id = ?`, newsID,
	).Scan(&n.NewsID, &n.WorkspaceID, &n.Title, &n.Content, &n.AuthorID, &n.AuthorType, &n.CreatedAt, &n.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("news not found: %s", newsID)
		}
		return nil, fmt.Errorf("get news: %w", err)
	}
	return &n, nil
}

func normalizeNewsCreateInput(input NewsCreateInput) (workspaceID, title, content, authorID, authorType, actorID, actorType string, err error) {
	workspaceID = strings.TrimSpace(input.WorkspaceID)
	title = strings.TrimSpace(input.Title)
	content = strings.TrimSpace(input.Content)
	authorID = strings.TrimSpace(input.AuthorID)
	actorID = firstNonEmpty(strings.TrimSpace(input.ActorID), authorID)
	authorType = strings.TrimSpace(input.AuthorType)
	actorType = firstNonEmpty(strings.TrimSpace(input.ActorType), authorType, "agent")
	if authorType == "" {
		authorType = actorType
	}
	if workspaceID == "" {
		err = errors.New("workspace_id is required")
		return
	}
	if title == "" {
		err = errors.New("title is required")
		return
	}
	if authorID == "" {
		err = errors.New("author_id is required")
		return
	}
	if actorID == "" {
		err = errors.New("actor_id is required")
		return
	}
	if authorID != actorID {
		err = errors.New("author_id must match actor_id")
		return
	}
	if authorType != actorType {
		err = errors.New("author_type must match actor_type")
		return
	}
	return
}

func attachNewsPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachNewsPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
		"news_id":      strings.TrimSpace(fields["news_id"]),
		"actor_id":     strings.TrimSpace(fields["actor_id"]),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("news.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}
