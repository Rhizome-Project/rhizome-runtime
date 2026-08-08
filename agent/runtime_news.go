package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type newsBatchOutcome struct {
	handled           int
	nextCursorCreated string
	nextCursorNewsID  string
	hadError          bool
}

func sortNewsForDelivery(items []NewsRecord) {
	sort.SliceStable(items, func(i, j int) bool {
		leftCreated := strings.TrimSpace(items[i].CreatedAt)
		rightCreated := strings.TrimSpace(items[j].CreatedAt)
		if leftCreated != rightCreated {
			return leftCreated < rightCreated
		}
		return strings.TrimSpace(items[i].NewsID) < strings.TrimSpace(items[j].NewsID)
	})
}

func (r *Runtime) newsCursor() (string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.TrimSpace(r.scratch.NewsCursorCreatedAt), strings.TrimSpace(r.scratch.NewsCursorID)
}

func (r *Runtime) syncNewsCursor(ctx context.Context, createdAt, newsID string) error {
	createdAt = strings.TrimSpace(createdAt)
	newsID = strings.TrimSpace(newsID)
	if createdAt == "" && newsID == "" {
		return nil
	}
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.NewsCursorCreatedAt = createdAt
		state.NewsCursorID = newsID
	})
}

func (r *Runtime) pollNews(ctx context.Context) error {
	afterCreatedAt, afterNewsID := r.newsCursor()
	result, err := r.client.PollNews(ctx, NewsPollInput{
		WorkspaceID:    r.cfg.WorkspaceID,
		AfterCreatedAt: afterCreatedAt,
		AfterNewsID:    afterNewsID,
		Limit:          r.cfg.UpdatesLimit,
		LookbackHours:  r.cfg.LookbackHours,
	})
	if err != nil {
		return err
	}
	if len(result.Items) == 0 {
		return r.syncNewsCursor(ctx, result.NextCursorCreatedAt, result.NextCursorNewsID)
	}
	_, err = r.processNewsBatch(ctx, result)
	return err
}

func (r *Runtime) processNewsBatch(ctx context.Context, result PollNewsResult) (newsBatchOutcome, error) {
	if len(result.Items) == 0 {
		outcome := newsBatchOutcome{
			nextCursorCreated: strings.TrimSpace(result.NextCursorCreatedAt),
			nextCursorNewsID:  strings.TrimSpace(result.NextCursorNewsID),
		}
		if err := r.syncNewsCursor(ctx, outcome.nextCursorCreated, outcome.nextCursorNewsID); err != nil {
			return outcome, err
		}
		return outcome, nil
	}

	items := append([]NewsRecord(nil), result.Items...)
	sortNewsForDelivery(items)

	outcome := newsBatchOutcome{}
	var errs []string
	for _, item := range items {
		if err := r.handleNewsItem(ctx, item); err != nil {
			outcome.hadError = true
			errs = append(errs, fmt.Sprintf("%s: %v", item.NewsID, err))
			continue
		}
		outcome.handled++
		outcome.nextCursorCreated = strings.TrimSpace(item.CreatedAt)
		outcome.nextCursorNewsID = strings.TrimSpace(item.NewsID)
	}
	if !outcome.hadError {
		outcome.nextCursorCreated = firstNonEmpty(strings.TrimSpace(result.NextCursorCreatedAt), outcome.nextCursorCreated)
		outcome.nextCursorNewsID = firstNonEmpty(strings.TrimSpace(result.NextCursorNewsID), outcome.nextCursorNewsID)
	}
	if err := r.syncNewsCursor(ctx, outcome.nextCursorCreated, outcome.nextCursorNewsID); err != nil {
		return outcome, err
	}
	if len(errs) == 0 {
		return outcome, nil
	}
	return outcome, errors.New(strings.Join(errs, "; "))
}

func (r *Runtime) handleNewsItem(ctx context.Context, item NewsRecord) error {
	summary := summarizeNewsItem(item)
	r.logNewsMemory(item, summary)
	if err := r.queueSystemNewsTrigger(ctx); err != nil {
		return err
	}
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.LastSummary = summary
		state.LastNewsID = strings.TrimSpace(item.NewsID)
		state.LastNewsAt = strings.TrimSpace(item.CreatedAt)
		state.LastNewsSummary = summary
	}); err != nil {
		return err
	}
	r.markBootstrapStale()

	payload := map[string]any{
		"status":      "SYSTEM_NEWS",
		"news_id":     item.NewsID,
		"title":       strings.TrimSpace(item.Title),
		"content":     strings.TrimSpace(item.Content),
		"author_id":   strings.TrimSpace(item.AuthorID),
		"author_type": strings.TrimSpace(item.AuthorType),
		"created_at":  strings.TrimSpace(item.CreatedAt),
		"next_action": "replan after system news",
	}
	rawPayload, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		UpdateType:  "news",
		Summary:     summary,
		PayloadJSON: string(rawPayload),
	})
}

func summarizeNewsItem(item NewsRecord) string {
	title := oneLine(item.Title)
	content := truncate(oneLine(item.Content), 160)
	switch {
	case title != "" && content != "":
		return fmt.Sprintf("System news: %s - %s", title, content)
	case title != "":
		return fmt.Sprintf("System news: %s", title)
	case content != "":
		return fmt.Sprintf("System news: %s", content)
	default:
		return fmt.Sprintf("System news published by %s", firstNonEmpty(strings.TrimSpace(item.AuthorID), "unknown"))
	}
}
