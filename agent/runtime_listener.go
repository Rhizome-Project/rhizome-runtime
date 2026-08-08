package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type retryBackoff struct {
	min     time.Duration
	max     time.Duration
	current time.Duration
}

func newRetryBackoff(min, max time.Duration) *retryBackoff {
	if min <= 0 {
		min = 500 * time.Millisecond
	}
	if max < min {
		max = min
	}
	return &retryBackoff{min: min, max: max}
}

func (b *retryBackoff) Reset() {
	if b == nil {
		return
	}
	b.current = 0
}

func (b *retryBackoff) Next() time.Duration {
	if b == nil {
		return 0
	}
	if b.current <= 0 {
		b.current = b.min
	} else {
		b.current *= 2
		if b.current > b.max {
			b.current = b.max
		}
	}
	return b.current
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func sortMessagesForDelivery(messages []MessageRecord) {
	sort.SliceStable(messages, func(i, j int) bool {
		leftCreated := strings.TrimSpace(messages[i].CreatedAt)
		rightCreated := strings.TrimSpace(messages[j].CreatedAt)
		if leftCreated != rightCreated {
			return leftCreated < rightCreated
		}
		return strings.TrimSpace(messages[i].MessageID) < strings.TrimSpace(messages[j].MessageID)
	})
}

func sortRequestsForDelivery(requests []AgentRequestRecord) {
	sort.SliceStable(requests, func(i, j int) bool {
		leftCreated := strings.TrimSpace(requests[i].CreatedAt)
		rightCreated := strings.TrimSpace(requests[j].CreatedAt)
		if leftCreated != rightCreated {
			return leftCreated < rightCreated
		}
		return strings.TrimSpace(requests[i].RequestID) < strings.TrimSpace(requests[j].RequestID)
	})
}

func messageCursorForRecord(message MessageRecord) string {
	createdAt := strings.TrimSpace(message.CreatedAt)
	if createdAt == "" {
		return ""
	}
	messageID := strings.TrimSpace(message.MessageID)
	if messageID == "" {
		return createdAt
	}
	return createdAt + "|" + messageID
}

type messageBatchOutcome struct {
	handled    int
	nextCursor string
	hadError   bool
}

func (r *Runtime) processMessageBatch(ctx context.Context, result PollMessagesResult) (messageBatchOutcome, error) {
	return r.processMessageList(ctx, result.Messages, true, result.NextCursor)
}

func (r *Runtime) processRequestBatch(ctx context.Context, requests []AgentRequestRecord) error {
	if len(requests) == 0 {
		return nil
	}

	items := append([]AgentRequestRecord(nil), requests...)
	sortRequestsForDelivery(items)

	var errs []string
	for _, request := range items {
		if err := r.handleRequest(ctx, request); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", request.RequestID, err))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "; "))
}

func (r *Runtime) markBootstrapStale() {
	r.invalidateBootstrap()
}
