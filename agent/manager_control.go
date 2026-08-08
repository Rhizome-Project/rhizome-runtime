package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var managedRuntimeControlPollInterval = 250 * time.Millisecond

type ManagedRuntimeControlClient struct {
	Client      *RhizomeClient
	WorkspaceID string
	FromAgentID string
	ToAgentID   string
}

func (c ManagedRuntimeControlClient) Request(ctx context.Context, method string, payload any, timeout time.Duration) (AgentRequestRecord, error) {
	if c.Client == nil {
		return AgentRequestRecord{}, fmt.Errorf("rhizome client is nil")
	}
	method = strings.TrimSpace(method)
	if method == "" {
		return AgentRequestRecord{}, fmt.Errorf("method is required")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return AgentRequestRecord{}, fmt.Errorf("marshal control payload: %w", err)
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	req, err := c.Client.RequestAgent(ctx, AgentRequestInput{
		WorkspaceID: c.WorkspaceID,
		FromAgentID: firstNonEmpty(c.FromAgentID, "manager"),
		ToAgentID:   c.ToAgentID,
		Method:      method,
		PayloadJSON: string(raw),
		TimeoutSec:  int(timeout.Seconds()),
	})
	if err != nil {
		return AgentRequestRecord{}, err
	}
	record, err := c.Wait(ctx, req.RequestID, timeout)
	if err != nil {
		return record, err
	}
	if err := classifyManagedRuntimeControlRequestResult(record); err != nil {
		return record, err
	}
	return record, nil
}

func (c ManagedRuntimeControlClient) Wait(ctx context.Context, requestID string, timeout time.Duration) (AgentRequestRecord, error) {
	if c.Client == nil {
		return AgentRequestRecord{}, fmt.Errorf("rhizome client is nil")
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return AgentRequestRecord{}, fmt.Errorf("request_id is required")
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	for {
		if err := ctx.Err(); err != nil {
			return AgentRequestRecord{}, formatManagedRuntimeControlWaitContextError(requestID, err)
		}
		record, err := c.Client.GetAgentRequestResult(ctx, c.WorkspaceID, requestID)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return AgentRequestRecord{}, formatManagedRuntimeControlWaitContextError(requestID, firstNonNilError(ctx.Err(), err))
			}
			return AgentRequestRecord{}, err
		}
		if isCompletedAgentRequest(record) {
			return record, nil
		}
		if time.Now().After(deadline) {
			if record.RequestID != "" {
				return record, formatManagedRuntimeControlWaitTimeout(requestID, timeout, record)
			}
			return AgentRequestRecord{}, formatManagedRuntimeControlWaitTimeout(requestID, timeout, AgentRequestRecord{})
		}
		if !sleepContext(ctx, managedRuntimeControlPollInterval) {
			return AgentRequestRecord{}, formatManagedRuntimeControlWaitContextError(requestID, ctx.Err())
		}
	}
}

func formatManagedRuntimeControlWaitContextError(requestID string, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("request %s timed out while waiting for result: %w", requestID, context.DeadlineExceeded)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("request %s canceled while waiting for result: %w", requestID, context.Canceled)
	default:
		return err
	}
}

func formatManagedRuntimeControlWaitTimeout(requestID string, timeout time.Duration, record AgentRequestRecord) error {
	status := strings.TrimSpace(record.Status)
	if status != "" {
		return fmt.Errorf("timed out waiting for request %s after %s (last status: %s)", requestID, timeout, status)
	}
	return fmt.Errorf("timed out waiting for request %s after %s", requestID, timeout)
}

func classifyManagedRuntimeControlRequestResult(record AgentRequestRecord) error {
	status := strings.ToUpper(strings.TrimSpace(record.Status))
	switch status {
	case "FAILED", "TIMEOUT", "CANCELLED", "REJECTED":
		requestID := strings.TrimSpace(record.RequestID)
		if detail := firstNonEmpty(prettyJSONText(strings.TrimSpace(record.Response)), strings.TrimSpace(record.Response)); detail != "" {
			return fmt.Errorf("request %s finished with status %s: %s", requestID, status, detail)
		}
		return fmt.Errorf("request %s finished with status %s", requestID, status)
	default:
		return nil
	}
}

func (c ManagedRuntimeControlClient) Status(ctx context.Context, timeout time.Duration, reason string) (AgentRequestRecord, error) {
	return c.Request(ctx, "runtime.status", map[string]any{"reason": reason}, timeout)
}

func (c ManagedRuntimeControlClient) Pause(ctx context.Context, reason string, timeout time.Duration) (AgentRequestRecord, error) {
	return c.Request(ctx, "runtime.pause", map[string]any{"reason": reason}, timeout)
}

func (c ManagedRuntimeControlClient) Resume(ctx context.Context, reason, taskID, sessionID string, timeout time.Duration) (AgentRequestRecord, error) {
	payload := map[string]any{"reason": reason}
	if strings.TrimSpace(taskID) != "" {
		payload["task_id"] = taskID
	}
	if strings.TrimSpace(sessionID) != "" {
		payload["session_id"] = sessionID
	}
	return c.Request(ctx, "runtime.resume", payload, timeout)
}

func (c ManagedRuntimeControlClient) SwitchTask(ctx context.Context, taskID, sessionID, reason string, timeout time.Duration) (AgentRequestRecord, error) {
	payload := map[string]any{
		"task_id": taskID,
		"reason":  reason,
	}
	if strings.TrimSpace(sessionID) != "" {
		payload["session_id"] = sessionID
	}
	return c.Request(ctx, "runtime.switch_task", payload, timeout)
}

func (c ManagedRuntimeControlClient) SwitchTension(ctx context.Context, tensionID, action, role, lifecycleState, reason string, timeout time.Duration) (AgentRequestRecord, error) {
	payload := map[string]any{
		"tension_id": tensionID,
		"action":     action,
		"reason":     reason,
	}
	if strings.TrimSpace(role) != "" {
		payload["role"] = role
	}
	if strings.TrimSpace(lifecycleState) != "" {
		payload["lifecycle_state"] = lifecycleState
	}
	return c.Request(ctx, "runtime.switch_tension", payload, timeout)
}
