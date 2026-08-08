package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var managedAgentRequestPollInterval = 500 * time.Millisecond

type managedAgentControlRequestResult struct {
	RequestID string
	Status    string
	Response  string
}

func sendManagedAgentControlRequest(ctx context.Context, record ManagedAgentRecord, method string, payload any) (managedAgentControlRequestResult, error) {
	client, err := managedAgentControlClientForRecord(record)
	if err != nil {
		return managedAgentControlRequestResult{}, err
	}
	recordResult, err := client.Request(ctx, method, payload, 2*time.Minute)
	result := managedAgentControlRequestResultFromRecord(recordResult)
	if err != nil {
		return result, err
	}
	return result, nil
}

func managedAgentControlRequestResultFromRecord(record AgentRequestRecord) managedAgentControlRequestResult {
	return managedAgentControlRequestResult{
		RequestID: strings.TrimSpace(record.RequestID),
		Status:    strings.TrimSpace(record.Status),
		Response:  strings.TrimSpace(record.Response),
	}
}

func hasManagedAgentControlRequestResult(result managedAgentControlRequestResult) bool {
	return strings.TrimSpace(result.RequestID) != "" || strings.TrimSpace(result.Status) != "" || strings.TrimSpace(result.Response) != ""
}

func managedAgentControlClientForRecord(record ManagedAgentRecord) (ManagedRuntimeControlClient, error) {
	record = normalizeManagedAgentRecord(record)
	global := LoadRhizomeProfile()
	local := LoadLocalRuntimeProfile(record.Workdir)

	hostURL := firstNonEmpty(local.HostURL, record.HostURL, global.HostURL, hostURLForRPC(global.RPCEndpoint))
	rpcEndpoint := firstNonEmpty(local.RPCEndpoint, defaultRPCEndpoint(hostURL), global.RPCEndpoint)
	if strings.TrimSpace(rpcEndpoint) == "" {
		return ManagedRuntimeControlClient{}, fmt.Errorf("rhizome host url is required for live control")
	}

	token, fromAgentID := managedAgentControlAuth(global, local, record)
	if strings.TrimSpace(token) == "" {
		return ManagedRuntimeControlClient{}, fmt.Errorf("rhizome operator or local agent token is required for live control")
	}

	workspaceID := firstNonEmpty(local.effectiveWorkspaceID(), record.WorkspaceID, global.WorkspaceID)
	if strings.TrimSpace(workspaceID) == "" {
		return ManagedRuntimeControlClient{}, fmt.Errorf("workspace id is required for live control")
	}

	if strings.TrimSpace(fromAgentID) == "" {
		return ManagedRuntimeControlClient{}, fmt.Errorf("agent id is required for live control")
	}

	return ManagedRuntimeControlClient{
		Client:      NewRhizomeClient(rpcEndpoint, token),
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   record.AgentID,
	}, nil
}

func managedAgentControlToken(global RhizomeConnectionProfile, local LocalRuntimeProfile) string {
	token, _ := managedAgentControlAuth(global, local, ManagedAgentRecord{})
	return token
}

func managedAgentControlAuth(global RhizomeConnectionProfile, local LocalRuntimeProfile, record ManagedAgentRecord) (string, string) {
	if token := strings.TrimSpace(local.AgentToken); token != "" {
		return token, firstNonEmpty(local.effectiveAgentID(), record.AgentID)
	}
	if token := strings.TrimSpace(global.AgentToken); token != "" {
		return token, firstNonEmpty(global.AgentID, local.effectiveAgentID(), record.AgentID, "manager")
	}
	return "", firstNonEmpty(local.effectiveAgentID(), record.AgentID, global.AgentID, "manager")
}

func waitForManagedAgentRequestResult(ctx context.Context, client *RhizomeClient, workspaceID, requestID string, timeout time.Duration) (AgentRequestRecord, error) {
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	var last AgentRequestRecord
	for {
		if err := ctx.Err(); err != nil {
			return AgentRequestRecord{}, formatManagedAgentRequestWaitContextError(requestID, err)
		}
		result, err := client.GetAgentRequestResult(ctx, workspaceID, requestID)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return AgentRequestRecord{}, formatManagedAgentRequestWaitContextError(requestID, firstNonNilError(ctx.Err(), err))
			}
			return AgentRequestRecord{}, err
		}
		last = result
		if isCompletedAgentRequest(result) {
			return result, nil
		}
		if time.Now().After(deadline) {
			return last, formatManagedAgentRequestWaitTimeout(requestID, timeout, last)
		}
		if !sleepContext(ctx, managedAgentRequestPollInterval) {
			return AgentRequestRecord{}, formatManagedAgentRequestWaitContextError(requestID, ctx.Err())
		}
	}
}

func formatManagedAgentRequestWaitContextError(requestID string, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("request %s timed out while waiting for result: %w", requestID, context.DeadlineExceeded)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("request %s canceled while waiting for result: %w", requestID, context.Canceled)
	default:
		return err
	}
}

func formatManagedAgentRequestWaitTimeout(requestID string, timeout time.Duration, record AgentRequestRecord) error {
	status := strings.TrimSpace(record.Status)
	if status != "" {
		return fmt.Errorf("timed out waiting for request %s after %s (last status: %s)", requestID, timeout, status)
	}
	return fmt.Errorf("timed out waiting for request %s after %s", requestID, timeout)
}

func firstNonNilError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func isCompletedAgentRequest(record AgentRequestRecord) bool {
	status := strings.ToUpper(strings.TrimSpace(record.Status))
	switch status {
	case "COMPLETED", "TIMEOUT", "FAILED", "CANCELLED", "REJECTED":
		return true
	default:
		return status == "" && strings.TrimSpace(record.Response) != ""
	}
}
