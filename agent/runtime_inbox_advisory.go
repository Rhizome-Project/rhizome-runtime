package main

import (
	"context"
	"fmt"
	"strings"
)

const runtimeInboxDrainRequestLimit = 5

func (r *Runtime) refreshInboxDrainAdvisory(ctx context.Context) (bool, error) {
	if r == nil || r.client == nil {
		return false, nil
	}
	if !r.cfg.InboxDrainAdvisory {
		return false, nil
	}
	requests, err := r.client.ListOpenRequests(ctx, r.cfg.WorkspaceID, r.cfg.AgentID, runtimeInboxDrainRequestLimit)
	if err != nil {
		return false, err
	}
	signal := runtimeInboxDrainAdvisorySignal(requests)
	if signal == "" {
		return false, nil
	}

	r.mu.Lock()
	currentSignals := append([]string(nil), r.scratch.AdvisorySignals...)
	r.mu.Unlock()
	if _, changed := appendRuntimeInboxDrainAdvisory(currentSignals, signal); !changed {
		return false, nil
	}

	changed := false
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		next, ok := appendRuntimeInboxDrainAdvisory(state.AdvisorySignals, signal)
		if !ok {
			return
		}
		state.AdvisorySignals = next
		state.LastSummary = "Pending peer requests require inbox drain"
		changed = true
	}); err != nil {
		return false, err
	}
	return changed, nil
}

func runtimeInboxDrainAdvisorySignal(requests []AgentRequestRecord) string {
	filtered := make([]AgentRequestRecord, 0, len(requests))
	for _, request := range requests {
		if strings.TrimSpace(request.RequestID) == "" {
			continue
		}
		status := strings.ToUpper(strings.TrimSpace(request.Status))
		if status != "" && status != "PENDING" && status != "PROCESSING" {
			continue
		}
		filtered = append(filtered, request)
		if len(filtered) >= runtimeInboxDrainRequestLimit {
			break
		}
	}
	if len(filtered) == 0 {
		return ""
	}

	parts := make([]string, 0, len(filtered))
	for _, request := range filtered {
		parts = append(parts, runtimeInboxDrainRequestSummary(request))
	}
	return fmt.Sprintf(
		"SYSTEM INBOX DRAIN: You have %d open peer request(s). Before continuing the claimed task, triage and answer or route them. Pending: %s.",
		len(filtered),
		strings.Join(parts, "; "),
	)
}

func runtimeInboxDrainRequestSummary(request AgentRequestRecord) string {
	id := strings.TrimSpace(request.RequestID)
	from := firstNonEmpty(strings.TrimSpace(request.FromAgentID), "unknown")
	method := firstNonEmpty(strings.TrimSpace(request.Method), "unknown")
	payload := compactSingleLine(strings.TrimSpace(request.Payload))
	if len(payload) > 120 {
		payload = strings.TrimSpace(payload[:120]) + "..."
	}
	if payload == "" {
		return fmt.Sprintf("%s from %s method %s", id, from, method)
	}
	return fmt.Sprintf("%s from %s method %s payload %q", id, from, method, payload)
}

func appendRuntimeInboxDrainAdvisory(signals []string, signal string) ([]string, bool) {
	signal = strings.TrimSpace(signal)
	if signal == "" {
		return signals, false
	}
	for _, existing := range signals {
		if strings.TrimSpace(existing) == signal {
			return signals, false
		}
	}
	return appendCappedAdvisorySignal(signals, signal), true
}

func compactSingleLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
