package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

const (
	agentRequestResponseUpdateType = "coordination"
	maxAgentRequestEvidenceText    = 12000
	maxAgentRequestPayloadText     = 6000
)

var agentRequestResponseTaskIDPattern = regexp.MustCompile(`\btask-[A-Za-z0-9][A-Za-z0-9_-]{1,}\b`)

func (r *Runtime) publishAgentRequestResponseEvidence(ctx context.Context, request AgentRequestRecord, response string) error {
	if r == nil || r.client == nil {
		return nil
	}
	workspaceID := firstNonEmpty(request.WorkspaceID, r.cfg.WorkspaceID)
	agentID := firstNonEmpty(r.cfg.AgentID, request.ToAgentID)
	requestID := strings.TrimSpace(request.RequestID)
	if workspaceID == "" || agentID == "" || requestID == "" {
		return nil
	}

	taskIDs := agentRequestResponseTaskIDs(request, response)
	docKey := agentRequestResponseDocKey(request, requestID, taskIDs, response)
	title := fmt.Sprintf("Agent response - %s to %s", firstNonEmpty(agentID, "agent"), firstNonEmpty(request.FromAgentID, "requester"))
	content := renderAgentRequestResponseEvidenceDoc(request, response, taskIDs)

	sha, docErr := r.client.PutDoc(ctx, WorkspaceDocPutInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       title,
		Content:     content,
		UpdatedBy:   agentID,
	})

	payload := map[string]any{
		"status":        "agent_request_response_public_evidence",
		"request_id":    requestID,
		"from_agent_id": strings.TrimSpace(request.FromAgentID),
		"to_agent_id":   strings.TrimSpace(request.ToAgentID),
		"method":        strings.TrimSpace(request.Method),
		"doc_key":       docKey,
		"doc_status":    "stored",
		"task_ids":      taskIDs,
		"response":      truncate(strings.TrimSpace(response), 2000),
	}
	if sha != "" {
		payload["doc_sha"] = sha
	}
	if createdAt := strings.TrimSpace(request.CreatedAt); createdAt != "" {
		payload["request_created_at"] = createdAt
	}
	if docErr != nil {
		payload["doc_status"] = "failed"
		payload["doc_error"] = docErr.Error()
	}
	rawPayload, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		rawPayload = []byte("{}")
	}

	updateErr := r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  agentRequestResponseUpdateType,
		Summary:     summarizeAgentRequestResponseEvidence(request, response, taskIDs, docKey),
		PayloadJSON: string(rawPayload),
	})
	if updateErr == nil {
		r.markBootstrapStale()
	}

	switch {
	case docErr != nil && updateErr != nil:
		return fmt.Errorf("workspace doc publish failed: %v; update publish failed: %w", docErr, updateErr)
	case docErr != nil:
		return fmt.Errorf("workspace doc publish failed: %w", docErr)
	case updateErr != nil:
		return fmt.Errorf("update publish failed: %w", updateErr)
	default:
		return nil
	}
}

func agentRequestResponseDocKey(request AgentRequestRecord, requestID string, taskIDs []string, response string) string {
	requestSegment := sanitizeDocKeySegment(firstNonEmpty(requestID, "request"))
	if len(taskIDs) > 0 && !agentRequestResponseIsCoordinationAck(request, response) {
		return "task." + sanitizeDocKeySegment(taskIDs[0]) + ".agent_response." + requestSegment
	}
	return "agent_response." + requestSegment
}

func summarizeAgentRequestResponseEvidence(request AgentRequestRecord, response string, taskIDs []string, docKey string) string {
	from := firstNonEmpty(request.FromAgentID, "peer")
	to := firstNonEmpty(request.ToAgentID, "agent")
	requestID := firstNonEmpty(request.RequestID, "request")
	taskSuffix := ""
	if len(taskIDs) > 0 {
		taskSuffix = " for " + strings.Join(taskIDs, ",")
	}
	excerpt := truncate(oneLine(response), 220)
	if excerpt == "" {
		excerpt = "empty response"
	}
	return fmt.Sprintf("Answered peer model.ask %s %s->%s%s; doc_key=%s; response=%s", requestID, from, to, taskSuffix, docKey, excerpt)
}

func renderAgentRequestResponseEvidenceDoc(request AgentRequestRecord, response string, taskIDs []string) string {
	var b strings.Builder
	b.WriteString("# Agent Request Response Evidence\n\n")
	b.WriteString("- request_id: ")
	b.WriteString(strings.TrimSpace(request.RequestID))
	b.WriteString("\n")
	b.WriteString("- workspace_id: ")
	b.WriteString(strings.TrimSpace(request.WorkspaceID))
	b.WriteString("\n")
	b.WriteString("- from_agent_id: ")
	b.WriteString(strings.TrimSpace(request.FromAgentID))
	b.WriteString("\n")
	b.WriteString("- to_agent_id: ")
	b.WriteString(strings.TrimSpace(request.ToAgentID))
	b.WriteString("\n")
	b.WriteString("- method: ")
	b.WriteString(strings.TrimSpace(request.Method))
	b.WriteString("\n")
	if len(taskIDs) > 0 {
		b.WriteString("- task_ids: ")
		b.WriteString(strings.Join(taskIDs, ", "))
		b.WriteString("\n")
	}
	if createdAt := strings.TrimSpace(request.CreatedAt); createdAt != "" {
		b.WriteString("- request_created_at: ")
		b.WriteString(createdAt)
		b.WriteString("\n")
	}
	b.WriteString("- evidence_scope: ")
	if agentRequestResponseIsCoordinationAck(request, response) {
		b.WriteString("coordination_ack_not_validation")
	} else {
		b.WriteString("public workspace coordination")
	}
	b.WriteString("\n\n")

	if payload := formatAgentRequestResponsePayload(request.Payload); payload != "" {
		b.WriteString("## Request Payload\n\n")
		b.WriteString("```json\n")
		b.WriteString(payload)
		b.WriteString("\n```\n\n")
	}

	b.WriteString("## Response\n\n")
	response = strings.TrimSpace(response)
	if response == "" {
		response = "(empty response)"
	}
	b.WriteString(truncate(response, maxAgentRequestEvidenceText))
	b.WriteString("\n")
	return b.String()
}

func agentRequestResponseIsCoordinationAck(request AgentRequestRecord, response string) bool {
	var payload map[string]any
	if raw := strings.TrimSpace(request.Payload); raw != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	if normalizeAgentRequestKind(stringMapValue(payload, "request_kind")) == agentRequestKindDelegateTask {
		return true
	}
	lower := strings.ToLower(strings.Join([]string{request.Payload, response}, "\n"))
	for _, marker := range []string{
		"queued runtime_switch_task",
		"switch_queued",
		"claim_admitted",
		"not covered until",
		"cannot accept delegated task",
		"already terminal",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func formatAgentRequestResponsePayload(payload string) string {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return ""
	}
	var decoded any
	if err := json.Unmarshal([]byte(payload), &decoded); err == nil {
		if raw, marshalErr := json.MarshalIndent(decoded, "", "  "); marshalErr == nil {
			return truncate(string(raw), maxAgentRequestPayloadText)
		}
	}
	return truncate(payload, maxAgentRequestPayloadText)
}

func agentRequestResponseTaskIDs(request AgentRequestRecord, response string) []string {
	text := strings.Join([]string{
		request.RequestID,
		request.Payload,
		request.Response,
		response,
	}, "\n")
	matches := agentRequestResponseTaskIDPattern.FindAllString(text, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		taskID := strings.Trim(strings.TrimSpace(match), ".,;:()[]{}<>\"'")
		if taskID == "" {
			continue
		}
		key := strings.ToLower(taskID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, taskID)
		if len(out) >= 8 {
			break
		}
	}
	return out
}
