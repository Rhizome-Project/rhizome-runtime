package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runAgentSession(args []string) error {
	if len(args) < 1 {
		printAgentUsage(os.Stderr)
		return errors.New("missing agent session subcommand")
	}

	switch args[0] {
	case "start":
		return runAgentSessionEvent(args[1:], model.SessionEventStart)
	case "status":
		return runAgentSessionEvent(args[1:], model.SessionEventStatus)
	case "blocked":
		return runAgentSessionEvent(args[1:], model.SessionEventBlocked)
	case "decision-needed":
		return runAgentSessionEvent(args[1:], model.SessionEventDecisionNeeded)
	case "keepalive":
		return runAgentSessionEvent(args[1:], model.SessionEventKeepalive)
	case "end":
		return runAgentSessionEvent(args[1:], model.SessionEventEnd)
	case "sync-queue":
		return runAgentSessionSyncQueue(args[1:])
	case "takeover":
		return runAgentSessionTakeover(args[1:])
	case "list":
		return runAgentSessionList(args[1:])
	default:
		printAgentUsage(os.Stderr)
		return fmt.Errorf("unknown agent session subcommand: %s", args[0])
	}
}

func runAgentSessionEvent(args []string, eventType string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("agent session "+eventType, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	sessionID := fs.String("session-id", "", "Session identifier")
	agentID := fs.String("agent-id", "", "Agent identifier")
	taskID := fs.String("task-id", "", "Optional task identifier")
	summary := fs.String("summary", "", "Session summary")
	status := fs.String("status", "", "Optional explicit session status override")
	ownerScope := fs.String("owner-scope", "", "Optional ownership scope summary")
	blockedOn := fs.String("blocked-on", "", "Comma separated kind:detail blockers")
	decisionNeededFrom := fs.String("decision-needed-from", "", "Actor expected to make the decision")
	decisionType := fs.String("decision-type", "", "Optional decision type label")
	keepSessionActive := fs.String("keep-session-active", "", "Optional true/false override")
	handoffTo := fs.String("handoff-to", "", "Optional agent receiving the handoff")
	docKeys := fs.String("doc-keys", "", "Comma separated related workspace doc keys")
	artifactRefs := fs.String("artifact-refs", "", "Comma separated artifact refs or path@kind@content_type tuples")
	updatedAt := fs.String("updated-at", "", "Optional explicit updated_at timestamp")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	keepPtr, err := parseOptionalBool(*keepSessionActive)
	if err != nil {
		return err
	}

	state, event, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:             eventType,
		WorkspaceID:           *workspaceID,
		SessionID:             *sessionID,
		AgentID:               *agentID,
		TaskID:                *taskID,
		Summary:               *summary,
		Status:                *status,
		OwnerScope:            *ownerScope,
		BlockedOn:             parseBlockedRefs(*blockedOn),
		DecisionNeededFrom:    *decisionNeededFrom,
		DecisionType:          *decisionType,
		KeepSessionActive:     keepPtr,
		HandoffTo:             *handoffTo,
		RelatedDocKeys:        parseCSV(*docKeys),
		RelatedArtifactRefs:   parseArtifactRefs(*artifactRefs),
		UpdatedAt:             *updatedAt,
		PromptContextEnvelope: cliAgentSessionPromptContextEnvelope(sessionLifecycleSurface(eventType), *workspaceID, *agentID),
	})
	if err != nil {
		return fmt.Errorf("record agent session event: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":         traceID,
		"state":            state,
		"runtime_event_id": event.EventID,
		"status":           "RECORDED",
	})
}

func runAgentSessionList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("agent session list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	activeOnly := fs.Bool("active-only", true, "Whether to return only active session states")
	limit := fs.Int("limit", 20, "Maximum number of session states to return")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	sessions, err := store.ListWorkspaceSessionStates(ctx, *workspaceID, *activeOnly, *limit)
	if err != nil {
		return fmt.Errorf("list workspace session states: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"sessions":     sessions,
		"count":        len(sessions),
	})
}

func runAgentSessionSyncQueue(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("agent session sync-queue", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	sessionID := fs.String("session-id", "", "Session identifier")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	state, err := store.GetAgentSessionState(ctx, *workspaceID, *sessionID)
	if err != nil {
		return fmt.Errorf("get agent session state: %w", err)
	}
	result, err := store.SyncOperatorQueueFromSessionState(ctx, state)
	if err != nil {
		return fmt.Errorf("sync operator queue from session state: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"state":    state,
		"result":   result,
		"status":   "SYNCED",
	})
}

func runAgentSessionTakeover(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("agent session takeover", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	sessionID := fs.String("session-id", "", "Current session identifier")
	takeoverAgentID := fs.String("takeover-agent-id", "", "New owner agent identifier")
	summary := fs.String("summary", "", "Why the takeover is happening")
	successorSessionID := fs.String("successor-session-id", "", "Optional successor session identifier")
	successorSummary := fs.String("successor-summary", "", "Optional successor session summary")
	updatedAt := fs.String("updated-at", "", "Optional explicit updated_at timestamp")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	record, event, err := store.TakeOverAgentSessionWithEvent(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:           *workspaceID,
		SessionID:             *sessionID,
		SuccessorSessionID:    *successorSessionID,
		TakeoverAgentID:       *takeoverAgentID,
		Summary:               *summary,
		SuccessorSummary:      *successorSummary,
		UpdatedAt:             *updatedAt,
		PromptContextEnvelope: cliAgentSessionPromptContextEnvelope("agent.session.takeover", *workspaceID, *takeoverAgentID),
	})
	if err != nil {
		return fmt.Errorf("take over agent session: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":         traceID,
		"source_state":     record.SourceState,
		"successor_state":  record.SuccessorState,
		"runtime_event_id": event.EventID,
		"status":           "TAKEN_OVER",
	})
}

func cliAgentSessionPromptContextEnvelope(surface, workspaceID, agentID string) map[string]any {
	return sqlite.BuildSessionPromptContextEnvelope(surface, "cli_local", workspaceID, "agent", strings.TrimSpace(agentID))
}

func sessionLifecycleSurface(eventType string) string {
	switch model.NormalizeSessionEventType(eventType) {
	case model.SessionEventStart:
		return "agent.session.start"
	case model.SessionEventStatus:
		return "agent.session.status"
	case model.SessionEventBlocked:
		return "agent.session.blocked"
	case model.SessionEventDecisionNeeded:
		return "agent.session.decision_needed"
	case model.SessionEventKeepalive:
		return "agent.session.keepalive"
	case model.SessionEventEnd:
		return "agent.session.end"
	default:
		return "agent." + model.NormalizeSessionEventType(eventType)
	}
}

func decodeAgentSessionPromptContextEnvelope(payloadJSON string) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return nil, err
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		return nil, errors.New("missing prompt_context_envelope")
	}
	return envelope, nil
}

func parseOptionalBool(raw string) (*bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	value, err := strconv.ParseBool(trimmed)
	if err != nil {
		return nil, fmt.Errorf("parse keep-session-active: %w", err)
	}
	return &value, nil
}
