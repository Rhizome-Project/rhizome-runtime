package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/hooks"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/plugins"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools/control"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools/memory"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/sessionmemory"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// Agent is the interface for all agent types.
type Agent interface {
	ID() string
	Run(ctx context.Context, task string) (*LoopResult, error)
}

// AgentConfig configures an InternalAgent.
type AgentConfig struct {
	ID           string
	WorkspaceID  string
	WorkspaceDir string
	AllowedTiers []string
	DisplayName  string
	Role         string
	StaticPrompt string
	LoopConfig   LoopConfig
	RepoMutation tools.RepoMutationPolicy
}

// InternalAgent is a fully autonomous agent running inside Rhizome.
type InternalAgent struct {
	id           string
	workspaceID  string
	store        *sqlite.Store
	loop         *Loop
	llmClient    *llm.Client
	toolRegistry *tools.Registry
	hookRunner   *hooks.Runner
	config       AgentConfig
	systemPrompt string
}

// NewInternalAgent creates a new internal agent with built-in tools.
func NewInternalAgent(store *sqlite.Store, llmClient *llm.Client, cfg AgentConfig) (*InternalAgent, error) {
	if store == nil {
		return nil, errors.New("store is required")
	}
	if llmClient == nil {
		return nil, errors.New("llm client is required")
	}
	if cfg.ID == "" {
		return nil, errors.New("agent ID is required")
	}
	if cfg.WorkspaceID == "" {
		return nil, errors.New("workspace ID is required")
	}
	if cfg.DisplayName == "" {
		cfg.DisplayName = cfg.ID
	}
	if cfg.Role == "" {
		cfg.Role = "internal-agent"
	}

	if len(cfg.AllowedTiers) == 0 {
		cfg.AllowedTiers = []string{"autonomous"}
	}

	// Create tool registry with built-ins
	toolReg := tools.NewRegistry()
	tools.RegisterBuiltins(toolReg, tools.BuiltinConfig{
		WorkspaceDir: cfg.WorkspaceDir,
		AllowedTiers: cfg.AllowedTiers,
		RepoMutation: cfg.RepoMutation,
	})

	// Register specific native agent toolsets
	memory.RegisterTools(toolReg, store, cfg.WorkspaceID, cfg.ID)
	memory.RegisterCoherenceTools(toolReg, store, cfg.WorkspaceID, cfg.ID)
	control.RegisterTensionTools(toolReg, store, cfg.WorkspaceID, cfg.ID)

	// Create hook runner
	hookRunner := hooks.NewRunner()

	// Build tool descriptions for prompt
	var toolDescs []ToolDescription
	for _, t := range toolReg.List() {
		toolDescs = append(toolDescs, ToolDescription{
			Name:        t.Name(),
			Description: t.Description(),
		})
	}

	// Build system prompt
	systemPrompt := BuildSystemPrompt(PromptConfig{
		StaticPrompt:     cfg.StaticPrompt,
		EnvironmentInfo:  EnvironmentInfoFromOS(),
		ToolDescriptions: toolDescs,
	})

	// Create loop
	agentLoop := NewLoop(llmClient, toolReg, hookRunner, systemPrompt, cfg.LoopConfig)
	agentLoop.SetCompactFunc(CompactMessages)

	return &InternalAgent{
		id:           cfg.ID,
		workspaceID:  cfg.WorkspaceID,
		store:        store,
		loop:         agentLoop,
		llmClient:    llmClient,
		toolRegistry: toolReg,
		hookRunner:   hookRunner,
		config:       cfg,
		systemPrompt: systemPrompt,
	}, nil
}

// ID returns the agent's identifier.
func (a *InternalAgent) ID() string { return a.id }

// Run executes the agent for the given task.
func (a *InternalAgent) Run(ctx context.Context, task string) (*LoopResult, error) {
	sessionID := fmt.Sprintf("sess-%s-%d", a.id, time.Now().UnixNano())
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)

	// Ensure the agent exists without re-clobbering existing executor metadata on every run.
	if _, err := a.store.EnsureAgentRegistered(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:     a.workspaceID,
		AgentID:         a.id,
		OwnerUserID:     "system",
		DisplayName:     a.config.DisplayName,
		Role:            a.config.Role,
		Status:          "ACTIVE",
		ProtocolVersion: "internal-agent/v1",
		Capabilities:    []string{"autonomous", "tool-use"},
		Summary:         "Internal autonomous agent",
	}); err != nil {
		log.Printf("[agent] registration warning (may already exist): %v", err)
	}

	// Create session
	sessionCreateInput := sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     a.id,
		WorkspaceID: a.workspaceID,
		StartedAt:   startedAt,
	}
	if _, err := a.store.CreateAgentSessionWithRuntimeEvent(ctx, sessionCreateInput); err != nil {
		return nil, fmt.Errorf("create internal agent session receipt: %w", err)
	}

	// Post session_started update
	a.store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: a.workspaceID,
		AgentID:     a.id,
		UpdateType:  "session_started",
		Summary:     fmt.Sprintf("Started session %s", sessionID),
	})
	a.recordSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: a.workspaceID,
		SessionID:   sessionID,
		AgentID:     a.id,
		Summary:     internalAgentSessionStartSummary(task),
		OwnerScope:  "task/session",
	})

	log.Printf("[agent] %s starting session %s", a.id, sessionID)

	observer := &sessionRuntimeObserver{
		store:       a.store,
		sessionID:   sessionID,
		workspaceID: a.workspaceID,
		agentID:     a.id,
	}
	a.loop.SetObserver(observer)
	defer a.loop.SetObserver(nil)

	// PRE-FLIGHT HYDRATION: Fetch session memory bounds
	packet, err := a.store.BuildMemoryKernelPacket(ctx, sqlite.MemoryPacketFilter{
		WorkspaceID: a.workspaceID,
		SessionID:   sessionID,
		AgentID:     a.id,
	})

	var dynamicContext string
	if err != nil {
		log.Printf("[agent] warning: memory kernel packet hydration failed: %v", err)
	} else {
		if packetJSON, err := json.MarshalIndent(packet, "", "  "); err == nil {
			dynamicContext = fmt.Sprintf("<memory_kernel>\n%s\n</memory_kernel>", string(packetJSON))
		}
	}

	// Run loop
	result, err := a.loop.RunWithContext(ctx, task, dynamicContext)
	if err != nil {
		return nil, err
	}

	// Persist session result
	completedAt := time.Now().UTC().Format(time.RFC3339Nano)
	status := "COMPLETED"
	errMsg := ""
	if result.Error != nil {
		status = "FAILED"
		errMsg = result.Error.Error()
	}

	if syncErr := observer.SyncState(ctx, LoopStateSnapshot{
		Messages:          result.Messages,
		Iterations:        result.Iterations,
		TotalInputTokens:  result.TotalInputTokens,
		TotalOutputTokens: result.TotalOutputTokens,
		ToolCalls:         result.ToolCalls,
		PromptTokens:      EstimateTokens(result.Messages),
		StopReason:        result.StopReason,
	}); syncErr != nil {
		log.Printf("[agent] failed to sync final session state: %v", syncErr)
	}

	sessionUpdate := sqlite.AgentSessionUpdateInput{
		SessionID:         sessionID,
		Status:            status,
		Iterations:        result.Iterations,
		TotalInputTokens:  result.TotalInputTokens,
		TotalOutputTokens: result.TotalOutputTokens,
		ToolCalls:         result.ToolCalls,
		StopReason:        string(result.StopReason),
		ErrorMessage:      errMsg,
		CompletedAt:       completedAt,
	}
	var updateErr error
	_, updateErr = a.store.UpdateAgentSessionWithRuntimeEvent(ctx, sessionUpdate)
	if updateErr != nil {
		log.Printf("[agent] failed to update session: %v", updateErr)
	}

	// Post final update
	updateType := "session_completed"
	summary := fmt.Sprintf("Session %s completed: %d iterations, %d tool calls", sessionID, result.Iterations, result.ToolCalls)
	if result.Error != nil {
		updateType = "session_failed"
		summary = fmt.Sprintf("Session %s failed: %v", sessionID, result.Error)
	}
	a.store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: a.workspaceID,
		AgentID:     a.id,
		UpdateType:  updateType,
		Summary:     summary,
	})
	a.recordSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: a.workspaceID,
		SessionID:   sessionID,
		AgentID:     a.id,
		Summary:     clipInternalAgentSessionSummary(summary, 240),
		UpdatedAt:   completedAt,
	})

	log.Printf("[agent] %s session %s %s: %d iterations, %d tools, %d/%d tokens",
		a.id, sessionID, status, result.Iterations, result.ToolCalls,
		result.TotalInputTokens, result.TotalOutputTokens)

	return result, nil
}

// RegisterPlugin registers a plugin's tools and hooks with this agent.
func (a *InternalAgent) RegisterPlugin(plugin plugins.Plugin) {
	for _, t := range plugin.Tools() {
		if err := a.toolRegistry.Register(t); err != nil {
			log.Printf("[agent] plugin %s: failed to register tool: %v", plugin.Name(), err)
		}
	}
	for _, h := range plugin.Hooks() {
		a.hookRunner.Register(h)
	}
}

// AddHook registers a hook directly with the hook runner.
func (a *InternalAgent) AddHook(hook hooks.Hook) {
	a.hookRunner.Register(hook)
}

func (a *InternalAgent) recordSessionCoordination(ctx context.Context, input sqlite.AgentSessionCoordinationInput) {
	state, err := a.store.RecordAgentSessionCoordination(ctx, input)
	if err != nil {
		log.Printf("[agent] failed to record session coordination event %s for %s: %v", input.EventType, input.SessionID, err)
		return
	}
	if err := sessionmemory.SyncEvent(ctx, a.store, state.WorkspaceID, state); err != nil {
		log.Printf("[agent] failed to sync session memory for %s: %v", state.SessionID, err)
	}
	if _, err := a.store.SyncOperatorQueueFromSessionState(ctx, state); err != nil {
		log.Printf("[agent] failed to sync operator queue for %s: %v", state.SessionID, err)
	}
	if err := a.store.SyncExecutionRunFromSessionState(ctx, state); err != nil {
		log.Printf("[agent] failed to sync execution run for %s: %v", state.SessionID, err)
	}
}

func internalAgentSessionStartSummary(task string) string {
	if trimmed := strings.TrimSpace(task); trimmed != "" {
		return clipInternalAgentSessionSummary(trimmed, 240)
	}
	return "Internal agent session started"
}

type sessionRuntimeObserver struct {
	store              *sqlite.Store
	sessionID          string
	workspaceID        string
	agentID            string
	lastPersistedCount int
	lastPromptTokenSum int
}

func (o *sessionRuntimeObserver) OnState(ctx context.Context, snapshot LoopStateSnapshot) error {
	return o.SyncState(ctx, snapshot)
}

func (o *sessionRuntimeObserver) OnCompaction(ctx context.Context, snapshot LoopCompactionSnapshot) error {
	if snapshot.CompactionError != nil {
		return nil
	}
	if err := o.SyncState(ctx, snapshot.After); err != nil {
		return err
	}

	var summaryMemoryID string
	summaryText := clipInternalAgentSessionSummary(strings.TrimSpace(snapshot.SummaryText), 4000)
	if summaryText != "" {
		record, err := o.store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
			WorkspaceID: o.workspaceID,
			MemoryType:  "SUMMARY",
			Title:       fmt.Sprintf("Compaction summary for %s", o.sessionID),
			Body:        summaryText,
			Summary:     clipInternalAgentSessionSummary(summaryText, 240),
			AgentID:     o.agentID,
			SessionID:   o.sessionID,
			SourceKind:  "compaction",
			SourceID:    o.sessionID,
			Tags:        []string{"compaction", "runtime-truth"},
			Importance:  0.4,
			Confidence:  0.7,
		})
		if err != nil {
			log.Printf("[agent] failed to record compaction memory for %s: %v", o.sessionID, err)
		} else {
			summaryMemoryID = record.MemoryID
		}
	}

	_, err := o.store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
		SessionID:              o.sessionID,
		WorkspaceID:            o.workspaceID,
		AgentID:                o.agentID,
		TriggerKind:            firstNonEmptyString(strings.TrimSpace(snapshot.Trigger), "token_budget_exceeded"),
		PackMode:               internalAgentEpisodePackMode(summaryText),
		SourceWindowDigest:     internalAgentCompactionSourceWindowDigest(snapshot.Before.Messages),
		TokenBudget:            snapshot.TokenBudget,
		MessageCountBefore:     len(snapshot.Before.Messages),
		MessageCountAfter:      len(snapshot.After.Messages),
		MessageTokensBefore:    snapshot.Before.PromptTokens,
		MessageTokensAfter:     snapshot.After.PromptTokens,
		TotalInputTokens:       snapshot.After.TotalInputTokens,
		TotalOutputTokens:      snapshot.After.TotalOutputTokens,
		SummaryText:            summaryText,
		SummaryWorkspaceMemory: summaryMemoryID,
	})
	return err
}

func (o *sessionRuntimeObserver) SyncState(ctx context.Context, snapshot LoopStateSnapshot) error {
	if o.store == nil {
		return nil
	}
	if err := o.store.UpdateAgentSession(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:         o.sessionID,
		Status:            "RUNNING",
		Iterations:        snapshot.Iterations,
		TotalInputTokens:  snapshot.TotalInputTokens,
		TotalOutputTokens: snapshot.TotalOutputTokens,
		ToolCalls:         snapshot.ToolCalls,
	}); err != nil {
		return fmt.Errorf("update runtime session metrics: %w", err)
	}

	if o.needsReplace(snapshot.Messages) {
		items, err := marshalSessionMessages(o.sessionID, snapshot.Messages)
		if err != nil {
			return err
		}
		if err := o.store.ReplaceAgentSessionMessages(ctx, o.sessionID, items); err != nil {
			return fmt.Errorf("replace runtime session messages: %w", err)
		}
		o.lastPersistedCount = len(snapshot.Messages)
		o.lastPromptTokenSum = snapshot.PromptTokens
		return nil
	}

	for i := o.lastPersistedCount; i < len(snapshot.Messages); i++ {
		item, err := marshalSessionMessage(o.sessionID, i, snapshot.Messages[i])
		if err != nil {
			return err
		}
		if err := o.store.AppendAgentSessionMessage(ctx, item); err != nil {
			return fmt.Errorf("append runtime session message %d: %w", i, err)
		}
	}
	o.lastPersistedCount = len(snapshot.Messages)
	o.lastPromptTokenSum = snapshot.PromptTokens
	return nil
}

func (o *sessionRuntimeObserver) needsReplace(messages []Message) bool {
	switch {
	case len(messages) < o.lastPersistedCount:
		return true
	case o.lastPersistedCount == 0 && len(messages) > 1:
		return false
	case len(messages) == o.lastPersistedCount && o.lastPromptTokenSum != EstimateTokens(messages):
		return true
	default:
		return false
	}
}

func marshalSessionMessages(sessionID string, messages []Message) ([]sqlite.AgentSessionMessageInput, error) {
	items := make([]sqlite.AgentSessionMessageInput, 0, len(messages))
	for i, msg := range messages {
		item, err := marshalSessionMessage(sessionID, i, msg)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func marshalSessionMessage(sessionID string, sequence int, msg Message) (sqlite.AgentSessionMessageInput, error) {
	contentJSON, err := json.Marshal(msg.Content)
	if err != nil {
		return sqlite.AgentSessionMessageInput{}, fmt.Errorf("marshal session message %d: %w", sequence, err)
	}
	return sqlite.AgentSessionMessageInput{
		SessionID:   sessionID,
		Sequence:    sequence,
		Role:        string(msg.Role),
		ContentJSON: string(contentJSON),
		TokenCount:  EstimateTokens([]Message{msg}),
	}, nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func clipInternalAgentSessionSummary(summary string, limit int) string {
	summary = strings.TrimSpace(summary)
	if summary == "" || limit <= 0 || len(summary) <= limit {
		return summary
	}
	if limit <= 3 {
		return summary[:limit]
	}
	return summary[:limit-3] + "..."
}

func internalAgentCompactionSourceWindowDigest(messages []Message) string {
	if len(messages) == 0 {
		return ""
	}
	payload, err := json.Marshal(messages)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func internalAgentEpisodePackMode(summaryText string) string {
	if strings.HasPrefix(strings.TrimSpace(summaryText), "[Previous conversation history was truncated due to length.") {
		return "DETERMINISTIC_FALLBACK"
	}
	return "COMPLETE"
}
