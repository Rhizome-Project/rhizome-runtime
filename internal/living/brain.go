package living

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
	"github.com/Rhizome-Project/rhizome-runtime/internal/living/memory"
)

// Brain is the top-level orchestrator for a living agent. It wires together
// all components: heartbeat, task execution, messaging, memory, reflection,
// and situation assessment.
type Brain struct {
	config            Config
	rhizome           RhizomeClient
	stateStore        StateStore
	memoryDB          *memory.MemoryDB
	memoryStore       *memory.MemoryStore
	memoryBackend     MemoryBackend
	heartbeat         *HeartbeatLoop
	taskExecutor      *TaskExecutor
	messageHandler    *MessageHandler
	contextInjector   *ContextInjector
	workerSpawner     *WorkerSpawner
	reflector         *Reflector
	situationAssessor *SituationAssessor
}

// BrainDeps holds injectable dependencies for testability.
type BrainDeps struct {
	Rhizome       RhizomeClient
	StateStore    StateStore
	TaskRunner    TaskRunner
	Triager       MessageTriager
	Evaluator     UpdateEvaluator
	ReflectionLLM ReflectionLLM
	SituationLLM  SituationLLM
	CompactLLM    CompactLLM
	WorkerRunner  WorkerRunner
}

// NewBrain creates a Brain wired with all components.
// If deps is nil, production defaults are used (which require Redis, LLM keys, etc).
// If deps is provided, the given mocks/implementations are used instead.
func NewBrain(cfg Config, deps *BrainDeps) (*Brain, error) {
	b := &Brain{config: cfg}

	if deps == nil {
		deps = &BrainDeps{}
	}
	if err := ValidateDependencyContract(cfg, deps); err != nil {
		return nil, err
	}

	// State store
	if deps.StateStore != nil {
		b.stateStore = deps.StateStore
	} else if cfg.RedisURL != "" {
		store, err := NewRedisStateStore(cfg.RedisURL, cfg.ID)
		if err != nil {
			return nil, fmt.Errorf("connect to Redis: %w", err)
		}
		b.stateStore = store
	} else {
		b.stateStore = NewMemoryStateStore()
	}

	// Rhizome client
	if deps.Rhizome != nil {
		b.rhizome = deps.Rhizome
	} else {
		return nil, fmt.Errorf("rhizome client is required (provide via BrainDeps)")
	}

	// Memory store (optional)
	if cfg.Memory.DBPath != "" {
		// Ensure parent directory exists
		if dir := filepath.Dir(cfg.Memory.DBPath); dir != "" {
			os.MkdirAll(dir, 0755)
		}
		db, err := memory.NewMemoryDB(cfg.Memory.DBPath)
		if err != nil {
			return nil, fmt.Errorf("open memory DB: %w", err)
		}
		b.memoryDB = db
		b.memoryStore = memory.NewMemoryStore(db)
	}
	if client, ok := b.rhizome.(WorkspaceMemoryAwareRhizomeClient); ok {
		b.memoryBackend = NewCanonicalMemoryBackend(client, cfg.WorkspaceID, cfg.ID)
	} else if b.memoryStore != nil {
		b.memoryBackend = b.memoryStore
	}

	// Task runner
	taskRunner := deps.TaskRunner
	if taskRunner == nil {
		taskRunner = &noopTaskRunner{}
	}

	// Task executor
	b.taskExecutor = NewTaskExecutor(cfg, b.rhizome, b.stateStore, taskRunner)

	// Worker spawner
	workerRunner := deps.WorkerRunner
	if workerRunner == nil {
		workerRunner = &noopWorkerRunner{}
	}
	b.workerSpawner = NewWorkerSpawner(b.memoryBackend, workerRunner)

	// Message handler
	triager := deps.Triager
	if triager == nil {
		triager = &noopTriager{}
	}
	b.messageHandler = NewMessageHandler(cfg, b.rhizome, b.taskExecutor, triager)

	// Context injector
	evaluator := deps.Evaluator
	if evaluator == nil {
		evaluator = &noopEvaluator{}
	}
	b.contextInjector = NewContextInjector(cfg, b.rhizome, b.taskExecutor, evaluator)

	// Reflector
	reflectionLLM := deps.ReflectionLLM
	if reflectionLLM == nil {
		reflectionLLM = &noopReflectionLLM{}
	}
	reflectEvery := cfg.ReflectEvery
	if reflectEvery <= 0 {
		reflectEvery = 10
	}
	b.reflector = NewReflector(reflectionLLM, b.memoryBackend, reflectEvery)

	// Situation assessor
	situationLLM := deps.SituationLLM
	if situationLLM == nil {
		situationLLM = &noopSituationLLM{}
	}
	b.situationAssessor = NewSituationAssessor(cfg, b.rhizome, b.taskExecutor, b.taskExecutor.ActiveTasks, situationLLM)

	// Wire heartbeat handlers
	handlers := HeartbeatHandlers{
		OnNewTasks:            b.taskExecutor.HandleNewTasks,
		OnTaskUpdates:         b.contextInjector.HandleTaskUpdates,
		OnMessages:            b.messageHandler.HandleMessages,
		OnCompactionNeeded:    func(ctx context.Context) error { return nil }, // placeholder
		OnSituationAssessment: b.situationAssessor.Assess,
	}
	b.heartbeat = NewHeartbeatLoop(cfg, b.rhizome, b.stateStore, handlers)

	return b, nil
}

// Run starts the brain. It restores state, then runs the heartbeat loop
// until the context is cancelled.
func (b *Brain) Run(ctx context.Context) error {
	// Restore task states
	if err := b.taskExecutor.RestoreFromStore(ctx); err != nil {
		log.Printf("[brain] warning: failed to restore task states: %v", err)
	}

	log.Printf("[brain] %s started (role=%s, workspace=%s)", b.config.ID, b.config.Role, b.config.WorkspaceID)

	// Run heartbeat loop (blocking)
	return b.heartbeat.Run(ctx)
}

// Shutdown gracefully stops the brain, saving state and closing connections.
func (b *Brain) Shutdown(ctx context.Context) error {
	log.Printf("[brain] shutting down...")

	// Save all active task states
	for _, ts := range b.taskExecutor.ActiveTasks() {
		if err := b.stateStore.SaveTaskState(ctx, ts); err != nil {
			log.Printf("[brain] failed to save task state %s: %v", ts.TaskID, err)
		}
	}

	// Close stores
	if err := b.stateStore.Close(); err != nil {
		log.Printf("[brain] state store close error: %v", err)
	}
	if b.memoryDB != nil {
		if err := b.memoryDB.Close(); err != nil {
			log.Printf("[brain] memory DB close error: %v", err)
		}
	}

	log.Printf("[brain] shutdown complete")
	return nil
}

// ToolRegistry returns a new tool registry with memory tools registered.
func (b *Brain) ToolRegistry() *tools.Registry {
	reg := tools.NewRegistry()
	if client, ok := b.rhizome.(WorkspaceMemoryAwareRhizomeClient); ok {
		_ = reg.Register(NewWorkspaceMemorySearchTool(client, b.config.WorkspaceID))
		_ = reg.Register(NewWorkspaceMemoryReadTool(client, b.config.WorkspaceID))
		_ = reg.Register(NewWorkspaceMemoryWriteTool(client, b.config.WorkspaceID, b.config.ID))
		if packetClient, ok := b.rhizome.(WorkspaceMemoryPacketAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceMemoryPacketKernelTool(packetClient, b.config.WorkspaceID))
			_ = reg.Register(NewWorkspaceMemoryPacketShellTool(packetClient, b.config.WorkspaceID, b.config.ID))
		}
		if promotionClient, ok := b.rhizome.(WorkspaceMemoryPromotionAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceMemoryPromotionReadTool(promotionClient, b.config.WorkspaceID))
		}
		if coherenceClient, ok := b.rhizome.(WorkspaceMemoryCoherenceAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceMemoryCoherenceReadTool(coherenceClient, b.config.WorkspaceID))
		}
		if invalidationClient, ok := b.rhizome.(WorkspaceMemoryInvalidationAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceMemoryInvalidationReadTool(invalidationClient, b.config.WorkspaceID, b.config.ID))
			_ = reg.Register(NewWorkspaceMemoryInvalidationItemReadTool(invalidationClient, b.config.WorkspaceID, b.config.ID))
			_ = reg.Register(NewWorkspaceMemoryInvalidationCursorReadTool(invalidationClient, b.config.WorkspaceID, b.config.ID))
		}
		if graphClient, ok := b.rhizome.(WorkspaceMemoryGraphAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceMemoryGraphListReadTool(graphClient, b.config.WorkspaceID, b.config.ID))
			_ = reg.Register(NewWorkspaceMemoryGraphGetReadTool(graphClient, b.config.WorkspaceID))
		}
		if nodeSearchClient, ok := b.rhizome.(WorkspaceMemoryNodeSearchAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceMemoryNodeSearchReadTool(nodeSearchClient, b.config.WorkspaceID, b.config.ID))
		}
		if tensionClient, ok := b.rhizome.(WorkspaceTensionAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceTensionListReadTool(tensionClient, b.config.WorkspaceID, b.config.ID))
			_ = reg.Register(NewWorkspaceTensionGetReadTool(tensionClient, b.config.WorkspaceID))
			_ = reg.Register(NewWorkspaceTensionFrontierReadTool(tensionClient, b.config.WorkspaceID, b.config.ID))
		}
		if tensionAttachableClient, ok := b.rhizome.(WorkspaceTensionAttachableAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceTensionAttachableReadTool(tensionAttachableClient, b.config.WorkspaceID, b.config.ID))
		}
		if rspStateClient, ok := b.rhizome.(WorkspaceRSPStateAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceRSPStateReadTool(rspStateClient, b.config.WorkspaceID, b.config.ID))
		}
		if rspForecastClient, ok := b.rhizome.(WorkspaceRSPForecastAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceRSPForecastReadTool(rspForecastClient, b.config.WorkspaceID, b.config.ID))
		}
		if rspBeliefClient, ok := b.rhizome.(WorkspaceRSPBeliefAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceRSPBeliefReadTool(rspBeliefClient, b.config.WorkspaceID, b.config.ID))
			_ = reg.Register(NewWorkspaceRSPBeliefClaimReadTool(rspBeliefClient, b.config.WorkspaceID))
		}
		if rspTelemetryClient, ok := b.rhizome.(WorkspaceRSPTelemetryAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceRSPTelemetryReadTool(rspTelemetryClient, b.config.WorkspaceID))
		}
		if unifiedControlClient, ok := b.rhizome.(WorkspaceUnifiedControlAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceUnifiedControlReadTool(unifiedControlClient, b.config.WorkspaceID, b.config.ID))
		}
		if controlReportClient, ok := b.rhizome.(WorkspaceControlReportAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceControlReportReadTool(controlReportClient, b.config.WorkspaceID))
		}
		if controlClusterClient, ok := b.rhizome.(WorkspaceControlClusterAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceControlClusterReadTool(controlClusterClient, b.config.WorkspaceID))
		}
		if controlStateClient, ok := b.rhizome.(WorkspaceControlStateAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceControlStateReadTool(controlStateClient, b.config.WorkspaceID))
		}
		if controlStateClusterClient, ok := b.rhizome.(WorkspaceControlStateClusterAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceControlStateClusterReadTool(controlStateClusterClient, b.config.WorkspaceID))
		}
		if rspCapabilityClient, ok := b.rhizome.(WorkspaceRSPCapabilityAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceRSPCapabilityReadTool(rspCapabilityClient, b.config.WorkspaceID))
		}
		if compactionClient, ok := b.rhizome.(CompactionAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceCompactionCandidatesReadTool(compactionClient, b.config.WorkspaceID, b.config.ID))
		}
		if compactionSnapshotClient, ok := b.rhizome.(CompactionSnapshotAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceCompactionSnapshotsReadTool(compactionSnapshotClient, b.config.WorkspaceID, b.config.ID))
		}
		if eventsListClient, ok := b.rhizome.(WorkspaceEventsListAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceEventsListReadTool(eventsListClient, b.config.WorkspaceID, b.config.ID))
		}
		if replayClient, ok := b.rhizome.(WorkspaceEventsReplayAwareRhizomeClient); ok {
			_ = reg.Register(NewWorkspaceEventsReplayReadTool(replayClient, b.config.WorkspaceID, b.config.ID))
			_ = reg.Register(NewWorkspaceEventsEvaluateReadTool(replayClient, b.config.WorkspaceID, b.config.ID))
		}
		return reg
	}
	if b.memoryStore != nil {
		_ = reg.Register(memory.NewMemorySearchTool(b.memoryStore))
		_ = reg.Register(memory.NewMemoryReadTool(b.memoryStore))
		_ = reg.Register(memory.NewMemoryWriteTool(b.memoryStore, b.config.Memory.MDPath))
	}
	return reg
}

// BuildSystemPrompt constructs the brain's system prompt.
func (b *Brain) BuildSystemPrompt() string {
	var parts []string

	parts = append(parts, fmt.Sprintf(`You are a living agent brain with ID "%s" and role "%s".
You are an autonomous agent operating within the Rhizome orchestration platform.
Your workspace is "%s".

## Your Responsibilities
- Monitor and manage assigned tasks
- Delegate work to worker agents when appropriate
- Maintain long-term memory of procedures, experiences, and entities
- Communicate with other agents when needed
- Escalate issues that require human attention

## Delegation Guidelines
- For straightforward execution tasks: delegate to a worker
- For tasks requiring planning or judgment: handle directly
- For tasks requiring information from memory: search memory first, then delegate

## Communication
- Use messages to coordinate with other agents
- Escalate to humans only when truly blocked or when policy requires it`,
		b.config.ID, b.config.Role, b.config.WorkspaceID))
	parts = append(parts, legacyLivingPromptCompilerNotice(
		"legacy_living_brain_prompt.v1",
		"legacy_living_brain_non_converged",
		"This living brain prompt is not the agent daemon active capability snapshot compiler.",
	))

	// Include MEMORY.md if it exists
	if b.config.Memory.MDPath != "" {
		if data, err := os.ReadFile(b.config.Memory.MDPath); err == nil && len(data) > 0 {
			parts = append(parts, "## Memory Index\n"+demoteLegacyDaemonProjectionMarkers(string(data)))
		}
	}

	return strings.Join(parts, "\n\n")
}

// --- No-op implementations for production defaults ---

type noopTaskRunner struct{}

func (n *noopTaskRunner) RunTask(ctx context.Context, task Task) (string, error) {
	return "", fmt.Errorf("no task runner configured")
}

type noopWorkerRunner struct{}

func (n *noopWorkerRunner) RunWorker(ctx context.Context, systemPrompt, task string) (*WorkerResult, error) {
	return nil, fmt.Errorf("no worker runner configured")
}

type noopTriager struct{}

func (n *noopTriager) Triage(ctx context.Context, msg Message, agentRole string) (*TriageResult, error) {
	return &TriageResult{Action: TriageIgnore}, nil
}

type noopEvaluator struct{}

const legacyLivingPromptEvidenceStatus = "not_accepted_for_daemon_prompt_compiler_convergence"

func legacyLivingPromptCompilerNotice(contract, status, note string) string {
	return strings.Join([]string{
		"## Prompt Compiler Status",
		"- prompt_compiler_status: " + strings.TrimSpace(status),
		"- prompt_contract: " + strings.TrimSpace(contract),
		"- c2_1_convergence: excluded_until_migrated",
		"- daemon_capability_snapshot: absent",
		"- deployment_evidence: " + legacyLivingPromptEvidenceStatus,
		"- note: " + strings.TrimSpace(note),
	}, "\n")
}

func demoteLegacyDaemonProjectionMarkers(value string) string {
	replacer := strings.NewReplacer(
		"## Active Capability Snapshot", "## Legacy-Supplied Active Capability Snapshot (ignored)",
		"- projection_source: agent.runtime_capability_snapshot", "- legacy_ignored_projection_source: agent.runtime_capability_snapshot",
		"- projection_contract: active_capability_snapshot_projection.v1", "- legacy_ignored_projection_contract: active_capability_snapshot_projection.v1",
		"- projection_digest:", "- legacy_ignored_projection_digest:",
	)
	return replacer.Replace(value)
}

func (n *noopEvaluator) Evaluate(ctx context.Context, update Update, taskContext string) (*EvalResult, error) {
	return &EvalResult{Action: UpdateContinue}, nil
}

type noopReflectionLLM struct{}

func (n *noopReflectionLLM) Reflect(ctx context.Context, prompt string) (string, error) {
	return "[]", nil
}

type noopSituationLLM struct{}

func (n *noopSituationLLM) Assess(ctx context.Context, report string) (string, error) {
	return "[]", nil
}
