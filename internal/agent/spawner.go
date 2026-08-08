package agent

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// SubAgentSpawner creates new Agent instances for sub-agent orchestration.
type SubAgentSpawner interface {
	Spawn(cfg AgentConfig, llmConfig llm.ClientConfig) (Agent, error)
}

// DefaultSpawner implements SubAgentSpawner by creating InternalAgent instances
// that share the same store as the parent agent.
type DefaultSpawner struct {
	store *sqlite.Store
}

var subAgentCounter uint64

// NewDefaultSpawner creates a new DefaultSpawner with the given store.
func NewDefaultSpawner(store *sqlite.Store) *DefaultSpawner {
	return &DefaultSpawner{store: store}
}

// Spawn creates a new InternalAgent with a unique ID derived from the config ID
// and a timestamp suffix. The spawned agent shares the same store and is fully
// independent — it can run tasks using the existing agent loop.
func (s *DefaultSpawner) Spawn(cfg AgentConfig, llmConfig llm.ClientConfig) (Agent, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}

	// Generate unique sub-agent ID
	cfg.ID = fmt.Sprintf("%s-sub-%d-%d", cfg.ID, time.Now().UnixNano(), atomic.AddUint64(&subAgentCounter, 1))

	// Create LLM client from config
	client := llm.NewClient(llmConfig)

	// Create and return the agent
	return NewInternalAgent(s.store, client, cfg)
}
