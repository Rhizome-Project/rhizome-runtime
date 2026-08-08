package living

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// CheckpointInfo is a lightweight checkpoint stored separately with a TTL.
type CheckpointInfo struct {
	TaskID         string     `json:"task_id"`
	Status         TaskStatus `json:"status"`
	IterationCount int        `json:"iteration_count"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// StateStore defines the persistence interface for TaskState.
type StateStore interface {
	// SaveTaskState serializes state to JSON and stores it.
	SaveTaskState(ctx context.Context, state *TaskState) error
	// LoadTaskState loads a task state by ID. Returns (nil, nil) if not found.
	LoadTaskState(ctx context.Context, taskID string) (*TaskState, error)
	// DeleteTaskState removes a task state.
	DeleteTaskState(ctx context.Context, taskID string) error
	// ListActiveTaskStates returns all non-terminal (not completed/failed/aborted) task states.
	ListActiveTaskStates(ctx context.Context) ([]*TaskState, error)
	// SaveCheckpoint stores arbitrary checkpoint data for a task.
	SaveCheckpoint(ctx context.Context, taskID string, checkpoint []byte) error
	// LoadCheckpoint loads checkpoint data. Returns (nil, nil) if not found.
	LoadCheckpoint(ctx context.Context, taskID string) ([]byte, error)
	// Close releases underlying resources.
	Close() error
}

// taskKey returns the Redis key for a full task state.
func taskKey(prefix, brainID, taskID string) string {
	return fmt.Sprintf("%s%s:task:%s", prefix, brainID, taskID)
}

// checkpointKey returns the Redis key for checkpoint data.
func checkpointKey(prefix, brainID, taskID string) string {
	return fmt.Sprintf("%s%s:checkpoint:%s", prefix, brainID, taskID)
}

// taskKeyPattern returns the SCAN pattern for all tasks of a brain.
func taskKeyPattern(prefix, brainID string) string {
	return fmt.Sprintf("%s%s:task:*", prefix, brainID)
}

const defaultKeyPrefix = "living:"
const checkpointTTL = 1 * time.Hour

// --- RedisStateStore ---

// RedisStateStore persists TaskState in Redis.
type RedisStateStore struct {
	client  *redis.Client
	brainID string
	prefix  string
}

// NewRedisStateStore creates a RedisStateStore, connects, and pings Redis.
func NewRedisStateStore(redisURL, brainID string) (*RedisStateStore, error) {
	var opts *redis.Options
	var err error

	if strings.HasPrefix(redisURL, "redis://") || strings.HasPrefix(redisURL, "rediss://") {
		opts, err = redis.ParseURL(redisURL)
		if err != nil {
			return nil, fmt.Errorf("parsing redis URL: %w", err)
		}
	} else {
		opts = &redis.Options{Addr: redisURL}
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &RedisStateStore{
		client:  client,
		brainID: brainID,
		prefix:  defaultKeyPrefix,
	}, nil
}

func (s *RedisStateStore) SaveTaskState(ctx context.Context, state *TaskState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal task state: %w", err)
	}
	key := taskKey(s.prefix, s.brainID, state.TaskID)
	return s.client.Set(ctx, key, data, 0).Err()
}

func (s *RedisStateStore) LoadTaskState(ctx context.Context, taskID string) (*TaskState, error) {
	key := taskKey(s.prefix, s.brainID, taskID)
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("get task state: %w", err)
	}
	var state TaskState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal task state: %w", err)
	}
	return &state, nil
}

func (s *RedisStateStore) DeleteTaskState(ctx context.Context, taskID string) error {
	key := taskKey(s.prefix, s.brainID, taskID)
	return s.client.Del(ctx, key).Err()
}

func (s *RedisStateStore) ListActiveTaskStates(ctx context.Context) ([]*TaskState, error) {
	pattern := taskKeyPattern(s.prefix, s.brainID)
	var states []*TaskState

	iter := s.client.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		data, err := s.client.Get(ctx, iter.Val()).Bytes()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return nil, fmt.Errorf("get task state during scan: %w", err)
		}
		var state TaskState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, fmt.Errorf("unmarshal task state during scan: %w", err)
		}
		if !isTerminalStatus(state.Status) {
			states = append(states, &state)
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan task states: %w", err)
	}

	if states == nil {
		states = []*TaskState{}
	}
	return states, nil
}

func (s *RedisStateStore) SaveCheckpoint(ctx context.Context, taskID string, checkpoint []byte) error {
	key := checkpointKey(s.prefix, s.brainID, taskID)
	return s.client.Set(ctx, key, checkpoint, checkpointTTL).Err()
}

func (s *RedisStateStore) LoadCheckpoint(ctx context.Context, taskID string) ([]byte, error) {
	key := checkpointKey(s.prefix, s.brainID, taskID)
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("get checkpoint: %w", err)
	}
	return data, nil
}

func (s *RedisStateStore) Close() error {
	return s.client.Close()
}

// --- MemoryStateStore ---

// MemoryStateStore is an in-memory StateStore for testing.
type MemoryStateStore struct {
	mu          sync.RWMutex
	tasks       map[string][]byte // taskID -> JSON
	checkpoints map[string][]byte // taskID -> checkpoint bytes
}

// NewMemoryStateStore creates an empty MemoryStateStore.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		tasks:       make(map[string][]byte),
		checkpoints: make(map[string][]byte),
	}
}

func (m *MemoryStateStore) SaveTaskState(ctx context.Context, state *TaskState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal task state: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[state.TaskID] = data
	return nil
}

func (m *MemoryStateStore) LoadTaskState(ctx context.Context, taskID string) (*TaskState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.tasks[taskID]
	if !ok {
		return nil, nil
	}
	var state TaskState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal task state: %w", err)
	}
	return &state, nil
}

func (m *MemoryStateStore) DeleteTaskState(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, taskID)
	return nil
}

func (m *MemoryStateStore) ListActiveTaskStates(ctx context.Context) ([]*TaskState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var states []*TaskState
	for _, data := range m.tasks {
		var state TaskState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, fmt.Errorf("unmarshal task state: %w", err)
		}
		if !isTerminalStatus(state.Status) {
			states = append(states, &state)
		}
	}
	if states == nil {
		states = []*TaskState{}
	}
	return states, nil
}

func (m *MemoryStateStore) SaveCheckpoint(ctx context.Context, taskID string, checkpoint []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpoints[taskID] = make([]byte, len(checkpoint))
	copy(m.checkpoints[taskID], checkpoint)
	return nil
}

func (m *MemoryStateStore) LoadCheckpoint(ctx context.Context, taskID string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.checkpoints[taskID]
	if !ok {
		return nil, nil
	}
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

func (m *MemoryStateStore) Close() error {
	return nil
}

// isTerminalStatus returns true if the status is a terminal state.
func isTerminalStatus(status TaskStatus) bool {
	return status == TaskStatusCompleted || status == TaskStatusFailed || status == TaskStatusAborted
}
